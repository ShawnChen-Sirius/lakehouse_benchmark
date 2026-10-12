"""Run the original 22 TPC-H statements in one ClickHouse Local process.

The CLI's --time reports each statement's execution time, while sentinels split
JSONCompactEachRow output into exact per-query row counts. The bearer is passed
only on stdin, never on the command line or in a file; stdout/stderr are
captured and any diagnostic is scrubbed before it reaches Actions logs.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, ICEBERG_ENDPOINT
from bench.store import EngineResult, Row, host_facts, write_engine_part
from bench.tpch.config import TpchConfig, chdb_cache_gib
from bench.tpch.queries import load

ENGINE = "clickhouse_local_iceberg"
_TIME = re.compile(r"\d+\.\d+")


def _config(cfg: TpchConfig, scratch: Path) -> Path:
    cache = scratch / "cache"
    spill = scratch / "spill"
    cache.mkdir(parents=True, exist_ok=True)
    spill.mkdir(parents=True, exist_ok=True)
    config = scratch / "config.xml"
    config.write_text(
        f"""<clickhouse>
  <tmp_path>{spill}/</tmp_path>
  <max_server_memory_usage>13000000000</max_server_memory_usage>
  <filesystem_caches><onelake_cache>
    <path>{cache}/onelake</path>
    <max_size>{chdb_cache_gib(cfg.estimated_gib)}Gi</max_size>
  </onelake_cache></filesystem_caches>
</clickhouse>""",
        encoding="utf-8",
    )
    return config


def _statements(cfg: TpchConfig, token: str, marker: str) -> tuple[list[str], list[str]]:
    setup = [
        "SET allow_database_iceberg = 1",
        "SET filesystem_cache_name = 'onelake_cache'",
        "SET max_download_threads = 0",
        f"SET iceberg_metadata_staleness_ms = {CATALOG_CACHE_SECONDS * 1000}",
        "SET join_use_nulls = 1",
        "SET union_default_mode = 'DISTINCT'",
        "SET max_threads = 4",
        "SET max_memory_usage = 12000000000",
        f"CREATE DATABASE onelake ENGINE = DataLakeCatalog('{ICEBERG_ENDPOINT}') "
        "SETTINGS catalog_type = 'onelake', "
        f"warehouse = '{cfg.warehouse}', onelake_bearer_token = '{token}'",
        "USE onelake",
    ]
    queries = load("chdb_iceberg", cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
    # Explicit output format so a row is a single escaped JSON line; each marker
    # is a separate statement and its own (excluded) --time line.
    statements = setup[:]
    for number, query in enumerate(queries, 1):
        statements.append(f"{query} FORMAT JSONCompactEachRow")
        statements.append(f"SELECT '{marker}{number:02d}' FORMAT TabSeparatedRaw")
    return setup, statements


def _parse(stdout: str, stderr: str, setup_count: int, marker: str, count: int):
    durations = [float(line) for line in stderr.splitlines() if _TIME.fullmatch(line.strip())]
    expected = setup_count + 2 * count
    if len(durations) != expected:
        raise ValueError(f"expected {expected} CLI timer lines, got {len(durations)}")
    lines = stdout.splitlines()
    cursor = 0
    measured: list[tuple[float, int]] = []
    for number in range(1, count + 1):
        end = f"{marker}{number:02d}"
        try:
            stop = lines.index(end, cursor)
        except ValueError:
            raise ValueError(f"missing result marker for Q{number}") from None
        # Each JSONCompactEachRow line is one fully materialized result row.
        if any(not lines[i].startswith("[") for i in range(cursor, stop)):
            raise ValueError(f"unexpected result format at Q{number}")
        measured.append((durations[setup_count + 2 * (number - 1)], stop - cursor))
        cursor = stop + 1
    if cursor != len(lines):
        raise ValueError("unexpected output after the last result marker")
    return sum(durations[:setup_count]), measured


def main() -> int:
    cfg = TpchConfig.from_env()
    scratch = Path(os.environ["RUNNER_TEMP"]) / "clickhouse-local-bench"
    scratch.mkdir(parents=True, exist_ok=True)
    config = _config(cfg, scratch)
    version = subprocess.check_output(["clickhouse", "local", "--version"], text=True).strip()
    if "26.9.17.8" not in version:
        raise RuntimeError(f"unexpected ClickHouse Local version: {version}")
    result = EngineResult(version=version)
    result.host = host_facts()
    marker = f"__CH_LOCAL_{uuid.uuid4().hex}_"
    # setup includes authentication and the catalog attach, outside query timings.
    token = auth.onelake_token()
    setup, statements = _statements(cfg, token, marker)
    command = [
        "clickhouse",
        "local",
        "--config-file",
        str(config),
        "--path",
        str(scratch / "session"),
        "--multiquery",
        "--time",
        "--progress=off",
        "--logger.console=0",
    ]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            input=";\n".join(statements) + ";\n",
            text=True,
            capture_output=True,
            timeout=1800,
            check=False,
        )
        if completed.returncode:
            finished = sum(
                line == f"{marker}{number:02d}"
                for number in range(1, cfg.N_QUERIES + 1)
                for line in completed.stdout.splitlines()
            )
            if finished:
                result.status = "error"
                result.rows.append(
                    Row(
                        "cold",
                        "query",
                        finished + 1,
                        None,
                        status="error",
                        error=f"ClickHouse Local exited {completed.returncode} after Q{finished}",
                    )
                )
            raise RuntimeError(
                f"ClickHouse Local exit {completed.returncode} after {finished} queries: "
                f"{scrub.scrub(completed.stderr)[-1600:]}"
            )
        setup_time, measured = _parse(
            completed.stdout,
            completed.stderr,
            len(setup),
            marker,
            cfg.N_QUERIES,
        )
        result.rows.append(Row("cold", "setup", 0, round(setup_time, 4)))
        for number, (duration, rows) in enumerate(measured, 1):
            result.rows.append(Row("cold", "query", number, round(duration, 4), rows=rows))
            scrub.safe_print(f"  Q{number:<2} cold {duration:8.3f}s  {rows} rows")
        scrub.safe_print(f"  setup {setup_time:.3f}s, process {time.perf_counter() - started:.3f}s")
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        if result.status == "ok":
            result.status = "setup_failed"
            result.rows.append(
                Row("cold", "setup", 0, None, status="error", error=scrub.scrub_exc(exc))
            )
        scrub.safe_print(f"::error::ClickHouse Local: {scrub.scrub_exc(exc, 350)}")
    out = write_engine_part(Path(sys.argv[1] if len(sys.argv) > 1 else "parts"), ENGINE, result)
    scrub.safe_print(f"ClickHouse Local {result.status} -> {out}")
    return 0 if result.status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
