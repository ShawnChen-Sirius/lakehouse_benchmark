"""Measure one SF10 engine on the same Parquet bytes, local or OneLake.

No per-query logging: boundary CPU/I/O counters and a 500-ms in-memory RSS
sample are written as one JSON document after the run. Kernel disk-read bytes
do NOT measure network traffic. CPU seconds versus wall seconds indicates
waiting but cannot by itself identify Fabric, scheduling or native locks.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import subprocess
import threading
import time
import uuid
from pathlib import Path

import psutil
from clickhouse_local_tpch import _TIME, _config, _parse, _statements
from smoke_sql import _chdb, _duckdb, _polars

from bench import auth, scrub
from bench.tpch.config import TpchConfig
from bench.tpch.engines import get_engine
from bench.tpch.queries import load

ENGINES = ("duckdb_iceberg", "polars_iceberg", "chdb_iceberg", "clickhouse_local_iceberg")


def _cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor()


class Meter:
    """Boundary process-tree counters, plus a quiet sampled RSS peak."""

    def __init__(self):
        self.parent = psutil.Process(os.getpid())
        self.engine = None
        self._peak = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()

    def processes(self):
        processes = [self.parent]
        conn = getattr(self.engine, "_conn", None)
        child = getattr(conn, "_proc", None)
        if child is not None and child.poll() is None:
            processes.append(psutil.Process(child.pid))
        return processes

    def snapshot(self) -> dict[str, int | float]:
        total = {
            "user": 0.0,
            "system": 0.0,
            "disk_read": 0,
            "disk_write": 0,
            "voluntary": 0,
            "involuntary": 0,
            "rss": 0,
            "disk_io_available": True,
        }
        for process in self.processes():
            try:
                cpu = process.cpu_times()
                io = process.io_counters() if hasattr(process, "io_counters") else None
                ctx = process.num_ctx_switches()
                total["user"] += cpu.user
                total["system"] += cpu.system
                if io is None:
                    total["disk_io_available"] = False
                else:
                    total["disk_read"] += io.read_bytes
                    total["disk_write"] += io.write_bytes
                total["voluntary"] += ctx.voluntary
                total["involuntary"] += ctx.involuntary
                total["rss"] += process.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return total

    def _watch(self) -> None:
        while not self._stop.wait(0.5):
            self._peak = max(self._peak, self.snapshot()["rss"])

    def measure(self, fn) -> tuple[dict, object]:
        before = self.snapshot()
        self._peak = before["rss"]
        started = time.perf_counter()
        value = fn()
        wall = time.perf_counter() - started
        after = self.snapshot()
        user = max(0.0, after["user"] - before["user"])
        system = max(0.0, after["system"] - before["system"])
        return {
            "wall_seconds": round(wall, 5),
            "cpu_user_seconds": round(user, 5),
            "cpu_system_seconds": round(system, 5),
            "cpu_seconds": round(user + system, 5),
            "cpu_over_wall": round((user + system) / wall, 3) if wall else None,
            "rss_peak_bytes_sampled": max(before["rss"], after["rss"], self._peak),
            "disk_read_bytes": max(0, after["disk_read"] - before["disk_read"])
            if before.get("disk_io_available") and after.get("disk_io_available")
            else None,
            "disk_write_bytes": max(0, after["disk_write"] - before["disk_write"])
            if before.get("disk_io_available") and after.get("disk_io_available")
            else None,
            "voluntary_context_switches": max(0, after["voluntary"] - before["voluntary"]),
            "involuntary_context_switches": max(0, after["involuntary"] - before["involuntary"]),
        }, value

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)


def local_engine(name: str, cfg: TpchConfig, data: Path):
    paths = {table: data / table / "*.parquet" for table in cfg.TABLES}
    if any(not list((data / table).glob("*.parquet")) for table in cfg.TABLES):
        raise RuntimeError("staged Parquet data is incomplete")
    if name == "duckdb_iceberg":
        return _duckdb(cfg, paths)
    if name == "polars_iceberg":
        return _polars(cfg, paths)
    if name == "chdb_iceberg":
        engine = _chdb(cfg, paths)
        engine._session.query("SET max_threads = 4")
        engine._session.query("SET max_memory_usage = 12000000000")
        return engine
    raise ValueError(name)


def library_run(name: str, scenario: str, cfg: TpchConfig, data: Path) -> dict:
    meter = Meter()
    engine = None
    result = {"queries": [], "status": "running"}
    started = time.perf_counter()
    # Existing benchmark adapters log per-query spill/refresh. This diagnostic
    # stores counters in memory and writes only once after the hot path.
    original_safe_print = scrub.safe_print

    def quiet(*_args: object) -> None:
        pass

    scrub.safe_print = quiet
    try:
        if scenario == "local":
            result["setup"], engine = meter.measure(lambda: local_engine(name, cfg, data))
        else:
            engine = get_engine(name, cfg)
            meter.engine = engine
            result["setup"], _ = meter.measure(engine.setup)
        meter.engine = engine
        result["version"] = engine.version
        sql = load(name, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
        for number, statement in enumerate(sql, 1):
            if scenario == "remote":
                refresh, _ = meter.measure(engine.refresh)
            else:
                refresh = None
            metrics, row_count = meter.measure(
                lambda statement=statement: engine.execute(statement)
            )
            result["queries"].append(
                {"query": number, "rows": row_count, "refresh": refresh, **metrics}
            )
        result["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - preserve partial diagnostic on failure
        result["status"] = "error"
        result["error"] = scrub.scrub_exc(exc)
    finally:
        if engine is not None:
            try:
                result["close"], _ = meter.measure(engine.close)
            except Exception as exc:  # noqa: BLE001
                result["close_error"] = scrub.scrub_exc(exc)
                result["status"] = "error"
        meter.close()
        scrub.safe_print = original_safe_print
        result["process_wall_seconds"] = round(time.perf_counter() - started, 5)
    return result


def clickhouse_run(scenario: str, cfg: TpchConfig, data: Path) -> dict:
    scratch = Path(os.environ["RUNNER_TEMP"]) / f"io-diag-clickhouse-{scenario}"
    scratch.mkdir(parents=True, exist_ok=True)
    config = _config(cfg, scratch)
    version = subprocess.check_output(["clickhouse", "local", "--version"], text=True).strip()
    if "26.9.17.8" not in version:
        raise RuntimeError(f"unexpected ClickHouse Local version: {version}")
    marker = f"__IO_DIAG_{uuid.uuid4().hex}_"
    if scenario == "remote":
        token = auth.onelake_token()
        setup, statements = _statements(cfg, token, marker)
    else:
        # The file() table function resolves relative paths below user_files_path.
        config.write_text(
            config.read_text(encoding="utf-8").replace(
                "</clickhouse>", f"<user_files_path>{data}/</user_files_path></clickhouse>"
            ),
            encoding="utf-8",
        )
        setup = [
            "SET join_use_nulls = 1",
            "SET union_default_mode = 'DISTINCT'",
            "SET max_threads = 4",
            "SET max_memory_usage = 12000000000",
            "CREATE DATABASE onelake",
            "USE onelake",
        ]
        for table in cfg.TABLES:
            setup.append(
                f"CREATE VIEW `{cfg.schema}.{table}` AS "
                f"SELECT * FROM file('{data}/{table}/*.parquet', Parquet)"
            )
        statements = setup[:]
        sql = load("chdb_iceberg", cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
        for number, query in enumerate(sql, 1):
            statements.append(f"{query} FORMAT JSONCompactEachRow")
            statements.append(f"SELECT '{marker}{number:02d}' FORMAT TabSeparatedRaw")

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
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        input=";\n".join(statements) + ";\n",
        text=True,
        capture_output=True,
        timeout=1800,
        check=False,
    )
    wall = time.perf_counter() - started
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    result = {
        "version": version,
        "status": "ok",
        "queries": [],
        "process_wall_seconds": round(wall, 5),
        "process_cpu_user_seconds": round(after.ru_utime - before.ru_utime, 5),
        "process_cpu_system_seconds": round(after.ru_stime - before.ru_stime, 5),
        "process_peak_rss_bytes": after.ru_maxrss * 1024,
    }
    if completed.returncode:
        result["status"] = "error"
        result["error"] = scrub.scrub(
            f"ClickHouse Local exit {completed.returncode}: {completed.stderr[-1600:]}"
        )
        return result
    setup_seconds, measures = _parse(completed.stdout, completed.stderr, len(setup), marker, 22)
    timer_lines = [
        float(line) for line in completed.stderr.splitlines() if _TIME.fullmatch(line.strip())
    ]
    marker_seconds = sum(timer_lines[len(setup) + 1 :: 2])
    result["setup"] = {"wall_seconds_cli_timer": round(setup_seconds, 5)}
    result["queries"] = [
        {
            "query": number,
            "rows": rows,
            "wall_seconds_cli_timer": round(seconds, 5),
            "cpu_seconds": None,
            "disk_read_bytes": None,
        }
        for number, (seconds, rows) in enumerate(measures, 1)
    ]
    result["marker_timer_seconds"] = round(marker_seconds, 5)
    result["unattributed_wall_seconds"] = round(
        wall - setup_seconds - sum(v[0] for v in measures) - marker_seconds, 5
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=ENGINES, required=True)
    parser.add_argument("--scenario", choices=("local", "remote"), required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    cfg = TpchConfig.from_env()
    start = time.perf_counter()
    try:
        if args.engine == "clickhouse_local_iceberg":
            result = clickhouse_run(args.scenario, cfg, args.data)
        else:
            result = library_run(args.engine, args.scenario, cfg, args.data)
    except Exception as exc:  # noqa: BLE001 - do not print token-bearing native exceptions
        result = {"status": "error", "queries": [], "error": scrub.scrub_exc(exc)}
    result.update(
        {
            "engine": args.engine,
            "scenario": args.scenario,
            "sf": cfg.sf,
            "cpu_model": _cpu_model(),
            "logical_cpus": psutil.cpu_count(),
            "memory_bytes": psutil.virtual_memory().total,
            "overall_wall_seconds": round(time.perf_counter() - start, 5),
        }
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    scrub.safe_print(
        f"{args.engine} {args.scenario}: {result['status']}, "
        f"{len(result['queries'])}/22 queries, process {result.get('process_wall_seconds')}s"
    )
    return 0 if result["status"] == "ok" and len(result["queries"]) == 22 else 1


if __name__ == "__main__":
    raise SystemExit(main())
