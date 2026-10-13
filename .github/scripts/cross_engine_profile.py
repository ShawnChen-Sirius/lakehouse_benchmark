"""Targeted horizontal Q1-3/Q7/Q21 profile on one runner and identical files.

This is deliberately NOT the timing baseline. Each scenario runs the original
22-query sequence to preserve cache/order, but detailed native profiling is
enabled only for selected queries. All exported plans/metrics are bounded,
projected and sanitized; raw plans, query_log rows and DuckDB JSON stay under
RUNNER_TEMP and are never uploaded as Actions artifacts.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path

from io_diagnosis import Meter, _cpu_model, local_engine
from profile_metrics import duckdb_profile, plan_features, processor_profile, result_digest

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, ICEBERG_ENDPOINT
from bench.tpch.config import TpchConfig
from bench.tpch.engines import get_engine
from bench.tpch.engines.chdb_iceberg import SEMANTIC_SETTINGS, ChdbIceberg, _last_document
from bench.tpch.queries import load

ENGINES = ("duckdb_iceberg", "polars_iceberg", "chdb_iceberg")
TARGETS = frozenset((1, 2, 3, 7, 21))
PROCESSOR_TARGETS = frozenset((7, 21))
_QUERY_ID = re.compile(r"[a-f0-9-]{36}\Z")
AZURE_EVENTS = (
    "AzureReadMicroseconds",
    "AzureReadRequestsCount",
    "AzureReadRequestsErrors",
    "AzureReadRequestsThrottling",
    "DiskAzureReadMicroseconds",
    "ReadBufferFromAzureMicroseconds",
    "ReadBufferFromAzureBytes",
    "CachedReadBufferReadFromCacheHits",
    "CachedReadBufferReadFromCacheMisses",
    "CachedReadBufferReadFromSourceMicroseconds",
    "CachedReadBufferReadFromSourceBytes",
    "CachedReadBufferReadFromCacheMicroseconds",
    "CachedReadBufferReadFromCacheBytes",
    "CachedReadBufferCacheWriteBytes",
    "OSCPUVirtualTimeMicroseconds",
    "OSIOWaitMicroseconds",
)


def _rows(session, statement: str) -> list:
    return _last_document(str(session.query(statement, "JSONCompact"))).get("data", [])


def event_snapshot(session) -> dict[str, int]:
    selected = ",".join(f"'{name}'" for name in AZURE_EVENTS)
    return {
        name: int(value)
        for name, value in _rows(
            session, f"SELECT event,value FROM system.events WHERE event IN ({selected})"
        )
    }


def event_delta(before: dict, after: dict) -> dict[str, int]:
    return {
        name: after.get(name, 0) - before.get(name, 0)
        for name in AZURE_EVENTS
        if after.get(name, 0) != before.get(name, 0)
    }


def _profile_config(engine: ChdbIceberg, scratch: Path) -> Path:
    config = engine._write_config(scratch)
    system_logs = """
<query_log><database>system</database><table>query_log</table>
  <flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
<processors_profile_log><database>system</database><table>processors_profile_log</table>
  <flush_interval_milliseconds>1000</flush_interval_milliseconds></processors_profile_log>
"""
    config.write_text(
        config.read_text(encoding="utf-8").replace("</clickhouse>", system_logs + "</clickhouse>"),
        encoding="utf-8",
    )
    return config


def profile_chdb_engine(cfg: TpchConfig, scenario: str, data: Path) -> ChdbIceberg:
    """Same benchmark settings, but disable log recording before bearer DDL."""
    from chdb import session

    engine = ChdbIceberg(cfg)
    scratch = Path(os.environ["RUNNER_TEMP"]) / f"profile-chdb-{scenario}"
    config = _profile_config(engine, scratch)
    conn = session.Session(f"{scratch}/session?config-file={config}")
    conn.query("SET log_queries=0, log_processors_profiles=0")
    for statement in (
        "SET allow_database_iceberg = 1",
        "SET filesystem_cache_name = 'onelake_cache'",
        "SET max_download_threads = 0",
        f"SET iceberg_metadata_staleness_ms = {CATALOG_CACHE_SECONDS * 1000}",
        *SEMANTIC_SETTINGS,
        "SET max_threads = 4",
        "SET max_memory_usage = 12000000000",
    ):
        conn.query(statement)
    if scenario == "remote":
        token = auth.onelake_token()
        engine._expires = auth.token_expires_on()
        conn.query(
            f"CREATE DATABASE onelake ENGINE = DataLakeCatalog('{ICEBERG_ENDPOINT}') "
            "SETTINGS catalog_type = 'onelake', "
            f"warehouse = '{cfg.warehouse}', onelake_bearer_token = '{token}'"
        )
    else:
        conn.query("CREATE DATABASE onelake")
    conn.query("USE onelake")
    if scenario == "local":
        for table in cfg.TABLES:
            if not list((data / table).glob("*.parquet")):
                raise RuntimeError(f"staged {table} data is incomplete")
            conn.query(
                f"CREATE VIEW `{cfg.schema}.{table}` AS "
                f"SELECT * FROM file('{data}/{table}/*.parquet', Parquet)"
            )
    engine._session = conn
    engine._attach = 1
    return engine


def _native_chdb_profile(session, comment: str, with_processors: bool) -> dict:
    session.query("SYSTEM FLUSH LOGS")  # outside any timed query
    log = _rows(
        session,
        (
            "SELECT query_id,query_duration_ms,read_rows,read_bytes,memory_usage,ProfileEvents "
            "FROM system.query_log WHERE type='QueryFinish' "
            f"AND log_comment='{comment}' AND read_rows>0 "
            "ORDER BY read_rows DESC, query_duration_ms DESC LIMIT 1"
        ),
    )
    if not log:
        return {"available": False, "reason": "no matching QueryFinish row"}
    query_id, duration, read_rows, read_bytes, peak_memory, events = log[0]
    if not _QUERY_ID.fullmatch(str(query_id)):
        raise ValueError("unexpected native query ID")
    metrics = {
        "available": True,
        "query_duration_ms": duration,
        "read_rows": read_rows,
        "logical_read_bytes": read_bytes,
        "peak_memory_bytes": peak_memory,
        "profile_events": {name: int(events[name]) for name in AZURE_EVENTS if name in events},
    }
    if with_processors:
        keys = (
            "plan_step_name",
            "name",
            "elapsed_us",
            "input_wait_elapsed_us",
            "output_wait_elapsed_us",
            "input_rows",
            "input_bytes",
            "output_rows",
            "output_bytes",
        )
        raw = _rows(
            session,
            (
                "SELECT " + ",".join(keys) + " FROM system.processors_profile_log "
                f"WHERE query_id='{query_id}' LIMIT 10000"
            ),
        )
        metrics["processors"] = processor_profile(
            [dict(zip(keys, row, strict=True)) for row in raw]
        )
    return metrics


def _full_output(engine, name: str, sql: str) -> list:
    if name == "duckdb_iceberg":
        return engine._conn.sql(sql).fetchall()
    if name == "polars_iceberg":
        return engine._ctx.execute(sql).collect(engine="streaming").rows()
    return _last_document(str(engine._session.query(sql, "JSONCompact"))).get("data", [])


def _plan(engine, name: str, sql: str) -> dict:
    if name == "duckdb_iceberg":
        raw = engine._conn.sql(f"EXPLAIN {sql}").fetchall()
        return plan_features("\n".join(str(cell) for row in raw for cell in row))
    if name == "polars_iceberg":
        raw = engine._ctx.execute(sql).explain(engine="streaming")
        return plan_features(raw)
    raw = str(engine._session.query(f"EXPLAIN PIPELINE {sql}", "TSVRaw"))
    return plan_features(raw)


def profile_one(name: str, scenario: str, cfg: TpchConfig, data: Path) -> dict:
    meter = Meter()
    engine = None
    started = time.perf_counter()
    result = {
        "engine": name,
        "scenario": scenario,
        "targets": {},
        "status": "running",
        "queries_completed": 0,
    }
    original_print = scrub.safe_print

    def quiet(*_args: object) -> None:
        pass

    scrub.safe_print = quiet
    try:
        if name == "chdb_iceberg":
            result["setup"], engine = meter.measure(
                lambda: profile_chdb_engine(cfg, scenario, data)
            )
        elif scenario == "local":
            result["setup"], engine = meter.measure(lambda: local_engine(name, cfg, data))
        else:
            engine = get_engine(name, cfg)
            meter.engine = engine
            result["setup"], _ = meter.measure(engine.setup)
        meter.engine = engine
        result["version"] = engine.version
        statements = load(name, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
        for number, sql in enumerate(statements, 1):
            if scenario == "remote" and name != "chdb_iceberg":
                engine.refresh()  # credential maintenance outside the query timer
            if number not in TARGETS:
                meter.measure(lambda sql=sql: engine.execute(sql))
                result["queries_completed"] = number
                continue
            target: dict = {"query": number}
            events_before = None
            duck_file = None
            if name == "chdb_iceberg":
                conn = engine._session
                events_before = event_snapshot(conn)
                comment = f"profile_{scenario}_q{number}"
                conn.query(f"SET log_comment='{comment}'")
                conn.query("SET log_queries=1")
                if number in PROCESSOR_TARGETS:
                    conn.query("SET log_processors_profiles=1")
            elif name == "duckdb_iceberg":
                duck_file = (
                    Path(os.environ["RUNNER_TEMP"])
                    / "duckdb-profiles"
                    / (f"{scenario}-q{number}.json")
                )
                duck_file.parent.mkdir(parents=True, exist_ok=True)
                engine._conn.sql(f"PRAGMA profiling_output='{duck_file}'")
                engine._conn.sql("PRAGMA enable_profiling='json'")
            metrics, output = meter.measure(lambda sql=sql: _full_output(engine, name, sql))
            target.update(metrics)
            target["rows"] = len(output)
            target["result_digest_cents"] = result_digest(output)
            if name == "chdb_iceberg":
                conn.query("SET log_queries=0, log_processors_profiles=0")
                target["azure_cache_event_deltas"] = event_delta(
                    events_before, event_snapshot(conn)
                )
                target["native"] = _native_chdb_profile(conn, comment, number in PROCESSOR_TARGETS)
            elif name == "duckdb_iceberg":
                engine._conn.sql("PRAGMA disable_profiling")
                target["duckdb"] = (
                    duckdb_profile(json.loads(duck_file.read_text(encoding="utf-8")))
                    if duck_file.is_file()
                    else {"available": False}
                )
            target["plan"] = _plan(engine, name, sql)
            result["targets"][str(number)] = target
            result["queries_completed"] = number
        result["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - redact native errors, preserve partial evidence
        result["status"] = "error"
        result["error"] = scrub.scrub_exc(exc, 1200)
    finally:
        if engine is not None:
            try:
                result["close"], _ = meter.measure(engine.close)
            except Exception as exc:  # noqa: BLE001
                result["close_error"] = scrub.scrub_exc(exc, 1200)
                result["status"] = "error"
        meter.close()
        scrub.safe_print = original_print
    result["process_wall_seconds"] = round(time.perf_counter() - started, 4)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=ENGINES, required=True)
    parser.add_argument("--scenario", choices=("local", "remote"), required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    cfg = TpchConfig.from_env()
    result = profile_one(args.engine, args.scenario, cfg, args.data)
    result["profiling_targets"] = sorted(TARGETS)
    result["cpu_model"] = _cpu_model()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    scrub.safe_print(
        f"{args.engine} {args.scenario}: {result['status']}, "
        f"{result['queries_completed']}/22 queries, "
        f"{len(result['targets'])}/{len(TARGETS)} sanitized target profiles"
    )
    return 0 if result["status"] == "ok" and len(result["targets"]) == len(TARGETS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
