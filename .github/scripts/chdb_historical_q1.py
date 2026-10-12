"""Run original TPC-DS Q1 with the historical adapter and settings, under GDB."""

from __future__ import annotations

import importlib.metadata
import json
import os
import time
from pathlib import Path

from bench import scrub
from bench.tpcds.config import TpcdsConfig
from bench.tpch.engines import get_engine
from bench.tpch.queries import load


def main() -> int:
    cfg = TpcdsConfig.from_env()
    target = Path(os.environ["DIAGNOSTIC_DIR"])
    target.mkdir(parents=True, exist_ok=True)
    result = {
        "core": importlib.metadata.version("chdb-core"),
        "chdb": importlib.metadata.version("chdb"),
        "source_sha": os.environ["HISTORICAL_BENCH_SHA"],
        "sf": cfg.sf,
        "query": 1,
        "phase": "setup",
        "status": "running",
    }

    def record() -> None:
        (target / "query-result.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8"
        )

    record()
    engine = get_engine("chdb_iceberg", cfg)
    statements = load(engine.name, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)
    try:
        print(f"Historical TPC-DS SF={cfg.sf}, Q1, core={result['core']}", flush=True)
        engine.setup()
        result["phase"] = "query"
        record()
        print("Executing original Q1", flush=True)
        started = time.perf_counter()
        result["rows"] = engine.execute(statements[0])
        result["query_seconds"] = time.perf_counter() - started
        result["phase"] = "close"
        record()
        engine.close()
        result["status"] = "ok"
        result["phase"] = "completed"
        record()
        print(json.dumps(result), flush=True)
        return 0
    except Exception as exc:
        result["status"] = "python_error"
        result["error"] = scrub.scrub_exc(exc, 1000)
        record()
        print(json.dumps(result), flush=True)
        engine.close()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
