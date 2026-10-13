"""Cross-engine report uses unprofiled medians and catches missing native evidence."""

import csv
import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/summarize_cross_engine_profile.py"
spec = importlib.util.spec_from_file_location("summarize_cross_engine_profile", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def _fixtures(tmp_path):
    current = tmp_path / "profiles"
    previous = tmp_path / "baseline"
    current.mkdir()
    previous.mkdir()
    (current / "manifest.json").write_text('{"total_bytes": 1024}\n')
    (current / "snapshot-verification.json").write_text('{"unchanged": true}\n')
    for phase in ("before", "after"):
        (current / f"cpu-control-{phase}.json").write_text(
            '{"sha256": "same", "wall_seconds": 1.0}\n'
        )
    (previous / "summary.json").write_text('{"valid": true}\n')
    with (previous / "query_rounds.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("engine", "scenario", "query", "wall_seconds"))
        writer.writeheader()
        for engine in module.ENGINES:
            for scenario in module.SCENARIOS:
                for query in module.TARGETS:
                    for seconds in (1, 2, 3):
                        factor = 6 if engine == "chdb_iceberg" else 1
                        writer.writerow(
                            {
                                "engine": engine,
                                "scenario": scenario,
                                "query": query,
                                "wall_seconds": factor * seconds,
                            }
                        )
    for engine in module.ENGINES:
        for scenario in module.SCENARIOS:
            (current / f"baseline-{engine}-{scenario}.json").write_text(
                json.dumps(
                    {
                        "status": "ok",
                        "cpu_model": "EPYC",
                        "queries": [
                            {
                                "query": number,
                                "rows": 9,
                                "wall_seconds": 12.0 if engine == "chdb_iceberg" else 2.0,
                            }
                            for number in range(1, 23)
                        ],
                    }
                )
            )
            targets = {}
            for number in module.TARGETS:
                target = {
                    "rows": 9,
                    "result_digest_cents": "same-result",
                    "wall_seconds": 2.0,
                    "cpu_seconds": 4.0,
                    "cpu_over_wall": 2,
                    "plan": {
                        "nodes": [{"kinds": ["JOIN"], "instances": 4}],
                        "kind_counts": {"JOIN": 1},
                    },
                }
                if engine == "chdb_iceberg":
                    target["native"] = {
                        "available": True,
                        "profile_events": {"AzureReadRequestsCount": 2},
                        "processors": {"groups": []},
                    }
                if engine == "duckdb_iceberg":
                    target["duckdb"] = {"operators": [{"name": "HASH_JOIN", "timing_seconds": 0.5}]}
                targets[str(number)] = target
            (current / f"{engine}-{scenario}.json").write_text(
                json.dumps(
                    {
                        "status": "ok",
                        "queries_completed": 22,
                        "cpu_model": "EPYC",
                        "targets": targets,
                    }
                )
            )
    for scenario in module.SCENARIOS:
        (current / f"baseline-{module.CLICKHOUSE_LOCAL}-{scenario}.json").write_text(
            json.dumps(
                {
                    "status": "ok",
                    "cpu_model": "EPYC",
                    "queries": [
                        {"query": number, "rows": 9, "wall_seconds_cli_timer": 10.0}
                        for number in range(1, 23)
                    ],
                    "process_wall_seconds": 300.0,
                    "process_cpu_user_seconds": 110.0,
                    "process_cpu_system_seconds": 15.0,
                    "unattributed_wall_seconds": 10.0,
                    "marker_timer_seconds": 0.3,
                }
            )
        )
    return current, previous


def test_report_compares_baseline_medians_not_profile_wall(tmp_path):
    current, previous = _fixtures(tmp_path)
    result = module.collect(current, previous)
    assert result["valid"] is True
    assert result["baseline"][("chdb_iceberg", "remote", 7)] == 12.0
    assert result["same_runner_baseline"][("chdb_iceberg", "remote", 7)] == 12.0
    text = module.report(result)
    assert "Q7 remote" in text
    assert "Q21 remote" in text
    assert "6.0×" in text  # baseline chDB / DuckDB, not the 2-second profiler timer
    assert "ClickHouse Local" in text
    assert "5.0×" in text  # same-runner CLI Q7 / DuckDB
    projection = module._json_projection(result)
    assert "profiles" not in projection
    assert "chdb_iceberg:remote:Q7" in projection["baseline"]
    assert "chdb_iceberg:remote:Q7" in projection["same_runner_baseline"]
    assert projection["same_runner_baseline"]["clickhouse_local_iceberg:remote:Q7"] == 10
    assert "eyJ" not in json.dumps(projection)


def test_missing_chdb_processor_profile_is_not_accepted(tmp_path):
    current, previous = _fixtures(tmp_path)
    path = current / "chdb_iceberg-remote.json"
    payload = json.loads(path.read_text())
    payload["targets"]["7"]["native"]["available"] = False
    path.write_text(json.dumps(payload))
    result = module.collect(current, previous)
    assert result["valid"] is False
    assert "missing chDB native Q7 profile in remote" in result["problems"]


def test_clickhouse_local_row_mismatch_invalidates_comparison(tmp_path):
    current, previous = _fixtures(tmp_path)
    path = current / "baseline-clickhouse_local_iceberg-remote.json"
    payload = json.loads(path.read_text())
    payload["queries"][6]["rows"] = 8
    path.write_text(json.dumps(payload))
    result = module.collect(current, previous)
    assert result["valid"] is False
    assert {"scenario": "remote", "query": 7, "source": "ClickHouse Local"} in result[
        "row_mismatches"
    ]
