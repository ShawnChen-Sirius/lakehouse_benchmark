"""Calibration keeps each round, checks result rows and labels incomplete runs."""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/summarize_sf10_calibration.py"
spec = importlib.util.spec_from_file_location("summarize_sf10_calibration", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_complete_three_rounds_have_median_variation_and_author_delta():
    rounds = [
        {
            "engine": "duckdb_iceberg",
            "round": n,
            "status": "ok",
            "version": "2.0",
            "seconds": seconds,
        }
        for n, seconds in enumerate((100, 105, 95), 1)
    ]
    author = {
        name: {"mean_seconds": 100 if name == "duckdb_iceberg" else None, "runs": []}
        for name in module.ENGINES
    }
    rows = [
        {"engine": "duckdb_iceberg", "phase": "query", "query": 1, "status": "ok", "rows": 4},
        {
            "engine": "clickhouse_local_iceberg",
            "phase": "query",
            "query": 1,
            "status": "ok",
            "rows": 4,
        },
    ]
    result = module.summarize(rows, rounds, author)
    duckdb = result["engines"]["duckdb_iceberg"]
    assert duckdb["complete_rounds"] == 3
    assert duckdb["same_version"] is True
    assert duckdb["median_seconds"] == 100
    assert duckdb["cv_percent"] == pytest.approx(4.08, abs=0.01)
    assert duckdb["max_min_percent_of_median"] == 10
    assert duckdb["versus_author_percent"] == 0
    assert result["row_count_mismatches"] == []


def test_missing_and_incomplete_rounds_are_not_mislabeled_as_stable(tmp_path):
    engine = "clickhouse_local_iceberg"
    artifact = tmp_path / f"sf10-{engine}-round-1" / "parts"
    artifact.mkdir(parents=True)
    (artifact / f"{engine}.json").write_text(
        json.dumps(
            {
                "status": "error",
                "version": "26.9",
                "host": {"cpu": 4},
                "rows": [
                    {
                        "phase": "query",
                        "query": 1,
                        "status": "error",
                        "dur": None,
                        "rows": None,
                        "error": "first query failed",
                    }
                ],
            }
        )
    )
    records, rounds = module.collect(tmp_path)
    local = [row for row in rounds if row["engine"] == engine]
    assert [row["status"] for row in local] == ["incomplete", "missing", "missing"]
    assert records[0]["query"] == 1 and records[0]["error"] == "first query failed"
    author = {name: {"mean_seconds": None, "runs": []} for name in module.ENGINES}
    result = module.summarize(records, rounds, author)
    assert result["engines"][engine]["median_seconds"] is None
