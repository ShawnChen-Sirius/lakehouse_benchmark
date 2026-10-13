"""Profiling artifacts keep numeric/operator evidence, not source paths or tokens."""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/profile_metrics.py"
spec = importlib.util.spec_from_file_location("profile_metrics", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_plan_features_exclude_url_and_sql_literal():
    plan = "SCAN CH0010.lineitem abfss://secret/path?token=eyJabc\nHASH_JOIN × 4 ON x='private'"
    projected = module.plan_features(plan)
    assert projected["nodes"][0]["tables"] == ["lineitem"]
    assert projected["nodes"][0]["line_index"] == 0
    assert projected["nodes"][1]["instances"] == 4
    assert "abfss" not in json.dumps(projected)
    assert "private" not in json.dumps(projected)
    assert "eyJabc" not in json.dumps(projected)


def test_clickhouse_pipeline_read_source_and_aggregate_are_retained():
    plan = "(Aggregating)\nAggregatingTransform\n(ReadFromIceberg) × 4\nResize 4 → 1"
    projected = module.plan_features(plan)
    assert projected["kind_counts"]["READ_SOURCE"] == 1
    assert projected["kind_counts"]["AGGREGATE"] >= 1
    assert any(node["instances"] == 4 for node in projected["nodes"])


def test_duckdb_profile_drops_query_and_extra_info():
    raw = {
        "query_name": "SELECT token FROM secret",
        "latency": 3.2,
        "children": [
            {
                "operator_name": "PARQUET_SCAN",
                "operator_timing": 1.2,
                "operator_rows_scanned": 42,
                "extra_info": {"File": "abfss://secret/part.parquet"},
            }
        ],
    }
    projected = module.duckdb_profile(raw)
    assert projected["root"]["latency"] == 3.2
    assert projected["operators"][0]["rows_scanned"] == 42
    assert projected["operators"][0]["depth"] == 1
    assert "secret" not in json.dumps(projected)


def test_processor_profile_aggregates_only_safe_names_and_numbers():
    rows = [
        {
            "plan_step_name": "ReadFromStorage",
            "name": "AzureBlobSource",
            "elapsed_us": 200,
            "input_wait_elapsed_us": 100,
            "input_rows": 10,
            "plan_step_description": "bearer eyJsecret",
        },
        {
            "plan_step_name": "ReadFromStorage",
            "name": "AzureBlobSource",
            "elapsed_us": 300,
            "input_wait_elapsed_us": 200,
            "input_rows": 20,
        },
    ]
    projected = module.processor_profile(rows)
    group = projected["groups"][0]
    assert group["processors"] == 2
    assert group["elapsed_us_sum"] == 500
    assert group["input_wait_us_max"] == 200
    assert group["input_rows_sum"] == 30
    assert projected["truncated"] is False
    assert "eyJsecret" not in json.dumps(projected)


def test_selected_result_digest_tolerates_numeric_representation():
    a = [["FRANCE", 1995, "12.3400"]]
    b = [["FRANCE", Decimal("1995.0"), Decimal("12.34")]]
    assert module.result_digest(a) == module.result_digest(b)
    assert module.result_digest(a) != module.result_digest([["GERMANY", 1995, "12.34"]])
