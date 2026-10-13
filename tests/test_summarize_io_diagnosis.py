"""Local/remote report must preserve exact counts and avoid fake network attribution."""

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/summarize_io_diagnosis.py"
spec = importlib.util.spec_from_file_location("summarize_io_diagnosis", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_summary_reports_wall_cpu_and_row_mismatch(tmp_path):
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "total_bytes": 1024**3,
                "download_wall_seconds": 10,
            }
        )
    )
    for scenario, wall, cpu, rows in (("local", 2.0, 1.7, 10), ("remote", 6.0, 2.1, 11)):
        (tmp_path / f"chdb_iceberg-{scenario}.json").write_text(
            json.dumps(
                {
                    "status": "ok",
                    "version": "main-wheel",
                    "cpu_model": "EPYC",
                    "process_wall_seconds": wall + 1,
                    "queries": [
                        {
                            "query": 1,
                            "rows": rows,
                            "wall_seconds": wall,
                            "cpu_seconds": cpu,
                            "disk_read_bytes": 0,
                        }
                    ],
                }
            )
        )
    summary, query_rows = module.summarize(tmp_path)
    chdb = summary["engines"]["chdb_iceberg"]
    assert chdb["status"] == "incomplete"  # only 1 of the required 22 queries
    assert chdb["remote_over_local_wall"] == 3
    assert chdb["local_query_cpu_seconds"] == 1.7
    assert chdb["remote_query_cpu_seconds"] == 2.1
    assert summary["row_count_mismatches"] == [
        {"engine": "chdb_iceberg", "query": 1, "local": 10, "remote": 11}
    ]
    assert query_rows[0]["remote_minus_local_s"] == 4
    text = module.report(summary, query_rows)
    assert "NOT pure network time" in text


def test_no_stage_manifest_produces_partial_report(tmp_path):
    summary, rows = module.summarize(tmp_path)
    assert summary["manifest"] == {}
    assert all(entry["status"] == "missing" for entry in summary["engines"].values())
    assert "Dataset staging did not complete" in module.report(summary, rows)


def test_full_local_remote_pairs_are_complete_and_counts_agree(tmp_path):
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "total_bytes": 1024**3,
                "download_wall_seconds": 10,
            }
        )
    )
    (tmp_path / "snapshot-verification.json").write_text('{"unchanged": true}\n')
    for scenario, seconds in (("local", 1.0), ("remote", 3.0)):
        (tmp_path / f"duckdb_iceberg-{scenario}.json").write_text(
            json.dumps(
                {
                    "status": "ok",
                    "version": "nightly",
                    "cpu_model": "EPYC",
                    "queries": [
                        {
                            "query": q,
                            "rows": q,
                            "wall_seconds": seconds,
                            "cpu_seconds": 0.8,
                            "disk_read_bytes": 0,
                        }
                        for q in range(1, 23)
                    ],
                }
            )
        )
    summary, rows = module.summarize(tmp_path)
    duckdb = summary["engines"]["duckdb_iceberg"]
    assert duckdb["status"] == "ok"
    assert duckdb["local_query_wall_seconds"] == 22.0
    assert duckdb["remote_query_wall_seconds"] == 66.0
    assert duckdb["remote_over_local_wall"] == 3.0
    assert duckdb["local_query_cpu_seconds"] == 17.6
    assert summary["row_count_mismatches"] == []
    assert summary["snapshot_verification"] == {"unchanged": True}
    assert len(rows) == 22 and [r["query"] for r in rows] == list(range(1, 23))
