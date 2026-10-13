"""Three true pairs are required before classifying local/remote stability."""

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/summarize_paired_stability.py"
spec = importlib.util.spec_from_file_location("summarize_paired_stability", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def _write_pair(root, engine, number, local_total, remote_total, *, snapshot=1, after_cpu=1.01):
    folder = root / f"paired-sf10-{engine}-round-{number}"
    folder.mkdir()
    (folder / "manifest.json").write_text(
        json.dumps(
            {
                "schema": "CH0010",
                "sf": 10,
                "tables": {"lineitem": {"snapshot_id": snapshot}},
            }
        )
    )
    (folder / "snapshot-verification.json").write_text('{"unchanged": true}\n')
    (folder / "python-packages.txt").write_text("pyiceberg==0.12.0\n")
    for when, wall in (("before", 1.0), ("after", after_cpu)):
        (folder / f"cpu-control-{when}.json").write_text(
            json.dumps(
                {
                    "sha256": "same-input",
                    "wall_seconds": wall,
                }
            )
        )
    for scenario, total in (("local", local_total), ("remote", remote_total)):
        (folder / f"{engine}-{scenario}.json").write_text(
            json.dumps(
                {
                    "status": "ok",
                    "version": f"fixed-{engine}",
                    "cpu_model": "EPYC",
                    "logical_cpus": 4,
                    "memory_bytes": 16 * 1024**3,
                    "queries": [
                        {"query": q, "rows": q, "wall_seconds": total / 22} for q in range(1, 23)
                    ],
                    "setup": {"wall_seconds": 1},
                    "close": {"wall_seconds": 0.1},
                    "process_wall_seconds": total + 1.1,
                }
            )
        )


def test_three_paired_rounds_detect_remote_only_instability(tmp_path):
    for engine in module.ENGINES:
        for number in module.ROUNDS:
            local = (10, 10.4, 9.8)[number - 1] if engine == "chdb_iceberg" else 10
            remote = (10, 15, 8)[number - 1] if engine == "chdb_iceberg" else 10
            _write_pair(tmp_path, engine, number, local, remote)
    pairs, rows, problems = module.collect(tmp_path)
    assert len(pairs) == 12 and len(rows) == 12 * 2 * 22
    assert problems == [] and all(not pair["problems"] for pair in pairs)
    summary = module.summarize(pairs, rows, problems)
    assert summary["valid"] is True
    assert summary["engines"]["chdb_iceberg"]["local"]["status"] == "stable"
    assert summary["engines"]["chdb_iceberg"]["remote"]["status"] == "unstable"
    assert summary["engines"]["chdb_iceberg"]["verdict"] == (
        "remote access path is less stable than local control"
    )
    assert summary["engines"]["duckdb_iceberg"]["verdict"] == ("paired stability gate passed")
    text = module.report(summary, rows)
    assert "Q1" in text and "Three samples" in text


def test_snapshot_change_invalidates_all_stability_verdicts(tmp_path):
    for engine in module.ENGINES:
        for number in module.ROUNDS:
            snapshot = 2 if engine == "chdb_iceberg" and number == 3 else 1
            _write_pair(tmp_path, engine, number, 10, 10, snapshot=snapshot)
    pairs, rows, problems = module.collect(tmp_path)
    assert "the 12 pairs did not use one identical eight-table snapshot" in problems
    summary = module.summarize(pairs, rows, problems)
    assert summary["valid"] is False
    assert summary["engines"]["chdb_iceberg"]["verdict"] == "invalid/incomplete"


def test_cpu_control_drift_is_not_classified_as_stable_pair(tmp_path):
    _write_pair(tmp_path, "duckdb_iceberg", 1, 10, 10, after_cpu=1.5)
    pairs, _, _ = module.collect(tmp_path)
    assert "CPU-only control drift exceeded 10%" in pairs[0]["problems"]


def test_cross_engine_row_mismatch_invalidates_stability(tmp_path):
    for engine in module.ENGINES:
        for number in module.ROUNDS:
            _write_pair(tmp_path, engine, number, 10, 10)
    path = tmp_path / "paired-sf10-chdb_iceberg-round-1" / "chdb_iceberg-local.json"
    changed = json.loads(path.read_text())
    changed["queries"][6]["rows"] = 999
    path.write_text(json.dumps(changed))
    pairs, rows, problems = module.collect(tmp_path)
    assert "Q7 row counts disagree across engines" in problems
    assert "local/remote row count differs at Q7" in pairs[6]["problems"]
    assert module.summarize(pairs, rows, problems)["valid"] is False


def test_incomplete_three_rounds_have_no_dispersion_verdict():
    result = module.stability([10.0, 11.0])
    assert result == {"status": "incomplete", "values": [10.0, 11.0]}
