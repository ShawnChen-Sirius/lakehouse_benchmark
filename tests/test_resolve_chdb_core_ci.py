"""A failed or irrelevant upstream build cannot silently become the benchmark core."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/resolve_chdb_core_ci.py"
spec = importlib.util.spec_from_file_location("resolve_chdb_core_ci", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_latest_successful_main_run_with_live_x86_artifact():
    runs = [
        {
            "id": 940,
            "head_branch": "main",
            "status": "in_progress",
            "conclusion": None,
            "event": "schedule",
            "created_at": "2026-10-13T01:00:00Z",
        },
        {
            "id": 939,
            "head_branch": "main",
            "status": "completed",
            "conclusion": "success",
            "event": "schedule",
            "created_at": "2026-10-12T16:02:35Z",
        },
        {
            "id": 938,
            "head_branch": "other",
            "status": "completed",
            "conclusion": "success",
            "event": "push",
            "created_at": "2026-10-12T15:00:00Z",
        },
    ]
    run, artifact = module.choose(
        runs,
        lambda _: [{"id": 1177, "name": module.ARTIFACT, "expired": False}],
    )
    assert run["id"] == 939 and artifact["id"] == 1177


def test_expired_artifact_is_not_accepted():
    run = {
        "id": 934,
        "head_branch": "main",
        "status": "completed",
        "conclusion": "success",
        "event": "push",
        "created_at": "2026-10-11T22:10:48Z",
    }
    with pytest.raises(RuntimeError, match="live wheel artifact"):
        module.choose([run], lambda _: [{"name": module.ARTIFACT, "expired": True}])
