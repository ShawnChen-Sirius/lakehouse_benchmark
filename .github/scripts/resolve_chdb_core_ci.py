"""Pin the latest successful main/x86_64 CI artifact for the whole benchmark.

Requires CHDB_CORE_ARTIFACT_TOKEN (a token with Actions read access to
chdb-io/chdb-core). Never falls back to a PyPI wheel or an older branch.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = "chdb-io/chdb-core"
WORKFLOW = "build_linux_x86_wheels.yml"
ARTIFACT = "chdb-artifacts-linux-x86_64"
WHEEL = re.compile(r"^chdb_core-[^-]+-cp\d+-abi3-.*x86_64\.whl$")


def _gh(path: str) -> dict:
    return json.loads(subprocess.check_output(["gh", "api", path], text=True))


def choose(runs: list[dict], artifacts_for) -> tuple[dict, dict]:
    """Select a successful main run, and only if its matching artifact is live."""
    eligible = sorted(
        (
            run
            for run in runs
            if run.get("head_branch") == "main"
            and run.get("status") == "completed"
            and run.get("conclusion") == "success"
            and run.get("event") in {"push", "schedule", "workflow_dispatch"}
        ),
        key=lambda run: run["created_at"],
        reverse=True,
    )
    for run in eligible:
        artifacts = [
            artifact
            for artifact in artifacts_for(run["id"])
            if artifact["name"] == ARTIFACT and not artifact["expired"]
        ]
        if artifacts:
            return run, artifacts[0]
    raise RuntimeError("no successful main Linux x86_64 CI run has a live wheel artifact")


def _output(name: str, value: str) -> None:
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"{name}={value}\n")


def main() -> int:
    if not os.environ.get("GH_TOKEN"):
        raise RuntimeError(
            "Set CHDB_CORE_ARTIFACT_TOKEN on this fork with Actions read access "
            "to chdb-io/chdb-core; never substitute the released core wheel."
        )
    runs = _gh(f"repos/{REPO}/actions/workflows/{WORKFLOW}/runs?branch=main&per_page=100")[
        "workflow_runs"
    ]
    run, artifact = choose(
        runs,
        lambda run_id: _gh(f"repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100")[
            "artifacts"
        ],
    )
    destination = Path(os.environ["RUNNER_TEMP"]) / "chdb-core-ci"
    destination.mkdir(parents=True, exist_ok=True)
    # gh downloads and unpacks the original upstream artifact. It is large
    # (~2.3 GB including libraries/debug data); we re-upload ONLY the wheel.
    subprocess.run(
        [
            "gh",
            "run",
            "download",
            str(run["id"]),
            "--repo",
            REPO,
            "--name",
            ARTIFACT,
            "--dir",
            str(destination),
        ],
        check=True,
    )
    wheels = [path for path in destination.rglob("*.whl") if WHEEL.fullmatch(path.name)]
    if len(wheels) != 1:
        raise RuntimeError(f"expected one CPython abi3 x86_64 core wheel, found {len(wheels)}")
    wheel = wheels[0]
    selected = destination / wheel.name
    if selected != wheel:
        shutil.copy2(wheel, selected)
    with selected.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    provenance = {
        "source_repo": REPO,
        "workflow": WORKFLOW,
        "run_id": run["id"],
        "run_url": run["html_url"],
        "head_sha": run["head_sha"],
        "created_at": run["created_at"],
        "artifact_id": artifact["id"],
        "artifact_name": ARTIFACT,
        "wheel_name": selected.name,
        "wheel_sha256": digest,
    }
    (destination / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
    )
    for name, value in (
        ("run-id", str(run["id"])),
        ("head-sha", run["head_sha"]),
        ("wheel-name", selected.name),
        ("wheel-sha256", digest),
    ):
        _output(name, value)
    print(json.dumps(provenance, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"::error::Cannot pin chdb-core CI wheel: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
