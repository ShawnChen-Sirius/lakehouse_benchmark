"""The CPU control hashes fixed bytes and records actual CPU versus wall time."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/cpu_control.py"
spec = importlib.util.spec_from_file_location("cpu_control", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_deterministic_cpu_workload_and_counter_fields():
    first = module.control(repeats=1)
    second = module.control(repeats=1)
    assert first["bytes_hashed"] == second["bytes_hashed"] == 2 * 1024**2
    assert first["sha256"] == second["sha256"]
    assert first["wall_seconds"] > 0
    assert first["cpu_seconds"] >= 0
    assert first["cpu_over_wall"] >= 0
