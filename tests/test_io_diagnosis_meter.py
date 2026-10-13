"""Boundary counters are deltas; kernel disk I/O is not mislabeled as network."""

import importlib.util
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / ".github/scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("io_diagnosis", SCRIPTS / "io_diagnosis.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_process_tree_delta_and_cpu_wall_ratio():
    meter = module.Meter.__new__(module.Meter)
    snapshots = iter(
        [
            {
                "user": 1.0,
                "system": 0.5,
                "disk_read": 100,
                "disk_write": 0,
                "voluntary": 10,
                "involuntary": 2,
                "rss": 1_000_000,
            },
            {
                "user": 2.5,
                "system": 1.0,
                "disk_read": 600,
                "disk_write": 30,
                "voluntary": 13,
                "involuntary": 4,
                "rss": 2_000_000,
            },
        ]
    )
    # The synthetic snapshots model a Linux runner with process disk I/O counters.
    snapshots = iter({**item, "disk_io_available": True} for item in snapshots)
    meter.snapshot = lambda: next(snapshots)
    meter._peak = 1_500_000
    result, value = meter.measure(lambda: 42)
    assert value == 42
    assert result["cpu_user_seconds"] == 1.5
    assert result["cpu_system_seconds"] == 0.5
    assert result["cpu_seconds"] == 2.0
    assert result["disk_read_bytes"] == 500
    assert result["disk_write_bytes"] == 30
    assert result["voluntary_context_switches"] == 3
    assert result["involuntary_context_switches"] == 2
    assert result["rss_peak_bytes_sampled"] == 2_000_000


def test_timing_only_meter_does_not_collect_process_metrics():
    meter = module.TimingOnlyMeter()
    result, value = meter.measure(lambda: 42)
    assert value == 42
    assert result["wall_seconds"] >= 0
    assert "cpu_seconds" not in result
    assert "rss_peak_bytes_sampled" not in result
    meter.close()
