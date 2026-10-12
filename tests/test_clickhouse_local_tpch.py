"""The batch protocol must attribute every row and timer to its actual query."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/clickhouse_local_tpch.py"
spec = importlib.util.spec_from_file_location("clickhouse_local_tpch", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_cli_timers_exclude_setup_and_result_markers():
    setup, measured = module._parse(
        "[1]\nMARK01\n[2]\n[3]\nMARK02\n",
        "0.2\n0.3\n1.25\n0.001\n2.50\n0.002\n",
        2,
        "MARK",
        2,
    )
    assert setup == pytest.approx(0.5)
    assert measured == [(1.25, 1), (2.5, 2)]


def test_missing_marker_is_failure_not_a_short_success():
    with pytest.raises(ValueError, match="Q2"):
        module._parse("[1]\nMARK01\n[2]\n", "0.1\n0.3\n0.001\n0.4\n0.001\n", 1, "MARK", 2)
