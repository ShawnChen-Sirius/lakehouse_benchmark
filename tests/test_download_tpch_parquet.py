"""Only original Parquet inside the expected Iceberg table may be staged."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from bench.tpch.config import TpchConfig

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/download_tpch_parquet.py"
spec = importlib.util.spec_from_file_location("download_tpch_parquet", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
CFG = TpchConfig("abcd", "lake", 10)


def test_expected_onelake_parquet_path_is_workspace_relative():
    path = "abfss://abcd@onelake.dfs.fabric.microsoft.com/lake/Tables/CH0010/lineitem/data/part-0.parquet"
    assert module.relative_file(path, CFG, "lineitem") == (
        "lake/Tables/CH0010/lineitem/data/part-0.parquet"
    )


@pytest.mark.parametrize(
    "path",
    [
        "abfss://other@onelake.dfs.fabric.microsoft.com/lake/Tables/CH0010/lineitem/part.parquet",
        "abfss://abcd@onelake.dfs.fabric.microsoft.com/lake/Tables/CH0010/orders/part.parquet",
        "abfss://abcd@onelake.dfs.fabric.microsoft.com/lake/Tables/CH0010/lineitem/../orders/p.parquet",
        "https://onelake.dfs.fabric.microsoft.com/lake/Tables/CH0010/lineitem/part.parquet",
    ],
)
def test_outside_table_and_non_abfss_paths_are_rejected(path):
    with pytest.raises(ValueError):
        module.relative_file(path, CFG, "lineitem")


def test_snapshot_verification_rejects_any_changed_source_table(monkeypatch):
    class Catalog:
        def load_table(self, identifier):
            snapshot_id = 99 if identifier.endswith(".lineitem") else 1
            return SimpleNamespace(
                current_snapshot=lambda: SimpleNamespace(snapshot_id=snapshot_id)
            )

    monkeypatch.setattr(module.auth, "catalog", lambda cfg: Catalog())
    manifest = {
        "schema": CFG.schema,
        "sf": CFG.sf,
        "tables": {table: {"snapshot_id": 1} for table in CFG.TABLES},
    }
    with pytest.raises(RuntimeError, match="lineitem"):
        module.verify(CFG, manifest)
