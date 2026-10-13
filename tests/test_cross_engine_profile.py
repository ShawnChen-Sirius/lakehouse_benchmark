"""Native metrics are matched by query ID and projected without sensitive SQL."""

import importlib.util
import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / ".github/scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location(
    "cross_engine_profile", SCRIPTS / "cross_engine_profile.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_event_deltas_are_selected_and_missing_is_zero():
    result = module.event_delta(
        {"AzureReadRequestsCount": 2, "CachedReadBufferReadFromCacheHits": 1},
        {"AzureReadRequestsCount": 5, "CachedReadBufferReadFromCacheHits": 1},
    )
    assert result == {"AzureReadRequestsCount": 3}


def test_profile_config_only_adds_ephemeral_query_and_processor_logs(tmp_path):
    class Engine:
        def _write_config(self, scratch):
            scratch.mkdir()
            config = scratch / "config.xml"
            config.write_text("<clickhouse></clickhouse>")
            return config

    config = module._profile_config(Engine(), tmp_path / "scratch")
    text = config.read_text()
    assert "<query_log>" in text and "<processors_profile_log>" in text
    assert "onelake_bearer_token" not in text


def test_query_log_profile_maps_exact_id_and_excludes_raw_sql():
    query_id = "11111111-2222-3333-4444-555555555555"

    class Session:
        def query(self, sql, fmt=None):
            if sql == "SYSTEM FLUSH LOGS":
                return ""
            if "FROM system.query_log" in sql:
                return json.dumps(
                    {
                        "data": [
                            [
                                query_id,
                                1500,
                                1_000_000,
                                100_000,
                                12345,
                                {"AzureReadRequestsCount": 8, "Secret": "eyJsecret"},
                            ]
                        ]
                    }
                )
            if "FROM system.processors_profile_log" in sql:
                return json.dumps(
                    {
                        "data": [
                            [
                                "ReadFromStorage",
                                "AzureBlobSource",
                                900,
                                600,
                                10,
                                1_000_000,
                                100_000,
                                100,
                                1000,
                            ]
                        ]
                    }
                )
            raise AssertionError(sql)

    result = module._native_chdb_profile(Session(), "profile_local_q7", True)
    assert result["available"] is True
    assert result["read_rows"] == 1_000_000
    assert result["profile_events"] == {"AzureReadRequestsCount": 8}
    assert result["processors"]["groups"][0]["input_wait_us_sum"] == 600
    assert "Secret" not in json.dumps(result)
    assert "eyJsecret" not in json.dumps(result)
