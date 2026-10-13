"""Bounded, non-sensitive projections of native query profiles and plans.

Never upload raw SQL, plans, DuckDB JSON profiles or ClickHouse system logs:
they may contain a OneLake path, literal or bearer from session setup.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation

TABLES = frozenset(
    ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")
)
_TABLE = re.compile(
    r"\b(?:CH0010\.)?(lineitem|orders|partsupp|part|customer|nation|region|supplier)\b", re.I
)
_CATEGORIES = (
    "HASH_JOIN",
    "NESTED_LOOP_JOIN",
    "JOIN",
    "PARQUET_SCAN",
    "ICEBERG_SCAN",
    "TABLE_SCAN",
    "SCAN",
    "FILTER",
    "AGGREGATE",
    "GROUP_BY",
    "SORT",
    "ORDER_BY",
    "PROJECTION",
    "EXPRESSION",
    "EXCHANGE",
    "SOURCE",
    "SINK",
    "LIMIT",
)
_PARALLEL = re.compile(r"(?:×|\bx)\s*(\d{1,3})\b", re.I)
_SAFE_NAME = re.compile(r"[a-zA-Z][a-zA-Z0-9_ ]{0,79}\Z")


def safe_name(value: object) -> str:
    """Names only, never plan-step descriptions, paths, SQL or extra_info."""
    text = str(value).strip()
    return text if _SAFE_NAME.fullmatch(text) else "REDACTED"


def plan_features(text: str) -> dict:
    """Extract category/parallelism/table-name structure without storing raw text."""
    nodes = []
    for line_index, line in enumerate(text.splitlines()):
        upper = line.upper().replace(" ", "_")
        categories = [category for category in _CATEGORIES if category in upper]
        for fragment, category in (
            ("READFROM", "READ_SOURCE"),
            ("READ_FROM", "READ_SOURCE"),
            ("AGGREGAT", "AGGREGATE"),
            ("RESIZE", "RESIZE"),
            ("NUMBERSRANGE", "RANGE_SOURCE"),
        ):
            if fragment in upper and category not in categories:
                categories.append(category)
        if not categories:
            continue
        tables = sorted({match.lower() for match in _TABLE.findall(line)})
        parallel = _PARALLEL.search(line)
        nodes.append(
            {
                "kinds": categories[:4],
                "tables": tables,
                "instances": int(parallel.group(1)) if parallel else 1,
                "line_index": line_index,
                "layout_depth_hint": len(line) - len(line.lstrip(" │┌└├─")),
            }
        )
        if len(nodes) == 200:
            break
    return {
        "plan_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "plan_lines": len(text.splitlines()),
        "nodes": nodes,
        "kind_counts": dict(Counter(k for node in nodes for k in node["kinds"])),
    }


def _canonical(value: object) -> str:
    if value is None:
        return "<NULL>"
    try:
        number = Decimal(str(value))
        if number.is_finite():
            return format(number.quantize(Decimal("0.01")), ".2f")
    except (InvalidOperation, ValueError):
        pass
    return str(value)


def result_digest(rows: list) -> str:
    """Compare small selected outputs to cents, without saving the rows."""
    canonical = sorted(tuple(_canonical(value) for value in row) for row in rows)
    payload = json.dumps(canonical, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def duckdb_profile(document: dict) -> dict:
    """Strip SQL, file paths, filters and all unrecognized JSON keys."""
    nodes = []

    def visit(node: dict, depth: int = 0) -> None:
        if len(nodes) >= 200:
            return
        if "operator_name" in node:
            nodes.append(
                {
                    "name": safe_name(node.get("operator_name")),
                    "operator_type": safe_name(node.get("operator_type")),
                    "timing_seconds": node.get("operator_timing"),
                    "cpu_seconds": node.get("cpu_time"),
                    "cardinality": node.get("operator_cardinality"),
                    "rows_scanned": node.get("operator_rows_scanned"),
                    "depth": depth,
                }
            )
        for child in node.get("children", []):
            visit(child, depth + 1)

    visit(document)
    root = {
        key: document.get(key)
        for key in (
            "latency",
            "cpu_time",
            "total_bytes_read",
            "blocked_thread_time",
            "system_peak_buffer_memory",
            "system_peak_temp_dir_size",
            "cumulative_rows_scanned",
            "rows_returned",
        )
    }
    return {"root": root, "operators": nodes}


def processor_profile(rows: list[dict]) -> dict:
    """Aggregate only normalized processor/plan-step names and numeric counters."""
    groups: dict[tuple[str, str], dict] = defaultdict(
        lambda: {
            "processors": 0,
            "elapsed_us_sum": 0,
            "elapsed_us_max": 0,
            "input_wait_us_sum": 0,
            "input_wait_us_max": 0,
            "output_wait_us_sum": 0,
            "input_rows_sum": 0,
            "input_bytes_sum": 0,
        }
    )
    for row in rows[:10000]:
        key = (safe_name(row.get("plan_step_name", "")), safe_name(row.get("name", "")))
        value = groups[key]
        value["processors"] += 1
        value["elapsed_us_sum"] += int(row.get("elapsed_us") or 0)
        value["elapsed_us_max"] = max(value["elapsed_us_max"], int(row.get("elapsed_us") or 0))
        value["input_wait_us_sum"] += int(row.get("input_wait_elapsed_us") or 0)
        value["input_wait_us_max"] = max(
            value["input_wait_us_max"], int(row.get("input_wait_elapsed_us") or 0)
        )
        value["output_wait_us_sum"] += int(row.get("output_wait_elapsed_us") or 0)
        value["input_rows_sum"] += int(row.get("input_rows") or 0)
        value["input_bytes_sum"] += int(row.get("input_bytes") or 0)
    projected = [
        {"plan_step": step, "processor": name, **counters}
        for (step, name), counters in groups.items()
    ]
    projected.sort(key=lambda item: item["elapsed_us_sum"], reverse=True)
    return {"profile_rows": len(rows), "truncated": len(rows) > 10000,
            "groups": projected[:100]}
