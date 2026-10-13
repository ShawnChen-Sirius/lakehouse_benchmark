"""Compare sanitized native evidence against unprofiled paired-run medians."""

from __future__ import annotations

import csv
import json
import statistics
import sys
from pathlib import Path

ENGINES = ("duckdb_iceberg", "polars_iceberg", "chdb_iceberg")
CLICKHOUSE_LOCAL = "clickhouse_local_iceberg"
SCENARIOS = ("local", "remote")
TARGETS = (1, 2, 3, 7, 21)


def baselines(directory: Path) -> dict[tuple[str, str, int], float]:
    groups: dict[tuple[str, str, int], list[float]] = {}
    with (directory / "query_rounds.csv").open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            query = int(row["query"])
            if query not in TARGETS or row["engine"] not in ENGINES:
                continue
            key = (row["engine"], row["scenario"], query)
            groups.setdefault(key, []).append(float(row["wall_seconds"]))
    return {
        key: round(statistics.median(values), 5)
        for key, values in groups.items()
        if len(values) == 3
    }


def collect(directory: Path, baseline_dir: Path) -> dict:
    baseline_summary = json.loads((baseline_dir / "summary.json").read_text(encoding="utf-8"))
    baseline = baselines(baseline_dir)
    problems = []
    if not baseline_summary["valid"]:
        problems.append("previous paired benchmark is invalid")
    manifest_path = directory / "manifest.json"
    verification = directory / "snapshot-verification.json"
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
    )
    verify = json.loads(verification.read_text(encoding="utf-8")) if verification.exists() else None
    if not manifest or not (verify or {}).get("unchanged"):
        problems.append("profiling dataset missing or source snapshot changed")
    profiles = {}
    same_runner_baseline = {}
    same_runner_rows = {}
    cpu_models = set()
    for engine in ENGINES:
        for scenario in SCENARIOS:
            unprofiled_path = directory / f"baseline-{engine}-{scenario}.json"
            if not unprofiled_path.exists():
                problems.append(f"missing same-runner {engine} {scenario} baseline")
            else:
                unprofiled = json.loads(unprofiled_path.read_text(encoding="utf-8"))
                cpu_models.add(unprofiled.get("cpu_model"))
                if unprofiled.get("status") != "ok" or len(unprofiled.get("queries", [])) != 22:
                    problems.append(f"incomplete same-runner {engine} {scenario} baseline")
                else:
                    for number in TARGETS:
                        query = unprofiled["queries"][number - 1]
                        same_runner_baseline[engine, scenario, number] = query.get(
                            "wall_seconds", query.get("wall_seconds_cli_timer")
                        )
                        same_runner_rows[engine, scenario, number] = query["rows"]
            path = directory / f"{engine}-{scenario}.json"
            if not path.exists():
                problems.append(f"missing {engine} {scenario} profile")
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            profiles[engine, scenario] = data
            cpu_models.add(data.get("cpu_model"))
            if data["status"] != "ok" or data["queries_completed"] != 22:
                problems.append(f"incomplete {engine} {scenario} query sequence")
            if set(data["targets"]) != {str(n) for n in TARGETS}:
                problems.append(f"missing selected queries in {engine} {scenario}")
            if engine == "chdb_iceberg":
                for number in (7, 21):
                    if (
                        not data.get("targets", {})
                        .get(str(number), {})
                        .get("native", {})
                        .get("available")
                    ):
                        problems.append(f"missing chDB native Q{number} profile in {scenario}")
    clickhouse_process = {}
    for scenario in SCENARIOS:
        path = directory / f"baseline-{CLICKHOUSE_LOCAL}-{scenario}.json"
        if not path.exists():
            problems.append(f"missing same-runner {CLICKHOUSE_LOCAL} {scenario} baseline")
            continue
        local_cli = json.loads(path.read_text(encoding="utf-8"))
        cpu_models.add(local_cli.get("cpu_model"))
        if local_cli.get("status") != "ok" or len(local_cli.get("queries", [])) != 22:
            problems.append(f"incomplete {CLICKHOUSE_LOCAL} {scenario} baseline")
            continue
        for number in TARGETS:
            query = local_cli["queries"][number - 1]
            same_runner_baseline[CLICKHOUSE_LOCAL, scenario, number] = query[
                "wall_seconds_cli_timer"
            ]
            same_runner_rows[CLICKHOUSE_LOCAL, scenario, number] = query["rows"]
        clickhouse_process[scenario] = {
            key: local_cli.get(key)
            for key in (
                "process_wall_seconds",
                "process_cpu_user_seconds",
                "process_cpu_system_seconds",
                "unattributed_wall_seconds",
                "marker_timer_seconds",
            )
        }
    if len(cpu_models) != 1:
        problems.append("unprofiled and profiled scenarios did not share one CPU model")
    controls = []
    for phase in ("before", "after"):
        path = directory / f"cpu-control-{phase}.json"
        if path.exists():
            controls.append(json.loads(path.read_text(encoding="utf-8")))
    if len(controls) != 2 or controls[0]["sha256"] != controls[1]["sha256"]:
        problems.append("CPU-only controls missing or used different bytes")

    row_mismatches, digest_mismatches = [], []
    for scenario in SCENARIOS:
        for number in TARGETS:
            targets = [
                profiles.get((engine, scenario), {}).get("targets", {}).get(str(number))
                for engine in ENGINES
            ]
            available = [target for target in targets if target]
            if len(available) != 3:
                continue
            if len({target["rows"] for target in available}) > 1:
                row_mismatches.append({"scenario": scenario, "query": number})
            cli_rows = same_runner_rows.get((CLICKHOUSE_LOCAL, scenario, number))
            if cli_rows is not None and cli_rows != available[0]["rows"]:
                row_mismatches.append(
                    {"scenario": scenario, "query": number, "source": "ClickHouse Local"}
                )
            if any(
                target["rows"] != same_runner_rows.get((engine, scenario, number))
                for engine, target in zip(ENGINES, targets, strict=True)
                if target
            ):
                row_mismatches.append(
                    {
                        "scenario": scenario,
                        "query": number,
                        "source": "unprofiled/profiled discrepancy",
                    }
                )
            if len({target["result_digest_cents"] for target in available}) > 1:
                digest_mismatches.append({"scenario": scenario, "query": number})
    if row_mismatches:
        problems.append("selected result row counts differ across engines")
    return {
        "valid": not problems,
        "problems": problems,
        "baseline_run": "38338000796",
        "baseline": baseline,
        "same_runner_baseline": same_runner_baseline,
        "cpu_control_wall_seconds": [control["wall_seconds"] for control in controls],
        "cpu_model": next(iter(cpu_models)) if len(cpu_models) == 1 else None,
        "manifest_bytes": manifest.get("total_bytes") if manifest else None,
        "snapshot_unchanged": bool((verify or {}).get("unchanged")),
        "profiles": profiles,
        "clickhouse_process": clickhouse_process,
        "row_mismatches": row_mismatches,
        "digest_mismatches_cents": digest_mismatches,
    }


def _event(native: dict, name: str) -> int | None:
    return native.get("profile_events", {}).get(name)


def report(data: dict) -> str:
    lines = [
        "# Q7/Q21 horizontal comparison: local Parquet versus OneLake",
        "",
        "All three engines in this diagnostic use ONE runner and the prior benchmark's exact",
        "chdb-core wheel, DuckDB nightly and Polars wheel. Native profiles are a separate run;",
        "do not rank engines by profiled wall time. First compare THIS runner's unprofiled",
        "full-suite Q7/Q21 timings; the prior three paired medians are a stability cross-check.",
        "",
        f"Profile source snapshot unchanged: {data['snapshot_unchanged']}; "
        f"CPU model: {data['cpu_model']}; data bytes: {data['manifest_bytes']}.",
        f"CPU-only controls before/after: {data['cpu_control_wall_seconds']} seconds.",
        f"Result row mismatches: {data['row_mismatches'] or 'none'}; "
        f"Result digest differences (rounded to cents): "
        f"{data['digest_mismatches_cents'] or 'none'}.",
    ]
    for title, source in (
        ("Same-runner unprofiled full-suite Q7/Q21 (seconds)", data["same_runner_baseline"]),
        ("Prior three unprofiled paired medians (run 38338000796)", data["baseline"]),
    ):
        lines += [
            "",
            f"## {title}",
            "",
            "| Query / scenario | DuckDB | Polars | chDB | chDB / DuckDB | chDB / Polars |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for number in (7, 21):
            for scenario in SCENARIOS:
                values = [source.get((engine, scenario, number)) for engine in ENGINES]
                if any(value is None for value in values):
                    lines.append(f"| Q{number} {scenario} | missing | missing | missing | — | — |")
                    continue
                duck, polars, chdb = values
                lines.append(
                    f"| Q{number} {scenario} | {duck:.2f} | {polars:.2f} | {chdb:.2f} | "
                    f"{chdb / duck:.1f}× | {chdb / polars:.1f}× |"
                )
    lines += [
        "",
        "## ClickHouse Local on the same runner (unprofiled CLI timers)",
        "",
        "| Query | Scenario | DuckDB | Polars | chDB | ClickHouse Local | "
        "Local / DuckDB | Local / Polars |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for number in (7, 21):
        for scenario in SCENARIOS:
            values = [
                data["same_runner_baseline"].get((engine, scenario, number))
                for engine in (*ENGINES, CLICKHOUSE_LOCAL)
            ]
            if any(value is None for value in values):
                lines.append(f"| Q{number} | {scenario} | incomplete | | | | | |")
                continue
            duck, polars, chdb, cli = values
            lines.append(
                f"| Q{number} | {scenario} | {duck:.2f}s | {polars:.2f}s | "
                f"{chdb:.2f}s | {cli:.2f}s | {cli / duck:.1f}× | {cli / polars:.1f}× |"
            )
    for scenario, process in data["clickhouse_process"].items():
        lines.append(
            f"- ClickHouse Local {scenario}: process {process['process_wall_seconds']}s, "
            f"CPU user/system {process['process_cpu_user_seconds']}/"
            f"{process['process_cpu_system_seconds']}s, "
            f"unattributed {process['unattributed_wall_seconds']}s, "
            f"marker timer {process['marker_timer_seconds']}s."
        )
    lines += [
        "",
        "## Profiled Q7 and Q21 execution (diagnostic only)",
        "",
        "| Query | Scenario | Engine | Wall | CPU seconds | CPU / wall | Rows | Plan nodes |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for number in (7, 21):
        for scenario in SCENARIOS:
            for engine in ENGINES:
                target = (
                    data["profiles"].get((engine, scenario), {}).get("targets", {}).get(str(number))
                )
                if not target:
                    continue
                lines.append(
                    f"| Q{number} | {scenario} | {engine} | {target['wall_seconds']:.2f}s | "
                    f"{target['cpu_seconds']:.2f}s | {target['cpu_over_wall']:.2f} | "
                    f"{target['rows']} | {len(target['plan']['nodes'])} |"
                )
    lines += ["", "## chDB Q7/Q21: Azure and filesystem-cache evidence", ""]
    wanted = (
        "AzureReadRequestsCount",
        "AzureReadMicroseconds",
        "ReadBufferFromAzureBytes",
        "CachedReadBufferReadFromCacheHits",
        "CachedReadBufferReadFromCacheMisses",
        "CachedReadBufferReadFromSourceMicroseconds",
        "CachedReadBufferReadFromSourceBytes",
        "CachedReadBufferReadFromCacheBytes",
        "CachedReadBufferCacheWriteBytes",
    )
    for number in (7, 21):
        for scenario in SCENARIOS:
            target = (
                data["profiles"]
                .get(("chdb_iceberg", scenario), {})
                .get("targets", {})
                .get(str(number))
            )
            if not target:
                continue
            native = target.get("native", {})
            events = {
                name: _event(native, name) for name in wanted if _event(native, name) is not None
            }
            lines.append(f"- Q{number} {scenario}: {json.dumps(events, sort_keys=True)}")
            processors = native.get("processors", {}).get("groups", [])[:8]
            lines.append(
                "  Processor groups (count, execution us sum, input-wait us sum): "
                + "; ".join(
                    f"{p['plan_step']}/{p['processor']} "
                    f"{p['processors']}/{p['elapsed_us_sum']}/{p['input_wait_us_sum']}"
                    for p in processors
                )
            )
    lines += ["", "## Plan and operator structure", ""]
    for number in (7, 21):
        for engine in ENGINES:
            for scenario in SCENARIOS:
                target = (
                    data["profiles"].get((engine, scenario), {}).get("targets", {}).get(str(number))
                )
                if not target:
                    continue
                counts = target.get("plan", {}).get("kind_counts", {})
                lines.append(
                    f"- Q{number} {engine} {scenario}: {json.dumps(counts, sort_keys=True)}"
                )
                if engine == "duckdb_iceberg":
                    operators = sorted(
                        target.get("duckdb", {}).get("operators", []),
                        key=lambda item: item.get("timing_seconds") or 0,
                        reverse=True,
                    )[:8]
                    lines.append(
                        "  DuckDB operator timing (not wall-additive): "
                        + "; ".join(
                            f"{item['name']} {item.get('timing_seconds')}s" for item in operators
                        )
                    )
    lines += [
        "",
        "## Interpretation guardrails",
        "",
        "- Polars 2.0 streaming removed LazyFrame.profile(); explain is plan-only.",
        "- DuckDB operator/ClickHouse processor times can overlap between threads.",
        "  Neither their sum nor Azure read microseconds is end-to-end wall time.",
        "- The local file() and scan_parquet() plans can differ from Iceberg scans.",
        "  Use rows/digest checks and plan structure before attributing a gap to networking.",
        "- No raw plans, SQL, paths, profiles, Parquet or tokens are uploaded.",
        "- ClickHouse Local has CLI per-query wall timers and whole-process CPU, but no",
        "  per-query native processor profile in this run. Do not attribute its process",
        "  CPU or unattributed wall to Q7/Q21, Fabric, or a specific operator.",
    ]
    if data["problems"]:
        lines += ["", "## Incomplete diagnostic", ""]
        lines.extend(f"- {problem}" for problem in data["problems"])
    return "\n".join(lines) + "\n"


def _json_projection(data: dict) -> dict:
    """Tuple keys are internal only; projected artifact has string keys."""
    return {
        **{
            key: value
            for key, value in data.items()
            if key not in ("baseline", "same_runner_baseline", "profiles")
        },
        "baseline": {
            f"{engine}:{scenario}:Q{number}": value
            for (engine, scenario, number), value in data["baseline"].items()
        },
        "same_runner_baseline": {
            f"{engine}:{scenario}:Q{number}": value
            for (engine, scenario, number), value in data["same_runner_baseline"].items()
        },
    }


def main() -> None:
    directory, baseline_dir = Path(sys.argv[1]), Path(sys.argv[2])
    data = collect(directory, baseline_dir)
    text = report(data)
    (directory / "report.md").write_text(text, encoding="utf-8")
    (directory / "comparison.json").write_text(
        json.dumps(_json_projection(data), indent=2) + "\n", encoding="utf-8"
    )
    print(text)
    if not data["valid"]:
        raise SystemExit("incomplete or non-comparable cross-engine profile")


if __name__ == "__main__":
    main()
