"""Evaluate three independent local/OneLake pairs without engine profiling.

Only complete, identical-source, same-version pairs enter stability metrics.
Thresholds are a predeclared screening gate, not a confidence interval from
three samples. An unstable result is a valid measurement, not a CI failure.
"""

from __future__ import annotations

import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ENGINES = ("duckdb_iceberg", "polars_iceberg", "chdb_iceberg", "clickhouse_local_iceberg")
ROUNDS = (1, 2, 3)
SCENARIOS = ("remote", "local")
MAX_CV_PERCENT = 10.0
MAX_SPAN_PERCENT = 20.0
MAX_CPU_DRIFT_PERCENT = 10.0
ROW_FIELDS = (
    "engine",
    "round",
    "scenario",
    "cpu_model",
    "version",
    "query",
    "wall_seconds",
    "rows",
    "process_wall_seconds",
    "setup_wall_seconds",
    "close_wall_seconds",
    "unattributed_wall_seconds",
)


def _read(path: Path) -> dict | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def query_wall(query: dict) -> float:
    return query.get("wall_seconds", query.get("wall_seconds_cli_timer", 0.0))


def phase_wall(phase: dict | None) -> float | None:
    if phase is None:
        return None
    return phase.get("wall_seconds", phase.get("wall_seconds_cli_timer"))


def _snapshot_key(manifest: dict) -> tuple[tuple[str, int], ...]:
    return tuple(sorted((name, table["snapshot_id"]) for name, table in manifest["tables"].items()))


def _packages_hash(path: Path) -> str | None:
    if not path.is_file():
        return None
    lines = sorted(path.read_text(encoding="utf-8").splitlines())
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def collect(root: Path) -> tuple[list[dict], list[dict], list[dict]]:
    pairs, rows, problems = [], [], []
    for engine in ENGINES:
        for number in ROUNDS:
            folder = root / f"paired-sf10-{engine}-round-{number}"
            local = _read(folder / f"{engine}-local.json")
            remote = _read(folder / f"{engine}-remote.json")
            manifest = _read(folder / "manifest.json")
            verified = _read(folder / "snapshot-verification.json")
            before = _read(folder / "cpu-control-before.json")
            after = _read(folder / "cpu-control-after.json")
            pair = {
                "engine": engine,
                "round": number,
                "remote": remote,
                "local": local,
                "manifest": manifest,
                "snapshot_verification": verified,
                "cpu_control_before": before,
                "cpu_control_after": after,
                "packages_sha256": _packages_hash(folder / "python-packages.txt"),
                "problems": [],
            }
            if not all((remote, local, manifest, verified, before, after)):
                pair["problems"].append("missing pair data or verification")
            else:
                if not verified.get("unchanged"):
                    pair["problems"].append("source snapshot changed inside this pair")
                if before["sha256"] != after["sha256"]:
                    pair["problems"].append("CPU control input changed")
                pair["cpu_control_drift_percent"] = round(
                    (after["wall_seconds"] / before["wall_seconds"] - 1) * 100, 2
                )
                if abs(pair["cpu_control_drift_percent"]) > MAX_CPU_DRIFT_PERCENT:
                    pair["problems"].append("CPU-only control drift exceeded 10%")
                for scenario, result in (("remote", remote), ("local", local)):
                    if result.get("status") != "ok" or len(result.get("queries", [])) != 22:
                        pair["problems"].append(f"{scenario} did not complete all 22 queries")
                    if result.get("logical_cpus") != 4 or not (
                        14 * 1024**3 <= result.get("memory_bytes", 0) <= 17 * 1024**3
                    ):
                        pair["problems"].append(f"{scenario} not a 4 CPU / ~16 GiB runner")
                if local.get("cpu_model") != remote.get("cpu_model"):
                    pair["problems"].append("CPU model changed inside a pair")
                for index, (remote_query, local_query) in enumerate(
                    zip(remote.get("queries", []), local.get("queries", []), strict=False), 1
                ):
                    if remote_query.get("query") != index or local_query.get("query") != index:
                        pair["problems"].append(f"query order differs at Q{index}")
                    if remote_query.get("rows") != local_query.get("rows"):
                        pair["problems"].append(f"local/remote row count differs at Q{index}")
                pair["snapshot_key"] = _snapshot_key(manifest)
            for scenario, result in (("remote", remote), ("local", local)):
                if result is None:
                    continue
                for query in result.get("queries", []):
                    rows.append(
                        {
                            "engine": engine,
                            "round": number,
                            "scenario": scenario,
                            "cpu_model": result.get("cpu_model"),
                            "version": result.get("version"),
                            "query": query["query"],
                            "wall_seconds": query_wall(query),
                            "rows": query["rows"],
                            "process_wall_seconds": result.get("process_wall_seconds"),
                            "setup_wall_seconds": phase_wall(result.get("setup")),
                            "close_wall_seconds": phase_wall(result.get("close")),
                            "unattributed_wall_seconds": result.get("unattributed_wall_seconds"),
                        }
                    )
            pairs.append(pair)

    snapshots = {p.get("snapshot_key") for p in pairs if p.get("snapshot_key")}
    if len(snapshots) != 1:
        problems.append("the 12 pairs did not use one identical eight-table snapshot")
    for engine in ENGINES:
        selected = [p for p in pairs if p["engine"] == engine]
        versions = {
            p[scenario].get("version")
            for p in selected
            for scenario in SCENARIOS
            if p[scenario] is not None
        }
        package_hashes = {p["packages_sha256"] for p in selected if p["packages_sha256"]}
        if len(versions) != 1:
            problems.append(f"{engine} changed engine version across pairs")
        if len(package_hashes) != 1 or any(p["packages_sha256"] is None for p in selected):
            problems.append(f"{engine} changed or omitted installed package manifest")
        for scenario in SCENARIOS:
            by_query: dict[int, set[int]] = {}
            for p in selected:
                if p[scenario] is None:
                    continue
                for q in p[scenario].get("queries", []):
                    by_query.setdefault(q["query"], set()).add(q["rows"])
            for query, counts in by_query.items():
                if len(counts) > 1:
                    problems.append(f"{engine} {scenario} Q{query} row count changed across rounds")
    cross_engine: dict[int, set[int]] = {}
    for row in rows:
        cross_engine.setdefault(row["query"], set()).add(row["rows"])
    for query, counts in sorted(cross_engine.items()):
        if len(counts) > 1:
            problems.append(f"Q{query} row counts disagree across engines")
    return pairs, rows, problems


def stability(values: list[float]) -> dict:
    if len(values) != 3:
        return {"status": "incomplete", "values": values}
    mean, median = statistics.mean(values), statistics.median(values)
    cv = statistics.pstdev(values) / mean * 100 if mean else 0
    span = (max(values) - min(values)) / median * 100 if median else 0
    return {
        "status": "stable" if cv <= MAX_CV_PERCENT and span <= MAX_SPAN_PERCENT else "unstable",
        "values": [round(v, 4) for v in values],
        "median_seconds": round(median, 4),
        "cv_percent": round(cv, 2),
        "span_percent_of_median": round(span, 2),
    }


def summarize(pairs: list[dict], rows: list[dict], problems: list[str]) -> dict:
    result = {
        "valid": not problems and all(not p["problems"] for p in pairs),
        "problems": problems,
        "thresholds": {
            "cv_percent": MAX_CV_PERCENT,
            "span_percent_of_median": MAX_SPAN_PERCENT,
            "cpu_control_drift_percent": MAX_CPU_DRIFT_PERCENT,
        },
        "engines": {},
    }
    for engine in ENGINES:
        selected = [p for p in pairs if p["engine"] == engine]
        scenarios = {}
        for scenario in SCENARIOS:
            values = [
                sum(query_wall(q) for q in p[scenario]["queries"])
                for p in selected
                if p[scenario] is not None
                and p[scenario].get("status") == "ok"
                and len(p[scenario].get("queries", [])) == 22
            ]
            scenarios[scenario] = stability(values)
        local_stable = scenarios["local"]["status"] == "stable"
        remote_stable = scenarios["remote"]["status"] == "stable"
        if any(p["problems"] for p in selected) or problems:
            verdict = "invalid/incomplete"
        elif local_stable and remote_stable:
            verdict = "paired stability gate passed"
        elif local_stable:
            verdict = "remote access path is less stable than local control"
        elif remote_stable:
            verdict = "local control unstable; inspect runner/page cache"
        else:
            verdict = "both scenarios unstable; cannot isolate source"
        result["engines"][engine] = {
            "local": scenarios["local"],
            "remote": scenarios["remote"],
            "verdict": verdict,
            "versions": [p["remote"].get("version") if p["remote"] else None
                         for p in selected],
            "package_sha256": [p["packages_sha256"] for p in selected],
            "cpu_models": [p["remote"].get("cpu_model") if p["remote"] else None for p in selected],
            "cpu_control_drift_percent": [p.get("cpu_control_drift_percent") for p in selected],
            "snapshot_verified": [
                bool((p["snapshot_verification"] or {}).get("unchanged")) for p in selected
            ],
            "pair_problems": {p["round"]: p["problems"] for p in selected if p["problems"]},
        }
    return result


def report(summary: dict, rows: list[dict]) -> str:
    def numbers(value: dict) -> str:
        return ", ".join(f"{number:.1f}" for number in value["values"]) or "—"

    def dispersion(value: dict) -> str:
        if "cv_percent" not in value:
            return value["status"]
        return (
            f"{value['cv_percent']:.1f}% / "
            f"{value['span_percent_of_median']:.1f}% ({value['status']})"
        )

    lines = [
        "# Three independent SF10 local/OneLake pairs (no engine profiling)",
        "",
        "Each paired job runs the remote cold pass BEFORE staging the original Parquet;",
        "local then reads identical bytes on the SAME 4-CPU/16-GiB runner. Jobs are serial.",
        f"Predeclared screening gate: CV <= {MAX_CV_PERCENT:.0f}% AND "
        f"max-min/median <= {MAX_SPAN_PERCENT:.0f}% for each scenario.",
        "Three samples are a screening result, not a statistical guarantee.",
        "",
        "| Engine | Local rounds (s) | Local CV / span | Remote rounds (s) | "
        "Remote CV / span | Verdict |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for name, value in summary["engines"].items():
        local, remote = value["local"], value["remote"]
        lines.append(
            f"| {name} | {numbers(local)} | {dispersion(local)} | "
            f"{numbers(remote)} | {dispersion(remote)} | {value['verdict']} |"
        )
    lines += [
        "",
        "## Cross-run integrity",
        "",
        f"Complete identical-snapshot/version pairs: {summary['valid']}.",
    ]
    if summary["problems"]:
        lines.extend(f"- {problem}" for problem in summary["problems"])
    for name, value in summary["engines"].items():
        models = ", ".join(model or "missing" for model in value["cpu_models"])
        drift = ", ".join(
            f"{number:+.1f}%" if number is not None else "missing"
            for number in value["cpu_control_drift_percent"]
        )
        lines.append(f"- {name} CPU models: {models}; CPU control drift: {drift}")
    lines += ["", "## Highest per-query span (rounds)", ""]
    for engine in ENGINES:
        for scenario in SCENARIOS:
            groups: dict[int, list[float]] = {}
            for row in rows:
                if row["engine"] == engine and row["scenario"] == scenario:
                    groups.setdefault(row["query"], []).append(row["wall_seconds"])
            ranked = sorted(
                ((q, max(v) - min(v)) for q, v in groups.items() if len(v) == 3),
                key=lambda item: item[1],
                reverse=True,
            )[:5]
            lines.append(
                f"- {engine} {scenario}: " + ", ".join(f"Q{q} {span:.2f}s" for q, span in ranked)
            )
    lines += [
        "",
        "Query timers exclude setup and close; per-job JSON records both separately.",
        "Local files may be page-cached; 'local stable / remote unstable' implicates the",
        "remote access path, not automatically the Fabric service alone. Actual CPU model",
        "may vary across fresh hosted jobs; each local/remote pair shares its own machine.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    root, output = Path(sys.argv[1]), Path(sys.argv[2])
    output.mkdir(parents=True, exist_ok=True)
    pairs, rows, problems = collect(root)
    summary = summarize(pairs, rows, problems)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (output / "query_rounds.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=ROW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    text = report(summary, rows)
    (output / "report.md").write_text(text, encoding="utf-8")
    print(text)
    if not summary["valid"]:
        raise SystemExit("missing or inconsistent paired measurements")


if __name__ == "__main__":
    main()
