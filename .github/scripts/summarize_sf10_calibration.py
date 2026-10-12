"""Save all four engines' SF10 rounds, stability and historical comparison.

The fork inherits the author's docs/data/tpch_results.csv. Reference means
use the author's latest three COMPLETE SF10 cold runs for each original engine;
the output retains each reference version/date so differing builds stay visible.
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from pathlib import Path

ENGINES = ("duckdb_iceberg", "polars_iceberg", "chdb_iceberg", "clickhouse_local_iceberg")
ROUNDS = (1, 2, 3)
FIELDS = (
    "engine",
    "round",
    "version",
    "runner",
    "cpu",
    "mem_gb",
    "python",
    "engine_status",
    "phase",
    "query",
    "seconds",
    "rows",
    "status",
    "error",
)


def author_references(path: Path) -> dict[str, dict]:
    groups: dict[tuple[str, str], list[dict]] = {}
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            if row["sf"] != "10" or row["test"] != "tpch" or row["run_type"] != "cold":
                continue
            if row["engine"] not in ENGINES:
                continue
            groups.setdefault((row["engine"], row["run_id"]), []).append(row)
    references: dict[str, list[dict]] = {engine: [] for engine in ENGINES}
    for (engine, run_id), rows in groups.items():
        query_rows = [r for r in rows if r["phase"] == "query"]
        if len(query_rows) != 22 or any(r["status"] != "ok" or not r["dur"] for r in query_rows):
            continue
        references[engine].append(
            {
                "run_id": run_id,
                "date": rows[0]["run_started_at"],
                "version": rows[0]["engine_version"],
                "seconds": round(sum(float(r["dur"]) for r in query_rows), 4),
            }
        )
    return {
        engine: {
            "runs": sorted(runs, key=lambda row: row["date"])[-3:],
            "mean_seconds": round(
                statistics.mean(
                    r["seconds"] for r in sorted(runs, key=lambda row: row["date"])[-3:]
                ),
                4,
            )
            if runs
            else None,
        }
        for engine, runs in references.items()
    }


def collect(root: Path) -> tuple[list[dict], list[dict]]:
    records, rounds = [], []
    for number in ROUNDS:
        for engine in ENGINES:
            part = root / f"sf10-{engine}-round-{number}" / "parts" / f"{engine}.json"
            if not part.exists():
                rounds.append({"engine": engine, "round": number, "status": "missing"})
                continue
            data = json.loads(part.read_text(encoding="utf-8"))
            rows = data.get("rows", [])
            successful = [row for row in rows if row["phase"] == "query" and row["status"] == "ok"]
            complete = data["status"] == "ok" and len(successful) == 22
            rounds.append(
                {
                    "engine": engine,
                    "round": number,
                    "status": "ok" if complete else "incomplete",
                    "version": data["version"],
                    "queries_ok": len(successful),
                    "seconds": round(sum(row["dur"] for row in successful), 4)
                    if complete
                    else None,
                    "host": data.get("host", {}),
                }
            )
            for row in rows:
                records.append(
                    {
                        "engine": engine,
                        "round": number,
                        "version": data["version"],
                        "runner": data.get("host", {}).get("runner", ""),
                        "cpu": data.get("host", {}).get("cpu", ""),
                        "mem_gb": data.get("host", {}).get("mem_gb", ""),
                        "python": data.get("host", {}).get("python", ""),
                        "engine_status": data["status"],
                        "phase": row["phase"],
                        "query": row["query"],
                        "seconds": row["dur"],
                        "rows": row.get("rows"),
                        "status": row["status"],
                        "error": row.get("error", ""),
                    }
                )
    return records, rounds


def summarize(records: list[dict], rounds: list[dict], author: dict) -> dict:
    by_engine = {}
    for engine in ENGINES:
        good = [r for r in rounds if r["engine"] == engine and r["status"] == "ok"]
        numbers = [r["seconds"] for r in good]
        versions = {r["version"] for r in good}
        mean = statistics.mean(numbers) if len(numbers) == 3 else None
        median = statistics.median(numbers) if len(numbers) == 3 else None
        baseline = author[engine]["mean_seconds"]
        by_engine[engine] = {
            "complete_rounds": len(good),
            "same_version": len(versions) == 1 if good else False,
            "median_seconds": round(median, 4) if median is not None else None,
            "cv_percent": round(statistics.pstdev(numbers) / mean * 100, 2) if mean else None,
            "max_min_percent_of_median": round((max(numbers) - min(numbers)) / median * 100, 2)
            if median
            else None,
            "author_mean_seconds": baseline,
            "versus_author_percent": round((median / baseline - 1) * 100, 2)
            if median and baseline
            else None,
        }
    counts: dict[int, dict[str, set[int]]] = {}
    for row in records:
        if row["phase"] == "query" and row["status"] == "ok" and row["rows"] is not None:
            counts.setdefault(row["query"], {}).setdefault(row["engine"], set()).add(row["rows"])
    mismatches = [
        {"query": q, "counts": {engine: sorted(values) for engine, values in engines.items()}}
        for q, engines in sorted(counts.items())
        if len({value for values in engines.values() for value in values}) > 1
    ]
    return {
        "rounds": rounds,
        "engines": by_engine,
        "author": author,
        "row_count_mismatches": mismatches,
    }


def markdown(summary: dict) -> str:
    def fmt(value: float | None, suffix: str = "s") -> str:
        return f"{value:.1f}{suffix}" if value is not None else "—"

    lines = [
        "# Four-engine TPC-H SF10 calibration",
        "",
        "Three independent ubuntu-latest runners/processes per engine; "
        "one cold 22-query pass per round.",
        "Only complete 22/22 results have totals. CV and range describe run-to-run variability;",
        "a small spread is not proof of the same CPU, network path or optimizer version.",
        "",
        "| Engine | Round 1 | Round 2 | Round 3 | Median | CV | "
        "Max–min / median | Author mean* | Difference* |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for engine, aggregate in summary["engines"].items():
        rounds = [r for r in summary["rounds"] if r["engine"] == engine]
        cells = [f"{r['seconds']:.1f}s" if r["status"] == "ok" else r["status"] for r in rounds]
        lines.append(
            f"| {engine} | {' | '.join(cells)} | {fmt(aggregate['median_seconds'])} | "
            f"{fmt(aggregate['cv_percent'], '%')} | "
            f"{fmt(aggregate['max_min_percent_of_median'], '%')} | "
            f"{fmt(aggregate['author_mean_seconds'])} | "
            f"{fmt(aggregate['versus_author_percent'], '%')} |"
        )
    lines += [
        "",
        "*Author means: latest three complete SF10 cold runs in the fork's inherited",
        "`docs/data/tpch_results.csv`. They may have different dates/versions and are",
        "context, not an equal-build performance claim. ClickHouse Local has no author result.",
        "",
        "## Version and provenance",
        "",
    ]
    for engine in ENGINES:
        versions = sorted(
            {r.get("version", "missing") for r in summary["rounds"] if r["engine"] == engine}
        )
        references = summary["author"][engine]["runs"]
        ref_versions = sorted({r["version"] for r in references})
        lines.append(
            f"- {engine}: ours {', '.join(versions)}; author {', '.join(ref_versions) or '—'}."
        )
    lines += ["", "## Row-count mismatches", ""]
    if summary["row_count_mismatches"]:
        for mismatch in summary["row_count_mismatches"]:
            lines.append(f"- Q{mismatch['query']}: {mismatch['counts']}")
    else:
        lines.append("None among the rows that completed (inspect incomplete runs separately).")
    lines += [
        "",
        "Raw per-query rows are in `rounds.csv`; full parts/logs are individual artifacts.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    source = Path(sys.argv[1])
    output = Path(sys.argv[2])
    output.mkdir(parents=True, exist_ok=True)
    records, rounds = collect(source)
    author = author_references(Path("docs/data/tpch_results.csv"))
    result = summarize(records, rounds, author)
    provenance = sorted(source.rglob("chdb-core-provenance.json"))
    if provenance:
        result["chdb_core_ci"] = json.loads(provenance[0].read_text(encoding="utf-8"))
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    text = markdown(result)
    (output / "summary.md").write_text(text, encoding="utf-8")
    with (output / "rounds.csv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(text)


if __name__ == "__main__":
    main()
