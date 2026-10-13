"""Summarize the single-run remote/local diagnosis without logging hot-path data."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ENGINES = ("duckdb_iceberg", "polars_iceberg", "chdb_iceberg", "clickhouse_local_iceberg")
FIELDS = (
    "engine",
    "query",
    "local_wall_s",
    "remote_wall_s",
    "remote_minus_local_s",
    "local_cpu_s",
    "remote_cpu_s",
    "local_disk_read_bytes",
    "remote_disk_read_bytes",
    "local_rss_peak_bytes_sampled",
    "remote_rss_peak_bytes_sampled",
    "local_voluntary_context_switches",
    "remote_voluntary_context_switches",
    "local_involuntary_context_switches",
    "remote_involuntary_context_switches",
    "local_rows",
    "remote_rows",
)


def query_wall(query: dict) -> float:
    return query.get("wall_seconds", query.get("wall_seconds_cli_timer", 0.0))


def _fmt(number: float | None, suffix: str = "s") -> str:
    return f"{number:.2f}{suffix}" if number is not None else "—"


def summarize(directory: Path) -> tuple[dict, list[dict]]:
    manifest_path = directory / "manifest.json"
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    )
    result = {"manifest": manifest, "engines": {}, "row_count_mismatches": []}
    verify_path = directory / "snapshot-verification.json"
    result["snapshot_verification"] = (
        json.loads(verify_path.read_text(encoding="utf-8")) if verify_path.exists() else None
    )
    controls = {}
    for phase in ("before", "after"):
        path = directory / f"cpu-control-{phase}.json"
        if path.exists():
            controls[phase] = json.loads(path.read_text(encoding="utf-8"))
    result["cpu_controls"] = controls
    result["cpu_control_same_input"] = (
        controls["before"]["sha256"] == controls["after"]["sha256"] if len(controls) == 2 else None
    )
    query_rows = []
    for name in ENGINES:
        scenarios = {}
        for scenario in ("local", "remote"):
            path = directory / f"{name}-{scenario}.json"
            scenarios[scenario] = (
                json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
            )
        local, remote = scenarios["local"], scenarios["remote"]
        if local is None or remote is None:
            result["engines"][name] = {"status": "missing", "local": local, "remote": remote}
            continue
        paired = min(len(local["queries"]), len(remote["queries"]))
        result["row_count_mismatches"].extend(
            {
                "engine": name,
                "query": i + 1,
                "local": local["queries"][i]["rows"],
                "remote": remote["queries"][i]["rows"],
            }
            for i in range(paired)
            if local["queries"][i]["rows"] != remote["queries"][i]["rows"]
        )
        local_wall = sum(query_wall(q) for q in local["queries"])
        remote_wall = sum(query_wall(q) for q in remote["queries"])
        local_cpu = sum(q.get("cpu_seconds") or 0 for q in local["queries"])
        remote_cpu = sum(q.get("cpu_seconds") or 0 for q in remote["queries"])
        result["engines"][name] = {
            "status": "ok"
            if local["status"] == remote["status"] == "ok" and paired == 22
            else "incomplete",
            "version": remote.get("version"),
            "cpu_model": remote.get("cpu_model"),
            "local_query_wall_seconds": round(local_wall, 5),
            "remote_query_wall_seconds": round(remote_wall, 5),
            "remote_over_local_wall": round(remote_wall / local_wall, 3) if local_wall else None,
            "local_query_cpu_seconds": round(local_cpu, 5) if name != ENGINES[-1] else None,
            "remote_query_cpu_seconds": round(remote_cpu, 5) if name != ENGINES[-1] else None,
            "local_process_wall_seconds": local.get("process_wall_seconds"),
            "remote_process_wall_seconds": remote.get("process_wall_seconds"),
            "local_process_cpu_seconds": (
                round(local["process_cpu_user_seconds"] + local["process_cpu_system_seconds"], 5)
                if name == ENGINES[-1]
                else None
            ),
            "remote_process_cpu_seconds": (
                round(remote["process_cpu_user_seconds"] + remote["process_cpu_system_seconds"], 5)
                if name == ENGINES[-1]
                else None
            ),
            "local_unattributed_wall_seconds": local.get("unattributed_wall_seconds"),
            "remote_unattributed_wall_seconds": remote.get("unattributed_wall_seconds"),
            "local": local["status"],
            "remote": remote["status"],
        }
        for left, right in zip(local["queries"], remote["queries"], strict=False):
            local_seconds, remote_seconds = query_wall(left), query_wall(right)
            query_rows.append(
                {
                    "engine": name,
                    "query": left["query"],
                    "local_wall_s": local_seconds,
                    "remote_wall_s": remote_seconds,
                    "remote_minus_local_s": round(remote_seconds - local_seconds, 5),
                    "local_cpu_s": left.get("cpu_seconds"),
                    "remote_cpu_s": right.get("cpu_seconds"),
                    "local_disk_read_bytes": left.get("disk_read_bytes"),
                    "remote_disk_read_bytes": right.get("disk_read_bytes"),
                    "local_rss_peak_bytes_sampled": left.get("rss_peak_bytes_sampled"),
                    "remote_rss_peak_bytes_sampled": right.get("rss_peak_bytes_sampled"),
                    "local_voluntary_context_switches": left.get("voluntary_context_switches"),
                    "remote_voluntary_context_switches": right.get("voluntary_context_switches"),
                    "local_involuntary_context_switches": left.get(
                        "involuntary_context_switches"
                    ),
                    "remote_involuntary_context_switches": right.get(
                        "involuntary_context_switches"
                    ),
                    "local_rows": left["rows"],
                    "remote_rows": right["rows"],
                }
            )
    return result, query_rows


def report(summary: dict, rows: list[dict]) -> str:
    manifest = summary["manifest"]
    lines = [
        "# SF10: OneLake Iceberg versus the SAME local Parquet bytes",
        "",
        (
            f"Staged {manifest['total_bytes'] / 1024**3:.2f} GiB of original files "
            f"in {manifest['download_wall_seconds']:.1f}s (outside the timers)."
        )
        if manifest
        else "Dataset staging did not complete.",
        "All four engine/scenario pairs ran sequentially on ONE GitHub runner; "
        "see JSON for CPU model.",
        "No frequent log output or instrumentation network calls inside the timed queries.",
        f"Source snapshots unchanged after the run: "
        f"{(summary['snapshot_verification'] or {}).get('unchanged', 'unknown')}.",
        "",
        "| Engine | Local 22 queries | OneLake 22 queries | Remote / local | "
        "Local CPU sec | Remote CPU sec | CLI extra local / remote | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for name in ENGINES:
        value = summary["engines"][name]
        if value["status"] == "missing":
            lines.append(f"| {name} | — | — | — | — | — | — | missing |")
            continue
        if name == ENGINES[-1]:
            cpu_local, cpu_remote = (
                value["local_process_cpu_seconds"],
                value["remote_process_cpu_seconds"],
            )
        else:
            cpu_local, cpu_remote = (
                value["local_query_cpu_seconds"],
                value["remote_query_cpu_seconds"],
            )
        ratio = value["remote_over_local_wall"]
        extras = (
            f"{_fmt(value['local_unattributed_wall_seconds'])} / "
            f"{_fmt(value['remote_unattributed_wall_seconds'])}"
        )
        lines.append(
            f"| {name} | {_fmt(value['local_query_wall_seconds'])} | "
            f"{_fmt(value['remote_query_wall_seconds'])} | "
            f"{f'{ratio:.2f}×' if ratio is not None else '—'} | "
            f"{_fmt(cpu_local)} | {_fmt(cpu_remote)} | "
            f"{extras if name == ENGINES[-1] else '—'} | {value['status']} |"
        )
    lines += ["", "## Largest remote-minus-local query gaps", ""]
    for name in ENGINES:
        candidates = sorted(
            (row for row in rows if row["engine"] == name),
            key=lambda row: row["remote_minus_local_s"],
            reverse=True,
        )[:5]
        lines.append(
            f"- {name}: "
            + ", ".join(
                f"Q{row['query']} +{row['remote_minus_local_s']:.2f}s" for row in candidates
            )
        )
    lines += [
        "",
        "## CPU-only controls",
        "",
        "Before / after wall: "
        + " / ".join(
            _fmt(summary["cpu_controls"].get(phase, {}).get("wall_seconds"))
            for phase in ("before", "after")
        )
        + f"; identical bytes/digest: {summary['cpu_control_same_input']}.",
        "",
        "## Interpretation limits",
        "",
        "- Local read_parquet/file() uses the same original bytes but bypasses Iceberg catalog",
        "  and can make different scan plans. The remote/local difference is NOT "
        "pure network time.",
        "- Local files were downloaded first and may be in the Linux page cache; local is a",
        "  compute-oriented lower bound, not a cold-SSD comparison.",
        "- CPU seconds include native worker threads (values may exceed wall seconds). Kernel",
        "  disk-read counters do not count HTTP/OneLake bytes. RSS is sampled at 500 ms.",
        "- ClickHouse Local CLI provides per-query wall timers but not per-query CPU. Its process",
        "  CPU and unexplained process wall are reported separately and must not "
        "be assigned to Fabric.",
        f"- Row-count mismatches: {summary['row_count_mismatches'] or 'none'}.",
        "",
        "See query_metrics.csv and the eight raw JSON files for all counters and phases.",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    directory = Path(sys.argv[1])
    summary, rows = summarize(directory)
    (directory / "analysis.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (directory / "query_metrics.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    text = report(summary, rows)
    (directory / "report.md").write_text(text, encoding="utf-8")
    print(text)
    if (
        summary["row_count_mismatches"]
        or any(item["status"] != "ok" for item in summary["engines"].values())
        or (summary["snapshot_verification"] or {}).get("unchanged") is not True
    ):
        raise SystemExit("incomplete or incorrect local/remote comparison")


if __name__ == "__main__":
    main()
