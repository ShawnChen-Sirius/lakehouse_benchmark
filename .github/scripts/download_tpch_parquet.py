"""Stage the exact CH0010 Iceberg data files on the current runner, read-only.

No Parquet payload is uploaded as an Actions artifact. The manifest contains
only table-level counts, bytes and snapshot identities, not OneLake paths or
credentials. Each table's original Parquet bytes are copied without rewriting.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

from bench import auth, onelake, scrub
from bench.config import ONELAKE_DFS
from bench.tpch.config import TpchConfig


def relative_file(path: str, cfg: TpchConfig, table: str) -> str:
    """Resolve only a Parquet file under the expected OneLake table root."""
    parsed = urlsplit(path)
    if (
        parsed.scheme != "abfss"
        or parsed.netloc.lower() != (f"{cfg.workspace_id}@{ONELAKE_DFS}").lower()
    ):
        raise ValueError("Iceberg file is not in the expected OneLake workspace")
    relative = unquote(parsed.path).lstrip("/")
    prefix = f"{cfg.lakehouse_id}/Tables/{cfg.schema}/{table}/"
    if not relative.startswith(prefix):
        raise ValueError(f"Iceberg file is outside expected {table} table root")
    parts = PurePosixPath(relative).parts
    if ".." in parts or not relative.endswith(".parquet"):
        raise ValueError(f"invalid Parquet file path in {table}")
    return relative


def stage(cfg: TpchConfig, directory: Path) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    catalog = auth.catalog(cfg)
    filesystem = onelake.file_system(cfg)
    manifest = {"schema": cfg.schema, "sf": cfg.sf, "tables": {}}
    started = time.perf_counter()
    planned = []
    for name in cfg.TABLES:
        table = catalog.load_table(f"{cfg.schema}.{name}")
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise RuntimeError(f"{name} has no Iceberg snapshot")
        tasks = list(table.scan().plan_files())
        if not tasks or any(task.delete_files for task in tasks):
            raise RuntimeError(f"{name} has no data files or contains delete files")
        metadata = []
        for index, task in enumerate(tasks):
            item = task.file
            relative = relative_file(item.file_path, cfg, name)
            destination = directory / name / f"{index:04d}-{PurePosixPath(relative).name}"
            metadata.append((relative, destination, item.file_size_in_bytes, item.record_count))
        planned.append((name, snapshot.snapshot_id, metadata))

    total_bytes = sum(size for _, _, files in planned for _, _, size, _ in files)
    free_bytes = shutil.disk_usage(directory).free
    if free_bytes < total_bytes * 2 + 10 * 1024**3:
        raise RuntimeError("runner disk cannot hold the source Parquet plus benchmark scratch")

    for name, snapshot_id, files in planned:
        download_start = time.perf_counter()
        rows, size = 0, 0
        for relative, destination, expected_bytes, record_count in files:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                raise FileExistsError(f"refusing to overwrite existing staged file for {name}")
            with destination.open("xb") as output:
                filesystem.get_file_client(relative).download_file(max_concurrency=4).readinto(
                    output
                )
            actual = destination.stat().st_size
            if actual != expected_bytes:
                raise RuntimeError(f"incomplete download for {name}: {actual} != {expected_bytes}")
            rows += record_count
            size += actual
        manifest["tables"][name] = {
            "snapshot_id": snapshot_id,
            "files": len(files),
            "rows": rows,
            "bytes": size,
            "download_seconds": round(time.perf_counter() - download_start, 4),
        }
    manifest["total_bytes"] = total_bytes
    manifest["download_wall_seconds"] = round(time.perf_counter() - started, 4)
    return manifest


def verify(cfg: TpchConfig, manifest: dict) -> None:
    if manifest["schema"] != cfg.schema or manifest["sf"] != cfg.sf:
        raise ValueError("manifest does not describe this TPC-H dataset")
    catalog = auth.catalog(cfg)
    for name in cfg.TABLES:
        snapshot = catalog.load_table(f"{cfg.schema}.{name}").current_snapshot()
        if snapshot is None or snapshot.snapshot_id != manifest["tables"][name]["snapshot_id"]:
            raise RuntimeError(f"Iceberg snapshot changed during diagnosis: {name}")
    scrub.safe_print("All eight source Iceberg snapshot IDs remained unchanged")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--verify-output", type=Path)
    args = parser.parse_args()
    cfg = TpchConfig.from_env()
    try:
        if args.verify:
            verify(cfg, json.loads(args.verify.read_text(encoding="utf-8")))
            if args.verify_output:
                args.verify_output.write_text('{"unchanged": true}\n', encoding="utf-8")
            return 0
        if args.directory is None or args.manifest is None:
            parser.error("staging requires directory and manifest output paths")
        manifest = stage(cfg, args.directory)
        output = args.manifest
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        file_count = sum(item["files"] for item in manifest["tables"].values())
        scrub.safe_print(
            f"Staged {file_count} original Parquet files, "
            f"{manifest['total_bytes']} bytes in {manifest['download_wall_seconds']}s"
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - no raw cloud SDK traceback in public Actions
        if args.verify_output:
            args.verify_output.write_text(
                json.dumps({"unchanged": False, "error": scrub.scrub_exc(exc, 600)}) + "\n",
                encoding="utf-8",
            )
        scrub.safe_print(f"::error::Parquet staging: {scrub.scrub_exc(exc, 600)}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
