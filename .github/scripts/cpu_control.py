"""A quiet, fixed-size CPU-only control before and after the I/O experiment."""

from __future__ import annotations

import hashlib
import json
import resource
import sys
import time
from pathlib import Path


def control(repeats: int = 1024) -> dict:
    block = bytes(range(256)) * 8192  # 2 MiB, fits in memory
    before = resource.getrusage(resource.RUSAGE_SELF)
    start = time.perf_counter()
    digest = hashlib.sha256()
    for _ in range(repeats):
        digest.update(block)
    wall = time.perf_counter() - start
    after = resource.getrusage(resource.RUSAGE_SELF)
    cpu = (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime)
    return {
        "bytes_hashed": len(block) * repeats,
        "sha256": digest.hexdigest(),
        "wall_seconds": round(wall, 5),
        "cpu_seconds": round(cpu, 5),
        "cpu_over_wall": round(cpu / wall, 4),
    }


if __name__ == "__main__":
    path = Path(sys.argv[1])
    path.parent.mkdir(parents=True, exist_ok=True)
    result = control()
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"CPU control {result['bytes_hashed']} bytes: {result['wall_seconds']}s")
