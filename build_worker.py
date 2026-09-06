"""
Build a store in a SEPARATE PROCESS so the CPU-bound parse never starves the web
worker (Python's GIL would otherwise freeze status polling + health checks during
a multi-minute parse). Writes progress.json as it goes; error.txt on failure.

Usage:  python build_worker.py <src.mid> <out_dir> <CODE>
"""
import sys
import os
import json

import store


def main():
    src, out_dir, code = sys.argv[1], sys.argv[2], sys.argv[3]
    os.makedirs(out_dir, exist_ok=True)
    pf = os.path.join(out_dir, "progress.json")

    def prog(phase, n):
        try:
            with open(pf, "w") as f:
                json.dump({"phase": phase, "notes": n}, f)
        except OSError:
            pass

    try:
        prog("parsing", 0)
        store.build_store(src, out_dir, code, progress=prog)
        prog("ready", -1)
    except Exception as e:  # noqa: BLE001 — surface any failure to the parent
        try:
            with open(os.path.join(out_dir, "error.txt"), "w") as f:
                f.write(str(e))
        except OSError:
            pass
        prog("error", 0)
        sys.exit(1)


if __name__ == "__main__":
    main()
