#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "environment/hidden/internal/api_surface.py"
TARGETS = [
    ROOT / "solution/api_surface.py",
    ROOT / "tests/api_surface.py",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync api_surface.py mirrors from the canonical hidden copy.")
    parser.add_argument("--check", action="store_true", help="Fail if any mirror differs from the canonical copy.")
    args = parser.parse_args()

    source_text = SOURCE.read_text(encoding="utf-8")
    drifted = [path for path in TARGETS if path.read_text(encoding="utf-8") != source_text]

    if args.check:
        if not drifted:
            print("api_surface mirrors are in sync")
            return 0
        for path in drifted:
            print(f"out of sync: {path.relative_to(ROOT)}")
        return 1

    for path in TARGETS:
        path.write_text(source_text, encoding="utf-8")
        print(f"synced {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
