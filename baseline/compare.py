#!/usr/bin/env python3
import difflib
import json
import sys


def compared_lines(path):
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    return json.dumps(doc["compared"], indent=1, sort_keys=True, ensure_ascii=False).splitlines()


def main(argv):
    if len(argv) != 3:
        print("usage: compare.py A.json B.json", file=sys.stderr)
        return 2
    a, b = argv[1], argv[2]
    ja = compared_lines(a)
    jb = compared_lines(b)
    diff = list(difflib.unified_diff(ja, jb, a, b, lineterm=""))
    print("\n".join(diff) if diff else "IDENTICAL (compared section, %d lines)" % len(ja))
    return 1 if diff else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
