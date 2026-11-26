#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

manifest=harness/Cargo.toml
export CARGO_TARGET_DIR="$PWD/harness/target"

cargo fetch --locked --manifest-path "$manifest"
info=$(cargo metadata --locked --offline --format-version 1 --manifest-path "$manifest" | python3 -c '
import json
import re
import sys

meta = json.load(sys.stdin)
pkg = next((p for p in meta["packages"] if p["name"] == "burncloud-router"), None)
source = (pkg or {}).get("source") or ""
m = re.fullmatch(r"git\+https://github\.com/burncloud/burncloud\?rev=([0-9a-f]{40})#([0-9a-f]{40})", source)
if not m or m.group(1) != m.group(2):
    sys.exit("burncloud-router 必须是钉在 40 位 SHA 上的 git 依赖，实际是: " + source)
print(m.group(1))
print(pkg["manifest_path"].rsplit("/", 1)[0])
')
old_rev=${info%%$'\n'*}
old_dir=${info#*$'\n'}

mkdir -p harness/old results
cp "$old_dir/tests/common.rs" harness/old/common.rs

if [[ "${1:-}" == "--perf" ]]; then
  n=${2:?"用法: run_twice.sh --perf N"}
  cargo build -q --locked --release --manifest-path "$manifest"
  rm -f results/perf-release.json results/perf-release.router.log
  OLD_COMMIT=$old_rev BASELINE_PERF=$n harness/target/release/old-relay-baseline \
    results/perf-release.json results/perf-release.router.log
  exit 0
fi

cargo build -q --locked --manifest-path "$manifest"
rm -f results/run1.json results/run2.json results/run1.router.log results/run2.router.log
for i in 1 2; do
  OLD_COMMIT=$old_rev harness/target/debug/old-relay-baseline "results/run$i.json" "results/run$i.router.log"
done
python3 compare.py results/run1.json results/run2.json | tee results/diff.txt
