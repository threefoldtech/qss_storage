#!/usr/bin/env bash
# The ADR 0010 A/B rig: one 16 GiB multipart ingest, one durability
# level, one number. Run once per level to reproduce the table that
# motivated the ADR (fsync 62 / fdatasync 75 / buffer 1025 MB/s on a
# btrfs nvme, 2026-08-01) -- and, once 0010 is implemented, to hold
# the regression floor.
#
#   tests/real/tools/durability-bench.sh fsync
#   tests/real/tools/durability-bench.sh buffer
#
# Owns ports 18034/19140/16399 (tools/bench.toml), a scratch store
# under target/durability-bench, and nothing else. Safe to run while
# a campaign owns /s3; do NOT run two of these at once.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
DUR="${1:?usage: durability-bench.sh <fsync|buffer>}"
SIZE_GIB="${2:-16}"
B="$ROOT/target/durability-bench"
BIN="$ROOT/target/release/s3cas"
export AWS_ACCESS_KEY_ID=qssrealtest AWS_SECRET_ACCESS_KEY=qssrealtestsecret
export AWS_DEFAULT_REGION=us-east-1
EP="--endpoint-url http://127.0.0.1:18034"

mkdir -p "$B"
if [ ! -f "$B/giant.bin" ] || [ "$(stat -c %s "$B/giant.bin")" != "$((SIZE_GIB * 1024 * 1024 * 1024))" ]; then
    # Deterministic, incompressible, non-dedupable: the campaign's own
    # generator, at ~GB/s.
    QSSRT_SEED=bench
    # shellcheck source=../lib/gen.sh
    . "$HERE/../lib/gen.sh"
    gen_stream bench-giant "$((SIZE_GIB * 1024 * 1024 * 1024))" >"$B/giant.bin"
fi

rm -rf "$B/store"
"$BIN" server --config "$HERE/bench.toml" --fs-root "$B/store" \
    --meta-root "$B/store" --durability "$DUR" \
    >"$B/daemon-$DUR.log" 2>&1 &
DPID=$!
for _ in $(seq 1 100); do
    curl -s -o /dev/null http://127.0.0.1:18034 && break
    kill -0 "$DPID" 2>/dev/null || { echo "daemon died, see $B/daemon-$DUR.log"; exit 1; }
    sleep 0.1
done

aws $EP s3api create-bucket --bucket bench >/dev/null 2>&1
t0=$(date +%s.%N)
aws $EP s3 cp "$B/giant.bin" s3://bench/giant --quiet
rc=$?
t1=$(date +%s.%N)
kill -TERM "$DPID"
while kill -0 "$DPID" 2>/dev/null; do sleep 0.1; done
python3 -c "print(f'$DUR: {$SIZE_GIB * 1024 / ($t1 - $t0):.0f} MB/s  ({$t1 - $t0:.1f}s, rc=$rc)')"
