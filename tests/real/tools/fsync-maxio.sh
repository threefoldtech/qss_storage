#!/usr/bin/env bash
# Parallel fsync ingest on /s3: the ADR 0010 acceptance number at K
# streams. Same shape as buffer-maxio.sh minus the crash leg.
# usage: fsync-maxio.sh <output-dir>
set -u

ROOT=/home/delandtj/prppl/qss_storage
TOML="${QSSMX_TOML:-$ROOT/tests/real/qss_storage-realtest.toml}"
BIN="${QSSMX_BIN:-$ROOT/target/release}"
MOUNT=/s3
STORE_ROOT=$MOUNT/qss-realtest
S3_STORE=$STORE_ROOT/s3
OUT="${1:?usage: fsync-maxio.sh <output-dir>}"
EP="--endpoint-url http://127.0.0.1:18014"

export AWS_ACCESS_KEY_ID=qssrealtest AWS_SECRET_ACCESS_KEY=qssrealtestsecret
export AWS_DEFAULT_REGION=us-east-1
export AWS_CONFIG_FILE="$OUT/aws-config"
export QSSRT_SEED=fsync-maxio

mkdir -p "$OUT"
exec >>"$OUT/run.log" 2>&1

# shellcheck source=tests/real/lib/gen.sh
. "$ROOT/tests/real/lib/gen.sh"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() {
    log "FAIL: $*"
    printf 'FAIL: %s\n' "$*" >"$OUT/VERDICT"
    touch "$OUT/DONE"
    exit 2
}

DPID=""
IOPID=""
cleanup() {
    [ -n "$IOPID" ] && kill "$IOPID" 2>/dev/null
    [ -n "$DPID" ] && kill "$DPID" 2>/dev/null
}
trap cleanup EXIT

cat >"$AWS_CONFIG_FILE" <<'EOF'
[default]
s3 =
  multipart_chunksize = 64MB
  max_concurrent_requests = 12
EOF

[ "$(findmnt -no FSTYPE $MOUNT)" = "${QSSMX_FSTYPE:-xfs}" ] || fail "$MOUNT is not ${QSSMX_FSTYPE:-xfs}"
[ -f "$MOUNT/.qss-realtest" ] || fail "no sentinel"
(echo >/dev/tcp/127.0.0.1/18014) 2>/dev/null && fail "port 18014 busy"

for entry in "$STORE_ROOT"/*; do
    [ -e "$entry" ] || continue
    case "$(basename "$entry")" in
    s3 | resp) ;;
    *) fail "unrecognised entry $entry" ;;
    esac
done
if [ -e "$S3_STORE" ]; then
    "$BIN/qss-storage-fsck" --config "$TOML" \
        --meta-root "$S3_STORE" --fs-root "$S3_STORE" >/dev/null 2>&1
    rc=$?
    [ "$rc" -ne 3 ] || fail "fsck could not open $S3_STORE; refusing to wipe"
    log "fsck confirmed a qss store (exit $rc); wiping"
    rm -rf "${STORE_ROOT:?}/s3" "${STORE_ROOT:?}/resp" || fail "wipe"
else
    log "no store at $S3_STORE; virgin mount, nothing to wipe"
fi
mkdir -p "$S3_STORE"

"$BIN/s3cas" server --config "$TOML" \
    --fs-root "$S3_STORE" --meta-root "$S3_STORE" \
    --durability fsync >>"$OUT/daemon.log" 2>&1 &
DPID=$!
for _ in $(seq 1 200); do
    (echo >/dev/tcp/127.0.0.1/18014) 2>/dev/null && break
    kill -0 "$DPID" 2>/dev/null || fail "daemon died on start"
    sleep 0.1
done
aws $EP s3api create-bucket --bucket maxio >/dev/null 2>&1

iostat -dxk 2 >"$OUT/io.log" 2>&1 &
IOPID=$!

K=6
GIB=16
BYTES=$((GIB * 1024 * 1024 * 1024))
log "leg: $K x ${GIB}GiB parallel multipart at fsync durability"
t0=$(date +%s.%N)
pids=()
for i in $(seq 1 "$K"); do
    (
        gen_stream "giant-$i" "$BYTES" |
            timeout 3600 aws $EP s3 cp - "s3://maxio/giant-$i" \
                --expected-size "$BYTES" --quiet
        echo $? >"$OUT/leg-$i.rc"
    ) &
    pids+=($!)
done
for p in "${pids[@]}"; do wait "$p"; done
t1=$(date +%s.%N)
for i in $(seq 1 "$K"); do
    [ "$(cat "$OUT/leg-$i.rc")" = 0 ] || fail "worker $i failed"
done
SECS=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
MBS=$(awk -v b="$((K * BYTES))" -v s="$SECS" 'BEGIN{printf "%.0f", b/1048576/s}')
log "leg: $((K * GIB)) GiB in ${SECS}s = ${MBS} MiB/s aggregate"

kill "$IOPID" 2>/dev/null
IOPID=""
kill -TERM "$DPID" 2>/dev/null
while kill -0 "$DPID" 2>/dev/null; do sleep 0.2; done
DPID=""

"$BIN/qss-storage-fsck" --config "$TOML" \
    --meta-root "$S3_STORE" --fs-root "$S3_STORE" \
    >"$OUT/fsck.txt" 2>&1
FSCK=$?
log "fsck: exit $FSCK -- $(tail -n 1 "$OUT/fsck.txt")"

{
    printf 'fsync leg: %s GiB in %ss = %s MiB/s aggregate (K=%s)\n' \
        "$((K * GIB))" "$SECS" "$MBS" "$K"
    printf 'fsck: exit %s\n' "$FSCK"
} >"$OUT/VERDICT"
touch "$OUT/DONE"
log "done"
