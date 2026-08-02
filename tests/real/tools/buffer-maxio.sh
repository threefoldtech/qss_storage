#!/usr/bin/env bash
# Buffer-mode max-IO run with an fsck safety verdict (post-ADR-0010).
#
# From scratch on the campaign disk: wipe the campaign's own store (after
# fsck confirms it is a qss store, same condition as the harness's --fresh
# rail), run s3cas at --durability buffer, and then:
#
#   leg A: K parallel multipart ingests flat out -- the max-IO number.
#   leg B: the same storm, kill -9 mid-flight, restart -- the residue.
#   verdict: qss-storage-fsck, plain and --scrub, on the result.
#
# Buffer durability promises nothing about acked writes across a kill; what
# it must still guarantee is a store fsck can open and classify with no
# critical findings. That is the bar this run grades.
#
# usage: buffer-maxio.sh <output-dir>
set -u

ROOT=/home/delandtj/prppl/qss_storage
TOML="${QSSMX_TOML:-$ROOT/tests/real/qss_storage-realtest.toml}"
BIN="${QSSMX_BIN:-$ROOT/target/release}"
MOUNT=/s3
STORE_ROOT=$MOUNT/qss-realtest
S3_STORE=$STORE_ROOT/s3
OUT="${1:?usage: buffer-maxio.sh <output-dir>}"
EP="--endpoint-url http://127.0.0.1:18014"

export AWS_ACCESS_KEY_ID=qssrealtest AWS_SECRET_ACCESS_KEY=qssrealtestsecret
export AWS_DEFAULT_REGION=us-east-1
export AWS_CONFIG_FILE="$OUT/aws-config"
export QSSRT_SEED=buffer-maxio

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

# --- rails, the harness's own conditions -------------------------------

[ "$(findmnt -no FSTYPE $MOUNT)" = xfs ] || fail "$MOUNT is not xfs"
[ -f "$MOUNT/.qss-realtest" ] || fail "no sentinel at $MOUNT/.qss-realtest"
[ -x "$BIN/s3cas" ] || fail "no release s3cas"
[ -x "$BIN/qss-storage-fsck" ] || fail "no release qss-storage-fsck"
(echo >/dev/tcp/127.0.0.1/18014) 2>/dev/null && fail "port 18014 is busy"
DEV=$(basename "$(findmnt -no SOURCE $MOUNT)")

for entry in "$STORE_ROOT"/*; do
    [ -e "$entry" ] || continue
    case "$(basename "$entry")" in
    s3 | resp) ;;
    *) fail "unrecognised entry $entry in the store root; refusing to wipe" ;;
    esac
done

# Wipe only after fsck confirms this is a qss store (exit 3 = could not
# open, anything else means fsck had an opinion about a real store).
"$BIN/qss-storage-fsck" --config "$TOML" \
    --meta-root "$S3_STORE" --fs-root "$S3_STORE" >/dev/null 2>&1
rc=$?
[ "$rc" -ne 3 ] || fail "fsck could not open $S3_STORE; refusing to wipe"
log "fsck confirmed a qss store (exit $rc); wiping the tb fill"
t0=$(date +%s)
rm -rf "${STORE_ROOT:?}/s3" "${STORE_ROOT:?}/resp" || fail "wipe failed"
WIPE_S=$(($(date +%s) - t0))
log "wipe took ${WIPE_S}s"
mkdir -p "$S3_STORE"

# --- the daemon, at buffer ---------------------------------------------

start_daemon() {
    "$BIN/s3cas" server --config "$TOML" \
        --fs-root "$S3_STORE" --meta-root "$S3_STORE" \
        --durability buffer >>"$OUT/daemon.log" 2>&1 &
    DPID=$!
    for _ in $(seq 1 200); do
        (echo >/dev/tcp/127.0.0.1/18014) 2>/dev/null && return 0
        kill -0 "$DPID" 2>/dev/null || return 1
        sleep 0.1
    done
    return 1
}
stop_daemon() {
    kill -TERM "$DPID" 2>/dev/null
    while kill -0 "$DPID" 2>/dev/null; do sleep 0.2; done
    DPID=""
}

start_daemon || fail "daemon would not start (see daemon.log)"
aws $EP s3api create-bucket --bucket maxio >/dev/null 2>&1

iostat -dxk 2 >"$OUT/io.log" 2>&1 &
IOPID=$!

# --- leg A: max IO ------------------------------------------------------

K=6
GIB=16
BYTES=$((GIB * 1024 * 1024 * 1024))
log "leg A: $K x ${GIB}GiB parallel multipart ingest at buffer durability"
t0=$(date +%s.%N)
pids=()
for i in $(seq 1 "$K"); do
    (
        gen_stream "giant-$i" "$BYTES" |
            timeout 3600 aws $EP s3 cp - "s3://maxio/giant-$i" \
                --expected-size "$BYTES" --quiet
        echo $? >"$OUT/legA-$i.rc"
    ) &
    pids+=($!)
done
for p in "${pids[@]}"; do wait "$p"; done
t1=$(date +%s.%N)
for i in $(seq 1 "$K"); do
    [ "$(cat "$OUT/legA-$i.rc")" = 0 ] || fail "leg A worker $i failed"
done
LEGA_MBS=$(awk -v b="$((K * BYTES))" -v s="$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')" \
    'BEGIN{printf "%.0f", b/1048576/s}')
LEGA_S=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
log "leg A: $((K * GIB)) GiB in ${LEGA_S}s = ${LEGA_MBS} MiB/s aggregate"

# --- leg B: kill -9 mid-storm ------------------------------------------

log "leg B: 4-stream storm, kill -9 at T+25s"
pids=()
for i in 1 2 3 4; do
    (
        gen_stream "crash-$i" $((8 * 1024 * 1024 * 1024)) |
            aws $EP s3 cp - "s3://maxio/crash-$i" \
                --expected-size $((8 * 1024 * 1024 * 1024)) --quiet
    ) &
    pids+=($!)
done
sleep 25
kill -9 "$DPID"
for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null; done
while kill -0 "$DPID" 2>/dev/null; do sleep 0.2; done
DPID=""
# Only the workers: a bare wait would block on the iostat sampler forever.
wait "${pids[@]}" 2>/dev/null

start_daemon || fail "daemon would not restart after kill -9"
OBJS=$(aws $EP s3api list-objects-v2 --bucket maxio \
    --query 'length(Contents)' --output text 2>/dev/null)
log "after crash restart: $OBJS objects listed in maxio"
stop_daemon

kill "$IOPID" 2>/dev/null
IOPID=""

# --- the verdict: fsck -------------------------------------------------

FSCK_PLAIN=-1
FSCK_SCRUB=-1
"$BIN/qss-storage-fsck" --config "$TOML" \
    --meta-root "$S3_STORE" --fs-root "$S3_STORE" \
    >"$OUT/fsck-plain.txt" 2>"$OUT/fsck-plain.err"
FSCK_PLAIN=$?
"$BIN/qss-storage-fsck" --config "$TOML" \
    --meta-root "$S3_STORE" --fs-root "$S3_STORE" --json \
    >"$OUT/fsck-plain.json" 2>/dev/null
"$BIN/qss-storage-fsck" --config "$TOML" \
    --meta-root "$S3_STORE" --fs-root "$S3_STORE" --scrub \
    >"$OUT/fsck-scrub.txt" 2>"$OUT/fsck-scrub.err"
FSCK_SCRUB=$?
"$BIN/qss-storage-fsck" --config "$TOML" \
    --meta-root "$S3_STORE" --fs-root "$S3_STORE" --scrub --json \
    >"$OUT/fsck-scrub.json" 2>/dev/null
log "fsck plain: exit $FSCK_PLAIN -- $(tail -n 1 "$OUT/fsck-plain.txt")"
log "fsck scrub: exit $FSCK_SCRUB -- $(tail -n 1 "$OUT/fsck-scrub.txt")"

{
    printf 'wipe: %ss\n' "$WIPE_S"
    printf 'legA: %s GiB in %ss = %s MiB/s aggregate (K=%s, buffer)\n' \
        "$((K * GIB))" "$LEGA_S" "$LEGA_MBS" "$K"
    printf 'legB: kill -9 mid-storm, restart ok, %s objects listed\n' "$OBJS"
    printf 'fsck plain: exit %s\n' "$FSCK_PLAIN"
    printf 'fsck scrub: exit %s\n' "$FSCK_SCRUB"
} >"$OUT/VERDICT"
touch "$OUT/DONE"
log "done"
