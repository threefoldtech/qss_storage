#!/usr/bin/env bash
# The open buffer-durability investigation's rig (campaign findings
# buffer-3/mp-94 and buffer-3/mp-61: acked CompleteMultipartUpload,
# RECORD ABSENT after kill -9 at buffer durability).
#
# Loop: start s3cas at buffer durability, hammer it with four
# concurrent PUT workers, do one multipart upload, kill -9 the instant
# complete returns, restart, HEAD the key. Score of 2026-08-01: 92
# kills, 0 losses -- the naive window does not reproduce it, which is
# itself evidence (prime suspect: fjall journal rotation under load;
# next lever is the campaign's crash rig at elevated cycle count).
#
#   tests/real/tools/buffer-loss-repro.sh [iterations]
#
# Owns ports 18034/19140/16399 (tools/bench.toml) and a scratch store
# under target/buffer-loss-repro. Do not run alongside the bench rig.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
B="$ROOT/target/buffer-loss-repro"
BIN="$ROOT/target/release/s3cas"
export AWS_ACCESS_KEY_ID=qssrealtest AWS_SECRET_ACCESS_KEY=qssrealtestsecret
export AWS_DEFAULT_REGION=us-east-1
EP="--endpoint-url http://127.0.0.1:18034"
ITER=${1:-20}

start() {
    "$BIN" server --config "$HERE/bench.toml" --fs-root "$B/store" \
        --meta-root "$B/store" --durability buffer \
        >>"$B/daemon.log" 2>&1 &
    DPID=$!
    for _ in $(seq 1 100); do
        curl -s -o /dev/null http://127.0.0.1:18034 2>/dev/null && return 0
        kill -0 "$DPID" 2>/dev/null || return 1
        sleep 0.1
    done
    return 1
}

hammer() { # continuous 1 MiB PUTs until killed
    local n=0 f="$B/hammer-$BASHPID"
    head -c 1048576 /dev/urandom >"$f"
    while :; do
        aws $EP s3api put-object --bucket repro --key "load/$BASHPID/$n" \
            --body "$f" >/dev/null 2>&1
        n=$((n + 1))
    done
}

mkdir -p "$B"
rm -rf "$B/store"
part="$B/part.bin"
head -c $((6 * 1024 * 1024)) /dev/urandom >"$part"
lost=0 ok=0 failed=0

for i in $(seq 1 "$ITER"); do
    start || { echo "iter $i: daemon would not start"; exit 1; }
    aws $EP s3api create-bucket --bucket repro >/dev/null 2>&1
    hpids=()
    for _ in 1 2 3 4; do hammer & hpids+=($!); done
    sleep 1
    key="mp-$i"
    uid=$(aws $EP s3api create-multipart-upload --bucket repro --key "$key" \
        --query UploadId --output text 2>/dev/null)
    etag=$(aws $EP s3api upload-part --bucket repro --key "$key" \
        --upload-id "$uid" --part-number 1 --body "$part" \
        --query ETag --output text 2>/dev/null)
    if aws $EP s3api complete-multipart-upload --bucket repro --key "$key" \
        --upload-id "$uid" \
        --multipart-upload "Parts=[{ETag=$etag,PartNumber=1}]" \
        >/dev/null 2>&1; then
        kill -9 "$DPID"
        for p in "${hpids[@]}"; do kill -9 "$p" 2>/dev/null; done
        while kill -0 "$DPID" 2>/dev/null; do sleep 0.05; done
        start || { echo "iter $i: no restart"; exit 1; }
        if aws $EP s3api head-object --bucket repro --key "$key" \
            >/dev/null 2>&1; then
            ok=$((ok + 1))
        else
            lost=$((lost + 1))
            echo "iter $i: RECORD ABSENT after acked complete"
        fi
    else
        failed=$((failed + 1))
        for p in "${hpids[@]}"; do kill -9 "$p" 2>/dev/null; done
        echo "iter $i: complete itself failed (not counted)"
    fi
    kill -TERM "$DPID" 2>/dev/null
    while kill -0 "$DPID" 2>/dev/null; do sleep 0.05; done
    rm -f "$B"/hammer-*
    wait 2>/dev/null
done

echo "RESULT: $ok survived, $lost LOST, $failed complete-failures, of $ITER"
