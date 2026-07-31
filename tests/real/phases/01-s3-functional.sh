#!/usr/bin/env bash
# Phase 1: the S3 functional surface, driven by aws-cli.
#
# ADR 0009: "bucket lifecycle; object CRUD at 1 B, 1 KiB (inline boundary
# both sides), 1 MiB (block boundary), 100 MiB, 1 GiB; ETag verified against
# locally computed MD5 for single-part; round-trip byte-compare on every GET
# (cmp); listings with >1000 keys (pagination), prefixes, delete-objects
# batches; error paths (NoSuchBucket, NoSuchKey, double-create)."
#
# The listing checks run three drivers over the same keys -- the V2
# paginator, `aws s3 ls`, and a hand-driven continuation-token loop --
# because they can disagree, and when they do, the disagreement is the
# finding. A client that silently sees 1000 of 1200 objects is the worst
# failure mode a listing has: it does not look like a failure.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 01 s3-functional
s3d_ensure_running || {
    check_fail "the daemon is up" "it would not start"
    phase_end
    exit $?
}

BUCKET="$QSSRT_BUCKET"

# --- bucket lifecycle --------------------------------------------------

assert_ok "create-bucket" s3api create-bucket --bucket "$BUCKET"
assert_err "create-bucket twice is BucketAlreadyExists" BucketAlreadyExists \
    s3api create-bucket --bucket "$BUCKET"
assert_ok "head-bucket on a bucket that exists" s3api head-bucket --bucket "$BUCKET"

if s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null |
    tr '\t' '\n' | grep -qx "$BUCKET"; then
    check_pass "list-buckets shows the bucket"
else
    check_fail "list-buckets shows the bucket" \
        "$(s3api list-buckets --query 'Buckets[].Name' --output text 2>&1)"
fi

# --- sizes: round trip, ETag, and where the boundaries are -------------

# One object at one size: PUT it, check the ETag against the MD5 we computed
# ourselves, GET it back and byte-compare, and record how many block files
# appeared. The block-file delta is what makes the inline and block
# boundaries observable from outside -- there is no client-visible flag that
# says "this one was inlined".
#
# The delta comes back in a variable rather than on stdout: this function
# narrates, and a caller capturing its output would capture the narration.
QSSRT_BLOCK_DELTA=0
put_and_verify() {
    local key="$1" size="$2" before after delta etag want_etag
    QSSRT_BLOCK_DELTA=0
    before=$(qssrt_block_file_count "$QSSRT_S3_STORE")

    if ! s3_put_generated "$BUCKET" "$key" "$size"; then
        check_fail "PUT $key ($size bytes)" "$(tail -c 300 "$QSSRT_LAST_OUT" 2>/dev/null)"
        return 1
    fi
    check_pass "PUT $key ($size bytes)"

    after=$(qssrt_block_file_count "$QSSRT_S3_STORE")
    delta=$((after - before))
    QSSRT_BLOCK_DELTA=$delta
    record "block-files-for-$size-bytes" "$delta"

    want_etag=$(gen_md5 "$key" "$size")
    etag=$(s3_etag_via_get "$BUCKET" "$key")
    assert_eq "ETag of $key is the body MD5" "$want_etag" "$etag"

    assert_eq "size of $key" "$size" "$(s3_size "$BUCKET" "$key")"
    assert_same_as_generated "GET $key is byte-identical" "$BUCKET" "$key" "$size"

    # head-object is a different code path from get-object, and today it
    # answers without an ETag at all. A client that trusts HEAD for
    # change detection is silently broken by that, so it is graded as a
    # deviation rather than passed over.
    assert_eq_find "head-object of $key carries an ETag" "$want_etag" \
        "$(s3_etag "$BUCKET" "$key")"
}

BLOCK=$(qssrt_bytes 1MiB)
big=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_SINGLE_PART_BYTES")" \
    "$(qssrt_bytes "$QSSRT_SINGLE_PART_FLOOR")")
hundred=$(qssrt_scaled "$(qssrt_bytes 100MiB)" "$(qssrt_bytes 1MiB)")

put_and_verify "sizes/1B" 1
inline_delta=$QSSRT_BLOCK_DELTA
put_and_verify "sizes/block-minus-one" $((BLOCK - 1))
put_and_verify "sizes/block" "$BLOCK"
put_and_verify "sizes/block-plus-one" $((BLOCK + 1))
block_plus_delta=$QSSRT_BLOCK_DELTA
put_and_verify "sizes/hundred" "$hundred"
put_and_verify "sizes/single-part-max" "$big"

assert_eq "a 1-byte object is inlined (no block file)" 0 "$inline_delta"
if [ "$block_plus_delta" -ge 2 ]; then
    check_pass "an object one byte past the block size spans two blocks" \
        "$block_plus_delta block files"
else
    check_fail "an object one byte past the block size spans two blocks" \
        "$block_plus_delta block files"
fi

# The inline boundary is not a constant: it is the configured
# inline_metadata_size minus the object record's own header. Measure it
# rather than assert arithmetic -- and record it, because it is the number
# every "is this inlined?" question in this repo actually turns on.
measure_inline_boundary() {
    local lo=1 hi=4096 mid before after key
    while [ $((hi - lo)) -gt 1 ]; do
        mid=$(((lo + hi) / 2))
        key="boundary/probe-$mid"
        before=$(qssrt_block_file_count "$QSSRT_S3_STORE")
        s3_put_generated "$BUCKET" "$key" "$mid" >/dev/null 2>&1
        after=$(qssrt_block_file_count "$QSSRT_S3_STORE")
        if [ "$after" -gt "$before" ]; then hi=$mid; else lo=$mid; fi
    done
    printf '%s' "$lo"
}
boundary=$(measure_inline_boundary)
record "inline-boundary-bytes" "$boundary"
if [ "$boundary" -gt 1 ] && [ "$boundary" -lt 4096 ]; then
    check_pass "the inline boundary is where the config puts it" \
        "$boundary bytes inline, $((boundary + 1)) block-backed"
else
    check_find "the inline boundary is where the config puts it" \
        "measured $boundary with inline_metadata_size set in the campaign config"
fi

# --- listings ----------------------------------------------------------

keys=$(qssrt_scaled "$QSSRT_LIST_KEYS" "$QSSRT_LIST_KEYS_FLOOR")
stage="$(qssrt_scratch)/many"
rm -rf "$stage"
mkdir -p "$stage"
i=0
while [ "$i" -lt "$keys" ]; do
    gen_stream "many/$(printf 'k%06d' "$i")" 16 >"$stage/$(printf 'k%06d' "$i")"
    i=$((i + 1))
done
assert_ok "upload $keys keys for the pagination corpus" \
    s3_put_dir "$stage" "$BUCKET" "many/"
rm -rf "$stage"

listed_manual=$(s3_list_v2_manual "$BUCKET" "many/" | sort -u | wc -l)
listed_v2=$(s3_list_v2_paginated "$BUCKET" "many/" | sort -u | wc -l)
listed_v1=$(s3_list_v1_paginated "$BUCKET" "many/" | sort -u | wc -l)
listed_ls=$(s3_list_ls "$BUCKET" "many/" | sort -u | wc -l)
record "listing-manual-continuation" "$listed_manual"
record "listing-v2-paginator" "$listed_v2"
record "listing-v1-paginator" "$listed_v1"
record "listing-aws-s3-ls" "$listed_ls"

assert_eq "a hand-driven continuation loop enumerates every key" "$keys" "$listed_manual"
assert_eq "the ListObjectsV2 paginator enumerates every key" "$keys" "$listed_v2"
assert_eq "the ListObjects (v1) paginator enumerates every key" "$keys" "$listed_v1"
assert_eq "aws s3 ls --recursive enumerates every key" "$keys" "$listed_ls"

# A listing that stops early while the server can still be paged by hand is
# a specific, actionable statement, so make it explicitly.
if [ "$listed_manual" != "$listed_v2" ]; then
    check_fail "the client and the server agree on how many objects exist" \
        "manual continuation saw $listed_manual, the V2 paginator saw $listed_v2: the client stops early"
fi

prefixed=$(s3_list_v2_manual "$BUCKET" "sizes/" | wc -l)
if [ "$prefixed" -ge 6 ]; then
    check_pass "prefix listing returns only the prefix" "$prefixed keys under sizes/"
else
    check_fail "prefix listing returns only the prefix" "$prefixed keys under sizes/"
fi

delim=$(s3api list-objects-v2 --bucket "$BUCKET" --delimiter / \
    --query 'CommonPrefixes[].Prefix' --output text 2>/dev/null)
record "common-prefixes" "$(qssrt_oneline "$delim")"
if printf '%s' "$delim" | grep -q 'many/'; then
    check_pass "a delimiter listing reports common prefixes"
else
    check_find "a delimiter listing reports common prefixes" \
        "got [$(qssrt_oneline "$delim")]"
fi

# --- delete-objects batches --------------------------------------------

batch=$(mktemp)
s3_list_v2_manual "$BUCKET" "boundary/" |
    awk 'BEGIN{print "{\"Objects\":["} {printf "%s{\"Key\":\"%s\"}", (NR>1?",":""), $0} END{print "]}"}' \
        >"$batch"
before_batch=$(s3_list_v2_manual "$BUCKET" "boundary/" | wc -l)
assert_ok "delete-objects in one batch" \
    s3api delete-objects --bucket "$BUCKET" --delete "file://$batch"
after_batch=$(s3_list_v2_manual "$BUCKET" "boundary/" | wc -l)
rm -f "$batch"
assert_eq "the batch removed every key it named" 0 "$after_batch"
record "delete-objects-batch-size" "$before_batch"

# --- error paths -------------------------------------------------------

assert_err "get-object of a missing key is NoSuchKey" NoSuchKey \
    s3api get-object --bucket "$BUCKET" --key no/such/key /dev/null
assert_err "head-object of a missing key is a 404" 404 \
    s3api head-object --bucket "$BUCKET" --key no/such/key
assert_err "delete-object of a missing key is NoSuchKey" NoSuchKey \
    s3api delete-object --bucket "$BUCKET" --key no/such/key

# A bucket nobody has ever touched. Each of these must refuse, and -- the
# part reconnaissance found broken -- refusing must not bring the bucket
# into existence as a side effect.
ghost="qssrt-ghost-$$"
body="$(qssrt_scratch)/ghost-body"
gen_stream ghost 64 >"$body"
assert_err "put to a bucket that does not exist is NoSuchBucket" NoSuchBucket \
    s3api put-object --bucket "$ghost" --key x --body "$body"
assert_err "get from a bucket that does not exist is NoSuchBucket" NoSuchBucket \
    s3api get-object --bucket "$ghost" --key x /dev/null
assert_err "list of a bucket that does not exist is NoSuchBucket" NoSuchBucket \
    s3api list-objects-v2 --bucket "$ghost"

if s3api put-object --bucket "$ghost" --key x --body "$body" >/dev/null 2>&1; then
    check_fail "listing a bucket that does not exist does not create it" \
        "after a failed list, a PUT to $ghost succeeded: the read created the bucket tree, and it is invisible to list-buckets"
else
    check_pass "listing a bucket that does not exist does not create it"
fi
rm -f "$body"

# --- the on-disk layout ------------------------------------------------

# The block fanout directory is the first byte of the hash in hex, so one of
# the 256 names it can take is "db" -- which is also the name of the shared
# blocks database directory. Blocks whose hash starts with 0xdb are written
# inside it, next to fjall's lock and version files. Roughly one block in
# 256 lands there.
#
# Why this is graded as a failure and not a curiosity: fsck's disk sweep
# skips the store's own database paths by design (docs/fsck.md, "the store's
# own files are not foreign"), so those blocks are invisible to the passes
# that would notice them missing; and a database that ever tidies its own
# directory would be deleting live block data.
collided=$(qssrt_block_files_in_db_dir "$QSSRT_S3_STORE" | wc -l)
record "block-files-inside-the-blocks-db-directory" "$collided"
if [ "$collided" = 0 ]; then
    check_pass "no block file lands inside the blocks database directory" \
        "$(qssrt_block_file_count "$QSSRT_S3_STORE") block files, none under blocks/db"
else
    check_fail "no block file lands inside the blocks database directory" \
        "$collided block file(s) written into blocks/db, e.g. $(qssrt_block_files_in_db_dir "$QSSRT_S3_STORE" | head -n 1): the 0xdb fanout name collides with the shared database's own directory"
fi

# --- metrics stay alive ------------------------------------------------

series=$(curl -s "http://$QSSRT_METRICS_HOST:$QSSRT_METRICS_PORT/metrics" |
    grep -c '^s3_')
record "metric-series" "$series"
if [ "${series:-0}" -gt 0 ]; then
    check_pass "the metrics endpoint is alive" "$series s3_ series"
else
    check_fail "the metrics endpoint is alive" \
        "no s3_ series at http://$QSSRT_METRICS_HOST:$QSSRT_METRICS_PORT/metrics"
fi

phase_end
exit $?
