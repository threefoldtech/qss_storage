#!/usr/bin/env bash
# Phase 2: the multipart lifecycle, client-driven and hand-driven.
#
# ADR 0009: "aws s3 cp at sizes forcing client-chosen multipart; manual
# s3api create/upload-part/complete with out-of-order parts; multipart ETag
# verified against the MD5-of-MD5s convention; abort mid-upload and verify
# listings show nothing; list-multipart-uploads / list-parts against
# concurrent uploads; the retryable-failed-complete behavior (name a missing
# part, then correct it)."
#
# The part size is pinned in the campaign's own AWS config rather than left
# to the tool's default, so a chunk-size change in a future aws-cli cannot
# silently reshape this test (ADR 0009's second known unknown). The tool
# version is recorded beside every measurement for the same reason.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 02 multipart
s3d_ensure_running || {
    check_fail "the daemon is up" "it would not start"
    phase_end
    exit $?
}

# Provoked on purpose below: a complete that names a part nobody uploaded,
# and an abort. The daemon logs both at ERROR, and the gate would otherwise
# read them as a daemon-side failure.
daemon_expect 'Missing part .* in multipart upload'
daemon_expect 'Part .* vanished between validation and claim'

BUCKET="$QSSRT_MP_BUCKET"
s3_ensure_bucket "$BUCKET"
record "aws-cli" "$("$QSSRT_AWS" --version 2>&1 | head -n 1)"
record "pinned-multipart-chunksize" "$QSSRT_MULTIPART_CHUNKSIZE"
record "pinned-multipart-threshold" "$QSSRT_MULTIPART_THRESHOLD"

# --- client-driven multipart -------------------------------------------

size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_MULTIPART_BYTES")" \
    "$(qssrt_bytes "$QSSRT_MULTIPART_FLOOR")")
key="client/multipart"
body="$(qssrt_scratch)/mp-body"
gen_file "$key" "$size" "$body"

# The concurrent-upload number: aws-cli drives QSSRT_S3_CONCURRENCY parts
# at once at the pinned chunk size, so this window is the closest thing the
# campaign has to a "how fast can one client fill this store" figure.
start=$(date +%s)
perf_begin "s3-multipart-client"
assert_ok "aws s3 cp uploads $(qssrt_human "$size") as client-driven multipart" \
    s3cmd cp --quiet "$body" "s3://$BUCKET/$key"
perf_end "s3-multipart-client" 1 "$size" \
    "$QSSRT_S3_CONCURRENCY concurrent parts of $QSSRT_MULTIPART_CHUNKSIZE"
elapsed=$(($(date +%s) - start))
[ "$elapsed" -gt 0 ] && record "client-multipart-MBps" $((size / elapsed / 1048576))
rm -f "$body"

assert_eq "the completed object has the size it was given" "$size" \
    "$(s3_size "$BUCKET" "$key")"
assert_same_as_generated "the completed object is byte-identical" \
    "$BUCKET" "$key" "$size"

# The S3 convention: MD5 of the concatenated part MD5s, then "-<count>".
# Computed from the generator, at the part size we pinned, so this checks
# the server's arithmetic and not just its self-consistency.
want_etag=$(gen_multipart_etag "$key" "$size" \
    "$(qssrt_bytes "$(printf '%s' "$QSSRT_MULTIPART_CHUNKSIZE" | sed 's/MB/MiB/')")")
got_etag=$(s3_etag_via_get "$BUCKET" "$key")
assert_eq "the multipart ETag is the MD5 of the part MD5s, with the part count" \
    "$want_etag" "$got_etag"

# --- hand-driven multipart, parts uploaded out of order -----------------

part=$(qssrt_bytes "$QSSRT_MANUAL_PART_BYTES")
mkey="manual/three-parts"
total=$((part * 2 + part / 2))
gen_file "$mkey" "$total" "$(qssrt_scratch)/manual-body"
split -b "$part" -d "$(qssrt_scratch)/manual-body" "$(qssrt_scratch)/manual-part-"

upload_id=$(s3api create-multipart-upload --bucket "$BUCKET" --key "$mkey" \
    --query UploadId --output text)
if [ -n "$upload_id" ] && [ "$upload_id" != None ]; then
    check_pass "create-multipart-upload returns an upload id"
else
    check_fail "create-multipart-upload returns an upload id" "got [$upload_id]"
fi

# Uploaded 3, 1, 2: the order parts arrive in is the client's business.
etags=()
for n in 3 1 2; do
    file="$(qssrt_scratch)/manual-part-0$((n - 1))"
    tag=$(s3api upload-part --bucket "$BUCKET" --key "$mkey" \
        --upload-id "$upload_id" --part-number "$n" --body "$file" \
        --query ETag --output text 2>/dev/null | tr -d '"')
    etags[$n]="$tag"
    if [ -n "$tag" ]; then
        check_pass "upload-part $n (out of order)" "$tag"
    else
        check_fail "upload-part $n (out of order)" "no ETag returned"
    fi
    # The part ETag is the part's own MD5, which the client can check.
    assert_eq "part $n ETag is the part body MD5" \
        "$(md5sum "$file" | cut -d' ' -f1)" "$tag"
done

listed=$(s3api list-parts --bucket "$BUCKET" --key "$mkey" --upload-id "$upload_id" \
    --query 'Parts[].PartNumber' --output text 2>/dev/null | tr '\t' ' ')
assert_eq "list-parts returns the parts in ascending order" "1 2 3" "$listed"

if s3api list-multipart-uploads --bucket "$BUCKET" \
    --query 'Uploads[].UploadId' --output text 2>/dev/null |
    tr '\t' '\n' | grep -qx "$upload_id"; then
    check_pass "list-multipart-uploads shows the upload in flight"
else
    check_fail "list-multipart-uploads shows the upload in flight" \
        "$(s3api list-multipart-uploads --bucket "$BUCKET" --output json 2>&1 | head -c 300)"
fi

# The retryable failed complete: name a part that was never uploaded. The
# upload must survive so a corrected complete still works -- that is what
# makes the error retryable rather than fatal (ADR 0003).
bad_parts=$(mktemp)
cat >"$bad_parts" <<EOF
{"Parts":[{"ETag":"${etags[1]}","PartNumber":1},{"ETag":"${etags[2]}","PartNumber":2},
{"ETag":"${etags[3]}","PartNumber":3},{"ETag":"${etags[3]}","PartNumber":4}]}
EOF
assert_err "a complete naming a part nobody uploaded is refused" "" \
    s3api complete-multipart-upload --bucket "$BUCKET" --key "$mkey" \
    --upload-id "$upload_id" --multipart-upload "file://$bad_parts"
rm -f "$bad_parts"

if s3api list-multipart-uploads --bucket "$BUCKET" \
    --query 'Uploads[].UploadId' --output text 2>/dev/null |
    tr '\t' '\n' | grep -qx "$upload_id"; then
    check_pass "the upload survives a failed complete"
else
    check_fail "the upload survives a failed complete" \
        "it is gone: a client cannot correct and retry"
fi

good_parts=$(mktemp)
cat >"$good_parts" <<EOF
{"Parts":[{"ETag":"${etags[1]}","PartNumber":1},{"ETag":"${etags[2]}","PartNumber":2},
{"ETag":"${etags[3]}","PartNumber":3}]}
EOF
assert_ok "the corrected complete succeeds" \
    s3api complete-multipart-upload --bucket "$BUCKET" --key "$mkey" \
    --upload-id "$upload_id" --multipart-upload "file://$good_parts"
rm -f "$good_parts"

assert_eq "the hand-built object has the size of its parts" "$total" \
    "$(s3_size "$BUCKET" "$mkey")"
assert_same_as_generated "the hand-built object is byte-identical" \
    "$BUCKET" "$mkey" "$total"
assert_eq "its ETag follows the multipart convention" \
    "$(gen_multipart_etag "$mkey" "$total" "$part")" \
    "$(s3_etag_via_get "$BUCKET" "$mkey")"

assert_err "completing an upload twice is NoSuchUpload" NoSuchUpload \
    s3api complete-multipart-upload --bucket "$BUCKET" --key "$mkey" \
    --upload-id "$upload_id" --multipart-upload "{\"Parts\":[{\"ETag\":\"${etags[1]}\",\"PartNumber\":1}]}"

rm -f "$(qssrt_scratch)"/manual-part-* "$(qssrt_scratch)/manual-body"

# --- abort -------------------------------------------------------------

akey="manual/aborted"
abort_id=$(s3api create-multipart-upload --bucket "$BUCKET" --key "$akey" \
    --query UploadId --output text)
gen_file "$akey" "$part" "$(qssrt_scratch)/abort-part"
assert_ok "upload a part, then abandon it" \
    s3api upload-part --bucket "$BUCKET" --key "$akey" --upload-id "$abort_id" \
    --part-number 1 --body "$(qssrt_scratch)/abort-part"
rm -f "$(qssrt_scratch)/abort-part"

before_abort=$(qssrt_block_file_count "$QSSRT_S3_STORE")
assert_ok "abort-multipart-upload" \
    s3api abort-multipart-upload --bucket "$BUCKET" --key "$akey" --upload-id "$abort_id"
after_abort=$(qssrt_block_file_count "$QSSRT_S3_STORE")
record "block-files-released-by-abort" $((before_abort - after_abort))

if s3api list-multipart-uploads --bucket "$BUCKET" \
    --query 'Uploads[].UploadId' --output text 2>/dev/null |
    tr '\t' '\n' | grep -qx "$abort_id"; then
    check_fail "an aborted upload disappears from list-multipart-uploads" \
        "$abort_id is still listed"
else
    check_pass "an aborted upload disappears from list-multipart-uploads"
fi

if s3_exists "$BUCKET" "$akey"; then
    check_fail "an aborted upload leaves no object" "$akey exists"
else
    check_pass "an aborted upload leaves no object"
fi

# The abort released what the part held: post-ADR 0003 the reaping is
# record-first, blocks-second, and the blocks go with it.
if [ "$after_abort" -le "$before_abort" ]; then
    check_pass "the abort released the part's blocks" \
        "$before_abort -> $after_abort block files"
else
    check_fail "the abort released the part's blocks" \
        "$before_abort -> $after_abort block files"
fi

assert_err "a part upload against an aborted id is NoSuchUpload" NoSuchUpload \
    s3api upload-part --bucket "$BUCKET" --key "$akey" --upload-id "$abort_id" \
    --part-number 1 --body "$QSSRT_DAEMON_CONFIG"

# --- concurrent uploads on one bucket ----------------------------------

ids=()
for n in 1 2 3; do
    ids+=("$(s3api create-multipart-upload --bucket "$BUCKET" \
        --key "concurrent/$n" --query UploadId --output text)")
done
listed=$(s3_uploads_in_flight "$BUCKET")
if [ "$listed" -ge 3 ]; then
    check_pass "list-multipart-uploads sees concurrent uploads" "$listed in flight"
else
    check_fail "list-multipart-uploads sees concurrent uploads" "$listed in flight, expected at least 3"
fi
# Left in flight on purpose: phase 3 needs abandoned uploads to look at.
record "uploads-left-in-flight-for-phase-3" "${#ids[@]}"

phase_end
exit $?
