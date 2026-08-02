#!/usr/bin/env bash
# Phase 9: teardown through the clients, and the reconciliation to empty.
#
# ADR 0009: "delete the entire dataset through the clients (S3 deletes,
# respd deletes, bucket removal); final fsck: the store should reconcile to
# empty (or enumerate exactly what remains and why); record the
# reclaimed-space curve."
#
# Post-0008 the expectation is absolute. Every abort releases, every delete
# releases, every overwrite already released -- so after the last bucket
# goes, the store holds zero live block files and fsck has nothing to say.
# Anything left is enumerated with its class, which is the "exactly what
# remains and why".

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 09 teardown
s3d_ensure_running || {
    check_fail "the daemon is up" "it would not start"
    phase_end
    exit $?
}

# --- the reclaim sampler ------------------------------------------------

curve="$QSSRT_PHASE_DIR/reclaim-curve.tsv"
printf 'epoch\tblock_bytes\tblock_files\n' >"$curve"
(
    while :; do
        printf '%s\t%s\t%s\n' "$(date +%s)" \
            "$(qssrt_block_bytes "$QSSRT_S3_STORE")" \
            "$(qssrt_block_file_count "$QSSRT_S3_STORE")" >>"$curve"
        sleep 5
    done
) &
sampler_pid=$!

bytes_start=$(qssrt_block_bytes "$QSSRT_S3_STORE")
record "blocks-before-teardown" "$(qssrt_human "$bytes_start")"

# --- S3: abort what is in flight, then delete everything ----------------

# The uploads phases 2 and 3 deliberately left in flight go first, through
# the client, exactly as an operator would clear them.
buckets=$(s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null |
    tr '\t' '\n' | grep -v '^$')
record "buckets-at-teardown" "$(qssrt_oneline "$buckets")"

for bucket in $buckets; do
    while IFS=$'\t' read -r key id; do
        [ -n "$id" ] || continue
        s3api abort-multipart-upload --bucket "$bucket" --key "$key" \
            --upload-id "$id" >/dev/null 2>&1
    done < <(s3api list-multipart-uploads --bucket "$bucket" \
        --query 'Uploads[].[Key,UploadId]' --output text 2>/dev/null)

    left=$(s3_uploads_in_flight "$bucket")
    assert_eq "no upload is left in flight in $bucket" 0 "${left:-0}"

    assert_ok "every object in $bucket deletes through the client" \
        s3_rm_prefix "$bucket" ""
    assert_ok "the emptied bucket $bucket deletes" \
        s3api delete-bucket --bucket "$bucket"
done

remaining=$(s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null |
    tr '\t' '\n' | grep -c . || true)
assert_eq "list-buckets is empty after teardown" 0 "${remaining:-0}"

# --- respcas: flush its namespace through its client ----------------------

if qssrt_have "$QSSRT_VALKEY_CLI"; then
    respcas_ensure_running || check_find "respcas is up for its teardown" \
        "it would not start; its store is untouched"
    if respcas_running || respcas_adopt; then
        vk_ns "$QSSRT_RESP_NAMESPACE" FLUSH >/dev/null 2>&1
        after_flush=$(vk_ns "$QSSRT_RESP_NAMESPACE" DBSIZE 2>/dev/null | tail -n 1)
        assert_eq_find "respcas's namespace is empty after FLUSH" 0 \
            "$(printf '%s' "$after_flush" | grep -oE '[0-9]+' | head -n 1)"
        respcas_ensure_stopped
    fi
else
    check_skip "respcas's namespace is flushed" "$QSSRT_VALKEY_CLI is not installed"
fi

kill "$sampler_pid" 2>/dev/null
wait "$sampler_pid" 2>/dev/null

bytes_end=$(qssrt_block_bytes "$QSSRT_S3_STORE")
files_end=$(qssrt_block_file_count "$QSSRT_S3_STORE")
record "blocks-after-teardown" "$(qssrt_human "$bytes_end")"
record "blocks-reclaimed" "$(qssrt_human $((bytes_start - bytes_end)))"

assert_eq "zero live block files after teardown" 0 "$files_end"

# --- the reconciliation -------------------------------------------------

# The store is empty; fsck must agree, absolutely. What remains, if
# anything, is enumerated by class in the evidence -- that enumeration is
# the ADR's fallback contract, and the failure it grades is real either
# way.
fsck_run final
fsck_assert_clean "the final fsck reconciles the store to empty"

phase_end
exit $?
