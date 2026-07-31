#!/usr/bin/env bash
# Phase 8: the full scrub over everything the campaign has written.
#
# ADR 0009: "daemon stopped, qss-storage-fsck --scrub over the fully
# populated store: zero corruption findings, throughput recorded." The
# throughput is the ADR 0005 sizing estimate's reality check at real NVMe
# speed -- recorded, never asserted, because a number nobody has measured
# yet is not a pass criterion.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 08 scrub

s3d_ensure_stopped
respd_ensure_stopped

store_bytes=$(qssrt_block_bytes "$QSSRT_S3_STORE")
store_files=$(qssrt_block_file_count "$QSSRT_S3_STORE")
record "store-block-bytes" "$(qssrt_human "$store_bytes")"
record "store-block-files" "$store_files"

start=$(date +%s)
fsck_run scrub --scrub
elapsed=$(($(date +%s) - start))
record "scrub-wall-seconds" "$elapsed"
[ "$elapsed" -gt 0 ] &&
    record "scrub-MBps" $((store_bytes / elapsed / 1048576))

# Zero corruption: no corrupt_block, no size_mismatch, nothing WARN or
# CRITICAL. The multipart classes phases 2 and 3 left in flight are INFO
# and are not corruption.
corrupt=$(fsck_count corrupt_block)
mismatch=$(fsck_count size_mismatch)
if [ "${corrupt:-0}" = 0 ] && [ "${mismatch:-0}" = 0 ]; then
    check_pass "the scrub finds zero corruption" "$(fsck_summary)"
else
    check_fail "the scrub finds zero corruption" \
        "corrupt_block=$corrupt size_mismatch=$mismatch; $(fsck_findings_brief)"
fi
fsck_assert_leak_only "everything the scrub found is leak-class"

phase_end
exit $?
