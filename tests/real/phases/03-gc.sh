#!/usr/bin/env bash
# Phase 3: the stale-upload garbage collector.
#
# ADR 0009 asks for: "with the short-TTL campaign config, create uploads,
# abandon them, wait out the TTL + sweep, verify reaping via
# list-multipart-uploads (empty), metrics counters
# (s3_multipart_uploads_reaped), and disk usage returning to baseline."
#
# THE ARITHMETIC THAT PHASE CANNOT SATISFY. The TTL is configured in DAYS
# (multipart.stale_ttl_days, minimum 1) and the sweep period is
# max(ttl/20, 1 hour) with the first tick one full period after startup
# (s3cas/src/main.rs). So the shortest possible wait for an observed reap is
# about 25 hours -- an order of magnitude outside the 2-4 hour budget the
# ADR sets for a default run.
#
# Adding a sub-day TTL would be a daemon change with no ADR behind it, and
# inventing one here is exactly the kind of silent scope creep a validation
# harness must not do. So this phase asserts everything that does not
# require waiting, and SKIPs the sweep observation with the arithmetic
# written out. Setting QSSRT_GC_WAIT=1 buys the real observation for an
# operator who has the day.
#
# The honest fix is a duration-typed TTL. That is an ADR-shaped decision and
# is flagged for the owner in docs/realtest.md.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 03 gc
s3d_ensure_running || {
    check_fail "the daemon is up" "it would not start"
    phase_end
    exit $?
}

BUCKET="$QSSRT_GC_BUCKET"
s3_ensure_bucket "$BUCKET"

metric() {
    curl -s "http://$QSSRT_METRICS_HOST:$QSSRT_METRICS_PORT/metrics" 2>/dev/null |
        awk -v m="$1" '$1 == m {print $2}' | tail -n 1
}

# --- the GC's configuration, from the daemon's own mouth ----------------

gc_line=$(grep -h 'stale multipart upload GC' "$(qssrt_s3_log)" 2>/dev/null |
    sed 's/\x1b\[[0-9;]*m//g' | tail -n 1)
record "gc-startup-line" "$(qssrt_oneline "$gc_line")"
if printf '%s' "$gc_line" | grep -q 'TTL 1 days'; then
    check_pass "the campaign runs the shortest TTL the daemon accepts" \
        "$(qssrt_oneline "$gc_line")"
elif printf '%s' "$gc_line" | grep -q 'disabled'; then
    check_fail "the campaign runs the shortest TTL the daemon accepts" \
        "the sweep is disabled: multipart.stale_ttl_days is 0"
else
    check_find "the campaign runs the shortest TTL the daemon accepts" \
        "$(qssrt_oneline "$gc_line")"
fi

sweep_minutes=$(printf '%s' "$gc_line" | sed -n 's/.*sweeping every \([0-9]*\) minutes.*/\1/p')
record "sweep-period-minutes" "${sweep_minutes:-unknown}"

# --- abandoned uploads are visible --------------------------------------

part=$(qssrt_bytes "$QSSRT_MANUAL_PART_BYTES")
before_blocks=$(qssrt_block_file_count "$QSSRT_S3_STORE")
abandoned=()
for n in 1 2 3; do
    id=$(s3api create-multipart-upload --bucket "$BUCKET" --key "abandoned/$n" \
        --query UploadId --output text)
    gen_file "abandoned/$n" "$part" "$(qssrt_scratch)/gc-part"
    s3api upload-part --bucket "$BUCKET" --key "abandoned/$n" --upload-id "$id" \
        --part-number 1 --body "$(qssrt_scratch)/gc-part" >/dev/null 2>&1
    abandoned+=("$id")
done
rm -f "$(qssrt_scratch)/gc-part"
after_blocks=$(qssrt_block_file_count "$QSSRT_S3_STORE")
record "block-files-held-by-abandoned-uploads" $((after_blocks - before_blocks))

in_flight=$(s3_uploads_in_flight "$BUCKET")
if [ "$in_flight" -ge 3 ]; then
    check_pass "abandoned uploads stay visible until something reaps them" \
        "$in_flight in flight"
else
    check_fail "abandoned uploads stay visible until something reaps them" \
        "$in_flight in flight, expected at least 3"
fi

if [ $((after_blocks - before_blocks)) -gt 0 ]; then
    check_pass "an abandoned upload holds its blocks" \
        "$((after_blocks - before_blocks)) block files"
else
    check_find "an abandoned upload holds its blocks" \
        "no block file appeared; the part may be inline at this scale"
fi

# --- an explicit abort reclaims, now -----------------------------------

# What the GC does on a timer, a client can do immediately. Same code path
# (the GC aborts exactly the way AbortMultipartUpload does, ADR 0003), so
# this pins the reclamation itself and leaves only the timer unobserved.
reaped_before=$(metric s3_multipart_uploads_reaped)
s3api abort-multipart-upload --bucket "$BUCKET" --key "abandoned/1" \
    --upload-id "${abandoned[0]}" >/dev/null 2>&1
aborted_blocks=$(qssrt_block_file_count "$QSSRT_S3_STORE")
if [ "$aborted_blocks" -lt "$after_blocks" ]; then
    check_pass "an abort returns the blocks it was holding" \
        "$after_blocks -> $aborted_blocks block files"
else
    check_find "an abort returns the blocks it was holding" \
        "$after_blocks -> $aborted_blocks block files"
fi

# --- the sweep itself ---------------------------------------------------

ttl_days=1
period_minutes=${sweep_minutes:-60}
wait_seconds=$((ttl_days * 86400 + period_minutes * 60 + 300))

if [ "${QSSRT_GC_WAIT:-0}" != "1" ]; then
    check_skip "the sweep reaps an upload past its TTL" \
        "needs ~$((wait_seconds / 3600))h: the TTL is whole days (minimum 1) and the sweep period has a one-hour floor, so the first possible reap is TTL + period after startup. Set QSSRT_GC_WAIT=1 to actually wait."
    record "gc-observation-would-need-seconds" "$wait_seconds"
else
    log "waiting ${wait_seconds}s for the sweep; this is the long way round"
    sleep "$wait_seconds"
    reaped_after=$(metric s3_multipart_uploads_reaped)
    record "uploads-reaped-counter" "${reaped_before:-0} -> ${reaped_after:-0}"
    if [ "${reaped_after:-0}" != "${reaped_before:-0}" ]; then
        check_pass "the sweep reaps an upload past its TTL" \
            "s3_multipart_uploads_reaped ${reaped_before:-0} -> ${reaped_after:-0}"
    else
        check_fail "the sweep reaps an upload past its TTL" \
            "s3_multipart_uploads_reaped did not move"
    fi
    left=$(s3_uploads_in_flight "$BUCKET")
    assert_eq "nothing is left in flight after the sweep" 0 "$left"
fi

# The counters exist and are scrapeable whether or not the sweep has run:
# an operator watching for reaping needs them to be there before it does.
for m in s3_multipart_uploads_reaped s3_multipart_orphan_parts_reaped; do
    v=$(metric "$m")
    if [ -n "$v" ]; then
        check_pass "$m is exported" "$v"
    else
        check_fail "$m is exported" "not present at the metrics endpoint"
    fi
done

phase_end
exit $?
