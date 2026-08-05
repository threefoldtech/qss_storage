#!/usr/bin/env bash
# Phase 5: sustained concurrency, and the overwrite storms ADR 0008 made
# exact.
#
# ADR 0009: "parallel aws-cli workers sustaining mixed put/get/delete for a
# configured wall clock; daemon RSS and fd counts sampled throughout; zero
# client-visible errors; then a metadata-only recount." Post-0008 the
# recount expectation is CLEAN on the refcount side: no crash has happened
# yet in this campaign and a successful overwrite releases what it
# displaced, so a refcount_over_count here is news, not background.
#
# The in-flight multipart uploads phases 2 and 3 deliberately left behind
# ARE expected in the report (they are real uploads, not residue), so the
# grading here is by class rather than by total.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 05 stress
s3d_ensure_running || {
    check_fail "the daemon is up" "it would not start"
    phase_end
    exit $?
}

BUCKET="$QSSRT_STRESS_BUCKET"
s3_ensure_bucket "$BUCKET"

seconds=$(qssrt_scaled "$QSSRT_STRESS_SECONDS" "$QSSRT_STRESS_SECONDS_FLOOR")
obj_size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_STRESS_OBJECT_SIZE")" \
    "$(qssrt_bytes "$QSSRT_STRESS_OBJECT_FLOOR")")
workers=${QSSRT_STRESS_WORKERS:-8}
record "stress-wall-seconds" "$seconds"
record "stress-object-bytes" "$obj_size"
record "stress-workers" "$workers"

# --- the resource sampler ----------------------------------------------

# One line every five seconds for the whole storm: epoch, RSS KiB, fd
# count. The curve is the evidence for the leak checks at the end; a
# number sampled only at the finish line cannot distinguish a plateau
# from a climb.
samples="$QSSRT_PHASE_DIR/daemon-samples.tsv"
printf 'epoch\trss_kib\tfds\n' >"$samples"
sampler_pid=""
QSSRT_SAMPLING=1
(
    while [ -n "$QSSRT_S3_PID" ] && kill -0 "$QSSRT_S3_PID" 2>/dev/null; do
        printf '%s\t%s\n' "$(date +%s)" "$(daemon_sample "$QSSRT_S3_PID")" >>"$samples"
        sleep 5
    done
) &
sampler_pid=$!

fd_start=$(daemon_sample "$QSSRT_S3_PID" | cut -f2)
record "fd-at-start" "${fd_start:-unknown}"

# --- the mixed storm ----------------------------------------------------

# Each worker loops PUT -> GET+verify -> (every third) DELETE over its own
# key range until the deadline. Failures land in the worker's own error
# file with the tool's words; the fold below grades them. A worker never
# aborts on one failure -- a storm that stops at the first error measures
# nothing.
stress_worker() {
    local w="$1" deadline="$2" n=0 key tmp want got
    local errs="$QSSRT_PHASE_DIR/errors-w$w.log"
    local ops="$QSSRT_PHASE_DIR/ops-w$w.count"
    local count=0
    tmp="$(qssrt_scratch)/stress-w$w"
    : >"$errs"
    while [ "$(date +%s)" -lt "$deadline" ]; do
        key="mixed/w$w/$n"
        if ! gen_file "$key" "$obj_size" "$tmp" ||
            ! s3_put_file "$BUCKET" "$key" "$tmp" >>"$errs" 2>&1; then
            printf 'PUT %s failed\n' "$key" >>"$errs"
        else
            count=$((count + 1))
            want=$(gen_md5 "$key" "$obj_size")
            got=$(s3_get_stream "$BUCKET" "$key" 2>>"$errs" | md5sum | cut -d' ' -f1)
            if [ "$want" = "$got" ]; then
                count=$((count + 1))
            else
                printf 'GET %s: generated %s, read %s\n' "$key" "$want" "$got" >>"$errs"
            fi
            if [ $((n % 3)) = 2 ]; then
                if s3api delete-object --bucket "$BUCKET" --key "$key" \
                    >>"$errs" 2>&1; then
                    count=$((count + 1))
                else
                    printf 'DELETE %s failed\n' "$key" >>"$errs"
                fi
            fi
        fi
        n=$((n + 1))
    done
    rm -f "$tmp"
    printf '%s' "$count" >"$ops"
}

deadline=$(($(date +%s) + seconds))
pids=()
perf_begin "s3-stress"
for w in $(seq 1 "$workers"); do
    stress_worker "$w" "$deadline" &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null; done

total_ops=0
for w in $(seq 1 "$workers"); do
    total_ops=$((total_ops + $(cat "$QSSRT_PHASE_DIR/ops-w$w.count" 2>/dev/null || echo 0)))
done

# The worker loop is PUT -> GET+verify -> (every third) DELETE, and the
# counter counts loop iterations. So the bytes that crossed the wire are one
# PUT plus one GET per iteration -- twice the object size -- and calling it
# one would halve a number that is the point of the phase.
perf_end "s3-stress" "$total_ops" "$((total_ops * obj_size * 2))" \
    "$workers workers, PUT+GET+every-third-DELETE at $(qssrt_human "$obj_size")"

record "stress-operations" "$total_ops"
[ "$seconds" -gt 0 ] && record "stress-ops-per-second" $((total_ops / seconds))

# Zero client-visible errors is a campaign pass criterion. Every worker
# error file should hold nothing but the tools' own quiet stdout; grep for
# the lines the workers write on failure.
errors=$(cat "$QSSRT_PHASE_DIR"/errors-w*.log 2>/dev/null |
    grep -cE 'failed|generated .* read')
if [ "${errors:-0}" = 0 ]; then
    check_pass "zero client-visible errors across the storm" "$total_ops operations"
else
    check_fail "zero client-visible errors across the storm" \
        "$errors error line(s); first: $(grep -hE 'failed|generated' \
            "$QSSRT_PHASE_DIR"/errors-w*.log | head -n 1)"
fi
if [ "$total_ops" = 0 ]; then
    check_fail "the storm did work" "0 operations in ${seconds}s"
fi

# --- the overwrite storm on ONE key (ADR 0008) --------------------------

# N rounds of DISTINCT content onto one key. Post-0008 each overwrite
# releases the record it displaced, so the store ends holding exactly the
# final object's blocks -- not N objects' worth waiting for an offline
# recount. Content is keyed by round (the S3 key stays fixed), so every
# round really is a replacement, not a dedup hit.
rounds=${QSSRT_OVERWRITE_ROUNDS:-8}
ow_size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_OVERWRITE_SIZE")" \
    "$(qssrt_bytes "$QSSRT_OVERWRITE_FLOOR")")
ow_key="overwrite/one-key"
tmp="$(qssrt_scratch)/overwrite"

files_before=$(qssrt_block_file_count "$QSSRT_S3_STORE")
bytes_before=$(qssrt_block_bytes "$QSSRT_S3_STORE")

files_after_first=""
r=1
while [ "$r" -le "$rounds" ]; do
    gen_file "$ow_key#round-$r" "$ow_size" "$tmp"
    if ! s3_put_file "$BUCKET" "$ow_key" "$tmp" >/dev/null 2>&1; then
        check_fail "overwrite round $r PUTs" "$(tail -c 200 "$QSSRT_LAST_OUT" 2>/dev/null)"
    fi
    [ "$r" = 1 ] && files_after_first=$(qssrt_block_file_count "$QSSRT_S3_STORE")
    r=$((r + 1))
done
rm -f "$tmp"

files_after=$(qssrt_block_file_count "$QSSRT_S3_STORE")
bytes_after=$(qssrt_block_bytes "$QSSRT_S3_STORE")
one_object=$((files_after_first - files_before))
record "overwrite-storm-block-files-for-one-object" "$one_object"
record "overwrite-storm-block-file-delta" $((files_after - files_before))

assert_eq "after $rounds distinct overwrites the store holds one object's blocks" \
    "$one_object" $((files_after - files_before))

# Disk growth over the storm: one object plus at most one block of slack.
growth=$((bytes_after - bytes_before))
slack=$((ow_size + 1048576))
if [ "$growth" -le "$slack" ]; then
    check_pass "overwrite storm disk growth tracks live data" \
        "$(qssrt_human "$growth") for a $(qssrt_human "$ow_size") object"
else
    check_fail "overwrite storm disk growth tracks live data" \
        "grew $(qssrt_human "$growth") for a $(qssrt_human "$ow_size") object: replaced blocks were not released"
fi

want=$(gen_md5 "$ow_key#round-$rounds" "$ow_size")
got=$(s3_get_stream "$BUCKET" "$ow_key" | md5sum | cut -d' ' -f1)
assert_eq "the key reads back as the LAST content written" "$want" "$got"

# --- the same-content re-PUT storm --------------------------------------

# The other half of the 0008 arithmetic: every dedup hit bumps and every
# overwrite releases, so a same-content re-PUT storm nets to nothing. The
# block-file count must not move at all after the first PUT.
sc_key="overwrite/same-content"
gen_file "$sc_key" "$ow_size" "$(qssrt_scratch)/same"
s3_put_file "$BUCKET" "$sc_key" "$(qssrt_scratch)/same" >/dev/null 2>&1
files_one=$(qssrt_block_file_count "$QSSRT_S3_STORE")
r=1
while [ "$r" -le "$rounds" ]; do
    s3_put_file "$BUCKET" "$sc_key" "$(qssrt_scratch)/same" >/dev/null 2>&1
    r=$((r + 1))
done
rm -f "$(qssrt_scratch)/same"
assert_eq "a same-content re-PUT storm leaves the block count untouched" \
    "$files_one" "$(qssrt_block_file_count "$QSSRT_S3_STORE")"

# --- resource verdicts --------------------------------------------------

# Quiesce, then read the fd count against the start. Descriptors held by
# in-flight work are fine; descriptors that never come back are a leak.
sleep 3
fd_end=$(daemon_sample "$QSSRT_S3_PID" | cut -f2)
record "fd-at-end" "${fd_end:-unknown}"
if [ -n "$fd_start" ] && [ -n "$fd_end" ]; then
    if [ "$fd_end" -le $((fd_start + ${QSSRT_FD_SLACK:-64})) ]; then
        check_pass "fd count returns to within slack of its start" \
            "$fd_start -> $fd_end (slack ${QSSRT_FD_SLACK:-64})"
    else
        check_fail "fd count returns to within slack of its start" \
            "$fd_start -> $fd_end: descriptors are not coming back"
    fi
else
    check_skip "fd count returns to within slack of its start" \
        "could not sample /proc for the daemon"
fi

# RSS: a plateau is healthy, a climb that never stops is a finding with
# the curve attached. Compare the average of the last quarter of samples
# against the first quarter, past a warmup.
rss_verdict=$(awk -F'\t' 'NR > 1 && $2 != "" && $2 > 0 { rss[n++] = $2 }
    END {
        if (n < 8) { print "too-few-samples"; exit }
        q = int(n / 4)
        for (i = 0; i < q; i++) head += rss[i]
        for (i = n - q; i < n; i++) tail += rss[i]
        head /= q; tail /= q
        printf "%d %d %s", head, tail, (tail > head * 2 ? "climbing" : "steady")
    }' "$samples")
record "rss-first-quarter-vs-last" "$rss_verdict"
case "$rss_verdict" in
*climbing)
    check_find "daemon RSS levels off under sustained load" \
        "$rss_verdict KiB; the curve is in daemon-samples.tsv"
    ;;
*steady)
    check_pass "daemon RSS levels off under sustained load" "$rss_verdict KiB"
    ;;
*)
    check_skip "daemon RSS levels off under sustained load" \
        "$rss_verdict: the storm was too short to grade a curve"
    ;;
esac

kill "$sampler_pid" 2>/dev/null
wait "$sampler_pid" 2>/dev/null

# --- the recount --------------------------------------------------------

# Post-0008: refcounts must be exact after a crash-free storm, overwrites
# included. The uploads phases 2 and 3 left in flight are real uploads and
# may appear as multipart classes; anything WARN or CRITICAL, or any
# refcount finding at all, is graded.
s3d_stop
fsck_run "post-stress"
warn=$(fsck_severity warn)
critical=$(fsck_severity critical)
over=$(fsck_count refcount_over_count)
under=$(fsck_count refcount_under_count)
if [ "$critical" != 0 ] || [ "$warn" != 0 ]; then
    check_fail "the post-stress recount finds no WARN or CRITICAL" \
        "$(fsck_summary); $(fsck_findings_brief)"
else
    check_pass "the post-stress recount finds no WARN or CRITICAL" "$(fsck_summary)"
fi
if [ "${under:-0}" != 0 ]; then
    check_fail "no refcount is under-counted" "refcount_under_count=$under: the loss direction"
elif [ "${over:-0}" != 0 ]; then
    check_find "refcounts are exact after a crash-free storm" \
        "refcount_over_count=$over: post-0008 this is news, not background"
else
    check_pass "refcounts are exact after a crash-free storm" "$(fsck_summary)"
fi

phase_end
exit $?
