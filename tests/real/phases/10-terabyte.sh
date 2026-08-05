#!/usr/bin/env bash
# Phase 10: the terabyte. Flag-gated (--tb); replaces phases 5, 8 and 9
# rather than following them.
#
# ADR 0009: fill ~3 TiB capped at 85% of the filesystem through the real S3
# path, in bands that exercise every regime at once; verify without a
# mirror (content is a pure function of the key); one kill -9 past 1 TiB
# with resume; three-million-key listings; the recount timed at 3M+
# objects; a full scrub at capacity; teardown to empty with the reclaim
# curve.
#
# There is no second copy of the data anywhere. Giants stream from the
# generator straight into aws-cli's stdin; small-object bands stage one
# shard at a time in scratch and push it with one cp --recursive, so local
# staging never exceeds one shard. Every band is sharded and every shard is
# checkpointed OUTSIDE the store and OUTSIDE the timestamped run dir, so a
# rerun resumes instead of restarting -- re-PUTting a key is idempotent by
# construction, the content being derived from the key.
#
# Sizes in a band's keys: a mid-band object's size is embedded in its own
# key name. Verification then needs nothing but the key listing itself --
# no manifest, which at three million objects would be a small database
# with its own bugs.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 10 terabyte

# Phase 10 owns the daemon log volume problem: every request is logged at
# INFO with its whole input, which at three million objects is a gigabyte
# of log. The filter keeps everything the error gate reads.
export QSSRT_DAEMON_LOG_FILTER="${QSSRT_TB_DAEMON_LOG_FILTER:-1}"

BUCKET="$QSSRT_TB_BUCKET"

# --- the cap, re-checked where the writing happens ----------------------

target=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_TARGET")" 1)
total_fs=$(qssrt_total_bytes "$QSSRT_STORE_ROOT")
cap=$((total_fs * ${QSSRT_TB_FS_CAP_PERCENT:-85} / 100))
record "tb-target" "$(qssrt_human "$target")"
record "tb-cap" "$(qssrt_human "$cap")"
if [ "$target" -gt "$cap" ]; then
    # A refusal, not a truncation (the ADR's cap rule): a disk that cannot
    # hold the configured target must not quietly run a smaller campaign
    # than the verdict will claim.
    check_fail "the terabyte target fits under the ${QSSRT_TB_FS_CAP_PERCENT:-85}% cap" \
        "target $(qssrt_human "$target") exceeds $(qssrt_human "$cap")"
    phase_end
    exit $?
fi

s3d_ensure_running || {
    check_fail "the daemon is up" "it would not start"
    phase_end
    exit $?
}
s3_ensure_bucket "$BUCKET"

# --- checkpoints --------------------------------------------------------

# A checkpoint says "this shard is already in the store". That claim is only
# ever true of ONE store, so the checkpoint set is keyed by the store's own
# identity -- the 32-byte header the store writes when it is created, which
# is fresh for every store (ADR 0012's pairing identity).
#
# It used to be keyed by seed and scale alone, under the results root, where
# --fresh does not reach: --fresh wipes the store's directories, and the
# stamps from the previous store survived to describe data that no longer
# existed. The next run then skipped every band it had a stamp for and
# reported each one "filled" against an empty disk -- a terabyte verdict on
# 30 GB, with no failure anywhere to notice it by. Caught on 2026-08-05 by
# 237 stamps dated 2026-08-01.
#
# Keyed this way a wiped store cannot inherit them, however it was wiped:
# by --fresh, by hand, or by a mkfs.
ck_store_identity() {
    local header="$QSSRT_S3_STORE/store_header.bin"
    if [ -r "$header" ]; then
        sha256sum "$header" | cut -c1-16
    else
        # No header means no store to have written anything, so no
        # checkpoint could describe it. A name nothing else uses.
        printf 'nostore'
    fi
}

CK="$QSSRT_RESULTS_ROOT/tb-checkpoint/${QSSRT_SEED}-scale${QSSRT_SCALE:-1}-$(ck_store_identity)"
mkdir -p "$CK"
ck_done() { [ -f "$CK/$1.done" ]; }
ck_stamp() { : >"$CK/$1.done"; }
record "tb-checkpoint-set" "$(basename "$CK")"
resumed=$(find "$CK" -name '*.done' 2>/dev/null | wc -l)
[ "$resumed" -gt 0 ] &&
    record "tb-resumed-with-checkpoints" "$resumed"

# A resumed run is a legitimate thing; a resumed run that resumes EVERYTHING
# is not, because then the fill wrote nothing and the bands' pass lines are
# about a previous run's disk. Said out loud rather than left for the reader
# of a suspiciously fast phase.
if [ "$resumed" -gt 0 ]; then
    check_skip "the fill starts from an empty store" \
        "$resumed checkpoints from an earlier run against this same store are being resumed"
fi

# --- the kill watcher ---------------------------------------------------

# One kill -9, at a randomized point past the configured written volume,
# from a watcher rather than a scheduled sleep: the fill's own pace decides
# when the threshold is crossed. The watcher restarts the daemon itself so
# whichever shard the kill interrupted fails its push, is not stamped, and
# is retried by the band loop.
kill_after=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_KILL_AFTER")" 1)
kill_flag="$QSSRT_PHASE_DIR/killed-at"
kill_jitter=$(qssrt_rand_between 0 $((kill_after / 10 + 1)))
kill_threshold=$((kill_after + kill_jitter))
if ck_done killed; then
    : # the crash already happened in the run this one resumes
else
    (
        while :; do
            sleep 15
            written=$(qssrt_block_bytes "$QSSRT_S3_STORE")
            if [ "${written:-0}" -ge "$kill_threshold" ]; then
                printf '%s\t%s\n' "$(date -Is)" "$written" >"$kill_flag"
                exit 0
            fi
        done
    ) &
    watcher_pid=$!
fi

# The daemon is killed from the fill loop (not the subshell) so the pid
# bookkeeping stays in one process. kill_pending answers "has the watcher
# fired and the kill not yet been done".
kill_pending() {
    [ -f "$kill_flag" ] && ! ck_done killed
}
do_the_kill() {
    if s3d_kill9; then
        check_pass "kill -9 landed mid-fill past $(qssrt_human "$kill_threshold")" \
            "$(cat "$kill_flag")"
    else
        check_fail "kill -9 landed mid-fill" "no daemon to kill"
    fi
    ck_stamp killed
    s3d_start ||
        check_fail "the daemon restarts after the mid-fill kill" "it did not come back"
}

# --- the resource curve -------------------------------------------------

curve="$QSSRT_PHASE_DIR/fill-curve.tsv"
printf 'epoch\tblock_bytes\trss_kib\tfds\tdb_bytes\n' >"$curve"
(
    while :; do
        printf '%s\t%s\t%s\t%s\n' "$(date +%s)" \
            "$(qssrt_block_bytes "$QSSRT_S3_STORE")" \
            "$(daemon_sample "$(cat "$(qssrt_daemon_dir)/s3cas.pid" 2>/dev/null)")" \
            "$(qssrt_du_bytes "$QSSRT_S3_STORE/db")" >>"$curve"
        sleep 60
    done
) &
curve_pid=$!

# --- shard plumbing -----------------------------------------------------

# push_with_retry <shard-label> <stage-dir> <prefix>
#
# One cp --recursive per shard. A failed push (the kill, most likely) is
# retried after the daemon is confirmed back; a shard is stamped only by
# its caller, only after this returns success.
push_with_retry() {
    local label="$1" stage="$2" prefix="$3" attempt
    for attempt in 1 2 3; do
        if kill_pending; then do_the_kill; fi
        if s3_put_dir "$stage/" "$BUCKET" "$prefix" >/dev/null 2>&1; then
            return 0
        fi
        s3d_ensure_running || true
    done
    check_fail "shard $label pushes" "3 attempts failed"
    return 1
}

# --- band: giants -------------------------------------------------------

giant_count=${QSSRT_TB_GIANT_COUNT:-4}
giant_size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_GIANT_SIZE")" \
    "$(qssrt_bytes "$QSSRT_TB_GIANT_FLOOR")")
band_giants() {
    local i key t0 attempt
    for i in $(seq 1 "$giant_count"); do
        key="giants/g$i"
        ck_done "giants.$i" && continue
        t0=$(date +%s)
        for attempt in 1 2 3; do
            if kill_pending; then do_the_kill; fi
            if s3_put_stream "$BUCKET" "$key" "$key" "$giant_size" >/dev/null 2>&1; then
                ck_stamp "giants.$i"
                record "giant-$i-MBps" \
                    $((giant_size / ($(date +%s) - t0 + 1) / 1048576))
                break
            fi
            s3d_ensure_running || true
        done
        if ! ck_done "giants.$i"; then
            check_fail "giant $i uploads" "3 attempts failed"
        fi
    done
    QSSRT_BAND_OBJECTS=$giant_count
    QSSRT_BAND_BYTES=$((giant_count * giant_size))
    check_pass "giants band filled" \
        "$giant_count x $(qssrt_human "$giant_size"), client-driven multipart"
}

# --- band: mid (1-16 MiB, the block-store working regime) ---------------

mid_total=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_MID_TOTAL")" \
    "$(qssrt_bytes 64MiB)")
mid_min=$(qssrt_bytes "$QSSRT_TB_MID_MIN")
mid_max=$(qssrt_bytes "$QSSRT_TB_MID_MAX")
shard_keys=${QSSRT_TB_SHARD_KEYS:-1000}

# A key's size is a deterministic function of its indices, embedded in the
# key itself: "mid/s0007/k000123-5242880" is 5242880 bytes, and every
# verifier knows it from the listing alone.
mid_size_for() {
    local shard="$1" n="$2" span=$((mid_max - mid_min + 1))
    printf '%s' $((mid_min + (shard * 7919 + n * 104729) % span))
}

band_mid() {
    local shard=0 filled=0 n size key stage
    while [ "$filled" -lt "$mid_total" ]; do
        local shard_bytes=0 label
        label=$(printf 's%04d' "$shard")
        # The shard's byte volume is computable without writing it, so a
        # checkpointed shard advances the fill accounting for free.
        n=0
        while [ "$n" -lt "$shard_keys" ]; do
            shard_bytes=$((shard_bytes + $(mid_size_for "$shard" "$n")))
            n=$((n + 1))
        done
        if ! ck_done "mid.$label"; then
            stage="$(qssrt_scratch)/mid-$label"
            rm -rf "$stage"
            mkdir -p "$stage"
            n=0
            : >"$stage.manifest"
            while [ "$n" -lt "$shard_keys" ]; do
                size=$(mid_size_for "$shard" "$n")
                key=$(printf 'k%06d-%s' "$n" "$size")
                printf '%s\t%s\t%s\n' "$stage/$key" "mid/$label/$key" "$size" \
                    >>"$stage.manifest"
                n=$((n + 1))
            done
            gen_files "$stage.manifest"
            rm -f "$stage.manifest"
            push_with_retry "mid.$label" "$stage" "mid/$label/" || {
                rm -rf "$stage"
                return 1
            }
            rm -rf "$stage"
            ck_stamp "mid.$label"
        fi
        filled=$((filled + shard_bytes))
        shard=$((shard + 1))
    done
    QSSRT_BAND_OBJECTS=$((shard * shard_keys))
    QSSRT_BAND_BYTES=$filled
    record "mid-band-bytes" "$(qssrt_human "$filled")"
    record "mid-band-shards" "$shard"
    check_pass "mid band filled" \
        "$(qssrt_human "$filled") of $(qssrt_human "$mid_min")-$(qssrt_human "$mid_max") objects"
}

# --- band: hundred (the multipart threshold band) -----------------------

hundred_total=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_HUNDRED_TOTAL")" \
    "$(qssrt_bytes 64MiB)")
hundred_size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_HUNDRED_SIZE")" \
    "$(qssrt_bytes 8MiB)")
band_hundred() {
    local count=$((hundred_total / hundred_size)) per_shard=$((shard_keys / 10))
    [ "$per_shard" -lt 1 ] && per_shard=1
    [ "$count" -lt 1 ] && count=1
    local shard=0 done_keys=0 n key stage label
    while [ "$done_keys" -lt "$count" ]; do
        label=$(printf 's%04d' "$shard")
        if ! ck_done "hundred.$label"; then
            stage="$(qssrt_scratch)/hundred-$label"
            rm -rf "$stage"
            mkdir -p "$stage"
            n=0
            : >"$stage.manifest"
            while [ "$n" -lt "$per_shard" ] && [ $((done_keys + n)) -lt "$count" ]; do
                key=$(printf 'k%06d' $((done_keys + n)))
                printf '%s\t%s\t%s\n' "$stage/$key" "hundred/$label/$key" \
                    "$hundred_size" >>"$stage.manifest"
                n=$((n + 1))
            done
            gen_files "$stage.manifest"
            rm -f "$stage.manifest"
            push_with_retry "hundred.$label" "$stage" "hundred/$label/" || {
                rm -rf "$stage"
                return 1
            }
            rm -rf "$stage"
            ck_stamp "hundred.$label"
        fi
        done_keys=$((done_keys + per_shard))
        shard=$((shard + 1))
    done
    QSSRT_BAND_OBJECTS=$count
    QSSRT_BAND_BYTES=$((count * hundred_size))
    record "hundred-band-objects" "$count"
    check_pass "hundred band filled" "$count x $(qssrt_human "$hundred_size")"
}

# --- band: tiny (the metadata regime) -----------------------------------

tiny_count=$(qssrt_scaled "${QSSRT_TB_TINY_COUNT:-3000000}" 1000)
tiny_size=$(qssrt_bytes "${QSSRT_TB_TINY_SIZE:-1KiB}")
tiny_shard=$((shard_keys * 10))
band_tiny() {
    local shard=0 done_keys=0 n key stage label
    while [ "$done_keys" -lt "$tiny_count" ]; do
        label=$(printf 's%05d' "$shard")
        if ! ck_done "tiny.$label"; then
            stage="$(qssrt_scratch)/tiny-$label"
            rm -rf "$stage"
            mkdir -p "$stage"
            n=0
            : >"$stage.manifest"
            while [ "$n" -lt "$tiny_shard" ] && [ $((done_keys + n)) -lt "$tiny_count" ]; do
                key=$(printf 'k%08d' $((done_keys + n)))
                printf '%s\t%s\t%s\n' "$stage/$key" "tiny/$label/$key" \
                    "$tiny_size" >>"$stage.manifest"
                n=$((n + 1))
            done
            gen_files "$stage.manifest"
            rm -f "$stage.manifest"
            push_with_retry "tiny.$label" "$stage" "tiny/$label/" || {
                rm -rf "$stage"
                return 1
            }
            rm -rf "$stage"
            ck_stamp "tiny.$label"
        fi
        done_keys=$((done_keys + tiny_shard))
        shard=$((shard + 1))
    done
    QSSRT_BAND_OBJECTS=$tiny_count
    QSSRT_BAND_BYTES=$((tiny_count * tiny_size))
    record "tiny-band-objects" "$tiny_count"
    check_pass "tiny band filled" "$tiny_count x $(qssrt_human "$tiny_size")"
}

# --- band: dedup (written twice, stored once) ---------------------------

dedup_total=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_DEDUP_TOTAL")" \
    "$(qssrt_bytes 64MiB)")
dedup_object=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_DEDUP_OBJECT")" \
    "$(qssrt_bytes 8MiB)")
band_dedup() {
    local count=$((dedup_total / dedup_object)) copy i key
    [ "$count" -lt 1 ] && count=1
    local before_bytes
    before_bytes=$(qssrt_block_bytes "$QSSRT_S3_STORE")
    for copy in a b; do
        for i in $(seq 1 "$count"); do
            ck_done "dedup.$copy.$i" && continue
            # Same generator key for both copies: same content, different
            # S3 keys. Logical 2x, stored 1x -- that is the claim.
            key="dedup-$copy/k$i"
            local attempt ok=0
            for attempt in 1 2 3; do
                if kill_pending; then do_the_kill; fi
                if s3_put_stream "$BUCKET" "$key" "dedup-content/k$i" \
                    "$dedup_object" >/dev/null 2>&1; then
                    ok=1
                    break
                fi
                s3d_ensure_running || true
            done
            if [ "$ok" = 1 ]; then
                ck_stamp "dedup.$copy.$i"
            else
                check_fail "dedup object $copy/$i uploads" "3 attempts failed"
            fi
        done
    done
    local grew=$(($(qssrt_block_bytes "$QSSRT_S3_STORE") - before_bytes))
    QSSRT_BAND_OBJECTS=$((count * 2))
    QSSRT_BAND_BYTES=$((dedup_object * count * 2))
    record "dedup-band-logical" "$(qssrt_human $((dedup_total * 2)))"
    record "dedup-band-stored-growth" "$(qssrt_human "$grew")"
    # Post-0008 this is an equality claim with one block of slack, not a
    # "pending reconciliation" one: rc is exact, dedup is total.
    if [ "$grew" -le $((dedup_total + 1048576)) ]; then
        check_pass "the dedup band is stored once" \
            "logical $(qssrt_human $((dedup_total * 2))), grew $(qssrt_human "$grew")"
    else
        check_fail "the dedup band is stored once" \
            "logical $(qssrt_human $((dedup_total * 2))), grew $(qssrt_human "$grew"): dedup is not deduplicating"
    fi
}

# --- the fill -----------------------------------------------------------

fill_t0=$(date +%s)
run_band() {
    local name="$1"
    QSSRT_BAND_OBJECTS=0
    QSSRT_BAND_BYTES=0

    # A band that resumes entirely from checkpoints writes nothing, and a
    # perf row for it would divide a band's worth of bytes by the fraction
    # of a second it took to notice they were already there. That is how a
    # fill came to report 228,480,036 MiB/s. Stamps created during the band
    # are the test -- cheap, and exactly the question being asked.
    local stamps_before stamps_after
    stamps_before=$(find "$CK" -name "$name.*.done" 2>/dev/null | wc -l)

    perf_begin "tb-$name"
    "band_$name"
    local rc=$?

    stamps_after=$(find "$CK" -name "$name.*.done" 2>/dev/null | wc -l)
    if [ "$stamps_after" -gt "$stamps_before" ]; then
        perf_end "tb-$name" "$QSSRT_BAND_OBJECTS" "$QSSRT_BAND_BYTES" \
            "terabyte band, $((stamps_after - stamps_before)) shard(s) written here"
    else
        # Close the window without recording it: the measurement would be
        # about a previous run's disk.
        perf_end "tb-$name" 0 0 "resumed from checkpoints, wrote nothing"
        log "band $name resumed entirely from checkpoints: no rate recorded"
    fi
    return $rc
}

run_band giants
run_band mid
run_band hundred
run_band tiny
run_band dedup
fill_elapsed=$(($(date +%s) - fill_t0))
record "fill-wall-seconds" "$fill_elapsed"
record "fill-block-bytes" "$(qssrt_human "$(qssrt_block_bytes "$QSSRT_S3_STORE")")"

[ -n "${watcher_pid:-}" ] && kill "$watcher_pid" 2>/dev/null
if ! ck_done killed; then
    check_find "the mid-fill kill -9 happened" \
        "the fill finished below the $(qssrt_human "$kill_threshold") threshold; at SCALE>1 that is the arithmetic, not a bug"
fi

# --- post-fill fsck: residue is leak-class only -------------------------

fsck_run post-fill
fsck_assert_leak_only "post-fill residue is leak-class only"
record "post-fill-residue" "$(fsck_summary)"
s3d_ensure_running || true

# --- verification without a mirror --------------------------------------

verify_stream() {
    local s3key="$1" genkey="$2" size="$3" got
    got=$(s3_get_stream "$BUCKET" "$s3key" | md5sum | cut -d' ' -f1)
    [ "$got" = "$(gen_md5 "$genkey" "$size")" ]
}

# Giants and dedup: full verification, every byte.
for i in $(seq 1 "$giant_count"); do
    if verify_stream "giants/g$i" "giants/g$i" "$giant_size"; then
        check_pass "giant $i reads back byte-identical ($(qssrt_human "$giant_size"))"
    else
        check_fail "giant $i reads back byte-identical" "md5 mismatch"
    fi
done
dedup_count=$((dedup_total / dedup_object))
[ "$dedup_count" -lt 1 ] && dedup_count=1
dedup_bad=0
for copy in a b; do
    for i in $(seq 1 "$dedup_count"); do
        verify_stream "dedup-$copy/k$i" "dedup-content/k$i" "$dedup_object" ||
            dedup_bad=$((dedup_bad + 1))
    done
done
if [ "$dedup_bad" = 0 ]; then
    check_pass "both dedup copies read back byte-identical, in full" \
        "$((dedup_count * 2)) objects"
else
    check_fail "both dedup copies read back byte-identical, in full" \
        "$dedup_bad of $((dedup_count * 2)) failed"
fi

# Mid, hundred, tiny: sampled at >= QSSRT_TB_SAMPLE_PERCENT. The keys come
# from the listing; a mid key carries its size in its own name.
sample_band() {
    local band="$1" every="$2" checked=0 bad=0 key size
    while IFS= read -r key; do
        case "$band" in
        mid) size=${key##*-} ;;
        hundred) size=$hundred_size ;;
        tiny) size=$tiny_size ;;
        esac
        verify_stream "$key" "$key" "$size" || {
            bad=$((bad + 1))
            [ "$bad" -le 3 ] && check_fail "sampled $band object reads back" "$key"
        }
        checked=$((checked + 1))
    done < <(s3_list_v2_manual "$BUCKET" "$band" | awk -v e="$every" 'NR % e == 0')
    record "$band-sampled" "$checked"
    if [ "$bad" = 0 ] && [ "$checked" -gt 0 ]; then
        check_pass "$band band sampled verification" "$checked objects, all byte-identical"
    elif [ "$checked" = 0 ]; then
        check_fail "$band band sampled verification" "the listing returned nothing to sample"
    fi
}
every=$((100 / ${QSSRT_TB_SAMPLE_PERCENT:-1}))
[ "$every" -lt 1 ] && every=1
sample_band mid "$every"
sample_band hundred "$every"
sample_band tiny "$every"

# --- the metadata regime at scale ---------------------------------------

expected_tiny=$tiny_count
t0=$(date +%s)
listed=$(s3_list_v2_manual "$BUCKET" "tiny/" | wc -l)
record "tiny-list-manual-seconds" $(($(date +%s) - t0))
assert_eq "a manual continuation loop enumerates every tiny key" \
    "$expected_tiny" "$listed"
t0=$(date +%s)
listed=$(s3_list_v2_paginated "$BUCKET" "tiny/" | wc -l)
record "tiny-list-paginator-seconds" $(($(date +%s) - t0))
assert_eq "the V2 paginator enumerates every tiny key" \
    "$expected_tiny" "$listed"

# The recount, timed, at the full object count.
t0=$(date +%s)
fsck_run at-capacity
recount_seconds=$(($(date +%s) - t0))
record "recount-seconds-at-capacity" "$recount_seconds"
record "db-bytes-at-capacity" "$(qssrt_human "$(qssrt_du_bytes "$QSSRT_S3_STORE/db")")"
fsck_assert_leak_only "the at-capacity recount stays leak-class only"

# --- the full scrub at capacity -----------------------------------------

store_bytes=$(qssrt_block_bytes "$QSSRT_S3_STORE")
store_files=$(qssrt_block_file_count "$QSSRT_S3_STORE")
t0=$(date +%s)
# The read pass over the whole filled store, with no daemon running: the
# one window in the campaign where device reads should track logical bytes
# one to one, and therefore the honest read-bandwidth number for this
# filesystem at capacity.
perf_begin "tb-scrub-at-capacity"
fsck_run scrub-at-capacity --scrub
perf_end "tb-scrub-at-capacity" "$store_files" "$store_bytes" \
    "full scrub of the filled store, daemon stopped"
scrub_seconds=$(($(date +%s) - t0))
record "scrub-wall-seconds" "$scrub_seconds"
[ "$scrub_seconds" -gt 0 ] &&
    record "scrub-MBps" $((store_bytes / scrub_seconds / 1048576))
corrupt=$(fsck_count corrupt_block)
if [ "${corrupt:-0}" = 0 ]; then
    check_pass "the scrub at capacity finds zero corruption" "$(fsck_summary)"
else
    check_fail "the scrub at capacity finds zero corruption" \
        "corrupt_block=$corrupt; $(fsck_findings_brief)"
fi

# --- the daemon across the whole fill -----------------------------------

# The gate has been running per phase_end; these are the campaign-level
# claims the ADR names for phase 10 specifically.
if grep -q 'panicked at' "$(qssrt_s3_log)" 2>/dev/null; then
    check_fail "no panic anywhere in the fill" \
        "$(grep 'panicked at' "$(qssrt_s3_log)" | head -n 1)"
else
    check_pass "no panic anywhere in the fill"
fi
if grep -qi 'too many open files' "$(qssrt_s3_log)" 2>/dev/null; then
    check_fail "no fd exhaustion anywhere in the fill" \
        "$(grep -i 'too many open files' "$(qssrt_s3_log)" | head -n 1)"
else
    check_pass "no fd exhaustion anywhere in the fill"
fi

# --- teardown at scale --------------------------------------------------

s3d_ensure_running || true
reclaim="$QSSRT_PHASE_DIR/reclaim-curve.tsv"
printf 'epoch\tblock_bytes\tblock_files\n' >"$reclaim"
(
    while :; do
        printf '%s\t%s\t%s\n' "$(date +%s)" \
            "$(qssrt_block_bytes "$QSSRT_S3_STORE")" \
            "$(qssrt_block_file_count "$QSSRT_S3_STORE")" >>"$reclaim"
        sleep 30
    done
) &
reclaim_pid=$!

t0=$(date +%s)
assert_ok "the whole dataset deletes through the client" \
    s3_rm_prefix "$BUCKET" ""
assert_ok "the emptied bucket deletes" s3api delete-bucket --bucket "$BUCKET"
record "teardown-wall-seconds" $(($(date +%s) - t0))

kill "$reclaim_pid" 2>/dev/null
wait "$reclaim_pid" 2>/dev/null
kill "$curve_pid" 2>/dev/null
wait "$curve_pid" 2>/dev/null

assert_eq "zero live block files after the teardown" 0 \
    "$(qssrt_block_file_count "$QSSRT_S3_STORE")"

fsck_run final
fsck_assert_clean "the final fsck reconciles the store to empty"

# The checkpoints outlived their purpose the moment the store reconciled.
rm -rf "$CK"

phase_end
exit $?
