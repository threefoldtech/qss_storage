#!/usr/bin/env bash
# The crash cycle, shared by phase 6 (fsync) and phase 7 (buffer).
#
# The campaign's crash grade is process kill and restart, and only that:
# no power-off, no dm-flakey, no broken-disk simulation anywhere (ADR 0009,
# answered open question). What a kill -9 proves is the crash-consistency
# layer -- rename-before-record, claim atomicity, residue classes -- not
# fsync-versus-power-loss, which needs a rig this campaign does not have.
#
# Phases 6 and 7 share this function so they cannot drift apart: the
# durability matrix is only a matrix if both halves run the same thing.
#
# The acknowledgement file is the heart of it. A worker appends
# "key<TAB>size" only AFTER the client command exited 0, so the post-crash
# verification set is exactly what the client saw succeed -- which is the
# claim the ADR asks to be checked, and it is not the same set as "what was
# attempted".

# crash_worker <bucket> <prefix> <size> <ack-file> <deadline-epoch>
#
# PUTs generated objects until the deadline, acknowledging each success.
crash_worker() {
    local bucket="$1" prefix="$2" size="$3" ack="$4" deadline="$5" n=0 key
    while [ "$(date +%s)" -lt "$deadline" ]; do
        key="$prefix/$n"
        if s3_put_generated "$bucket" "$key" "$size" >/dev/null 2>&1; then
            printf '%s\t%s\n' "$key" "$size" >>"$ack"
        fi
        n=$((n + 1))
    done
}

# crash_multipart_worker <bucket> <prefix> <size> <ack-file> <deadline-epoch>
#
# The same, through client-driven multipart, so the kill lands inside an
# upload's lifetime as often as inside a PUT's.
crash_multipart_worker() {
    local bucket="$1" prefix="$2" size="$3" ack="$4" deadline="$5" n=0 key tmp
    local rc t0 t1 mplog="${QSSRT_CRASH_MP_LOG:-}"
    tmp="$(qssrt_scratch)/mp-${BASHPID:-$$}"
    while [ "$(date +%s)" -lt "$deadline" ]; do
        key="$prefix/mp-$n"
        gen_file "$key" "$size" "$tmp"
        # Instrumented mode (2026-08-02 false-ack investigation): keep every
        # cp's exit code, wall window, and stderr, so an acknowledgement can
        # be audited against what the client tool actually reported.
        if [ -n "$mplog" ]; then
            t0=$(date +%s.%N)
            s3cmd cp --quiet "$tmp" "s3://$bucket/$key" \
                >/dev/null 2>>"$mplog.err.${prefix//\//-}-mp-$n"
            rc=$?
            t1=$(date +%s.%N)
            printf '%s\t%s\t%s\t%s\n' "$key" "$rc" "$t0" "$t1" >>"$mplog"
        else
            s3cmd cp --quiet "$tmp" "s3://$bucket/$key" >/dev/null 2>&1
            rc=$?
        fi
        if [ "$rc" = 0 ]; then
            printf '%s\t%s\n' "$key" "$size" >>"$ack"
        fi
        n=$((n + 1))
    done
    rm -f "$tmp"
}

# crash_cycle <label> <durability> <cycle-number>
#
# One full cycle: storm, kill at a randomized point inside it, restart,
# verify every acknowledged object, fsck, repair, re-fsck.
crash_cycle() {
    local label="$1" durability="$2" cycle="$3"
    local bucket="$QSSRT_CRASH_BUCKET"
    local ack="$QSSRT_PHASE_DIR/acked-$label-$cycle.tsv"
    local put_size mp_size window kill_at deadline pid pids=()

    put_size=$(qssrt_scaled "$(qssrt_bytes "${QSSRT_CRASH_PUT_SIZE}")" 4096)
    mp_size=$(qssrt_scaled "$(qssrt_bytes "${QSSRT_CRASH_MULTIPART_SIZE}")" 5242880)
    window=${QSSRT_CRASH_WINDOW_SECONDS:-60}

    : >"$ack"

    s3d_running || s3d_start "$durability"
    s3_ensure_bucket "$bucket"

    deadline=$(($(date +%s) + window))
    # The kill lands somewhere in the middle two thirds of the window: early
    # enough that the storm is running, late enough that there is something
    # to be inconsistent about.
    kill_at=$(qssrt_rand_between $((window / 6)) $((window * 2 / 3)))
    logf 'cycle %s (%s, %s): %ss window, kill -9 at +%ss' \
        "$cycle" "$label" "$durability" "$window" "$kill_at"

    local w
    for w in $(seq 1 "${QSSRT_CRASH_WORKERS:-4}"); do
        crash_worker "$bucket" "$label-$cycle/w$w" "$put_size" "$ack.$w" "$deadline" &
        pids+=($!)
    done
    crash_multipart_worker "$bucket" "$label-$cycle" "$mp_size" "$ack.mp" "$deadline" &
    pids+=($!)

    sleep "$kill_at"
    if s3d_kill9; then
        check_pass "cycle $cycle: daemon killed mid-storm" "kill -9 at +${kill_at}s"
    else
        check_fail "cycle $cycle: daemon killed mid-storm" "no daemon to kill"
    fi

    # The workers keep going against a dead endpoint; their failures are
    # expected and are simply not acknowledged.
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null; done
    cat "$ack".* >>"$ack" 2>/dev/null
    rm -f "$ack".*

    local acked
    acked=$(wc -l <"$ack")
    record "cycle-$cycle-acked-objects" "$acked"

    # Buffer-loss investigation (findings buffer-3/mp-94, mp-61): the
    # store's METADATA exactly as the kill left it, BEFORE recovery gets to
    # rewrite the journal's tail. Opt-in via QSSRT_CRASH_SNAPSHOT_DIR; off,
    # nothing changes. The block fanout is listed rather than copied -- a
    # RECORD ABSENT corpse is fjall's journal and sstables, and the listing
    # proves which files existed without copying gigabytes per cycle. The
    # operator (or the driver) deletes snapshots of cycles that verified
    # clean; one that reproduces the loss ships as the fjall-rs report.
    if [ -n "${QSSRT_CRASH_SNAPSHOT_DIR:-}" ]; then
        local snap entry
        snap="$QSSRT_CRASH_SNAPSHOT_DIR/$label-cycle$cycle"
        mkdir -p "$snap/store/blocks"
        for entry in "$QSSRT_S3_STORE"/* "$QSSRT_S3_STORE"/.[!.]*; do
            [ -e "$entry" ] || continue
            [ "$(basename "$entry")" = blocks ] && continue
            cp -a "$entry" "$snap/store/"
        done
        [ -d "$QSSRT_S3_STORE/blocks/.db" ] &&
            cp -a "$QSSRT_S3_STORE/blocks/.db" "$snap/store/blocks/"
        find "$QSSRT_S3_STORE/blocks" -path '*/.db' -prune -o \
            -type f -printf '%P\t%s\n' >"$snap/block-files.tsv" 2>/dev/null
        cp "$ack" "$snap/acked.tsv"
        check_pass "cycle $cycle: post-kill metadata snapshot" "$snap"
    fi

    s3d_start "$durability" ||
        check_fail "cycle $cycle: daemon restarts after kill -9" "it did not come back"

    # The claim under test: everything the client saw succeed is readable and
    # byte-identical afterwards.
    local key size bad=0 checked=0 want got exists
    while IFS=$'\t' read -r key size; do
        [ -n "$key" ] || continue
        checked=$((checked + 1))
        want=$(gen_md5 "$key" "$size")
        got=$(s3_get_stream "$bucket" "$key" 2>/dev/null | md5sum | cut -d' ' -f1)
        if [ "$want" != "$got" ]; then
            # One retry after a beat: a GET that hiccups right after the
            # restart reads as the md5 of an empty stream, which is not
            # the same verdict as an object that is gone.
            sleep 2
            got=$(s3_get_stream "$bucket" "$key" 2>/dev/null | md5sum | cut -d' ' -f1)
        fi
        if [ "$want" != "$got" ]; then
            # Name which failure this IS: a record that answers HEAD with
            # wrong bytes is a content problem; no record at all is loss.
            if s3api head-object --bucket "$bucket" --key "$key" \
                >/dev/null 2>&1; then
                exists="record present"
            else
                exists="RECORD ABSENT"
            fi
            bad=$((bad + 1))
            [ "$bad" -le 5 ] &&
                check_fail "cycle $cycle: acknowledged object survives the crash" \
                    "$key: generated $want, read back $got ($exists)"
        fi
    done <"$ack"

    if [ "$bad" = 0 ]; then
        check_pass "cycle $cycle: all $checked acknowledged objects readable and identical"
    elif [ "$bad" -gt 5 ]; then
        check_fail "cycle $cycle: acknowledged objects lost or altered" \
            "$bad of $checked failed the byte comparison"
    fi

    # Residue: leak classes only, then a repair that converges.
    s3d_stop
    fsck_run "cycle$cycle-before-repair"
    fsck_assert_leak_only "cycle $cycle: post-crash residue is leak-class only"
    record "cycle-$cycle-residue" "$(fsck_summary)"

    fsck_run "cycle$cycle-repair" --repair
    fsck_run "cycle$cycle-after-repair"
    fsck_assert_converged "cycle $cycle: repair converges"
}

# Creates a bucket if it is not there, tolerating the race with a re-run.
s3_ensure_bucket() {
    s3api head-bucket --bucket "$1" >/dev/null 2>&1 && return 0
    s3api create-bucket --bucket "$1" >/dev/null 2>&1
    return 0
}
