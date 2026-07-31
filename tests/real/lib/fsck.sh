#!/usr/bin/env bash
# qss-storage-fsck invocation, and the finding-class assertions the campaign
# grades stores with.
#
# fsck is offline: fjall's LOCK file makes it and a running daemon mutually
# exclusive, so every run here stops the daemon first and treats exit 3
# (could-not-run) as a phase failure with the lock named.
#
# The class lists below are pinned from cas-storage/src/scrub/findings.rs
# and docs/fsck.md. They are not judgement calls -- they are the ADR 0005
# severity table, transcribed.

# INFO: expected leakage. Crash residue lands here, and after ADR 0008
# nothing else routinely does -- a successful overwrite releases what it
# displaced, so a healthy store's steady-state findings approach zero.
QSSRT_LEAK_CLASSES=(
    refcount_over_count
    orphan_file
    off_depth_file
    adoptable_dangling_record
    degraded_record
    multipart_upload
    orphan_part
)

# What --repair claims to fix. Anything here that survives a repair means
# the repair did not work; fsck itself says so by raising
# post_repair_recount_dirty, which must never appear at all.
QSSRT_REPAIRABLE_CLASSES=(
    refcount_over_count
    refcount_under_count
    orphan_file
    off_depth_file
    adoptable_dangling_record
    orphan_part
    half_deleted_bucket
    corrupt_block
)

QSSRT_FSCK_EXIT=0
QSSRT_FSCK_JSON=""
QSSRT_FSCK_TEXT=""

# Can fsck open this store at all? Used by --fresh before it deletes
# anything: a directory fsck will not open is a directory this campaign does
# not wipe.
fsck_can_open() {
    local rc
    "$QSSRT_BIN_DIR/qss-storage-fsck" \
        --config "$QSSRT_DAEMON_CONFIG" \
        --meta-root "$1" --fs-root "$1" >/dev/null 2>&1
    rc=$?
    # 3 is could-not-run: no store, or a store somebody else has open.
    # Anything else means fsck opened it and had an opinion.
    [ "$rc" -ne 3 ]
}

# fsck_run <label> [extra flags...]
#
# Captures both renderings: the text report an operator reads and the JSON
# the assertions parse. Returns fsck's own exit code and leaves it in
# QSSRT_FSCK_EXIT.
fsck_run() {
    local label="$1"
    shift
    local base="$QSSRT_PHASE_DIR/fsck-$label"

    if s3d_running; then
        s3d_stop
    fi

    "$QSSRT_BIN_DIR/qss-storage-fsck" \
        --config "$QSSRT_DAEMON_CONFIG" \
        --meta-root "$QSSRT_S3_STORE" --fs-root "$QSSRT_S3_STORE" \
        "$@" >"$base.txt" 2>"$base.err"
    QSSRT_FSCK_EXIT=$?

    "$QSSRT_BIN_DIR/qss-storage-fsck" \
        --config "$QSSRT_DAEMON_CONFIG" \
        --meta-root "$QSSRT_S3_STORE" --fs-root "$QSSRT_S3_STORE" \
        --json "$@" >"$base.json" 2>/dev/null

    QSSRT_FSCK_TEXT="$base.txt"
    QSSRT_FSCK_JSON="$base.json"

    if [ "$QSSRT_FSCK_EXIT" = 3 ]; then
        check_fail "fsck ($label) could not run" \
            "exit 3: $(tail -c 300 "$base.err"); a store the daemon still holds fails at the open"
    fi
    log "fsck ($label): exit $QSSRT_FSCK_EXIT -- $(tail -n 1 "$base.txt")"
    return "$QSSRT_FSCK_EXIT"
}

# Findings of one class in the last report.
fsck_count() {
    local class="$1" n
    if qssrt_have jq && [ -s "$QSSRT_FSCK_JSON" ]; then
        n=$(jq --arg c "$class" '[.findings[] | select(.class == $c)] | length' \
            "$QSSRT_FSCK_JSON" 2>/dev/null)
    else
        # grep -c prints its count and exits 1 on no match; the
        # substitution wants the number either way.
        n=$(grep -c "$class" "$QSSRT_FSCK_TEXT" 2>/dev/null)
    fi
    printf '%s' "${n:-0}"
}

# Findings of one severity in the last report. Falls back to the text
# report's documented summary line
# ("N critical, M warn, K info (T finding(s)); exit E") when jq is absent --
# a rendering, not a guess.
fsck_severity() {
    local sev="$1" n
    if qssrt_have jq && [ -s "$QSSRT_FSCK_JSON" ]; then
        n=$(jq -r ".summary.$sev" "$QSSRT_FSCK_JSON" 2>/dev/null)
    else
        n=$(sed -n 's/^\([0-9]*\) critical, \([0-9]*\) warn, \([0-9]*\) info.*/\1 \2 \3/p' \
            "$QSSRT_FSCK_TEXT" | tail -n 1 |
            awk -v s="$sev" '{if (s=="critical") print $1; else if (s=="warn") print $2; else print $3}')
    fi
    printf '%s' "${n:-0}"
}

fsck_total() {
    local n
    if qssrt_have jq && [ -s "$QSSRT_FSCK_JSON" ]; then
        n=$(jq -r '.summary.total' "$QSSRT_FSCK_JSON" 2>/dev/null)
    else
        n=$(($(fsck_severity critical) + $(fsck_severity warn) + $(fsck_severity info)))
    fi
    printf '%s' "${n:-0}"
}

# A one-line rendering of what a report holds, for evidence columns.
fsck_summary() {
    printf 'critical=%s warn=%s info=%s exit=%s' \
        "$(fsck_severity critical)" "$(fsck_severity warn)" \
        "$(fsck_severity info)" "$QSSRT_FSCK_EXIT"
}

# The store is clean: no findings at all. What a healthy store must look
# like after ADR 0008, and what teardown must reconcile to.
fsck_assert_clean() {
    local desc="$1" total
    total=$(fsck_total)
    if [ "$total" = 0 ]; then
        check_pass "$desc" "$(fsck_summary)"
    else
        check_fail "$desc" "$(fsck_summary); $(fsck_findings_brief)"
    fi
}

# Only the documented leak classes: zero WARN, zero CRITICAL. Never an
# under-count, never corruption. This is what a post-crash store may look
# like -- and the ONLY thing it may look like.
fsck_assert_leak_only() {
    local desc="$1" warn critical
    warn=$(fsck_severity warn)
    critical=$(fsck_severity critical)
    if [ "$critical" = 0 ] && [ "$warn" = 0 ]; then
        check_pass "$desc" "$(fsck_summary)"
    else
        check_fail "$desc" "$(fsck_summary); $(fsck_findings_brief)"
    fi
}

# After --repair: nothing --repair claims to fix is still standing, and the
# tool's own convergence check did not fire.
fsck_assert_converged() {
    local desc="$1" class n dirty=0 detail=""
    for class in "${QSSRT_REPAIRABLE_CLASSES[@]}"; do
        n=$(fsck_count "$class")
        if [ "${n:-0}" -gt 0 ]; then
            dirty=$((dirty + n))
            detail="$detail $class=$n"
        fi
    done
    n=$(fsck_count post_repair_recount_dirty)
    if [ "${n:-0}" -gt 0 ]; then
        check_fail "$desc" "post_repair_recount_dirty=$n: the repair did not work"
    elif [ "$dirty" = 0 ]; then
        check_pass "$desc" "$(fsck_summary)"
    else
        check_fail "$desc" "repairable findings survived a repair:$detail"
    fi
}

# The first few findings, class and evidence, for an evidence column.
fsck_findings_brief() {
    if qssrt_have jq && [ -s "$QSSRT_FSCK_JSON" ]; then
        jq -r '[.findings[] | "\(.severity) \(.class)"] | group_by(.) |
               map("\(.[0]) x\(length)") | join(", ")' \
            "$QSSRT_FSCK_JSON" 2>/dev/null
    else
        grep -E '^(INFO|WARN|CRITICAL) ' "$QSSRT_FSCK_TEXT" 2>/dev/null |
            sort | uniq -c | head -n 8 | tr '\n' ';'
    fi
}
