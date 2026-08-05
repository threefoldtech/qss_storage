#!/usr/bin/env bash
# tests/real/lib.sh -- the only file a phase sources.
#
# Owns the check vocabulary (PASS / FAIL / FINDING / SKIP), the phase
# lifecycle, and the grading fold that turns check lines into the campaign's
# exit code. Everything else lives in lib/*.sh and is sourced from here.
#
# Grading, fixed here and nowhere else (ADR 0009's verdict contract):
#
#   any FAIL     -> the run exits 2 (a pass criterion was violated)
#   else FINDING -> the run exits 1 (a deviation worth filing, not loss)
#   else            the run exits 0
#
# SKIP never moves the exit code but is always listed in the verdict: a
# check that could not run is not a check that passed.
#
# Phases never call `exit` on a failed assertion and never run under a bare
# `set -e`. A failing check has to be RECORDED; a phase that aborts on its
# first failure hides everything after it.

[ -n "${QSSRT_LIB_SOURCED:-}" ] && return 0
QSSRT_LIB_SOURCED=1

QSSRT_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export QSSRT_LIB_DIR

# shellcheck source=tests/real/lib/util.sh
. "$QSSRT_LIB_DIR/lib/util.sh"
# shellcheck source=tests/real/lib/gen.sh
. "$QSSRT_LIB_DIR/lib/gen.sh"
# shellcheck source=tests/real/lib/rail.sh
. "$QSSRT_LIB_DIR/lib/rail.sh"
# shellcheck source=tests/real/lib/daemon.sh
. "$QSSRT_LIB_DIR/lib/daemon.sh"
# shellcheck source=tests/real/lib/s3.sh
. "$QSSRT_LIB_DIR/lib/s3.sh"
# shellcheck source=tests/real/lib/resp.sh
. "$QSSRT_LIB_DIR/lib/resp.sh"
# shellcheck source=tests/real/lib/fsck.sh
. "$QSSRT_LIB_DIR/lib/fsck.sh"
# shellcheck source=tests/real/lib/crash.sh
. "$QSSRT_LIB_DIR/lib/crash.sh"
# shellcheck source=tests/real/lib/perf.sh
. "$QSSRT_LIB_DIR/lib/perf.sh"
# shellcheck source=tests/real/lib/verdict.sh
. "$QSSRT_LIB_DIR/lib/verdict.sh"
# shellcheck source=tests/real/lib/report.sh
. "$QSSRT_LIB_DIR/lib/report.sh"

# --- phase lifecycle ---------------------------------------------------

# Opens a phase: its directory, its checks file, its timer.
#
# Re-entrant on purpose. A phase re-run inside the same run directory
# appends to the same checks file, so `--phase 6` twice reads as six cycles
# rather than three plus a lie.
phase_begin() {
    QSSRT_PHASE_NN="$1"
    QSSRT_PHASE_NAME="$2"
    QSSRT_PHASE_DIR="$QSSRT_RUN_DIR/phase-${QSSRT_PHASE_NN}-${QSSRT_PHASE_NAME}"
    QSSRT_PHASE_CHECKS="$QSSRT_PHASE_DIR/checks.tsv"
    QSSRT_PHASE_START=$(date +%s)
    QSSRT_PHASE_EXPECTED_ERRORS=()
    mkdir -p "$QSSRT_PHASE_DIR"
    : >>"$QSSRT_PHASE_CHECKS"
    daemon_log_mark
    log "=== phase $QSSRT_PHASE_NN ($QSSRT_PHASE_NAME) starting at $(date -Is) ==="
}

# Closes a phase: runs the daemon-side error gate, writes the timing file,
# prints the phase summary, and returns the phase's worst status as an exit
# code (0 clean, 1 findings, 2 fail).
phase_end() {
    daemon_gate
    local elapsed=$(($(date +%s) - QSSRT_PHASE_START))
    printf 'phase\t%s\nname\t%s\nseconds\t%s\n' \
        "$QSSRT_PHASE_NN" "$QSSRT_PHASE_NAME" "$elapsed" \
        >"$QSSRT_PHASE_DIR/timing.tsv"

    local pass fail find_ skip
    pass=$(phase_count PASS)
    fail=$(phase_count FAIL)
    find_=$(phase_count FINDING)
    skip=$(phase_count SKIP)
    log "=== phase $QSSRT_PHASE_NN ($QSSRT_PHASE_NAME) done in ${elapsed}s:" \
        "$pass pass, $fail fail, $find_ finding, $skip skip ==="

    [ "$fail" -gt 0 ] && return 2
    [ "$find_" -gt 0 ] && return 1
    return 0
}

# How many check lines of a status this phase has recorded.
phase_count() {
    local n
    [ -f "${QSSRT_PHASE_CHECKS:-}" ] || {
        printf '0'
        return 0
    }
    # grep -c prints 0 and exits 1 on no match; the substitution wants the
    # number either way.
    n=$(grep -c "^$1"$'\t' "$QSSRT_PHASE_CHECKS" 2>/dev/null)
    printf '%s' "${n:-0}"
}

# --- logging -----------------------------------------------------------

# A phase's stdout and stderr are already redirected into its log by the
# driver, so narration is a plain echo with a timestamp.
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
logf() {
    local fmt="$1"
    shift
    # shellcheck disable=SC2059  # the format string is the point
    printf "[%s] $fmt\n" "$(date +%H:%M:%S)" "$@"
}

# --- the check vocabulary ----------------------------------------------

_qssrt_check() {
    local status="$1" desc="$2" evidence="${3:-}"
    printf '%s\t%s\t%s\n' \
        "$status" "$(qssrt_oneline "$desc")" "$(qssrt_oneline "$evidence")" \
        >>"$QSSRT_PHASE_CHECKS"
    printf '[%s] %-7s %s%s\n' "$(date +%H:%M:%S)" "$status" "$desc" \
        "${evidence:+ -- $(qssrt_oneline "$evidence")}"
}

check_pass() { _qssrt_check PASS "$1" "${2:-}"; }
check_fail() { _qssrt_check FAIL "$1" "${2:-}"; }
check_find() { _qssrt_check FINDING "$1" "${2:-}"; }
check_skip() { _qssrt_check SKIP "$1" "${2:-}"; }

# Records a measurement: never graded, always in the verdict's evidence.
# Throughput numbers and reclaim curves go through here -- a number nobody
# has measured before is not a pass criterion.
record() {
    printf '%s\t%s\n' "$1" "$2" >>"$QSSRT_PHASE_DIR/measurements.tsv"
    log "measured: $1 = $2"
}

# --- assertions --------------------------------------------------------

assert_eq() {
    local desc="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        check_pass "$desc" "$got"
    else
        check_fail "$desc" "expected [$want], got [$got]"
    fi
}

# Same comparison, graded as a deviation rather than a violation. For
# client-visible behaviour that is wrong but is not loss.
assert_eq_find() {
    local desc="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        check_pass "$desc" "$got"
    else
        check_find "$desc" "expected [$want], got [$got]"
    fi
}

# Runs a command; it must succeed. Output lands in the phase's out/ dir and
# is quoted as evidence when it does not.
assert_ok() {
    local desc="$1"
    shift
    if qssrt_run "$@"; then
        check_pass "$desc"
    else
        check_fail "$desc" "exit $QSSRT_LAST_STATUS: $(tail -c 400 "$QSSRT_LAST_OUT")"
    fi
}

# Runs a command; it must fail, and its output must name `code`. The code is
# the S3 error code (or any distinctive substring) the phase expects -- an
# operation that fails for the wrong reason has not been tested.
assert_err() {
    local desc="$1" code="$2"
    shift 2
    if qssrt_run "$@"; then
        check_fail "$desc" "expected failure naming $code, but the command succeeded"
    elif grep -qF "$code" "$QSSRT_LAST_OUT"; then
        check_pass "$desc" "$code"
    else
        check_fail "$desc" "failed without naming $code: $(tail -c 400 "$QSSRT_LAST_OUT")"
    fi
}

# Byte-compares two files.
assert_same() {
    local desc="$1" a="$2" b="$3" out
    if out=$(cmp "$a" "$b" 2>&1); then
        check_pass "$desc"
    else
        check_fail "$desc" "$out"
    fi
}

# Byte-compares a stored object against the generator, streaming: nothing is
# ever written to disk twice, which is what makes the terabyte phase
# possible at all.
assert_same_as_generated() {
    local desc="$1" bucket="$2" key="$3" size="$4" want got
    want=$(gen_md5 "$key" "$size")
    got=$(s3_get_stream "$bucket" "$key" | md5sum | cut -d' ' -f1)
    if [ "$want" = "$got" ]; then
        check_pass "$desc" "md5 $want"
    else
        check_fail "$desc" "generated md5 $want, stored md5 $got"
    fi
}

# --- command running ---------------------------------------------------

# Runs a command with its combined output captured to a file. Sets
# QSSRT_LAST_OUT (path) and QSSRT_LAST_STATUS; returns the command's status.
#
# Every client invocation in the campaign goes through here so that no
# failure is ever reported without the tool's own words attached.
qssrt_run() {
    local dir="${QSSRT_PHASE_DIR:-${TMPDIR:-/tmp}}/out"
    mkdir -p "$dir"
    QSSRT_LAST_OUT="$dir/cmd-$$-${QSSRT_CMD_SEQ:-0}.txt"
    QSSRT_CMD_SEQ=$((${QSSRT_CMD_SEQ:-0} + 1))
    "$@" >"$QSSRT_LAST_OUT" 2>&1
    QSSRT_LAST_STATUS=$?
    return $QSSRT_LAST_STATUS
}

# The last command's output, for a phase that wants to inspect it.
qssrt_last_out() { cat "$QSSRT_LAST_OUT"; }
