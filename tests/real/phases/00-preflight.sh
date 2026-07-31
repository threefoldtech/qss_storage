#!/usr/bin/env bash
# Phase 0: preflight.
#
# ADR 0009: "mount/fs/ownership/sentinel checks; tool versions; release
# build; store empty or explicitly resumed (--resume)."
#
# This phase is the rail. If it records a failure the driver stops, because
# every later phase writes to a disk this one has not vouched for.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 00 preflight

# --- the hardware ------------------------------------------------------

rail_check_hardware
rail_check_store_root || {
    phase_end
    exit $?
}

# --- the tools ---------------------------------------------------------

# Refusal rather than silent degradation (ADR 0009 ground rule 4). The one
# exception is a smoke run with the rail waived: there, a missing client
# downgrades the phases that need it to SKIP rather than failing a harness
# test on a machine that was never going to run the campaign.
require_tool() {
    local tool="$1" why="$2"
    if qssrt_have "$tool"; then
        check_pass "$tool is installed" "$(command -v "$tool")"
    elif rail_waived; then
        check_skip "$tool is installed" "absent; $why"
    else
        check_fail "$tool is installed" "absent, and $why"
    fi
}

require_tool "$QSSRT_AWS" "the S3 phases are aws-cli"
require_tool "$QSSRT_VALKEY_CLI" "phase 4 is valkey-cli"
for tool in cmp md5sum openssl xargs stat df curl find awk sed; do
    require_tool "$tool" "the harness is built on it"
done
# jq is how the fsck assertions read a report; lib/fsck.sh falls back to the
# text report's documented summary line without it, so its absence is a
# finding rather than a refusal.
if qssrt_have jq; then
    check_pass "jq is installed" "$(command -v jq)"
else
    check_find "jq is installed" \
        "absent: fsck assertions fall back to parsing the text report"
fi

aws_version=$("$QSSRT_AWS" --version 2>&1 | head -n 1)
record "aws-cli" "$aws_version"
case "$aws_version" in
aws-cli/2.*) check_pass "aws-cli is version 2" "$aws_version" ;;
*) check_fail "aws-cli is version 2" "$aws_version; the phases are written against v2" ;;
esac
record "valkey-cli" "$("$QSSRT_VALKEY_CLI" --version 2>&1 | head -n 1)"
record "bash" "$BASH_VERSION"

# --- the binaries ------------------------------------------------------

for bin in s3cas respd qss-storage-fsck; do
    if [ -x "$QSSRT_BIN_DIR/$bin" ]; then
        check_pass "$bin is built" "$(stat -c '%s bytes, %y' "$QSSRT_BIN_DIR/$bin")"
    else
        check_fail "$bin is built" "not at $QSSRT_BIN_DIR/$bin"
    fi
done
if [ "$QSSRT_PROFILE" = release ]; then
    check_pass "the campaign runs release binaries" "$QSSRT_BIN_DIR"
else
    check_find "the campaign runs release binaries" \
        "profile is $QSSRT_PROFILE: this measures something we do not ship"
fi

# --- the configuration -------------------------------------------------

if [ -f "$QSSRT_DAEMON_CONFIG" ]; then
    check_pass "daemon config present" "$QSSRT_DAEMON_CONFIG"

    # The credentials live in two files that must agree: the daemon reads
    # the toml, aws-cli reads what campaign.conf put in the run directory.
    # Drift between them is a whole phase of 403s and a confused operator.
    toml_ak=$(sed -n 's/^access_key *= *"\(.*\)"/\1/p' "$QSSRT_DAEMON_CONFIG" | head -n 1)
    toml_sk=$(sed -n 's/^secret_key *= *"\(.*\)"/\1/p' "$QSSRT_DAEMON_CONFIG" | head -n 1)
    assert_eq "access key agrees between campaign.conf and the daemon config" \
        "$QSSRT_ACCESS_KEY" "$toml_ak"
    assert_eq "secret key agrees between campaign.conf and the daemon config" \
        "$QSSRT_SECRET_KEY" "$toml_sk"

    toml_dur=$(sed -n 's/^durability *= *"\(.*\)"/\1/p' "$QSSRT_DAEMON_CONFIG" | head -n 1)
    assert_eq "the campaign's default durability is fsync" fsync "$toml_dur"
else
    check_fail "daemon config present" "not at $QSSRT_DAEMON_CONFIG"
fi

# --- the store ---------------------------------------------------------

if [ "${QSSRT_FRESH:-0}" = "1" ]; then
    rail_fresh
elif ! rail_store_root_empty; then
    if [ "${QSSRT_RESUME:-0}" = "1" ]; then
        check_pass "store precondition" "non-empty, --resume given"
    else
        check_fail "store precondition" \
            "$QSSRT_STORE_ROOT is not empty; pass --resume to run against it or --fresh to wipe it"
    fi
else
    check_pass "store precondition" "empty store root"
fi
mkdir -p "$QSSRT_S3_STORE" "$QSSRT_RESP_STORE"

# --- the ports ---------------------------------------------------------

# Nothing may be listening yet. A daemon left over from an earlier run
# answers on the campaign's port with somebody else's store behind it, and
# every phase then measures that store instead of this one -- silently, and
# with total confidence.
port_free() {
    local host="$1" port="$2" label="$3"
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
        check_fail "$label port $host:$port is free" \
            "something is already listening; stop it before running the campaign"
    else
        check_pass "$label port $host:$port is free"
    fi
}
port_free "$QSSRT_S3_HOST" "$QSSRT_S3_PORT" S3
port_free "$QSSRT_METRICS_HOST" "$QSSRT_METRICS_PORT" metrics
port_free "$QSSRT_RESP_HOST" "$QSSRT_RESP_PORT" RESP

# --- space -------------------------------------------------------------

free=$(qssrt_free_bytes "$QSSRT_STORE_ROOT")
total=$(qssrt_total_bytes "$QSSRT_STORE_ROOT")
record "filesystem-total" "$(qssrt_human "${total:-0}")"
record "filesystem-free" "$(qssrt_human "${free:-0}")"

# What the selected phases will write. The terabyte is its own arithmetic;
# everything else is dominated by phase 1's single-part object and phase 2's
# multipart, plus stress.
need=$((
    $(qssrt_scaled "$(qssrt_bytes "$QSSRT_SINGLE_PART_BYTES")" \
        "$(qssrt_bytes "$QSSRT_SINGLE_PART_FLOOR")") +
        $(qssrt_scaled "$(qssrt_bytes "$QSSRT_MULTIPART_BYTES")" \
            "$(qssrt_bytes "$QSSRT_MULTIPART_FLOOR")") * 2 +
        $(qssrt_bytes 8GiB)
))
if [ "${QSSRT_TB:-0}" = "1" ]; then
    target=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_TARGET")" 1)
    cap=$((total * QSSRT_TB_FS_CAP_PERCENT / 100))
    record "tb-target" "$(qssrt_human "$target")"
    record "tb-cap-at-${QSSRT_TB_FS_CAP_PERCENT}-percent" "$(qssrt_human "$cap")"
    if [ "$target" -gt "$cap" ]; then
        # A refusal, not a truncation: a disk that cannot hold the
        # configured target must not quietly run a smaller campaign than the
        # verdict will claim.
        check_fail "the terabyte fits under the ${QSSRT_TB_FS_CAP_PERCENT}% cap" \
            "target $(qssrt_human "$target") exceeds $(qssrt_human "$cap"); lower QSSRT_TB_TARGET or raise the cap deliberately"
    else
        check_pass "the terabyte fits under the ${QSSRT_TB_FS_CAP_PERCENT}% cap" \
            "$(qssrt_human "$target") of $(qssrt_human "$cap")"
    fi
    need=$target
fi

if [ "${free:-0}" -ge "$need" ]; then
    check_pass "free space covers the selected phases" \
        "need $(qssrt_human "$need"), have $(qssrt_human "$free")"
else
    check_fail "free space covers the selected phases" \
        "need $(qssrt_human "$need"), have $(qssrt_human "$free")"
fi

# --- results live outside the store ------------------------------------

case "$QSSRT_RUN_DIR/" in
"$QSSRT_STORE_ROOT"/*)
    check_fail "results live outside the store" \
        "$QSSRT_RUN_DIR is inside $QSSRT_STORE_ROOT: fsck would report the campaign's own bookkeeping as foreign files"
    ;;
*)
    check_pass "results live outside the store" "$QSSRT_RUN_DIR"
    ;;
esac

phase_end
exit $?
