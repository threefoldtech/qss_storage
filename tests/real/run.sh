#!/usr/bin/env bash
# tests/real/run.sh -- the campaign's single entrypoint (ADR 0009).
#
# Runs the numbered phases against the designated hardware, captures
# everything into target/realtest/<timestamp>/, and writes a verdict.
#
# It is not CI. It consumes the disk it runs on, it kills daemons on
# purpose, and a full run is hours. See docs/realtest.md.
#
# Exit codes, the scripting contract: 0 pass, 1 findings, 2 fail.

set -uo pipefail

QSSRT_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
QSSRT_REAL_DIR="$QSSRT_REPO_ROOT/tests/real"
export QSSRT_REPO_ROOT QSSRT_REAL_DIR

# shellcheck source=tests/real/campaign.conf
. "$QSSRT_REAL_DIR/campaign.conf"
# shellcheck source=tests/real/lib.sh
. "$QSSRT_REAL_DIR/lib.sh"

# --- flags -------------------------------------------------------------

QSSRT_SELECTED=()
QSSRT_TB=0
QSSRT_FRESH=0
QSSRT_RESUME=0
QSSRT_BUILD=1
QSSRT_FROM=""

usage() {
    cat <<'EOF'
usage: tests/real/run.sh [options]

  --phase N        run phase N (repeatable). Preflight always runs.
  --from N         run every phase from N onwards
  --tb             include phase 10, the terabyte. It replaces phases 5, 8
                   and 9 rather than following them: it owns the disk for a
                   session and covers their ground at scale.
  --fresh          wipe the campaign's own store directory first, after
                   fsck confirms it is a qss store. Never the mount.
  --resume         run against the store as it stands
  --scale N        divide every configured size by N (smoke runs)
  --no-build       do not run cargo build --release first
  --list           list the phases and exit
  --print-config   print the resolved configuration and exit
  --selftest       run the harness's own tests and exit
  --help

The campaign runs on the designated disk only. To exercise the harness
somewhere else, set QSSRT_UNSAFE_ALLOW_ANY_PATH=1 and a QSSRT_MOUNT of your
own: the hardware rail becomes a list of SKIPs and the verdict is stamped
NOT CAMPAIGN GRADE.

Exit codes: 0 pass, 1 findings, 2 fail.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
    --phase)
        QSSRT_SELECTED+=("$2")
        shift 2
        ;;
    --from)
        QSSRT_FROM="$2"
        shift 2
        ;;
    --tb)
        QSSRT_TB=1
        shift
        ;;
    --fresh)
        QSSRT_FRESH=1
        shift
        ;;
    --resume)
        QSSRT_RESUME=1
        shift
        ;;
    --scale)
        QSSRT_SCALE="$2"
        shift 2
        ;;
    --no-build)
        QSSRT_BUILD=0
        shift
        ;;
    --list)
        for f in "$QSSRT_REAL_DIR"/phases/[0-9][0-9]-*.sh; do
            [ -e "$f" ] || continue
            printf '%s\n' "$(basename "$f" .sh)"
        done
        exit 0
        ;;
    --print-config)
        # `set` rather than `env`: the campaign config is sourced, not
        # exported, until a phase is actually launched.
        (
            set -o posix
            set
        ) | grep '^QSSRT_' | sort
        printf -- '--- derived ---\n'
        printf 'single-part  %s\n' "$(qssrt_human "$(qssrt_scaled "$(qssrt_bytes "$QSSRT_SINGLE_PART_BYTES")" "$(qssrt_bytes "$QSSRT_SINGLE_PART_FLOOR")")")"
        printf 'multipart    %s\n' "$(qssrt_human "$(qssrt_scaled "$(qssrt_bytes "$QSSRT_MULTIPART_BYTES")" "$(qssrt_bytes "$QSSRT_MULTIPART_FLOOR")")")"
        printf 'list keys    %s\n' "$(qssrt_scaled "$QSSRT_LIST_KEYS" "$QSSRT_LIST_KEYS_FLOOR")"
        printf 'stress secs  %s\n' "$(qssrt_scaled "$QSSRT_STRESS_SECONDS" "$QSSRT_STRESS_SECONDS_FLOOR")"
        printf 'tb target    %s\n' "$(qssrt_human "$(qssrt_scaled "$(qssrt_bytes "$QSSRT_TB_TARGET")" 1)")"
        exit 0
        ;;
    --selftest)
        exec bash "$QSSRT_REAL_DIR/lib/selftest.sh"
        ;;
    --help | -h)
        usage
        exit 0
        ;;
    *)
        printf 'unknown option: %s\n\n' "$1" >&2
        usage >&2
        exit 2
        ;;
    esac
done

export QSSRT_SCALE QSSRT_FRESH QSSRT_RESUME QSSRT_TB

# --- phase selection ---------------------------------------------------

# Every phase script on disk, by number.
qssrt_all_phases() {
    local f
    for f in "$QSSRT_REAL_DIR"/phases/[0-9][0-9]-*.sh; do
        [ -e "$f" ] || continue
        basename "$f" | cut -c1-2
    done
}

# What this invocation runs. Preflight is always in: it is the rail.
#
# --tb replaces 5, 8 and 9 rather than adding to them (ADR 0009 phase 10:
# "owns the disk for a session, replaces phases 5/8/9's scale rather than
# repeating them").
qssrt_plan() {
    local n plan=()
    if [ ${#QSSRT_SELECTED[@]} -gt 0 ]; then
        plan=("${QSSRT_SELECTED[@]}")
    else
        for n in $(qssrt_all_phases); do
            [ "$n" = 10 ] && continue
            if [ "$QSSRT_TB" = 1 ]; then
                case "$n" in 05 | 08 | 09) continue ;; esac
            fi
            [ -n "$QSSRT_FROM" ] && [ "$((10#$n))" -lt "$((10#$QSSRT_FROM))" ] && continue
            plan+=("$n")
        done
        [ "$QSSRT_TB" = 1 ] && plan+=(10)
    fi
    printf '00\n'
    for n in "${plan[@]}"; do
        printf '%02d\n' "$((10#$n))"
    done
    : # keep the function's status clean whatever the last test decided
}

qssrt_phase_script() {
    local f
    for f in "$QSSRT_REAL_DIR"/phases/"$1"-*.sh; do
        [ -e "$f" ] && {
            printf '%s' "$f"
            return 0
        }
    done
    return 1
}

# --- the run -----------------------------------------------------------

QSSRT_RUN_DIR="$QSSRT_RESULTS_ROOT/$(date +%Y%m%dT%H%M%S)"
mkdir -p "$QSSRT_RUN_DIR/daemon"
export QSSRT_RUN_DIR

# Everything a phase needs, exported once. Phases run as subprocesses so a
# phase that dies cannot take the driver with it.
set -a
# shellcheck disable=SC1090  # re-sourced to export, the path is computed
. "$QSSRT_REAL_DIR/campaign.conf"
set +a

if [ "$QSSRT_BUILD" = 1 ]; then
    printf 'building release binaries...\n'
    if ! (cd "$QSSRT_REPO_ROOT" && cargo build --release --workspace \
        >"$QSSRT_RUN_DIR/build.log" 2>&1); then
        printf 'cargo build --release failed; see %s\n' "$QSSRT_RUN_DIR/build.log" >&2
        exit 2
    fi
fi

s3_setup_client

{
    printf 'run\t%s\n' "$(basename "$QSSRT_RUN_DIR")"
    printf 'date\t%s\n' "$(date -Is)"
    printf 'host\t%s\n' "$(uname -a)"
    printf 'git\t%s\n' "$(cd "$QSSRT_REPO_ROOT" && git rev-parse HEAD 2>/dev/null)"
    printf 'git_dirty\t%s\n' \
        "$(cd "$QSSRT_REPO_ROOT" && git status --porcelain 2>/dev/null | wc -l)"
    printf 'scale\t%s\n' "$QSSRT_SCALE"
    printf 'seed\t%s\n' "$QSSRT_SEED"
    printf 'mount\t%s\n' "$QSSRT_MOUNT"
    printf 'store\t%s\n' "$QSSRT_STORE_ROOT"
    printf 'rail_waived\t%s\n' "${QSSRT_UNSAFE_ALLOW_ANY_PATH:-0}"
    printf 'aws\t%s\n' "$("$QSSRT_AWS" --version 2>&1 | head -n 1)"
    printf 'valkey-cli\t%s\n' "$("$QSSRT_VALKEY_CLI" --version 2>&1 | head -n 1)"
    printf 'bash\t%s\n' "$BASH_VERSION"
    for b in s3cas respd qss-storage-fsck; do
        printf 'bin_%s\t%s\n' "$b" \
            "$(stat -c '%n %s %y' "$QSSRT_BIN_DIR/$b" 2>/dev/null || echo MISSING)"
    done
} >"$QSSRT_RUN_DIR/run.env"

printf 'campaign run: %s\n' "$QSSRT_RUN_DIR"
[ "${QSSRT_UNSAFE_ALLOW_ANY_PATH:-0}" = "1" ] &&
    printf '*** NOT CAMPAIGN GRADE: the hardware rail is waived ***\n'

for nn in $(qssrt_plan); do
    script=$(qssrt_phase_script "$nn") || {
        printf 'no phase script numbered %s\n' "$nn" >&2
        continue
    }
    name=$(basename "$script" .sh)
    printf -- '--- %s ---\n' "$name"
    phase_dir="$QSSRT_RUN_DIR/phase-$name"
    mkdir -p "$phase_dir"

    bash "$script" >"$phase_dir/log" 2>&1
    rc=$?
    tail -n 3 "$phase_dir/log"

    # A phase that died without recording a failure gets one: an exit code
    # nobody wrote down is exactly the kind of silence this campaign exists
    # to remove.
    if [ "$rc" -gt 1 ] &&
        ! grep -q '^FAIL'$'\t' "$phase_dir/checks.tsv" 2>/dev/null; then
        printf 'FAIL\tphase %s exited %s without recording a failure\tsee %s/log\n' \
            "$name" "$rc" "$phase_dir" >>"$phase_dir/checks.tsv"
    fi

    # Preflight is the rail. If it refused, nothing after it should run.
    if [ "$nn" = "00" ] && grep -q '^FAIL'$'\t' "$phase_dir/checks.tsv" 2>/dev/null; then
        printf 'preflight refused; stopping\n'
        break
    fi
done

# Whatever happened, the disk is left without a daemon holding its lock.
QSSRT_PHASE_DIR="$QSSRT_RUN_DIR" \
    QSSRT_PHASE_CHECKS="$QSSRT_RUN_DIR/teardown.tsv" \
    s3d_ensure_stopped
respd_ensure_stopped

code=$(verdict_render "$QSSRT_RUN_DIR")
printf '\n%s\n' "$(head -n 3 "$QSSRT_RUN_DIR/verdict.md" | tail -n 1)"
printf 'verdict: %s\n' "$QSSRT_RUN_DIR/verdict.md"
exit "$code"
