#!/usr/bin/env bash
# Small shared utilities: sizes, text, tool probing, retries, randomness.
#
# Sourced through lib.sh. Nothing here writes a check line -- these are the
# primitives the check vocabulary is built from.

# --- text --------------------------------------------------------------

# Squashes a value onto one line so it can live in a TSV evidence column.
# Tabs and newlines become spaces; runs of spaces collapse; the result is
# clipped so one runaway error message cannot make verdict.md unreadable.
qssrt_oneline() {
    local text="$*" max=${QSSRT_EVIDENCE_MAX:-400}
    text=${text//$'\t'/ }
    text=${text//$'\n'/ }
    text=${text//$'\r'/ }
    while [[ $text == *"  "* ]]; do text=${text//  / }; done
    text=${text# }
    text=${text% }
    if [ ${#text} -gt "$max" ]; then
        text="${text:0:$max}..."
    fi
    printf '%s' "$text"
}

# --- sizes -------------------------------------------------------------

# Parses a human size ("4GiB", "512K", "1048576") into bytes on stdout.
# Accepts the IEC units the config is written in; refuses anything else
# loudly, because a silently mis-parsed size is a silently mis-run campaign.
qssrt_bytes() {
    local raw="${1:-}" num unit
    if [ -z "$raw" ]; then
        printf 'qssrt_bytes: empty size\n' >&2
        return 1
    fi
    num=${raw%%[!0-9]*}
    unit=${raw#"$num"}
    if [ -z "$num" ]; then
        printf 'qssrt_bytes: %s does not start with a number\n' "$raw" >&2
        return 1
    fi
    case "$unit" in
    '' | B | b) printf '%s' "$((num))" ;;
    K | KiB | k) printf '%s' "$((num * 1024))" ;;
    M | MiB | m) printf '%s' "$((num * 1024 * 1024))" ;;
    G | GiB | g) printf '%s' "$((num * 1024 * 1024 * 1024))" ;;
    T | TiB | t) printf '%s' "$((num * 1024 * 1024 * 1024 * 1024))" ;;
    *)
        printf 'qssrt_bytes: unknown unit %s in %s\n' "$unit" "$raw" >&2
        return 1
        ;;
    esac
}

# Renders bytes the way an operator reads them.
qssrt_human() {
    local b=${1:-0}
    if command -v numfmt >/dev/null 2>&1; then
        numfmt --to=iec-i --suffix=B "$b"
    else
        printf '%sB' "$b"
    fi
}

# Divides a byte count by the scale knob, never below a floor. The floor is
# what keeps a SCALE=4096 smoke run from turning a multipart test into a
# single-part one.
qssrt_scaled() {
    local bytes=$1 floor=${2:-1} scale=${QSSRT_SCALE:-1} out
    [ "$scale" -ge 1 ] 2>/dev/null || scale=1
    out=$((bytes / scale))
    [ "$out" -lt "$floor" ] && out=$floor
    printf '%s' "$out"
}

# --- tools -------------------------------------------------------------

# True when a tool is on PATH.
qssrt_have() { command -v "$1" >/dev/null 2>&1; }

# --- disk --------------------------------------------------------------

# Free bytes on the filesystem holding a path.
qssrt_free_bytes() {
    df -B1 --output=avail "$1" 2>/dev/null | tail -n 1 | tr -d ' '
}

# Total bytes of the filesystem holding a path.
qssrt_total_bytes() {
    df -B1 --output=size "$1" 2>/dev/null | tail -n 1 | tr -d ' '
}

# Apparent size in bytes of a directory tree, 0 when it does not exist.
qssrt_du_bytes() {
    [ -d "$1" ] || {
        printf '0'
        return 0
    }
    du -sb "$1" 2>/dev/null | cut -f1
}

# Live block files under a store's blocks root: one fanout level, full-hex
# names. The store's own database, the write staging area and the
# quarantine are pruned -- they are not block data (docs/fsck.md, "the
# store's own files are not foreign").
qssrt_block_files() {
    local blocks="$1/blocks"
    [ -d "$blocks" ] || return 0
    # Block files sit at exactly depth 2: blocks/<fanout>/<full-hex>. So do
    # the shared database's files, hence the three path exclusions.
    find "$blocks" -mindepth 2 -maxdepth 2 -type f \
        -not -path "$blocks/db/*" \
        -not -path "$blocks/.tmp/*" \
        -not -path "$blocks/.quarantine/*" \
        -print 2>/dev/null
}

qssrt_block_file_count() { qssrt_block_files "$1" | wc -l; }

# Bytes held by block data files only (not the metadata databases).
qssrt_block_bytes() {
    local total=0 f
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        total=$((total + $(stat -c %s "$f" 2>/dev/null || echo 0)))
    done < <(qssrt_block_files "$1")
    printf '%s' "$total"
}

# --- randomness --------------------------------------------------------

# A random integer in [lo, hi]. Used for crash timing, which the ADR wants
# randomized so a crash window is not always the same window.
qssrt_rand_between() {
    local lo=$1 hi=$2 span=$((hi - lo + 1))
    [ "$span" -gt 0 ] || span=1
    printf '%s' "$((lo + RANDOM % span))"
}

# --- retries -----------------------------------------------------------

# Runs a command until it succeeds or the deadline passes. Used only for
# waiting on a daemon to come up, never to paper over a client error: a
# retried PUT that finally works is not the same statement as a PUT that
# worked.
qssrt_wait_for() {
    local seconds=$1
    shift
    local deadline=$((SECONDS + seconds))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    return 1
}
