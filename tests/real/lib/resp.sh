#!/usr/bin/env bash
# valkey-cli wrappers.
#
# respcas's surface is thinner than S3's -- inline-only storage, no multipart,
# no GC -- so this file is thinner too. What it does carry is the binary
# safety plumbing: RESP values are bytes, and a wrapper that round-trips
# them through a shell variable would quietly stop testing that.

# One command, output on stdout.
vk() {
    "$QSSRT_VALKEY_CLI" -h "$QSSRT_RESP_HOST" -p "$QSSRT_RESP_PORT" "$@"
}

# One command in a namespace: SELECT then the command, on one connection.
# valkey-cli has no --select for arbitrary named namespaces, so the pair
# goes down the same connection through stdin.
vk_ns() {
    local ns="$1"
    shift
    printf 'SELECT %s\n%s\n' "$ns" "$*" |
        "$QSSRT_VALKEY_CLI" -h "$QSSRT_RESP_HOST" -p "$QSSRT_RESP_PORT"
}

# SET with the value read from stdin: the only way to store bytes a shell
# variable would mangle (embedded NUL, high bytes, trailing newlines).
vk_set_stdin() {
    vk -x SET "$1"
}

# GET straight to stdout with no interpretation, for byte comparison.
vk_get_raw() {
    vk --no-raw GET "$1"
}

# The value of a key as raw bytes.
vk_get_bytes() {
    vk GET "$1"
}

# Feeds a batch of commands through the pipe protocol: the client behaviour
# the ADR names (valkey-cli --pipe), which frames differently from one
# command per invocation.
vk_pipe() {
    "$QSSRT_VALKEY_CLI" -h "$QSSRT_RESP_HOST" -p "$QSSRT_RESP_PORT" --pipe
}

# respcas's dispatch surface, re-derived from the source at run time.
#
# The ADR asks for the enumeration to be a tripwire: "if respd grows
# block-backed values, the phase grows with it -- the enumeration step at
# the start of the phase script is the tripwire". Deriving it from the
# dispatch table is the only way that tripwire can fire without a human
# noticing first.
#
# The whole cmd/ tree, not cmd.rs alone. The dispatch table moved into
# cmd/parse.rs when cmd.rs was split (831a2ec) and this function kept
# reading the file it used to be in -- so it returned the empty string, the
# comparison against the pinned list failed, and the phase reported a
# surface change that had not happened while a real one (CSET, ADR 0014)
# went uncovered underneath it. A tripwire that fires on its own absence is
# worse than none: it trains its reader to ignore it.
resp_surface_from_source() {
    local dir="$QSSRT_REPO_ROOT/respcas/src"
    cat "$dir/cmd.rs" "$dir"/cmd/*.rs 2>/dev/null |
        grep -oE '^[[:space:]]*"[A-Z]+" =>' |
        tr -d ' "=>' | sort -u | tr '\n' ' ' | sed 's/ $//'
}

# --- the binary-safe client ---------------------------------------------
#
# valkey-cli stays the client for everything it can express. These wrap the
# campaign's own client for the two things it cannot: keys containing NUL
# (ADR 0014 mode B) and many in-flight commands over stable connections.

respcli() {
    python3 "$QSSRT_REAL_DIR/tools/respcli.py" \
        -H "$QSSRT_RESP_HOST" -p "$QSSRT_RESP_PORT" "$@"
}

# One command in a namespace, reply rendered as asked (text/raw/hex/len).
# Arguments accept the tool's encodings: hex:..., @file, rand:N.
resp_cmd() {
    local ns="$1" reply="$2"
    shift 2
    respcli -s "$ns" cmd --reply "$reply" "$@"
}

# Same, with a namespace password.
resp_cmd_auth() {
    local ns="$1" pw="$2" reply="$3"
    shift 3
    respcli -s "$ns" -w "$pw" cmd --reply "$reply" "$@"
}

# BLAKE3-256 of a file, as hex -- the address ADR 0014 puts on the wire.
resp_b3() { "$QSSRT_BIN_DIR/examples/b3sum" <"$1"; }

# Is the hasher built? Phases degrade to SKIP rather than FAIL without it:
# a missing dev tool is not a storage defect.
resp_have_b3() { [ -x "$QSSRT_BIN_DIR/examples/b3sum" ]; }
