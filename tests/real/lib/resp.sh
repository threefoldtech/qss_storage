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
resp_surface_from_source() {
    grep -oE '^[[:space:]]*"[A-Z]+" =>' "$QSSRT_REPO_ROOT/respcas/src/cmd.rs" |
        tr -d ' "=>' | sort | tr '\n' ' ' | sed 's/ $//'
}
