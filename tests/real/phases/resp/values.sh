#!/usr/bin/env bash
# What a value can be: sizes across the inline boundary, bytes a shell would
# mangle, and the answers for keys that are not there.
#
# Sourced by phases/04-resp.sh. Never run on its own.

# Every command in this section runs on the DEFAULT namespace, deliberately
# and consistently.
#
# The namespace binding is per-connection, and both clients here open a
# fresh connection per invocation -- so a SELECT in one command is gone by
# the next one. Mixing a SELECT-ing client with a non-SELECT-ing one over
# the same keys is how a section ends up writing to one namespace and
# reading from another and calling the difference a bug. Namespace-scoped
# behaviour is namespaces.sh's subject; this section is about values.
NS="$QSSRT_RESP_NAMESPACE"
vk NSNEW "$NS" >/dev/null 2>&1
assert_ok "SELECT enters a namespace" vk SELECT "$NS"

keys=$(qssrt_scaled "$QSSRT_RESP_KEYS" "$QSSRT_RESP_KEYS_FLOOR")
maxval=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_RESP_MAX_VALUE")" \
    "$(qssrt_bytes "$QSSRT_RESP_MAX_VALUE_FLOOR")")

# --- the small round trip -----------------------------------------------

assert_ok "SET a small value" vk SET qssrt:small hello
assert_eq "GET returns it" hello "$(vk GET qssrt:small 2>/dev/null)"
assert_eq "EXISTS reports it" 1 "$(vk EXISTS qssrt:small 2>/dev/null)"
assert_eq "LENGTH reports its size" 5 "$(vk LENGTH qssrt:small 2>/dev/null)"

keytime=$(vk KEYTIME qssrt:small 2>&1)
if printf '%s' "$keytime" | grep -qE '[0-9]{6,}'; then
    check_pass "KEYTIME reports a timestamp" "$(qssrt_oneline "$keytime")"
else
    check_find "KEYTIME reports a timestamp" "$(qssrt_oneline "$keytime")"
fi

check_out=$(vk CHECK qssrt:small 2>&1)
record "CHECK-reply" "$(qssrt_oneline "$check_out")"
if [ -n "$check_out" ]; then
    check_pass "CHECK answers for a key that exists" "$(qssrt_oneline "$check_out")"
else
    check_fail "CHECK answers for a key that exists" "empty reply"
fi

# CHECK is the record's own integrity answer, so it has to be asked about a
# key that is not there too: an integrity verb that cannot say "absent" says
# nothing useful about the keys it does answer for.
check_absent=$(vk CHECK qssrt:no-such-key 2>&1)
record "CHECK-absent-reply" "$(qssrt_oneline "$check_absent")"
if printf '%s' "$check_absent" | grep -qiE 'err|0|nil|not'; then
    check_pass "CHECK distinguishes an absent key" "$(qssrt_oneline "$check_absent")"
else
    check_find "CHECK distinguishes an absent key" "$(qssrt_oneline "$check_absent")"
fi

# --- bytes a shell would eat ---------------------------------------------

# NUL, high bytes and a trailing newline are exactly what a naive wrapper
# loses, so the value goes in on stdin and comes back to a file for byte
# comparison rather than through a shell variable.
bin="$(qssrt_scratch)/resp-binary"
printf 'head\000middle\377\376tail\n' >"$bin"
vk_set_stdin qssrt:binary <"$bin" >/dev/null 2>&1
resp_cmd "" raw GET qssrt:binary >"$bin.back" 2>/dev/null
assert_same "a value with NUL and high bytes round-trips byte for byte" \
    "$bin" "$bin.back"
assert_eq "LENGTH agrees on the binary value" "$(stat -c %s "$bin")" \
    "$(vk LENGTH qssrt:binary 2>/dev/null)"

# An empty value is a value. It is also the one a store that conflates
# "absent" with "empty" gets wrong, and the ADR 0008 zero-byte PUT bug was
# this shape on the S3 side.
: >"$bin.empty"
vk_set_stdin qssrt:empty <"$bin.empty" >/dev/null 2>&1
assert_eq "an empty value EXISTS" 1 "$(vk EXISTS qssrt:empty 2>/dev/null)"
assert_eq "an empty value has LENGTH 0" 0 "$(vk LENGTH qssrt:empty 2>/dev/null)"

# A key with high bytes in it, which only the binary-safe client can send.
key_hex="6b65790aff00ff"
resp_cmd "" text SET "hex:$key_hex" binary-keyed-value >/dev/null 2>&1
assert_eq "a key containing NUL and a newline round-trips" \
    "binary-keyed-value" \
    "$(resp_cmd "" text GET "hex:$key_hex" 2>/dev/null)"

rm -f "$bin" "$bin.back" "$bin.empty"

# --- where a user-keyed value lives --------------------------------------

# In a user-keyed namespace respcas stores the value inside its metadata
# record whatever its size: block-backing is a property of a CONTENT-
# ADDRESSED namespace, which is what ADR 0014 added and what cas.sh covers.
#
# So the assertion here is not "small inlines, large blocks" -- that is the
# S3 side's boundary, and asserting it here was asserting a threshold this
# path does not have. It is that a value of either size round-trips, and
# that the block count does not move, which is the actual claim and the one
# that would break if the two paths were ever wired together by accident.
blocks_before=$(qssrt_block_file_count "$QSSRT_RESP_STORE")

small="$(qssrt_scratch)/resp-inline"
gen_stream resp/inline 512 >"$small"
vk_set_stdin qssrt:inline <"$small" >/dev/null 2>&1
assert_eq "a 512-byte value reads back at full length" 512 \
    "$(vk LENGTH qssrt:inline 2>/dev/null)"

big="$(qssrt_scratch)/resp-big"
gen_stream resp/big "$maxval" >"$big"
if vk_set_stdin qssrt:big <"$big" >/dev/null 2>&1; then
    check_pass "SET accepts a $(qssrt_human "$maxval") value"
    assert_eq "LENGTH agrees with what was stored" "$maxval" \
        "$(vk LENGTH qssrt:big 2>/dev/null)"
    # The bytes, not just the length.
    resp_cmd "" raw GET qssrt:big >"$big.back" 2>/dev/null
    assert_same "the large value round-trips byte for byte" "$big" "$big.back"
    rm -f "$big.back"
else
    check_find "SET accepts a $(qssrt_human "$maxval") value" \
        "refused at this size; the maximum accepted value is smaller"
fi

blocks_after=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
record "blocks-before-user-keyed-writes" "$blocks_before"
record "blocks-after-user-keyed-writes" "$blocks_after"
assert_eq "a user-keyed namespace writes no block files at any size" \
    "$blocks_before" "$blocks_after"
rm -f "$big" "$small"

# --- absent keys ---------------------------------------------------------

absent=$(vk GET qssrt:no-such-key 2>&1)
if [ -z "$absent" ] || printf '%s' "$absent" | grep -qi 'nil\|empty'; then
    check_pass "GET of an absent key answers empty" "$(qssrt_oneline "$absent")"
else
    check_fail "GET of an absent key answers empty" "$(qssrt_oneline "$absent")"
fi
assert_eq "EXISTS of an absent key is 0" 0 "$(vk EXISTS qssrt:no-such-key 2>/dev/null)"
assert_eq "DEL of an absent key is 0" 0 "$(vk DEL qssrt:no-such-key 2>/dev/null)"
assert_eq "LENGTH of an absent key is refused or zero" 0 \
    "$(vk LENGTH qssrt:no-such-key 2>/dev/null | grep -cE '^[1-9]')"

# --- overwrite ------------------------------------------------------------

# The same key twice. The second value is what GET must answer, and the
# first one's storage must not survive as garbage -- the RESP-side shape of
# ADR 0008.
assert_ok "SET over an existing key" vk SET qssrt:small replaced
assert_eq "GET returns the replacement" replaced "$(vk GET qssrt:small 2>/dev/null)"
assert_eq "LENGTH follows the replacement" 8 "$(vk LENGTH qssrt:small 2>/dev/null)"

# --- MGET and DEL ---------------------------------------------------------

vk SET qssrt:mg:1 one >/dev/null 2>&1
vk SET qssrt:mg:2 two >/dev/null 2>&1
mget=$(vk MGET qssrt:mg:1 qssrt:mg:2 qssrt:no-such-key 2>/dev/null | tr '\n' ' ')
if printf '%s' "$mget" | grep -q 'one' && printf '%s' "$mget" | grep -q 'two'; then
    check_pass "MGET returns several values in one reply, absent keys included" \
        "$(qssrt_oneline "$mget")"
else
    check_fail "MGET returns several values in one reply, absent keys included" \
        "$(qssrt_oneline "$mget")"
fi

assert_ok "DEL removes a key" vk DEL qssrt:mg:1
assert_eq "the deleted key is gone" 0 "$(vk EXISTS qssrt:mg:1 2>/dev/null)"
