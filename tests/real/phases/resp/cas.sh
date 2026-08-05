#!/usr/bin/env bash
# ADR 0014: content-addressed namespaces. The key IS the value's hash.
#
# This section had no campaign coverage at all before it was written: the
# surface tripwire that was supposed to demand it had been reading the wrong
# file since cmd.rs was split, so CSET arrived and nothing noticed.
#
# What the ADR claims, and what is asserted here:
#
#   1. the key of every record is the BLAKE3-256 of its value, 32 raw bytes
#   2. two ways in, one storage path: `CSET <v>` and `SET "" <v>` both hash
#      server-side and reply with the address
#   3. `SET <32-byte H> <v>` stores under a claimed address, and the FIRST
#      write of an address is verified: blake3(v) == H or nothing is written
#   4. presence substitutes for verification afterwards -- in this namespace
#      it is an ack, in another cas namespace it is a clone by reference
#   5. a 32-byte key in a NON-cas namespace is a coincidence and must never
#      be used as a clone source
#
# Claim 3's refusal path is the reason this file needs a hasher rather than
# only replaying addresses the server handed back. Without one the campaign
# can only ever test the paths that succeed.
#
# Sourced by phases/04-resp.sh. Never run on its own.

CAS_NS="$QSSRT_RESP_CAS_NAMESPACE"
CAS_NS2="${CAS_NS}-2"
cas_size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_RESP_CAS_VALUE")" \
    "$(qssrt_bytes "$QSSRT_RESP_CAS_VALUE_FLOOR")")

# --- the namespace --------------------------------------------------------

vk NSNEW "$CAS_NS" >/dev/null 2>&1
mode_reply=$(vk NSSET "$CAS_NS" key_mode cas 2>&1)
record "NSSET-key_mode-cas" "$(qssrt_oneline "$mode_reply")"
if printf '%s' "$mode_reply" | grep -qi 'err'; then
    check_fail "NSSET key_mode cas is accepted on an empty namespace" \
        "$(qssrt_oneline "$mode_reply")"
    return 0 2>/dev/null || true
fi
check_pass "NSSET key_mode cas is accepted on an empty namespace"

cas_info=$(vk NSINFO "$CAS_NS" 2>&1)
record "NSINFO-cas" "$(qssrt_oneline "$cas_info")"
if printf '%s' "$cas_info" | grep -qi '^mode: cas'; then
    check_pass "NSINFO reports the namespace is content-addressed"
else
    check_fail "NSINFO reports the namespace is content-addressed" \
        "$(qssrt_oneline "$cas_info")"
fi

# Switching a NON-empty namespace to cas is incoherent -- the keys already
# in it were never addresses -- and the ADR says the mode is only settable
# while empty.
vk NSNEW qssrt-cas-nonempty >/dev/null 2>&1
printf 'SELECT qssrt-cas-nonempty\nSET a-key a-value\n' | vk >/dev/null 2>&1
nonempty=$(vk NSSET qssrt-cas-nonempty key_mode cas 2>&1)
record "NSSET-cas-on-nonempty" "$(qssrt_oneline "$nonempty")"
if printf '%s' "$nonempty" | grep -qi 'err\|empty'; then
    check_pass "key_mode cas is refused on a namespace that already has keys" \
        "$(qssrt_oneline "$nonempty")"
else
    check_fail "key_mode cas is refused on a namespace that already has keys" \
        "accepted: $(qssrt_oneline "$nonempty")"
fi

# --- mode A: the server hashes --------------------------------------------

val="$(qssrt_scratch)/cas-value"
gen_stream cas/value-1 "$cas_size" >"$val"

cset_key=$(resp_cmd "$CAS_NS" hex CSET "@$val" 2>/dev/null)
record "CSET-returned-key" "$cset_key"
assert_eq "CSET replies with a 32-byte address" 64 "${#cset_key}"

if resp_have_b3; then
    want=$(resp_b3 "$val")
    assert_eq "the address CSET returned is the BLAKE3-256 of the value" \
        "$want" "$cset_key"
else
    check_skip "the address CSET returned is the BLAKE3-256 of the value" \
        "cas-storage's b3sum example is not built"
    want="$cset_key"
fi

# The zdb-shaped spelling of the same thing. Same bytes in, same address
# out, or the two wire forms are not one storage path.
empty_key=$(resp_cmd "$CAS_NS" hex SET "" "@$val" 2>/dev/null)
assert_eq 'SET "" <value> returns the same address as CSET' \
    "$cset_key" "$empty_key"

# And the value comes back.
resp_cmd "$CAS_NS" raw GET "hex:$cset_key" >"$val.back" 2>/dev/null
assert_same "GET of the address returns the value byte for byte" "$val" "$val.back"
rm -f "$val.back"

assert_eq "EXISTS of a stored address is 1" 1 \
    "$(resp_cmd "$CAS_NS" text EXISTS "hex:$cset_key" 2>/dev/null)"

# --- dedup: the same content twice ----------------------------------------

# The write that matters for cost. A second CSET of identical content must
# be an ack, not a second copy: same address, and not one more block on
# disk. This is the claim that makes a content-addressed store worth having.
blocks_before=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
copies=$(qssrt_scaled "$QSSRT_RESP_DEDUP_COPIES" 2)
i=0
while [ "$i" -lt "$copies" ]; do
    resp_cmd "$CAS_NS" hex CSET "@$val" >/dev/null 2>&1
    i=$((i + 1))
done
blocks_after=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
record "cas-dedup-copies" "$copies"
record "cas-blocks-before-dedup" "$blocks_before"
record "cas-blocks-after-dedup" "$blocks_after"
assert_eq "$copies more writes of the same content cost no more blocks" \
    "$blocks_before" "$blocks_after"

# --- mode B: the client claims the address --------------------------------

# The address is right and the content is new: this is the first write that
# materialises it, so it is the one write the server must actually verify.
if resp_have_b3; then
    fresh="$(qssrt_scratch)/cas-fresh"
    gen_stream cas/fresh-1 "$cas_size" >"$fresh"
    fresh_h=$(resp_b3 "$fresh")
    assert_eq "the fresh address is not already present" 0 \
        "$(resp_cmd "$CAS_NS" text EXISTS "hex:$fresh_h" 2>/dev/null)"

    keyed=$(resp_cmd "$CAS_NS" text SET "hex:$fresh_h" "@$fresh" 2>&1)
    record "cas-mode-B-first-write" "$(qssrt_oneline "$keyed")"
    if printf '%s' "$keyed" | grep -qi 'err'; then
        check_fail "SET <correct address> <value> is accepted" \
            "$(qssrt_oneline "$keyed")"
    else
        check_pass "SET <correct address> <value> is accepted"
    fi
    resp_cmd "$CAS_NS" raw GET "hex:$fresh_h" >"$fresh.back" 2>/dev/null
    assert_same "the client-addressed value round-trips" "$fresh" "$fresh.back"
    rm -f "$fresh.back"

    # The address is wrong. Nothing may be written under it, and the reply
    # has to say so: this is the check that stops a client filing bytes
    # under an address they do not hash to.
    wrong_h="00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
    wrong=$(resp_cmd "$CAS_NS" text SET "hex:$wrong_h" "@$fresh" 2>&1)
    record "cas-mode-B-mismatch" "$(qssrt_oneline "$wrong")"
    if printf '%s' "$wrong" | grep -qi 'err\|mismatch\|hash\|address'; then
        check_pass "SET <wrong address> <value> is refused" "$(qssrt_oneline "$wrong")"
    else
        check_fail "SET <wrong address> <value> is refused" \
            "accepted: $(qssrt_oneline "$wrong")"
    fi
    assert_eq "the refused address stored nothing" 0 \
        "$(resp_cmd "$CAS_NS" text EXISTS "hex:$wrong_h" 2>/dev/null)"
    rm -f "$fresh"
else
    check_skip "SET <correct address> <value> is accepted" "no b3sum example built"
    check_skip "SET <wrong address> <value> is refused" "no b3sum example built"
fi

# A key that is not 32 bytes is not an address, whatever it contains.
short=$(resp_cmd "$CAS_NS" text SET "hex:aabbcc" "@$val" 2>&1)
record "cas-short-key" "$(qssrt_oneline "$short")"
if printf '%s' "$short" | grep -qi 'err\|length\|32'; then
    check_pass "a key that is not 32 bytes is refused in a cas namespace" \
        "$(qssrt_oneline "$short")"
else
    check_fail "a key that is not 32 bytes is refused in a cas namespace" \
        "accepted: $(qssrt_oneline "$short")"
fi

# --- across namespaces: the clone by reference ----------------------------

# A second cas namespace asking for content the first one already holds must
# not pay for the bytes again. Same address, no new blocks.
vk NSNEW "$CAS_NS2" >/dev/null 2>&1
vk NSSET "$CAS_NS2" key_mode cas >/dev/null 2>&1

assert_eq "the address is absent in the second namespace before the write" 0 \
    "$(resp_cmd "$CAS_NS2" text EXISTS "hex:$cset_key" 2>/dev/null)"

blocks_before=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
cross=$(resp_cmd "$CAS_NS2" text SET "hex:$cset_key" "@$val" 2>&1)
blocks_after=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
record "cas-cross-namespace-reply" "$(qssrt_oneline "$cross")"
record "cas-blocks-across-namespace-clone" "$blocks_before -> $blocks_after"
if printf '%s' "$cross" | grep -qi 'err'; then
    check_fail "the same address in a second cas namespace is accepted" \
        "$(qssrt_oneline "$cross")"
else
    check_pass "the same address in a second cas namespace is accepted"
fi
assert_eq "cloning into a second namespace writes no new blocks" \
    "$blocks_before" "$blocks_after"
assert_eq "the second namespace serves the value" 1 \
    "$(resp_cmd "$CAS_NS2" text EXISTS "hex:$cset_key" 2>/dev/null)"
resp_cmd "$CAS_NS2" raw GET "hex:$cset_key" >"$val.clone" 2>/dev/null
assert_same "the cloned record returns the same bytes" "$val" "$val.clone"
rm -f "$val.clone"

# --- a 32-byte key in a user-keyed namespace is not a source --------------

# The ADR is explicit: the cross-namespace lookup enumerates cas namespaces
# only, because nothing ever checked that a user-keyed record's 32-byte key
# hashes to its value. If that rule leaks, a cas namespace can be made to
# serve unverified bytes under an address -- which is the whole guarantee.
decoy_ns="qssrt-cas-decoy"
decoy_h="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
vk NSNEW "$decoy_ns" >/dev/null 2>&1
resp_cmd "$decoy_ns" text SET "hex:$decoy_h" "not-the-content-of-that-hash" \
    >/dev/null 2>&1
assert_eq "the decoy is stored in the user-keyed namespace" 1 \
    "$(resp_cmd "$decoy_ns" text EXISTS "hex:$decoy_h" 2>/dev/null)"
assert_eq "a user-keyed 32-byte key is invisible to a cas namespace" 0 \
    "$(resp_cmd "$CAS_NS" text EXISTS "hex:$decoy_h" 2>/dev/null)"

decoy_clone=$(resp_cmd "$CAS_NS" text SET "hex:$decoy_h" "some-other-bytes" 2>&1)
record "cas-decoy-clone-attempt" "$(qssrt_oneline "$decoy_clone")"
if printf '%s' "$decoy_clone" | grep -qi 'err\|mismatch\|hash'; then
    check_pass "a cas namespace refuses to clone from a user-keyed namespace" \
        "$(qssrt_oneline "$decoy_clone")"
else
    check_fail "a cas namespace refuses to clone from a user-keyed namespace" \
        "accepted: $(qssrt_oneline "$decoy_clone")"
fi

# --- release --------------------------------------------------------------

# Two namespaces hold the address; deleting one must not take the other's
# bytes with it, and deleting both must release them.
del1=$(resp_cmd "$CAS_NS2" text DEL "hex:$cset_key" 2>&1)
record "cas-del-second-namespace" "$(qssrt_oneline "$del1")"
assert_eq "the first namespace still holds the address after the second deletes" 1 \
    "$(resp_cmd "$CAS_NS" text EXISTS "hex:$cset_key" 2>/dev/null)"
resp_cmd "$CAS_NS" raw GET "hex:$cset_key" >"$val.after-del" 2>/dev/null
assert_same "and still serves the same bytes" "$val" "$val.after-del"
rm -f "$val.after-del"

blocks_before=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
resp_cmd "$CAS_NS" text DEL "hex:$cset_key" >/dev/null 2>&1
blocks_after=$(qssrt_block_file_count "$QSSRT_RESP_STORE")
record "cas-blocks-after-final-delete" "$blocks_before -> $blocks_after"
assert_eq "the address is gone once the last namespace deletes it" 0 \
    "$(resp_cmd "$CAS_NS" text EXISTS "hex:$cset_key" 2>/dev/null)"
if [ "$cas_size" -gt 1024 ] && [ "$blocks_after" -ge "$blocks_before" ]; then
    check_find "the last delete releases the blocks" \
        "block count did not fall: $blocks_before to $blocks_after"
else
    check_pass "the last delete releases the blocks" \
        "$blocks_before to $blocks_after"
fi

rm -f "$val"
