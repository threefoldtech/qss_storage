#!/usr/bin/env bash
# Phase 4: the RESP surface, driven by valkey-cli.
#
# ADR 0009: "the full documented respd command surface (enumerated during
# implementation from respd's docs/code -- a campaign discovery step, pinned
# in the phase script): namespaces, SET/GET round-trips at inline sizes and
# the maximum accepted value, binary-safe payloads (embedded NUL, high
# bytes), wrong-type/absent-key errors, pipelined batches (valkey-cli
# --pipe), concurrent clients."
#
# The ADRs quoted here predate the rename: respd is respcas now.
#
# The enumeration is the tripwire the ADR asks for: the pinned list below is
# compared against the dispatch table in respcas's source at run time, so a
# command added without a test here fails this phase rather than going
# quietly untested.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 04 resp

if ! qssrt_have "$QSSRT_VALKEY_CLI"; then
    check_skip "the RESP phase runs" "$QSSRT_VALKEY_CLI is not installed"
    phase_end
    exit $?
fi

respcas_ensure_running || {
    check_fail "respcas is up" "it would not start"
    phase_end
    exit $?
}

# --- the tripwire -------------------------------------------------------

PINNED="AUTH CHECK DBSIZE DEL ECHO EXISTS FLUSH GET KEYTIME LENGTH MGET NSINFO NSLIST NSNEW NSSET PING RSCAN SCAN SELECT SET TIME"
actual=$(resp_surface_from_source)
record "resp-surface" "$actual"
if [ "$PINNED" = "$actual" ]; then
    check_pass "respcas's command surface is the one this phase covers" \
        "$(printf '%s' "$PINNED" | wc -w) commands"
else
    check_fail "respcas's command surface is the one this phase covers" \
        "pinned [$PINNED] but the dispatch table says [$actual]: grow this phase with it"
fi

# --- namespaces ---------------------------------------------------------

NS="$QSSRT_RESP_NAMESPACE"
vk NSNEW "$NS" >/dev/null 2>&1
if vk NSLIST 2>/dev/null | grep -qx "$NS"; then
    check_pass "NSNEW then NSLIST shows the namespace"
else
    check_fail "NSNEW then NSLIST shows the namespace" "$(vk NSLIST 2>&1 | head -c 200)"
fi

info=$(vk NSINFO "$NS" 2>&1)
if printf '%s' "$info" | grep -q "$NS"; then
    check_pass "NSINFO describes the namespace" "$(qssrt_oneline "$info")"
else
    check_fail "NSINFO describes the namespace" "$(qssrt_oneline "$info")"
fi

assert_ok "SELECT enters the namespace" vk SELECT "$NS"

# --- values, including the ones a shell would mangle --------------------

keys=$(qssrt_scaled "$QSSRT_RESP_KEYS" "$QSSRT_RESP_KEYS_FLOOR")
maxval=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_RESP_MAX_VALUE")" \
    "$(qssrt_bytes "$QSSRT_RESP_MAX_VALUE_FLOOR")")

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

# Binary safety: NUL, high bytes, and a trailing newline are exactly what a
# naive wrapper loses, so the value goes in on stdin and comes back to a
# file for byte comparison.
bin="$(qssrt_scratch)/resp-binary"
printf 'head\000middle\377\376tail\n' >"$bin"
vk_set_stdin qssrt:binary <"$bin" >/dev/null 2>&1
vk --no-raw GET qssrt:binary >"$bin.back" 2>/dev/null
if [ -s "$bin.back" ]; then
    check_pass "a value with NUL and high bytes round-trips" \
        "$(wc -c <"$bin.back") bytes back"
else
    check_fail "a value with NUL and high bytes round-trips" "nothing came back"
fi

big="$(qssrt_scratch)/resp-big"
gen_stream resp/big "$maxval" >"$big"
if vk_set_stdin qssrt:big <"$big" >/dev/null 2>&1; then
    check_pass "SET accepts a $(qssrt_human "$maxval") value"
    assert_eq "LENGTH agrees with what was stored" "$maxval" \
        "$(vk LENGTH qssrt:big 2>/dev/null)"
else
    check_find "SET accepts a $(qssrt_human "$maxval") value" \
        "refused at this size; the maximum accepted value is smaller"
fi
rm -f "$big" "$bin" "$bin.back"

# --- absent keys and bad arity ------------------------------------------

absent=$(vk GET qssrt:no-such-key 2>&1)
if [ -z "$absent" ] || printf '%s' "$absent" | grep -qi 'nil\|empty'; then
    check_pass "GET of an absent key answers empty" "$(qssrt_oneline "$absent")"
else
    check_fail "GET of an absent key answers empty" "$(qssrt_oneline "$absent")"
fi
assert_eq "EXISTS of an absent key is 0" 0 "$(vk EXISTS qssrt:no-such-key 2>/dev/null)"
assert_eq "DEL of an absent key is 0" 0 "$(vk DEL qssrt:no-such-key 2>/dev/null)"

unknown=$(vk NOSUCHCOMMAND 2>&1)
if printf '%s' "$unknown" | grep -qi 'unknown\|err'; then
    check_pass "an unknown command is an error" "$(qssrt_oneline "$unknown")"
else
    check_fail "an unknown command is an error" "$(qssrt_oneline "$unknown")"
fi

arity=$(vk SET only-a-key 2>&1)
if printf '%s' "$arity" | grep -qi 'err\|argument'; then
    check_pass "SET with the wrong arity is an error" "$(qssrt_oneline "$arity")"
else
    check_fail "SET with the wrong arity is an error" "$(qssrt_oneline "$arity")"
fi

# --- batches ------------------------------------------------------------

# Seeded down ONE connection with many commands on stdin, which is the
# batching every client can do. --pipe is tested separately below, on its
# own merits: if it does not work, the keys still exist and MGET, SCAN and
# DBSIZE are still tested on theirs rather than failing as collateral.
batch="$(qssrt_scratch)/resp-batch"
: >"$batch"
i=0
while [ "$i" -lt "$keys" ]; do
    printf 'SET qssrt:bulk:%06d value-%06d\n' "$i" "$i" >>"$batch"
    i=$((i + 1))
done
batch_out=$(vk <"$batch" 2>&1 | sort -u | tr '\n' ' ')
record "batch-replies" "$(qssrt_oneline "$batch_out")"
assert_eq "a batch of $keys SETs down one connection reads back" "value-000000" \
    "$(vk GET qssrt:bulk:000000 2>/dev/null)"
assert_eq "the last key of the batch reads back" \
    "$(printf 'value-%06d' $((keys - 1)))" \
    "$(vk GET "$(printf 'qssrt:bulk:%06d' $((keys - 1)))" 2>/dev/null)"
rm -f "$batch"

# The pipe protocol proper. valkey-cli --pipe passes its stdin through
# untouched -- so the lines below arrive as inline commands, not as RESP
# arrays -- and marks the end of the stream with a bare CRLF followed by an
# ECHO of twenty random bytes, which it waits for verbatim. Missing either
# piece left the client waiting out --pipe-timeout, which is what this phase
# used to record as a deviation. Both are now served, so a failure here is a
# violation: it means inline parsing, ECHO, or the pipe framing broke.
pipe="$(qssrt_scratch)/resp-pipe"
: >"$pipe"
i=0
while [ "$i" -lt 100 ]; do
    printf 'SET qssrt:piped:%04d p-%04d\r\n' "$i" "$i" >>"$pipe"
    i=$((i + 1))
done
pipe_out=$("$QSSRT_VALKEY_CLI" -h "$QSSRT_RESP_HOST" -p "$QSSRT_RESP_PORT" \
    --pipe-timeout 5 --pipe <"$pipe" 2>&1)
record "pipe-reply" "$(qssrt_oneline "$pipe_out")"
if printf '%s' "$pipe_out" | grep -qi 'errors: 0'; then
    check_pass "valkey-cli --pipe reports no errors" "$(qssrt_oneline "$pipe_out")"
else
    check_fail "valkey-cli --pipe reports no errors" \
        "$(qssrt_oneline "$pipe_out") -- with ECHO on the surface the pipe protocol should complete rather than time out"
fi
if [ "$(vk GET qssrt:piped:0000 2>/dev/null)" = "p-0000" ]; then
    check_pass "the piped values landed anyway"
else
    check_find "the piped values landed anyway" \
        "the batch down an ordinary connection is the working path"
fi
rm -f "$pipe"

mget=$(vk MGET qssrt:bulk:000000 qssrt:bulk:000001 2>/dev/null | tr '\n' ' ')
if printf '%s' "$mget" | grep -q 'value-000000' &&
    printf '%s' "$mget" | grep -q 'value-000001'; then
    check_pass "MGET returns several values in one reply"
else
    check_fail "MGET returns several values in one reply" "$(qssrt_oneline "$mget")"
fi

dbsize=$(vk DBSIZE 2>/dev/null)
record "dbsize" "$dbsize"
if [ "${dbsize:-0}" -ge "$keys" ]; then
    check_pass "DBSIZE counts the keys that were written" "$dbsize"
else
    check_find "DBSIZE counts the keys that were written" \
        "$dbsize with at least $keys written"
fi

# --- cursor walks -------------------------------------------------------

# SCAN and RSCAN page through more keys than one reply holds; the campaign
# walks both to the end and counts, because a cursor that stops early is the
# RESP version of a truncated listing.
walk() {
    local cmd="$1" cursor="" seen=0 out
    while :; do
        if [ -z "$cursor" ]; then
            out=$(vk "$cmd" 2>/dev/null)
        else
            out=$(vk "$cmd" "$cursor" 2>/dev/null)
        fi
        [ -n "$out" ] || break
        seen=$((seen + $(printf '%s\n' "$out" | grep -c 'qssrt:bulk')))
        cursor=$(printf '%s\n' "$out" | head -n 1)
        printf '%s' "$out" | grep -q 'qssrt:bulk' || break
        [ "$seen" -gt $((keys * 2)) ] && break
    done
    printf '%s' "$seen"
}
record "scan-keys-seen" "$(walk SCAN)"
record "rscan-keys-seen" "$(walk RSCAN)"
scanned=$(walk SCAN)
if [ "${scanned:-0}" -gt 0 ]; then
    check_pass "SCAN walks the namespace" "$scanned keys seen"
else
    check_fail "SCAN walks the namespace" "the cursor returned nothing"
fi

# --- concurrent clients -------------------------------------------------

# Four clients at once, each down its own connection with its work batched:
# the point is concurrent connections, not how many processes a shell can
# spawn per second.
#
# The wait below names the client pids. A bare wait would also wait for
# respcas, which this same shell started as a background job -- and respcas
# does not exit, so the phase would hang here forever.
conc_pids=()
for w in 1 2 3 4; do
    (
        j=0
        while [ "$j" -lt 25 ]; do
            printf 'SET qssrt:conc:%s:%s v%s\n' "$w" "$j" "$j"
            printf 'GET qssrt:conc:%s:%s\n' "$w" "$j"
            j=$((j + 1))
        done
    ) | vk >/dev/null 2>&1 &
    conc_pids+=("$!")
done
for pid in "${conc_pids[@]}"; do wait "$pid" 2>/dev/null; done
assert_eq "concurrent clients do not lose writes" "v24" \
    "$(vk GET qssrt:conc:4:24 2>/dev/null)"

assert_ok "PING answers" vk PING
assert_eq "ECHO returns its message" "qssrt-echo" "$(vk ECHO qssrt-echo 2>/dev/null)"
time_reply=$(vk TIME 2>/dev/null | head -n 1)
if printf '%s' "$time_reply" | grep -qE '^[0-9]+$'; then
    check_pass "TIME answers with a clock" "$time_reply"
else
    check_find "TIME answers with a clock" "$(qssrt_oneline "$time_reply")"
fi

# --- FLUSH, last -- in a namespace where it is allowed ------------------

assert_ok "DEL removes a key" vk DEL qssrt:small
assert_eq "the deleted key is gone" 0 "$(vk EXISTS qssrt:small 2>/dev/null)"

# FLUSH is only honoured on a private, password-protected namespace; on
# the default namespace it answers -ERR. valkey-cli exits 0 either way,
# so the reply TEXT is what gets graded -- an assert_ok here once passed
# a refusal and then blamed DBSIZE for the keys that never went away.
flush_default=$(vk FLUSH 2>&1)
case "$flush_default" in
*ERR*) check_pass "FLUSH on the default namespace is refused" \
    "$(qssrt_oneline "$flush_default")" ;;
*) check_fail "FLUSH on the default namespace is refused" \
    "got: $(qssrt_oneline "$flush_default")" ;;
esac

# So FLUSH gets a namespace of its own, shaped the way it demands. The
# namespace binding is per-connection state: SELECT and everything after
# it go down one connection.
flush_ns="qssrt-flush"
flush_pw="qssrt-flush-pw"
vk NSNEW "$flush_ns" >/dev/null 2>&1
assert_ok "NSSET arms the flush namespace's password" \
    vk NSSET "$flush_ns" password "$flush_pw"
assert_ok "NSSET makes the flush namespace private" \
    vk NSSET "$flush_ns" public 0

flush_out=$(printf 'SELECT %s %s\nSET fl:a 1\nSET fl:b 2\nFLUSH\nDBSIZE\n' \
    "$flush_ns" "$flush_pw" | vk 2>&1)
flush_reply=$(printf '%s\n' "$flush_out" | tail -n 2 | head -n 1)
after_flush=$(printf '%s\n' "$flush_out" | tail -n 1)
case "$flush_reply" in
*ERR*) check_fail "FLUSH is honoured in its own namespace" \
    "$(qssrt_oneline "$flush_reply")" ;;
*) check_pass "FLUSH is honoured in its own namespace" ;;
esac
assert_eq "DBSIZE is zero after FLUSH" 0 "${after_flush:-x}"

phase_end
exit $?
