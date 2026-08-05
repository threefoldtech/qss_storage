#!/usr/bin/env bash
# Many keys, many commands, many connections -- and surviving a restart.
#
# Sourced by phases/04-resp.sh. Never run on its own.

keys=$(qssrt_scaled "$QSSRT_RESP_KEYS" "$QSSRT_RESP_KEYS_FLOOR")

# --- a batch down one connection ------------------------------------------

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
perf_begin "resp-batch-set"
batch_out=$(vk <"$batch" 2>&1 | sort -u | tr '\n' ' ')
# 12 bytes per value, exactly: "value-%06d". Counted, not estimated -- the
# whole point of the perf table is that every byte in it was accounted for.
perf_end "resp-batch-set" "$keys" "$((keys * 12))" "one connection, stdin batch"
record "batch-replies" "$(qssrt_oneline "$batch_out")"
assert_eq "a batch of $keys SETs down one connection reads back" "value-000000" \
    "$(vk GET qssrt:bulk:000000 2>/dev/null)"
assert_eq "the last key of the batch reads back" \
    "$(printf 'value-%06d' $((keys - 1)))" \
    "$(vk GET "$(printf 'qssrt:bulk:%06d' $((keys - 1)))" 2>/dev/null)"
rm -f "$batch"

# --- the pipe protocol ----------------------------------------------------

# valkey-cli --pipe passes its stdin through untouched -- so the lines below
# arrive as inline commands, not as RESP arrays -- and marks the end of the
# stream with a bare CRLF followed by an ECHO of twenty random bytes, which
# it waits for verbatim. Missing either piece left the client waiting out
# --pipe-timeout, which is what this phase used to record as a deviation.
# Both are now served, so a failure here is a violation: it means inline
# parsing, ECHO, or the pipe framing broke.
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

# --- what the store says it holds -----------------------------------------

dbsize=$(vk DBSIZE 2>/dev/null)
record "dbsize" "$dbsize"
if [ "${dbsize:-0}" -ge "$keys" ]; then
    check_pass "DBSIZE counts the keys that were written" "$dbsize"
else
    check_find "DBSIZE counts the keys that were written" \
        "$dbsize with at least $keys written"
fi

# --- cursor walks ---------------------------------------------------------

# SCAN and RSCAN page through more keys than one reply holds; the campaign
# walks both to the end and counts, because a cursor that stops early is the
# RESP version of a truncated listing.
#
# The reply is [cursor, [key ...]] flattened one element per line, and the
# cursor is "0" on the page that reached the end. That -- not the presence
# of any particular key -- is what ends the walk.
#
# The earlier version stopped as soon as a page held no `qssrt:bulk` key,
# which quietly turned RSCAN's answer into zero: RSCAN starts at the END of
# the keyspace, and every key sorting above `qssrt:bulk` comes first, so the
# walk broke on page one having seen nothing. It was recorded and not
# graded, so it read as a measurement rather than as the bug it was.
walk() {
    local cmd="$1" cursor="" seen=0 pages=0 out next
    while :; do
        if [ -z "$cursor" ]; then
            out=$(vk "$cmd" 2>/dev/null)
        else
            out=$(vk "$cmd" "$cursor" 2>/dev/null)
        fi
        [ -n "$out" ] || break
        next=$(printf '%s\n' "$out" | head -n 1)
        seen=$((seen + $(printf '%s\n' "$out" | tail -n +2 | grep -c .)))
        pages=$((pages + 1))
        [ "$next" = "0" ] && break
        [ "$next" = "$cursor" ] && break
        cursor="$next"
        [ "$pages" -ge "${QSSRT_LIST_MAX_PAGES:-10000}" ] && break
    done
    printf '%s' "$seen"
}
scanned=$(walk SCAN)
rscanned=$(walk RSCAN)
record "scan-keys-seen" "$scanned"
record "rscan-keys-seen" "$rscanned"

# The namespace holds at least the batch this section wrote, plus everything
# values.sh left behind, so a walk that ends early is a walk that lied.
if [ "${scanned:-0}" -ge "$keys" ]; then
    check_pass "SCAN walks the whole namespace" "$scanned keys seen, $keys written here"
else
    check_fail "SCAN walks the whole namespace" \
        "$scanned keys seen but $keys were written in this section alone"
fi
if [ "${rscanned:-0}" -ge "$keys" ]; then
    check_pass "RSCAN walks the whole namespace" "$rscanned keys seen"
else
    check_fail "RSCAN walks the whole namespace" \
        "$rscanned keys seen but $keys were written in this section alone"
fi
# Forwards and backwards over the same namespace must see the same keys.
assert_eq "SCAN and RSCAN agree on how many keys there are" "$scanned" "$rscanned"

# --- concurrent clients ---------------------------------------------------

# Clients at once, each down its own connection with its work batched: the
# point is concurrent connections, not how many processes a shell can spawn
# per second.
#
# The wait below names the client pids. A bare wait would also wait for
# respcas, which this same shell started as a background job -- and respcas
# does not exit, so the phase would hang here forever.
conc_workers=8
conc_each=50
conc_pids=()
perf_begin "resp-concurrent-clients"
for w in $(seq 1 "$conc_workers"); do
    (
        j=0
        while [ "$j" -lt "$conc_each" ]; do
            printf 'SET qssrt:conc:%s:%s v%s\n' "$w" "$j" "$j"
            printf 'GET qssrt:conc:%s:%s\n' "$w" "$j"
            j=$((j + 1))
        done
    ) | vk >/dev/null 2>&1 &
    conc_pids+=("$!")
done
for pid in "${conc_pids[@]}"; do wait "$pid" 2>/dev/null; done
# Values are "v0".."v49": two bytes for the first ten, three for the rest.
conc_bytes=$((conc_workers * (10 * 2 + (conc_each - 10) * 3)))
perf_end "resp-concurrent-clients" "$((conc_workers * conc_each))" \
    "$conc_bytes" "$conc_workers connections, SET+GET each"

assert_eq "concurrent clients do not lose writes" "v$((conc_each - 1))" \
    "$(vk GET "qssrt:conc:$conc_workers:$((conc_each - 1))" 2>/dev/null)"

lost=0
for w in $(seq 1 "$conc_workers"); do
    [ "$(vk GET "qssrt:conc:$w:0" 2>/dev/null)" = "v0" ] || lost=$((lost + 1))
done
assert_eq "every concurrent client's first write survived" 0 "$lost"

# --- survival across a restart --------------------------------------------

# The store is on disk; a restart must find it there. This is also the only
# place the phase exercises respcas's open path against a store that already
# has content -- every other section runs against one this run created.
before_restart=$(vk DBSIZE 2>/dev/null)
if respcas_stop && respcas_start; then
    check_pass "respcas stops and starts again on a populated store"
    after_restart=$(vk DBSIZE 2>/dev/null)
    record "dbsize-before-restart" "$before_restart"
    record "dbsize-after-restart" "$after_restart"
    assert_eq "DBSIZE is unchanged across the restart" \
        "$before_restart" "$after_restart"
    assert_eq "a key written before the restart reads back after it" \
        "value-000000" "$(vk GET qssrt:bulk:000000 2>/dev/null)"
    assert_eq "the binary-keyed record survives the restart" \
        "binary-keyed-value" \
        "$(resp_cmd "" text GET "hex:6b65790aff00ff" 2>/dev/null)"
else
    check_fail "respcas stops and starts again on a populated store" \
        "it would not come back: $(tail -n 3 "$(qssrt_respcas_log)")"
fi
