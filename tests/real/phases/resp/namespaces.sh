#!/usr/bin/env bash
# Namespaces: creation, listing, properties, protection, and who may do what.
#
# Everything here runs down ONE connection per assertion, because a
# namespace binding and the password that opened it are per-connection
# state. A SELECT in one invocation and the command it was meant to protect
# in the next are two different sessions, and testing them apart tests
# nothing.
#
# Sourced by phases/04-resp.sh. Never run on its own.

NS="$QSSRT_RESP_NAMESPACE"

# --- creation and listing -------------------------------------------------

vk NSNEW "$NS" >/dev/null 2>&1
if vk NSLIST 2>/dev/null | grep -qx "$NS"; then
    check_pass "NSNEW then NSLIST shows the namespace"
else
    check_fail "NSNEW then NSLIST shows the namespace" "$(vk NSLIST 2>&1 | head -c 200)"
fi

info=$(vk NSINFO "$NS" 2>&1)
record "NSINFO-default" "$(qssrt_oneline "$info")"
if printf '%s' "$info" | grep -q "$NS"; then
    check_pass "NSINFO describes the namespace" "$(qssrt_oneline "$info")"
else
    check_fail "NSINFO describes the namespace" "$(qssrt_oneline "$info")"
fi

for field in public password data_size_bytes data_limits_bytes mode worm locked; do
    if printf '%s' "$info" | grep -q "^$field:"; then
        check_pass "NSINFO reports $field"
    else
        check_find "NSINFO reports $field" "absent from the reply"
    fi
done

# Creating the same namespace twice must not silently succeed and must not
# damage what is there.
dup=$(vk NSNEW "$NS" 2>&1)
record "NSNEW-duplicate" "$(qssrt_oneline "$dup")"
if printf '%s' "$dup" | grep -qi 'err\|exist'; then
    check_pass "NSNEW of an existing namespace is refused" "$(qssrt_oneline "$dup")"
else
    check_find "NSNEW of an existing namespace is refused" "$(qssrt_oneline "$dup")"
fi

missing=$(vk NSINFO qssrt-no-such-namespace 2>&1)
if printf '%s' "$missing" | grep -qi 'err\|not'; then
    check_pass "NSINFO of an absent namespace is an error" "$(qssrt_oneline "$missing")"
else
    check_fail "NSINFO of an absent namespace is an error" "$(qssrt_oneline "$missing")"
fi

select_missing=$(vk SELECT qssrt-no-such-namespace 2>&1)
if printf '%s' "$select_missing" | grep -qi 'err\|not'; then
    check_pass "SELECT of an absent namespace is an error" \
        "$(qssrt_oneline "$select_missing")"
else
    check_fail "SELECT of an absent namespace is an error" \
        "$(qssrt_oneline "$select_missing")"
fi

# --- isolation ------------------------------------------------------------

# Two namespaces, the same key, different values. The one thing a namespace
# is FOR.
vk NSNEW qssrt-iso-a >/dev/null 2>&1
vk NSNEW qssrt-iso-b >/dev/null 2>&1
printf 'SELECT qssrt-iso-a\nSET shared-key value-from-a\n' | vk >/dev/null 2>&1
printf 'SELECT qssrt-iso-b\nSET shared-key value-from-b\n' | vk >/dev/null 2>&1
got_a=$(printf 'SELECT qssrt-iso-a\nGET shared-key\n' | vk 2>/dev/null | tail -n 1)
got_b=$(printf 'SELECT qssrt-iso-b\nGET shared-key\n' | vk 2>/dev/null | tail -n 1)
assert_eq "a key in one namespace is not the key in another (a)" "value-from-a" "$got_a"
assert_eq "a key in one namespace is not the key in another (b)" "value-from-b" "$got_b"

# And the default namespace is a third one, not an alias for the first.
vk SET shared-key value-from-default >/dev/null 2>&1
assert_eq "the default namespace is its own namespace" "value-from-default" \
    "$(vk GET shared-key 2>/dev/null)"

# --- protection: password and public --------------------------------------

prot_ns="qssrt-protected"
prot_pw="qssrt-protected-pw"
vk NSNEW "$prot_ns" >/dev/null 2>&1
assert_ok "NSSET arms a namespace password" vk NSSET "$prot_ns" password "$prot_pw"
assert_ok "NSSET makes a namespace private" vk NSSET "$prot_ns" public 0

prot_info=$(vk NSINFO "$prot_ns" 2>&1)
record "NSINFO-protected" "$(qssrt_oneline "$prot_info")"
if printf '%s' "$prot_info" | grep -qi '^password: yes'; then
    check_pass "NSINFO reports the password is set"
else
    check_find "NSINFO reports the password is set" "$(qssrt_oneline "$prot_info")"
fi

# What a password actually gates here is WRITING, not entering.
#
# server.rs answers an unauthenticated SELECT of a private namespace with
# "OK (read-only access)" and refuses writes on that connection afterwards.
# So the assertion is not that SELECT is refused -- it is not, by design --
# but that the degraded session is genuinely read-only. Asserting the
# refusal at SELECT would have been asserting a design this daemon does not
# have, and would have passed only if someone changed it.
no_pw=$(printf 'SELECT %s\n' "$prot_ns" | vk 2>&1)
record "select-private-no-password" "$(qssrt_oneline "$no_pw")"
if printf '%s' "$no_pw" | grep -qi 'read-only'; then
    check_pass "SELECT of a private namespace without a password is read-only" \
        "$(qssrt_oneline "$no_pw")"
else
    check_fail "SELECT of a private namespace without a password is read-only" \
        "$(qssrt_oneline "$no_pw")"
fi

bad_pw=$(printf 'SELECT %s wrong-password\n' "$prot_ns" | vk 2>&1)
record "select-private-wrong-password" "$(qssrt_oneline "$bad_pw")"
if printf '%s' "$bad_pw" | grep -qi 'read-only'; then
    check_pass "SELECT with the wrong password is read-only, not authenticated" \
        "$(qssrt_oneline "$bad_pw")"
else
    check_fail "SELECT with the wrong password is read-only, not authenticated" \
        "$(qssrt_oneline "$bad_pw")"
fi

# The authenticated session first, so there is something to read.
good=$(printf 'SELECT %s %s\nSET protected-key protected-value\nGET protected-key\n' \
    "$prot_ns" "$prot_pw" | vk 2>&1 | tail -n 1)
assert_eq "SELECT with the right password opens the namespace for writing" \
    "protected-value" "$good"

# The two properties gate different things, and the campaign has to say
# which is which:
#
#   password  gates WRITING. Without it, writes are refused.
#   public    gates READING. On public=0, an unauthenticated session cannot
#             read either.
#
# So there are two shapes to test, not one, and the interesting one is the
# combination: a private namespace denies both, a public-but-passworded one
# denies only the write.

# Shape 1: private and passworded -- neither read nor write.
ro_write=$(printf 'SELECT %s\nSET protected-key overwritten-without-a-password\n' \
    "$prot_ns" | vk 2>&1)
record "private-namespace-unauthenticated-write" "$(qssrt_oneline "$ro_write")"
if printf '%s' "$ro_write" | grep -qi 'err\|authentication\|denied'; then
    check_pass "an unauthenticated session cannot write a private namespace" \
        "$(qssrt_oneline "$ro_write")"
else
    check_fail "an unauthenticated session cannot write a private namespace" \
        "the write was accepted: $(qssrt_oneline "$ro_write")"
fi

ro_read=$(printf 'SELECT %s\nGET protected-key\n' "$prot_ns" | vk 2>&1)
record "private-namespace-unauthenticated-read" "$(qssrt_oneline "$ro_read")"
if printf '%s' "$ro_read" | grep -qi 'err\|authentication\|denied'; then
    check_pass "an unauthenticated session cannot read a private namespace" \
        "$(qssrt_oneline "$ro_read")"
else
    check_fail "an unauthenticated session cannot read a private namespace" \
        "the read was served: $(qssrt_oneline "$ro_read")"
fi

# SELECT still greets that session with "OK (read-only access)" even though
# it has no read access either. The enforcement is right and nothing leaks;
# the reply is what is wrong, and a client that believes it will report a
# working read-only connection that errors on the first GET.
if printf '%s' "$no_pw" | grep -qi 'read-only' &&
    printf '%s' "$ro_read" | grep -qi 'authentication required for read'; then
    check_find "SELECT calls a private namespace 'read-only access' but denies reads too" \
        "reply was [$(qssrt_oneline "$no_pw")], the GET after it was [$(qssrt_oneline "$ro_read")]"
fi

assert_eq "the value an unauthenticated write tried to replace is intact" \
    "protected-value" \
    "$(printf 'SELECT %s %s\nGET protected-key\n' "$prot_ns" "$prot_pw" |
        vk 2>/dev/null | tail -n 1)"

# Shape 2: public and passworded -- reads served, writes refused. This is
# the configuration the "read-only access" reply actually describes.
assert_ok "NSSET makes the passworded namespace public again" \
    vk NSSET "$prot_ns" public 1
pub_read=$(printf 'SELECT %s\nGET protected-key\n' "$prot_ns" | vk 2>&1 | tail -n 1)
assert_eq "a public passworded namespace serves reads without the password" \
    "protected-value" "$pub_read"
pub_write=$(printf 'SELECT %s\nSET protected-key still-not-allowed\n' "$prot_ns" |
    vk 2>&1)
record "public-passworded-unauthenticated-write" "$(qssrt_oneline "$pub_write")"
if printf '%s' "$pub_write" | grep -qi 'err\|authentication\|denied'; then
    check_pass "a public passworded namespace still refuses the write" \
        "$(qssrt_oneline "$pub_write")"
else
    check_fail "a public passworded namespace still refuses the write" \
        "the write was accepted: $(qssrt_oneline "$pub_write")"
fi

# --- protection: worm ------------------------------------------------------

# WORM: write once, read many. An overwrite of an existing key must be
# refused; a new key must still be accepted.
worm_ns="qssrt-worm"
vk NSNEW "$worm_ns" >/dev/null 2>&1
printf 'SELECT %s\nSET worm-key first-value\n' "$worm_ns" | vk >/dev/null 2>&1
assert_ok "NSSET arms worm" vk NSSET "$worm_ns" worm 1

worm_over=$(printf 'SELECT %s\nSET worm-key second-value\n' "$worm_ns" | vk 2>&1)
record "worm-overwrite-reply" "$(qssrt_oneline "$worm_over")"
if printf '%s' "$worm_over" | grep -qi 'err\|worm\|denied'; then
    check_pass "worm refuses an overwrite" "$(qssrt_oneline "$worm_over")"
else
    check_fail "worm refuses an overwrite" "$(qssrt_oneline "$worm_over")"
fi
assert_eq "the worm-protected value is unchanged" "first-value" \
    "$(printf 'SELECT %s\nGET worm-key\n' "$worm_ns" | vk 2>/dev/null | tail -n 1)"

worm_new=$(printf 'SELECT %s\nSET worm-key-2 new-value\n' "$worm_ns" | vk 2>&1)
if printf '%s' "$worm_new" | grep -qi 'err'; then
    check_find "worm still accepts a key that does not exist yet" \
        "$(qssrt_oneline "$worm_new")"
else
    check_pass "worm still accepts a key that does not exist yet"
fi

worm_del=$(printf 'SELECT %s\nDEL worm-key\n' "$worm_ns" | vk 2>&1)
record "worm-delete-reply" "$(qssrt_oneline "$worm_del")"
if printf '%s' "$worm_del" | grep -qi 'err\|worm\|denied'; then
    check_pass "worm refuses a delete" "$(qssrt_oneline "$worm_del")"
else
    check_find "worm refuses a delete" \
        "delete was accepted: $(qssrt_oneline "$worm_del")"
fi

# --- protection: lock ------------------------------------------------------

lock_ns="qssrt-locked"
vk NSNEW "$lock_ns" >/dev/null 2>&1
printf 'SELECT %s\nSET lock-key before-lock\n' "$lock_ns" | vk >/dev/null 2>&1
assert_ok "NSSET arms lock" vk NSSET "$lock_ns" lock 1

lock_write=$(printf 'SELECT %s\nSET lock-key after-lock\n' "$lock_ns" | vk 2>&1)
record "lock-write-reply" "$(qssrt_oneline "$lock_write")"
if printf '%s' "$lock_write" | grep -qi 'err\|lock\|denied'; then
    check_pass "a locked namespace refuses a write" "$(qssrt_oneline "$lock_write")"
else
    check_fail "a locked namespace refuses a write" "$(qssrt_oneline "$lock_write")"
fi
assert_eq "a locked namespace still reads" "before-lock" \
    "$(printf 'SELECT %s\nGET lock-key\n' "$lock_ns" | vk 2>/dev/null | tail -n 1)"

assert_ok "NSSET releases lock" vk NSSET "$lock_ns" lock 0
after=$(printf 'SELECT %s\nSET lock-key after-unlock\nGET lock-key\n' "$lock_ns" |
    vk 2>/dev/null | tail -n 1)
assert_eq "an unlocked namespace writes again" "after-unlock" "$after"

# --- limits: max_size ------------------------------------------------------

# A namespace with a byte ceiling. The write that crosses it must be
# refused, and refusing it must not damage what is already stored.
size_ns="qssrt-capped"
vk NSNEW "$size_ns" >/dev/null 2>&1
if vk NSSET "$size_ns" max_size 65536 >/dev/null 2>&1; then
    check_pass "NSSET sets a namespace size limit" "65536 bytes"
    cap_info=$(vk NSINFO "$size_ns" 2>&1)
    record "NSINFO-capped" "$(qssrt_oneline "$cap_info")"

    # The binary client, not valkey-cli: `-x` takes the value from stdin,
    # which leaves no way to put a SELECT on the same connection, and a
    # write that lands in the default namespace tests nothing about this
    # one's limit.
    fill="$(qssrt_scratch)/resp-cap"
    gen_stream resp/cap 32768 >"$fill"
    first=$(resp_cmd "$size_ns" text SET cap:1 "@$fill" 2>&1)
    record "capped-first-write" "$(qssrt_oneline "$first")"
    # 32 KiB into a 64 KiB namespace: the first fits, and the limit has to
    # bite before the fourth.
    over=""
    for n in 2 3 4; do
        over=$(resp_cmd "$size_ns" text SET "cap:$n" "@$fill" 2>&1)
        printf '%s' "$over" | grep -qi 'err\|limit\|full\|space' && break
    done
    record "capped-overflow-reply" "$(qssrt_oneline "$over")"
    if printf '%s' "$over" | grep -qi 'err\|limit\|full\|space'; then
        check_pass "a namespace over its size limit refuses the write" \
            "$(qssrt_oneline "$over")"
    else
        check_find "a namespace over its size limit refuses the write" \
            "four 32 KiB writes into a 64 KiB namespace were all accepted"
    fi
    rm -f "$fill"
else
    check_find "NSSET sets a namespace size limit" \
        "max_size was refused: $(vk NSSET "$size_ns" max_size 65536 2>&1 | head -c 200)"
fi

bad_prop=$(vk NSSET "$NS" no_such_property 1 2>&1)
if printf '%s' "$bad_prop" | grep -qi 'err\|unknown'; then
    check_pass "NSSET of an unknown property is an error" "$(qssrt_oneline "$bad_prop")"
else
    check_fail "NSSET of an unknown property is an error" "$(qssrt_oneline "$bad_prop")"
fi

# --- FLUSH ------------------------------------------------------------------

# FLUSH is only honoured on a private, password-protected namespace; on the
# default namespace it answers -ERR. valkey-cli exits 0 either way, so the
# reply TEXT is what gets graded -- an assert_ok here once passed a refusal
# and then blamed DBSIZE for the keys that never went away.
flush_default=$(vk FLUSH 2>&1)
case "$flush_default" in
*ERR*) check_pass "FLUSH on the default namespace is refused" \
    "$(qssrt_oneline "$flush_default")" ;;
*) check_fail "FLUSH on the default namespace is refused" \
    "got: $(qssrt_oneline "$flush_default")" ;;
esac

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

# A flush of one namespace is not a flush of another. The default
# namespace's keys were written in values.sh and must still be there.
assert_eq "FLUSH did not reach the default namespace" "replaced" \
    "$(vk GET qssrt:small 2>/dev/null)"
