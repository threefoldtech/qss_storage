#!/usr/bin/env bash
# The dispatch table, and the protocol verbs that carry no data.
#
# Sourced by phases/04-resp.sh. Never run on its own.

# --- the tripwire -------------------------------------------------------
#
# The pinned list is the contract: every command respcas dispatches must be
# covered somewhere in this phase. Growing the surface without growing the
# phase fails here, which is the point.
#
# CSET arrived with ADR 0014 and is covered in cas.sh.
PINNED="AUTH CHECK CSET DBSIZE DEL ECHO EXISTS FLUSH GET KEYTIME LENGTH MGET NSINFO NSLIST NSNEW NSSET PING RSCAN SCAN SELECT SET TIME"
actual=$(resp_surface_from_source)
record "resp-surface" "$actual"
record "resp-surface-count" "$(printf '%s' "$actual" | wc -w)"

if [ -z "$actual" ]; then
    # The tripwire itself is broken. Say so as a failure rather than
    # reporting a surface change: an empty answer is not an empty surface,
    # and the phase must not be able to pass by failing to look.
    check_fail "the command surface can be read from the source at all" \
        "resp_surface_from_source found no dispatch arms; it is looking in the wrong file"
elif [ "$PINNED" = "$actual" ]; then
    check_pass "respcas's command surface is the one this phase covers" \
        "$(printf '%s' "$PINNED" | wc -w) commands"
else
    check_fail "respcas's command surface is the one this phase covers" \
        "pinned [$PINNED] but the dispatch table says [$actual]: grow this phase with it"
fi

# --- the verbs that carry no data ---------------------------------------

assert_ok "PING answers" vk PING
assert_eq "ECHO returns its message" "qssrt-echo" "$(vk ECHO qssrt-echo 2>/dev/null)"

time_reply=$(vk TIME 2>/dev/null | head -n 1)
if printf '%s' "$time_reply" | grep -qE '^[0-9]+$'; then
    check_pass "TIME answers with a clock" "$time_reply"
else
    check_find "TIME answers with a clock" "$(qssrt_oneline "$time_reply")"
fi

# AUTH exists on the surface but this daemon runs without an admin password,
# which is what makes every connection admin (server.rs: is_admin =
# admin_password.is_none()). So the assertion is that AUTH answers rather
# than that it grants anything: a campaign that configured a password here
# would be testing a different daemon than the rest of the phase.
auth_reply=$(vk AUTH whatever 2>&1)
record "AUTH-reply-no-admin-password" "$(qssrt_oneline "$auth_reply")"
if [ -n "$auth_reply" ]; then
    check_pass "AUTH answers when no admin password is configured" \
        "$(qssrt_oneline "$auth_reply")"
else
    check_fail "AUTH answers when no admin password is configured" "empty reply"
fi

# --- how the protocol refuses -------------------------------------------

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

# Too many arguments, not too few: the other side of the same check, and the
# one a parser written around a minimum arity gets wrong.
arity_long=$(vk GET a b c 2>&1)
if printf '%s' "$arity_long" | grep -qi 'err\|argument'; then
    check_pass "GET with too many arguments is an error" \
        "$(qssrt_oneline "$arity_long")"
else
    check_fail "GET with too many arguments is an error" \
        "$(qssrt_oneline "$arity_long")"
fi

# A command sent with no arguments at all. An empty inline command is what a
# client sends when a stray newline reaches the socket, and it must not
# disturb the connection.
empty_cmd=$(printf '\nPING\n' | vk 2>&1 | tail -n 1)
if printf '%s' "$empty_cmd" | grep -qi 'pong\|ok'; then
    check_pass "a stray empty line does not disturb the connection" \
        "$(qssrt_oneline "$empty_cmd")"
else
    check_find "a stray empty line does not disturb the connection" \
        "$(qssrt_oneline "$empty_cmd")"
fi
