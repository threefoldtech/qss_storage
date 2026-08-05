#!/usr/bin/env bash
# Phase 4: the RESP surface, end to end.
#
# ADR 0009 asked for "the full documented respd command surface: namespaces,
# SET/GET round-trips at inline sizes and the maximum accepted value,
# binary-safe payloads, wrong-type/absent-key errors, pipelined batches,
# concurrent clients", with the enumeration itself as a tripwire so a command
# added without a test here fails the phase rather than going quietly
# untested.
#
# The ADRs quoted here predate the rename: respd is respcas now. And the
# tripwire has since fired for real -- ADR 0014 gave respcas content-addressed
# namespaces and a CSET verb, which is the whole cas/ section below.
#
# The phase is sectioned rather than linear because it outgrew one file. Each
# section owns one claim and leaves the daemon usable by the next:
#
#   surface     the dispatch table is the one this phase covers, and the
#               protocol's own verbs answer
#   values      what a value can be, including the bytes a shell would eat
#   namespaces  creation, properties, protection, and who may do what
#   cas         ADR 0014: the key IS the hash, and what that costs
#   scale       batches, pipelining, concurrent connections, cursors,
#               and survival across a restart
#   bench       throughput and real device bandwidth, measured not guessed
#
# Sections are sourced, not run: they share one phase, one checks file, and
# one daemon.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 04 resp

if ! qssrt_have "$QSSRT_VALKEY_CLI"; then
    check_skip "the RESP phase runs" "$QSSRT_VALKEY_CLI is not installed"
    phase_end
    exit $?
fi
if ! qssrt_have python3; then
    check_skip "the RESP phase runs" "python3 is not installed"
    phase_end
    exit $?
fi

respcas_ensure_running || {
    check_fail "respcas is up" "it would not start"
    phase_end
    exit $?
}

# The refusals this phase provokes on purpose.
#
# respcas logs every refused command at ERROR, and the daemon-side error
# gate fails a phase for any error line it was not told to expect. That gate
# is right to be strict -- it is what catches the errors a client tool
# swallowed -- so a phase whose subject IS the refusal path has to declare
# what it is about to cause. Each pattern below corresponds to an assertion
# in a section; nothing here is a blanket silence, and an error respcas logs
# for any other reason still fails the phase.
daemon_expect 'namespace .* already exists'
daemon_expect 'Namespace not found'
daemon_expect 'Namespace is protected by worm mode'
daemon_expect 'Cannot delete a key when namespace is in worm mode'
daemon_expect 'Namespace is temporarily locked'
daemon_expect 'Authentication required for (read|write) operations'
daemon_expect 'FLUSH command is only allowed on'
daemon_expect 'Cannot flush the default namespace'
daemon_expect 'Unknown property'
daemon_expect 'does not hash to the key it was sent under'
daemon_expect 'a key in a cas namespace is 32 bytes'
daemon_expect 'the key mode can only be changed while a namespace is empty'
daemon_expect '(quota|limit|max_size|would take this namespace past)'

QSSRT_RESP_SECTIONS="$QSSRT_REAL_DIR/phases/resp"

for section in surface values namespaces cas scale bench; do
    log "--- respcas section: $section ---"
    # shellcheck disable=SC1090  # the path is computed, by design
    . "$QSSRT_RESP_SECTIONS/$section.sh"
done

phase_end
exit $?
