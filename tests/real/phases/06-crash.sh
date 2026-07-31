#!/usr/bin/env bash
# Phase 6: kill -9 mid-storm at the campaign's default durability (fsync).
#
# ADR 0009: "start a mixed storm, kill -9 the daemon at a randomized point,
# restart, verify every operation the client saw succeed is readable and
# byte-identical; fsck residue restricted to the leak classes; --repair
# converges." The cycle itself lives in lib/crash.sh and is shared verbatim
# with phase 7: the durability matrix is only a matrix if both halves run
# the same thing.
#
# Nothing is declared to daemon_expect here. A kill -9 leaves no time to
# log, and a restart that replays its journal is INFO-grade work: a daemon
# that logs ERROR while recovering from the crash grade this store is
# specified to survive is itself a finding, and the gate makes it one.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 06 crash

cycles=${QSSRT_CRASH_CYCLES:-3}
record "crash-cycles" "$cycles"
record "durability" fsync

for cycle in $(seq 1 "$cycles"); do
    crash_cycle fsync fsync "$cycle"
done

phase_end
exit $?
