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

# A crash cycle must start the daemon itself, at the cycle's durability.
# In the standard sequence phase 5 always left the port free; the --tb
# sequence replaces phase 5, arrived here with phase 1's daemon still
# listening, and every kill, restart and fsck of the phase failed against
# a daemon the cycle never owned. Adopt-and-stop makes the phase
# self-contained in either sequence.
s3d_ensure_stopped

cycles=${QSSRT_CRASH_CYCLES:-3}
record "crash-cycles" "$cycles"
record "durability" fsync

for cycle in $(seq 1 "$cycles"); do
    crash_cycle fsync fsync "$cycle"
done

phase_end
exit $?
