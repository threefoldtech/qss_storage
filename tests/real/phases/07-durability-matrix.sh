#!/usr/bin/env bash
# Phase 7: the same crash cycle at durability = buffer. In the default run
# (answered review ask 2).
#
# A kill -9 is not power loss: the page cache survives the process, so the
# honest expectation at buffer level is that the DIFFERENCE from phase 6 is
# residue volume, not loss. Any client-acknowledged object that does not
# read back byte-identical is a FAIL at either level, and the campaign says
# so in the same words. What buffer level actually buys and costs against
# power loss needs a rig this campaign does not have (ADR 0009, answered
# open question: a later, separate machine).
#
# The residue tables land in this phase's measurements beside phase 6's, so
# the two levels read side by side in the verdict.

set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

phase_begin 07 durability-matrix

cycles=${QSSRT_CRASH_CYCLES:-3}
record "crash-cycles" "$cycles"
record "durability" buffer

for cycle in $(seq 1 "$cycles"); do
    crash_cycle buffer buffer "$cycle"
done

# The side-by-side: phase 6's residue lines, if it ran in this campaign,
# copied into this phase's measurements under a fsync- prefix so the
# verdict renders the matrix in one place.
p6=$(find "$QSSRT_RUN_DIR" -maxdepth 1 -type d -name 'phase-06-*' | head -n 1)
if [ -n "$p6" ] && [ -f "$p6/measurements.tsv" ]; then
    while IFS=$'\t' read -r k v; do
        case "$k" in
        cycle-*-residue | cycle-*-acked-objects) record "fsync-$k" "$v" ;;
        esac
    done <"$p6/measurements.tsv"
fi

phase_end
exit $?
