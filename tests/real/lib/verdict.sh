#!/usr/bin/env bash
# The verdict: a fold over every phase's checks.tsv, and nothing else.
#
# ADR 0009's scripting contract, once shipped: verdict.md plus an exit code,
# 0 pass / 1 findings / 2 fail. Keeping the fold here -- rather than letting
# the driver accumulate a running status as it goes -- means the verdict can
# be recomputed from the artifacts of a run that died halfway, and means a
# phase run on its own grades exactly as it would inside a full campaign.

# Counts one status across every phase of a run.
verdict_count() {
    local run_dir="$1" status="$2" n=0 f c
    while IFS= read -r f; do
        c=$(grep -c "^$status"$'\t' "$f" 2>/dev/null)
        n=$((n + ${c:-0}))
    done < <(find "$run_dir" -name checks.tsv | sort)
    printf '%s' "$n"
}

# 0 pass, 1 findings, 2 fail.
verdict_exit_code() {
    local run_dir="$1"
    [ "$(verdict_count "$run_dir" FAIL)" -gt 0 ] && {
        printf '2'
        return 0
    }
    [ "$(verdict_count "$run_dir" FINDING)" -gt 0 ] && {
        printf '1'
        return 0
    }
    printf '0'
}

verdict_word() {
    case "$1" in
    0) printf 'PASS' ;;
    1) printf 'FINDINGS' ;;
    *) printf 'FAIL' ;;
    esac
}

# Renders verdict.md and verdict.tsv, returns the exit code.
verdict_render() {
    local run_dir="$1" md="$1/verdict.md" tsv="$1/verdict.tsv"
    local code word phase_dir name nn pass fail finding skip seconds

    code=$(verdict_exit_code "$run_dir")
    word=$(verdict_word "$code")

    {
        printf '# qss_storage real-hardware campaign -- verdict\n\n'
        printf '**VERDICT: %s** (exit %s)\n\n' "$word" "$code"

        if [ "${QSSRT_UNSAFE_ALLOW_ANY_PATH:-0}" = "1" ]; then
            printf '> **NOT CAMPAIGN GRADE.** This run set '
            printf 'QSSRT_UNSAFE_ALLOW_ANY_PATH=1, so the hardware rail\n'
            printf '> (mount, filesystem type, ownership, sentinel) was waived. '
            printf 'It exercised the\n> harness, not the disk. A campaign '
            printf 'verdict comes from a run on the designated\n> hardware '
            printf 'with the rail intact.\n\n'
        fi

        printf 'The exit code is a fold over every check line, and only '
        printf 'that:\n\n'
        printf -- '- any FAIL  -> 2, a campaign pass criterion was violated;\n'
        printf -- '- else any FINDING -> 1, a deviation worth filing that is not loss;\n'
        printf -- '- else 0. SKIP never moves the code, and is always listed below.\n\n'

        printf '| phase | status | pass | fail | finding | skip | seconds |\n'
        printf '| --- | --- | --- | --- | --- | --- | --- |\n'
        while IFS= read -r phase_dir; do
            name=$(basename "$phase_dir")
            nn=${name#phase-}
            pass=$(_verdict_phase_count "$phase_dir" PASS)
            fail=$(_verdict_phase_count "$phase_dir" FAIL)
            finding=$(_verdict_phase_count "$phase_dir" FINDING)
            skip=$(_verdict_phase_count "$phase_dir" SKIP)
            seconds=$(awk -F'\t' '$1=="seconds"{print $2}' \
                "$phase_dir/timing.tsv" 2>/dev/null)
            printf '| %s | %s | %s | %s | %s | %s | %s |\n' \
                "$nn" \
                "$(verdict_word "$(_verdict_phase_code "$fail" "$finding")")" \
                "$pass" "$fail" "$finding" "$skip" "${seconds:-}"
        done < <(find "$run_dir" -maxdepth 1 -type d -name 'phase-*' | sort)
        printf '\n'

        _verdict_section "$run_dir" FAIL "Failures" \
            'Every one of these violates a campaign pass criterion.'
        _verdict_section "$run_dir" FINDING "Findings" \
            'Deviations worth filing. None of these is loss or corruption.'
        _verdict_section "$run_dir" SKIP "Skipped" \
            'Checks that did not run, and why. A skip is not a pass.'

        printf '## Measurements\n\n'
        printf 'Recorded, never graded: a number nobody has measured before '
        printf 'is not a pass\ncriterion.\n\n'
        printf '```\n'
        while IFS= read -r f; do
            printf '%s:\n' "$(basename "$(dirname "$f")")"
            sed 's/^/  /' "$f"
        done < <(find "$run_dir" -name measurements.tsv | sort)
        printf '```\n\n'

        printf '## Environment\n\n```\n'
        cat "$run_dir/run.env" 2>/dev/null
        printf '```\n'
    } >"$md"

    {
        printf 'status\tcount\n'
        local s
        for s in PASS FAIL FINDING SKIP; do
            printf '%s\t%s\n' "$s" "$(verdict_count "$run_dir" "$s")"
        done
        printf 'exit_code\t%s\n' "$code"
    } >"$tsv"

    printf '%s' "$code"
}

_verdict_phase_count() {
    local n
    n=$(grep -c "^$2"$'\t' "$1/checks.tsv" 2>/dev/null)
    printf '%s' "${n:-0}"
}

_verdict_phase_code() {
    [ "${1:-0}" -gt 0 ] && {
        printf '2'
        return 0
    }
    [ "${2:-0}" -gt 0 ] && {
        printf '1'
        return 0
    }
    printf '0'
}

# One section of the verdict: every line of a status, grouped by phase.
_verdict_section() {
    local run_dir="$1" status="$2" title="$3" blurb="$4" phase_dir any=0

    printf '## %s\n\n' "$title"
    while IFS= read -r phase_dir; do
        [ -f "$phase_dir/checks.tsv" ] || continue
        grep -q "^$status"$'\t' "$phase_dir/checks.tsv" 2>/dev/null || continue
        if [ "$any" = 0 ]; then
            printf '%s\n\n' "$blurb"
            any=1
        fi
        printf '### %s\n\n' "$(basename "$phase_dir" | sed 's/^phase-//')"
        awk -F'\t' -v s="$status" '$1==s {
            if ($3 == "") print "- " $2;
            else print "- " $2 " -- " $3
        }' "$phase_dir/checks.tsv"
        printf '\n'
    done < <(find "$run_dir" -maxdepth 1 -type d -name 'phase-*' | sort)

    [ "$any" = 0 ] && printf 'None.\n\n'
    return 0
}
