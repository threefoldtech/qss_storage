#!/usr/bin/env bash
# report.md -- the performance report, folded out of perf.tsv and the
# sampler's curve.
#
# Separate from verdict.md on purpose. The verdict answers "is it correct",
# grades every line, and must stay readable by someone who only wants to
# know whether to ship. This answers "how fast, and where did the speed
# go", grades nothing, and is read with a different question in mind.
#
# Every number here is recomputed from the run's artifacts, so a report can
# be regenerated from a run that died halfway -- including one killed at the
# terabyte mark.

# Renders <run_dir>/report.md. Never grades, never sets an exit code.
report_render() {
    local run_dir="$1" md="$1/report.md" perf="$1/perf.tsv"

    {
        printf '# qss_storage campaign -- throughput and bandwidth\n\n'

        if [ ! -s "$perf" ]; then
            printf 'No measurement windows were recorded for this run.\n'
            return 0
        fi

        _report_rig "$run_dir"
        _report_headline "$run_dir"
        _report_windows "$run_dir"
        _report_by_phase "$run_dir"
        _report_curve "$run_dir"
        _report_notes
    } >"$md"
}

# --- the rig ------------------------------------------------------------

_report_rig() {
    local rig="$1/rig.tsv"
    printf '## The rig\n\n'
    if [ ! -s "$rig" ]; then
        printf 'Not recorded.\n\n'
        return 0
    fi
    printf '| | |\n| --- | --- |\n'
    awk -F'\t' '{
        gsub(/_/, " ", $1)
        printf "| %s | `%s` |\n", $1, ($2 == "" ? "-" : $2)
    }' "$rig"
    printf '\n'

    # The one sentence that makes every number below interpretable.
    local fstype opts
    fstype=$(awk -F'\t' '$1=="fstype"{print $2}' "$rig")
    opts=$(awk -F'\t' '$1=="mount_options"{print $2}' "$rig")
    printf 'Campaign content is an AES-256-CTR keystream, so it is '
    printf 'incompressible: on a filesystem\nwith compression armed, the '
    printf 'device bytes below are the real bytes, not a compression\n'
    printf 'artefact. Filesystem `%s`, mounted `%s`.\n\n' "$fstype" "$opts"
}

# --- the headline -------------------------------------------------------

# The three numbers someone asks for by name: peak object rate, peak
# application bandwidth, and what the device sustained under them.
_report_headline() {
    local perf="$1/perf.tsv"
    printf '## Headline\n\n'
    awk -F'\t' 'NR > 1 && $5 > 0 {
        tot_bytes += $5; tot_objs += $4; tot_secs += $3
        dev_w += $13; dev_r += $12
        if ($6 != "" && $6 + 0 > best_ops) { best_ops = $6 + 0; best_ops_l = $1 }
        if ($7 + 0 > best_app) { best_app = $7 + 0; best_app_l = $1 }
        if ($9 + 0 > best_dev) { best_dev = $9 + 0; best_dev_l = $1 }
    }
    END {
        if (tot_secs <= 0) { print "No windows moved any bytes.\n"; exit }
        printf "| measure | value | where |\n| --- | --- | --- |\n"
        printf "| objects acknowledged | %d | all windows |\n", tot_objs
        printf "| logical bytes moved | %.1f GiB | all windows |\n", tot_bytes / 1073741824
        printf "| device bytes written | %.1f GiB | all windows |\n", dev_w / 1024
        printf "| device bytes read | %.1f GiB | all windows |\n", dev_r / 1024
        printf "| overall write amplification | %.3f | device write / logical |\n",
            (tot_bytes > 0 ? dev_w * 1048576 / tot_bytes : 0)
        printf "| peak object rate | %.1f obj/s | %s |\n", best_ops, best_ops_l
        printf "| peak application bandwidth | %.1f MiB/s | %s |\n", best_app, best_app_l
        printf "| peak device write bandwidth | %.1f MiB/s | %s |\n", best_dev, best_dev_l
        printf "\n"
    }' "$perf"
}

# --- every window -------------------------------------------------------

_report_windows() {
    local perf="$1/perf.tsv"
    printf '## Every measured window\n\n'
    printf 'Application rates are acknowledged objects and their logical '
    printf 'bytes over wall time.\nDevice rates are `/proc/diskstats` '
    printf 'deltas for the backing devices over the same\nwindow. '
    printf '`amp` is device bytes written per logical byte.\n\n'
    printf '| window | phase | s | objects | logical | obj/s | app MiB/s | '
    printf 'dev rd MiB/s | dev wr MiB/s | amp | util%% | discard MiB |\n'
    printf '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n'
    awk -F'\t' 'NR > 1 {
        gib = $5 / 1073741824
        size = (gib >= 1) ? sprintf("%.2f GiB", gib) : sprintf("%.1f MiB", $5 / 1048576)
        printf "| %s | %s | %.1f | %s | %s | %s | %.1f | %.1f | %.1f | %s | %.0f | %.0f |\n",
            $1, $2, $3, ($4 > 0 ? $4 : "-"), size,
            ($6 == "" ? "-" : $6), $7, $8, $9,
            ($14 == "" ? "-" : $14), $10, $16
    }' "$perf"
    printf '\n'
}

# --- per phase ----------------------------------------------------------

_report_by_phase() {
    local perf="$1/perf.tsv"
    printf '## Rolled up by phase\n\n'
    printf '| phase | windows | objects | logical GiB | seconds | obj/s | '
    printf 'app MiB/s | dev wr MiB/s | amp |\n'
    printf '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n'
    awk -F'\t' 'NR > 1 {
        n[$2]++; o[$2] += $4; b[$2] += $5; s[$2] += $3; w[$2] += $13
    }
    END {
        for (p in n) {
            if (s[p] <= 0) continue
            printf "| %s | %d | %d | %.2f | %.1f | %s | %.1f | %.1f | %s |\n",
                p, n[p], o[p], b[p] / 1073741824, s[p],
                (o[p] > 0 ? sprintf("%.1f", o[p] / s[p]) : "-"),
                b[p] / s[p] / 1048576,
                w[p] / s[p],
                (b[p] > 0 ? sprintf("%.3f", w[p] * 1048576 / b[p]) : "-")
        }
    }' "$perf" | sort
    printf '\n'
}

# --- the curve ----------------------------------------------------------

# The sampler's shape, summarised. A fill that averages 400 MiB/s while
# spending a quarter of its time at zero is a different filesystem from one
# that holds 400 MiB/s flat, and the average alone cannot tell them apart.
_report_curve() {
    local bw="$1/bandwidth.tsv"
    printf '## The bandwidth curve\n\n'
    if [ ! -s "$bw" ]; then
        printf 'The sampler recorded nothing.\n\n'
        return 0
    fi
    printf 'Whole-run samples of the backing devices, one every '
    printf '%s seconds.\n\n' "${QSSRT_PERF_SAMPLE:-10}"
    awk -F'\t' 'NR > 1 {
        n++; rs += $2; ws += $3; us += $4; ds += $5
        w[n] = $3
        if ($3 > wmax) wmax = $3
        if ($3 < 1) idle++
    }
    END {
        if (n == 0) { print "No samples.\n"; exit }
        asort(w)
        med = (n % 2) ? w[int(n/2) + 1] : (w[n/2] + w[n/2 + 1]) / 2
        p95 = w[int(n * 0.95) + (int(n * 0.95) < n ? 1 : 0)]
        printf "| measure | value |\n| --- | --- |\n"
        printf "| samples | %d |\n", n
        printf "| mean write | %.1f MiB/s |\n", ws / n
        printf "| median write | %.1f MiB/s |\n", med
        printf "| p95 write | %.1f MiB/s |\n", p95
        printf "| peak write | %.1f MiB/s |\n", wmax
        printf "| mean read | %.1f MiB/s |\n", rs / n
        printf "| mean device utilisation | %.1f %% |\n", us / n
        printf "| mean discard | %.1f MiB/s |\n", ds / n
        printf "| samples under 1 MiB/s written | %d (%.0f%% of the run) |\n",
            idle, idle * 100.0 / n
        printf "\n"
    }' "$bw" 2>/dev/null || printf 'The curve needs an awk with asort (gawk).\n\n'
    printf 'Full curve: `bandwidth.tsv`.\n\n'
}

_report_notes() {
    printf '## How to read this\n\n'
    printf -- '- Application bandwidth above device bandwidth means the page '
    printf 'cache absorbed the\n  write and the run has not paid for it yet. '
    printf 'Look at the window that follows, and\n  at the fsync-bounded '
    printf 'windows, before believing a number the cache flattered.\n'
    printf -- '- Write amplification is device bytes written per logical byte '
    printf 'acknowledged. Above\n  1.0 is metadata, journal, checksums and '
    printf 'copygc; a store that writes 1 MiB blocks\n  with a metadata '
    printf 'record each has a floor above 1.0 by construction.\n'
    printf -- '- Utilisation near 100%% with low bandwidth is a seek-bound or '
    printf 'fsync-bound window,\n  not a saturated one.\n'
    printf -- '- Discard traffic is counted separately and does not enter the '
    printf 'amplification ratio;\n  on a discard-mounted filesystem it is '
    printf 'real device work that no logical byte asked\n  for.\n\n'
}
