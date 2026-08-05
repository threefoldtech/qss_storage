#!/usr/bin/env bash
# Throughput and real device bandwidth, measured over named windows.
#
# The campaign already grades correctness. This file answers the other
# question an operator asks: how fast, and how much of that speed reached
# the disk. Two layers, always reported together, because either one alone
# lies:
#
#   - the application layer -- objects and bytes the client acknowledged,
#     divided by wall time. This is what a user of s3cas or respcas gets.
#   - the device layer -- /proc/diskstats deltas for the block devices
#     backing the mount, over exactly the same window. This is what the
#     hardware actually moved.
#
# The ratio between them is the interesting number. Application MiB/s above
# device MiB/s means page cache absorbed the write and the campaign has not
# yet paid for it; device above application is write amplification --
# metadata, journal, checksums, and on bcachefs the copygc. A campaign that
# reports only the first number reports a cache benchmark.
#
# Discards and flushes are counted too. On a filesystem mounted with discard
# they are part of the write path's cost, and a run that fills a terabyte
# has to be able to say whether the trim traffic was material.
#
# Windows are named and may nest; each one stands alone, so a window inside
# another double-counts on purpose (a band inside a phase inside a run).

# --- device resolution -------------------------------------------------

# The /proc/diskstats names backing a path, space separated.
#
# bcachefs mounts can name several devices colon-joined in one SOURCE
# field, so the answer is a list, and every counter below sums across it.
# A device that does not appear in /proc/diskstats under its own name is
# resolved through /sys/dev/block, which is what turns /dev/mapper/x into
# dm-0.
perf_devices() {
    local path="${1:-$QSSRT_MOUNT}" src part name devs=()
    src=$(findmnt -no SOURCE --target "$path" 2>/dev/null)
    [ -n "$src" ] || return 0
    # bcachefs names every device of a multi-device filesystem in one
    # colon-joined SOURCE field, so the split is not optional here.
    # `|| [ -n "$part" ]` is load-bearing: tr emits no trailing newline for
    # the last field, so a single-device mount leaves read returning false
    # with the device still in the buffer. Without it the loop body never
    # ran, the device list came back empty, and every bandwidth column in
    # the report was a confident 0.00.
    while IFS= read -r part || [ -n "$part" ]; do
        [ -n "$part" ] || continue
        name=$(_perf_diskstat_name "$part") || continue
        devs+=("$name")
    done < <(printf '%s\n' "$src" | tr ':' '\n')
    printf '%s' "${devs[*]:-}"
}

_perf_diskstat_name() {
    local dev="$1" base rdev
    base=$(basename "$dev")
    if grep -qE "[[:space:]]$base[[:space:]]" /proc/diskstats 2>/dev/null; then
        printf '%s' "$base"
        return 0
    fi
    # Not there under that name: ask the kernel what block device it is.
    rdev=$(stat -c '%t:%T' "$dev" 2>/dev/null) || return 1
    local maj min
    maj=$((16#${rdev%%:*}))
    min=$((16#${rdev##*:}))
    base=$(basename "$(readlink -f "/sys/dev/block/$maj:$min" 2>/dev/null)" 2>/dev/null)
    [ -n "$base" ] && [ "$base" != "." ] || return 1
    printf '%s' "$base"
}

# One snapshot of the backing devices, summed, as a single TSV line:
#
#   epoch_ns  read_sectors  write_sectors  io_ms  discards  discard_sectors  flushes
#
# Sectors are the kernel's 512-byte units in every field, including the
# discard one. The nanosecond clock comes first so a window's elapsed time
# is measured by the same call that reads the counters, rather than by a
# separate `date` a scheduling delay apart.
perf_snapshot() {
    local devs="${QSSRT_PERF_DEVICES:-}"
    printf '%s\t' "$(date +%s%N)"
    awk -v devs=" $devs " '
        BEGIN { r=0; w=0; t=0; dc=0; ds=0; fl=0 }
        {
            if (index(devs, " " $3 " ") == 0) next
            r  += $6; w += $10; t += $13
            dc += $15; ds += $17; fl += $19
        }
        END { printf "%d\t%d\t%d\t%d\t%d\t%d", r, w, t, dc, ds, fl }
    ' /proc/diskstats
}

# --- windows -----------------------------------------------------------

# perf_begin <label> -- opens a measurement window.
#
# The label is the row's identity in the report and must be unique within a
# phase; nesting is fine because each window keeps its own start snapshot in
# its own variable.
perf_begin() {
    local label="$1" var
    var="QSSRT_PERF_T_$(_perf_slug "$label")"
    printf -v "$var" '%s' "$(perf_snapshot)"
    export "${var?}"
}

# perf_end <label> <objects> <bytes> [note]
#
# Closes the window and appends one row to the run's perf.tsv. `objects` is
# the count the client acknowledged and `bytes` the logical volume behind
# them -- both are the caller's honest accounting, not an estimate: a row
# whose object count came from a guess is worse than no row.
#
# Pass 0 objects for a window that moves bytes without objects (a scrub, a
# fill's verify pass); the objects-per-second column is left empty rather
# than filled with a zero that reads like a measurement.
perf_end() {
    local label="$1" objects="${2:-0}" bytes="${3:-0}" note="${4:-}"
    local var start stop
    var="QSSRT_PERF_T_$(_perf_slug "$label")"
    start="${!var:-}"
    if [ -z "$start" ]; then
        log "perf: no open window named $label; nothing recorded"
        return 0
    fi
    stop=$(perf_snapshot)
    unset "$var"

    local row
    row=$(awk -F'\t' -v label="$label" \
        -v phase="${QSSRT_PHASE_NN:-}-${QSSRT_PHASE_NAME:-}" \
        -v objects="$objects" -v bytes="$bytes" -v note="$note" \
        -v a="$start" -v b="$stop" '
        BEGIN {
            split(a, s, "\t"); split(b, e, "\t")
            secs = (e[1] - s[1]) / 1000000000.0
            if (secs <= 0) secs = 0.000000001
            SEC = 512.0; MIB = 1048576.0

            dev_r  = (e[2] - s[2]) * SEC
            dev_w  = (e[3] - s[3]) * SEC
            io_ms  = (e[4] - s[4])
            disc_n = (e[5] - s[5])
            disc_b = (e[6] - s[6]) * SEC
            flush  = (e[7] - s[7])

            ops = (objects > 0) ? sprintf("%.1f", objects / secs) : ""
            app = bytes / secs / MIB
            util = io_ms / (secs * 1000.0) * 100.0
            if (util > 100) util = 100
            amp = (bytes > 0) ? sprintf("%.3f", dev_w / bytes) : ""

            printf "%s\t%s\t%.3f\t%d\t%d\t%s\t%.2f\t%.2f\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\t%s\t%d\t%.1f\t%d\t%s",
                label, phase, secs, objects, bytes, ops, app,
                dev_r / secs / MIB, dev_w / secs / MIB, util,
                bytes / MIB, dev_r / MIB, dev_w / MIB,
                amp, disc_n, disc_b / MIB, flush, note
        }')

    _perf_ensure_header
    printf '%s\n' "$row" >>"$(perf_file)"

    # The phase log gets the human sentence; the TSV keeps the columns.
    log "perf: $(printf '%s' "$row" | awk -F'\t' '{
        printf "%s -- %.1fs", $1, $3
        if ($6 != "") printf ", %s obj/s", $6
        printf ", app %.1f MiB/s, dev-write %.1f MiB/s, amp %s, util %.0f%%",
            $7, $9, ($14 == "" ? "n/a" : $14), $10
        if ($16 > 0) printf ", discard %.0f MiB", $16
    }')"
    return 0
}

# A window that wraps a command: perf_run <label> <objects> <bytes> -- cmd...
# The command's own exit status is preserved, so a failed measured operation
# still fails its caller.
perf_run() {
    local label="$1" objects="$2" bytes="$3"
    shift 3
    [ "${1:-}" = "--" ] && shift
    perf_begin "$label"
    "$@"
    local rc=$?
    perf_end "$label" "$objects" "$bytes"
    return $rc
}

perf_file() { printf '%s/perf.tsv' "${QSSRT_RUN_DIR:?}"; }

_perf_ensure_header() {
    local f
    f=$(perf_file)
    [ -s "$f" ] && return 0
    printf 'label\tphase\tseconds\tobjects\tbytes\tobj_per_s\tapp_mib_s\t'      >"$f"
    printf 'dev_read_mib_s\tdev_write_mib_s\tutil_pct\tapp_mib\tdev_read_mib\t' >>"$f"
    printf 'dev_write_mib\twrite_amp\tdiscards\tdiscard_mib\tflushes\tnote\n'   >>"$f"
}

# Labels become shell variable names, so anything that is not a word
# character becomes an underscore.
_perf_slug() {
    local s="${1//[^a-zA-Z0-9]/_}"
    printf '%s' "$s"
}

# --- the continuous sampler --------------------------------------------

# The whole-run bandwidth curve, one line every QSSRT_PERF_SAMPLE seconds.
# Windows give per-operation averages; this gives the shape -- the stall in
# the middle of a fill that an average hides.
perf_sampler_start() {
    local out="${QSSRT_RUN_DIR:?}/bandwidth.tsv" step="${QSSRT_PERF_SAMPLE:-10}"
    [ -f "$QSSRT_RUN_DIR/sampler.pid" ] && return 0
    (
        printf 'epoch\tread_mib_s\twrite_mib_s\tutil_pct\tdiscard_mib_s\n'
        local prev now
        prev=$(perf_snapshot)
        while :; do
            sleep "$step"
            now=$(perf_snapshot)
            awk -F'\t' -v a="$prev" -v b="$now" '
                BEGIN {
                    split(a, s, "\t"); split(b, e, "\t")
                    secs = (e[1] - s[1]) / 1000000000.0
                    if (secs <= 0) next
                    printf "%d\t%.1f\t%.1f\t%.1f\t%.1f\n",
                        e[1] / 1000000000,
                        (e[2] - s[2]) * 512 / secs / 1048576,
                        (e[3] - s[3]) * 512 / secs / 1048576,
                        ((e[4] - s[4]) / (secs * 1000) * 100 > 100) ? 100 : (e[4] - s[4]) / (secs * 1000) * 100,
                        (e[6] - s[6]) * 512 / secs / 1048576
                }'
            prev=$now
        done
    ) >"$out" 2>/dev/null &
    printf '%s' "$!" >"$QSSRT_RUN_DIR/sampler.pid"
}

perf_sampler_stop() {
    local pid_file="${QSSRT_RUN_DIR:?}/sampler.pid" pid
    [ -f "$pid_file" ] || return 0
    pid=$(cat "$pid_file" 2>/dev/null)
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
    rm -f "$pid_file"
    return 0
}

# --- the rig ------------------------------------------------------------

# Everything about the filesystem and the hardware that makes a bandwidth
# number interpretable, as TSV. A MiB/s figure without the mount options
# beside it is a number, not a measurement.
perf_rig_record() {
    local out="${QSSRT_RUN_DIR:?}/rig.tsv" mount="${QSSRT_MOUNT}" dev d
    {
        printf 'mount\t%s\n' "$mount"
        printf 'fstype\t%s\n' "$(stat -f -c %T "$mount" 2>/dev/null)"
        printf 'mount_source\t%s\n' "$(findmnt -no SOURCE --target "$mount" 2>/dev/null)"
        printf 'mount_options\t%s\n' "$(findmnt -no OPTIONS --target "$mount" 2>/dev/null)"
        printf 'fs_size_bytes\t%s\n' "$(qssrt_total_bytes "$mount")"
        printf 'fs_free_bytes\t%s\n' "$(qssrt_free_bytes "$mount")"
        printf 'diskstat_devices\t%s\n' "${QSSRT_PERF_DEVICES:-}"

        for d in ${QSSRT_PERF_DEVICES:-}; do
            printf 'dev_%s_model\t%s\n' "$d" \
                "$(cat "/sys/block/$d/device/model" 2>/dev/null | tr -s ' ')"
            printf 'dev_%s_size_bytes\t%s\n' "$d" \
                "$(($(cat "/sys/block/$d/size" 2>/dev/null || echo 0) * 512))"
            printf 'dev_%s_rotational\t%s\n' "$d" \
                "$(cat "/sys/block/$d/queue/rotational" 2>/dev/null)"
            printf 'dev_%s_scheduler\t%s\n' "$d" \
                "$(cat "/sys/block/$d/queue/scheduler" 2>/dev/null)"
            printf 'dev_%s_discard_max_bytes\t%s\n' "$d" \
                "$(cat "/sys/block/$d/queue/discard_max_bytes" 2>/dev/null)"
            printf 'dev_%s_discard_granularity\t%s\n' "$d" \
                "$(cat "/sys/block/$d/queue/discard_granularity" 2>/dev/null)"
        done

        # bcachefs keeps the answers the mount options do not carry: whether
        # discard is armed on the device, and what compression will do to
        # the bytes on their way down. Both change how a MiB/s reads.
        local sysfs
        for d in ${QSSRT_PERF_DEVICES:-}; do
            sysfs="/sys/fs/bcachefs"
            [ -d "$sysfs" ] || break
            local fsdir
            for fsdir in "$sysfs"/*; do
                [ -d "$fsdir" ] || continue
                [ -e "$fsdir/$d" ] || [ "$(basename "$fsdir")" = "$d" ] || continue
                printf 'bcachefs_discard_flag\t%s\n' \
                    "$(grep -o 'discard_mount_opt_set' "$fsdir/internal/flags" 2>/dev/null ||
                        echo not_set)"
                printf 'bcachefs_compression\t%s\n' \
                    "$(cat "$fsdir/options/compression" 2>/dev/null)"
                printf 'bcachefs_data_checksum\t%s\n' \
                    "$(cat "$fsdir/options/data_checksum" 2>/dev/null)"
                break
            done
        done

        printf 'kernel\t%s\n' "$(uname -r)"
        printf 'cpus\t%s\n' "$(nproc 2>/dev/null)"
        printf 'mem_total_kb\t%s\n' "$(awk '/MemTotal/{print $2}' /proc/meminfo 2>/dev/null)"
    } >"$out"
}
