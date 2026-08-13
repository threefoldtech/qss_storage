#!/usr/bin/env bash
# What respcas actually moves: objects per second, bytes per second, and the
# device bandwidth underneath both.
#
# Nothing here is graded. A rate is evidence; the campaign has no pass
# criterion for speed and inventing one from a single run's numbers would be
# worse than having none. What IS graded is that the load ran without
# errors -- a throughput figure measured over a stream of -ERR replies is
# not a throughput figure.
#
# The sweep spans the inline/block boundary on purpose. Small values measure
# the metadata path's per-object cost, where the answer is objects per
# second and the disk is barely involved; large values measure the block
# write path, where the answer is MiB/s and the device is the whole story.
#
# Sourced by phases/04-resp.sh. Never run on its own.

bench_secs=$(qssrt_scaled "$QSSRT_RESP_BENCH_SECONDS" "$QSSRT_RESP_BENCH_SECONDS_FLOOR")
conns="$QSSRT_RESP_BENCH_CONNECTIONS"
depth="$QSSRT_RESP_BENCH_PIPELINE"

bench_ns="qssrt-bench"
vk NSNEW "$bench_ns" >/dev/null 2>&1

# One bench leg: run it, fold its TSV into a perf window and a check line.
#
#   resp_bench <label> <op> <size-bytes> [extra respcli flags...]
resp_bench() {
    local label="$1" op="$2" size="$3"
    shift 3
    local out="$QSSRT_PHASE_DIR/bench-$label.tsv"

    perf_begin "$label"
    respcli -s "$bench_ns" bench \
        --op "$op" --size "$size" \
        --seconds "$bench_secs" \
        --connections "$conns" --pipeline "$depth" \
        --prefix "$label" \
        "$@" >"$out" 2>"$out.err"
    local rc=$?

    if [ "$rc" != 0 ] || [ ! -s "$out" ]; then
        perf_end "$label" 0 0 "bench failed"
        check_fail "the $label load runs" \
            "respcli exited $rc: $(tail -c 300 "$out.err" 2>/dev/null)"
        return 1
    fi

    local ops bytes errors first_error p50 p99 mibs opss
    ops=$(awk -F'\t' 'NR==2{print $3}' "$out")
    bytes=$(awk -F'\t' 'NR==2{print $4}' "$out")
    opss=$(awk -F'\t' 'NR==2{print $5}' "$out")
    mibs=$(awk -F'\t' 'NR==2{print $6}' "$out")
    errors=$(awk -F'\t' 'NR==2{print $7}' "$out")
    p50=$(awk -F'\t' 'NR==2{print $8}' "$out")
    p99=$(awk -F'\t' 'NR==2{print $10}' "$out")
    # Column 15, not 14: the driver's row ends misses<TAB>first_error, and
    # reading 14 put the miss count where the error text belongs -- so a leg
    # that failed reported "0" as its reason.
    first_error=$(awk -F'\t' 'NR==2{print $15}' "$out")

    perf_end "$label" "$ops" "$bytes" \
        "$conns conns, pipeline $depth, $(qssrt_human "$size") values"

    record "$label-ops-per-s" "$opss"
    record "$label-mib-per-s" "$mibs"
    record "$label-p50-ms" "$p50"
    record "$label-p99-ms" "$p99"

    if [ "${errors:-0}" = 0 ]; then
        check_pass "the $label load runs without errors" \
            "$ops ops, $opss/s, $mibs MiB/s, p99 ${p99}ms"
    else
        check_fail "the $label load runs without errors" \
            "$errors errors, first: $(qssrt_oneline "$first_error")"
    fi
    return 0
}

# --- the user-keyed sweep --------------------------------------------------

last_label=""
last_size=""
for size_str in $QSSRT_RESP_BENCH_SIZES; do
    size=$(qssrt_bytes "$size_str") || continue
    # Scale divides the value size but never below the inline boundary --
    # a smoke run that turned every leg into a 1-byte write would measure
    # the same code path four times.
    size=$(qssrt_scaled "$size" 1024)
    label="resp-set-$(qssrt_human "$size" | tr -d ' ')"
    resp_bench "$label" set "$size" && {
        last_label="$label"
        last_size="$size"
    }
done

# Reads over the keys the last write leg actually left behind. The read path
# has no block write and no fsync in it, so this is where the device stops
# being the limit and the metadata lookup starts being it.
#
# The prefix and the keyspace both come from that leg, and neither is a
# guess: the writer names keys <prefix>:<connection>:<n>, so reading any
# other prefix -- or any n past what it reached -- would measure the cost of
# a miss and call it a read.
if [ -n "$last_label" ]; then
    written=$(awk -F'\t' 'NR==2{print $3}' "$QSSRT_PHASE_DIR/bench-$last_label.tsv")
    per_conn=$(((${written:-0} / conns)))
    [ "$per_conn" -lt 1 ] && per_conn=1
    record "resp-get-keyspace-per-connection" "$per_conn"
    resp_bench "resp-get" get "$last_size" \
        --prefix "$last_label" --keyspace "$per_conn"
else
    check_skip "the resp-get load runs" "no write leg completed to read back"
fi

# --- the content-addressed sweep -------------------------------------------

# The same load through a cas namespace, which adds a whole-value BLAKE3 and
# a presence lookup to every write. The gap between this and the user-keyed
# leg at the same size is what content addressing costs.
cas_bench_ns="qssrt-bench-cas"
vk NSNEW "$cas_bench_ns" >/dev/null 2>&1
if vk NSSET "$cas_bench_ns" key_mode cas 2>&1 | grep -qi 'err'; then
    check_skip "the cas ingest load runs" "the bench namespace would not take key_mode cas"
else
    bench_ns="$cas_bench_ns"
    cas_size=$(qssrt_scaled "$(qssrt_bytes "$QSSRT_RESP_CAS_VALUE")" 1024)

    # Unique content every write: the ingest path, hashing and storing.
    resp_bench "cas-ingest-unique" cset "$cas_size"

    # Identical content every write: the dedup path, hashing and then
    # discarding. Both legs at the same value size, so the difference
    # between them is the write the second one did not do.
    resp_bench "cas-ingest-dedup" cset "$cas_size" --dedup

    dedup_ops=$(awk -F'\t' 'NR==2{print $5}' "$QSSRT_PHASE_DIR/bench-cas-ingest-dedup.tsv" 2>/dev/null)
    uniq_ops=$(awk -F'\t' 'NR==2{print $5}' "$QSSRT_PHASE_DIR/bench-cas-ingest-unique.tsv" 2>/dev/null)
    if [ -n "$dedup_ops" ] && [ -n "$uniq_ops" ]; then
        record "cas-dedup-speedup" \
            "$(awk -v d="$dedup_ops" -v u="$uniq_ops" \
                'BEGIN { printf "%.2fx", (u > 0 ? d / u : 0) }')"
    fi

    bench_ns="qssrt-bench"
fi
