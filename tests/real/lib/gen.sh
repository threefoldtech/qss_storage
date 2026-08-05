#!/usr/bin/env bash
# The seeded, per-key, deterministic content generator (ADR 0009 phase 10).
#
# The terabyte phase's central constraint: there is no second copy of the
# data anywhere. Three terabytes of test content cannot be kept beside three
# terabytes of stored content, so content is a pure function of the object
# key -- written by streaming the generator into the client, verified by
# streaming it back out and comparing against the same generator.
#
# Implementation: an AES-256-CTR keystream over /dev/zero, keyed by
# sha256(seed NUL key). Two properties are load-bearing:
#
#   - the key and IV are passed explicitly (-K/-iv), never derived by
#     openssl from a password. Password derivation goes through a KDF whose
#     defaults have changed across openssl releases; the campaign has to
#     reproduce the same bytes on a different machine years later.
#   - a short read is a prefix of a long one, because CTR is a keystream.
#     Ranged GETs verify against the same generator with no special case.
#
# Measured at ~5 GB/s on one core with AES-NI: the client is the
# bottleneck, which is the right way round.

# The 32-byte AES key for an object key, as hex.
_gen_key_hex() {
    printf '%s\0%s' "${QSSRT_SEED:-qss-realtest}" "$1" | sha256sum | cut -c1-64
}

# The 16-byte counter block for an object key, as hex.
_gen_iv_hex() {
    printf '%s\0%s\0iv' "${QSSRT_SEED:-qss-realtest}" "$1" | sha256sum | cut -c1-32
}

# gen_stream <key> <size> -- writes exactly <size> deterministic bytes.
gen_stream() {
    local key="$1" size="$2" k iv
    k=$(_gen_key_hex "$key")
    iv=$(_gen_iv_hex "$key")
    # head -c closes the pipe, which openssl reports as a write error. The
    # stderr redirect hides the message, but under pipefail the exit
    # status would still fail the whole pipeline -- and with it every
    # caller that checks, which is how the stress phase counted 14k
    # phantom PUT failures without one byte going wrong. The generator's
    # status is head's alone.
    { openssl enc -aes-256-ctr -K "$k" -iv "$iv" -nosalt -in /dev/zero 2>/dev/null || true; } |
        head -c "$size"
}

# gen_file <key> <size> <path> -- materialises content, for the phases small
# enough to afford it (aws-cli needs a seekable body for some operations).
gen_file() {
    gen_stream "$1" "$2" >"$3"
}

# --- many files at once -------------------------------------------------
#
# gen_file costs about four forks -- two sha256sum, one openssl, one head --
# which is nothing beside a 250 GiB object and everything beside a 1 KiB
# one. Measured on the campaign rig, the terabyte phase's tiny band spent
# 71.5 seconds per 10,000-file shard, of which ~47 was generation, at 8,283
# forks per second with the disk 95% idle. Three million objects would have
# been 5.8 hours of forking to store a few minutes of data.
#
# cas-storage/examples/qssgen.rs does a whole shard in one process at ~37,000
# files/s -- the same bytes, which is the only reason it may be used at all:
# the verification path re-derives content with gen_stream, so a generator
# that differed anywhere would fail the campaign rather than speed it up.
# The selftest compares the two.

gen_have_fast() { [ -x "$QSSRT_BIN_DIR/examples/qssgen" ]; }

# gen_files <manifest> -- materialises every file named in a TSV of
# <path> <object-key> <size> lines. Falls back to the shell generator when
# the fast one is not built, so the harness never depends on it.
gen_files() {
    local manifest="$1"
    if gen_have_fast; then
        "$QSSRT_BIN_DIR/examples/qssgen" \
            --seed "${QSSRT_SEED:-qss-realtest}" <"$manifest" 2>/dev/null
        return $?
    fi
    local dest key size
    while IFS=$'\t' read -r dest key size; do
        [ -n "$dest" ] || continue
        gen_file "$key" "$size" "$dest" || return 1
    done <"$manifest"
}

# gen_md5 <key> <size> -- the MD5 of the generated content, which is also
# the ETag a single-part PUT of it must answer.
gen_md5() {
    gen_stream "$1" "$2" | md5sum | cut -d' ' -f1
}

# gen_verify <key> <size> -- reads a stream on stdin and compares it against
# the generated content. Streaming on both sides: nothing lands on disk.
gen_verify() {
    cmp -s - <(gen_stream "$1" "$2")
}

# The multipart ETag S3 defines: MD5 of the concatenated part MD5s, then
# "-<count>". Computed here from the generator so a completed multipart
# object's ETag is checked against the convention rather than against
# whatever the server said.
#
# gen_multipart_etag <key> <size> <part-size>
#
# One pass over one generator process, splitting the stream with `head -c`
# (which consumes exactly the bytes it is asked for). Regenerating from the
# start for each part would be quadratic: a 16 GiB object at 16 MiB parts
# would generate 8 TiB of keystream to answer one question.
gen_multipart_etag() {
    local key="$1" total="$2" part="$3" left="$2" count=0 n tmp etag
    tmp=$(mktemp)
    {
        while [ "$left" -gt 0 ]; do
            n=$part
            [ "$n" -gt "$left" ] && n=$left
            # Raw digests, concatenated: the S3 convention hashes the
            # 16-byte digests, not their hex spelling.
            head -c "$n" <&3 | openssl dgst -md5 -binary >>"$tmp"
            left=$((left - n))
            count=$((count + 1))
        done
    } 3< <(gen_stream "$key" "$total")
    etag=$(md5sum "$tmp" | cut -d' ' -f1)
    rm -f "$tmp"
    printf '%s-%s' "$etag" "$count"
}
