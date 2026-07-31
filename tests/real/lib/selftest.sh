#!/usr/bin/env bash
# The harness's own test suite: it needs no store, no daemon and no disk.
#
# Run it with `tests/real/run.sh --selftest`, or directly. It exists because
# a campaign whose asserts are wrong reports a wrong verdict with total
# confidence, which is worse than not running at all. Everything here is
# about lib/*, never about qss_storage.
#
# It writes only into a temporary directory it creates and removes.

set -uo pipefail

QSSRT_SELFTEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=tests/real/lib.sh
. "$QSSRT_SELFTEST_DIR/lib.sh"

st_pass=0
st_fail=0

st_ok() {
    st_pass=$((st_pass + 1))
    printf 'ok   %s\n' "$1"
}
st_no() {
    st_fail=$((st_fail + 1))
    printf 'FAIL %s\n     %s\n' "$1" "${2:-}"
}
st_eq() {
    if [ "$2" = "$3" ]; then st_ok "$1"; else st_no "$1" "want [$2] got [$3]"; fi
}

# --- sizes -------------------------------------------------------------

st_eq 'qssrt_bytes plain' 1048576 "$(qssrt_bytes 1048576)"
st_eq 'qssrt_bytes KiB' 4096 "$(qssrt_bytes 4KiB)"
st_eq 'qssrt_bytes MiB' 1048576 "$(qssrt_bytes 1M)"
st_eq 'qssrt_bytes GiB' 4294967296 "$(qssrt_bytes 4GiB)"
st_eq 'qssrt_bytes TiB' 3298534883328 "$(qssrt_bytes 3TiB)"
if qssrt_bytes 4ZiB >/dev/null 2>&1; then
    st_no 'qssrt_bytes refuses an unknown unit' 'it accepted 4ZiB'
else
    st_ok 'qssrt_bytes refuses an unknown unit'
fi

QSSRT_SCALE=4
st_eq 'qssrt_scaled divides' 256 "$(qssrt_scaled 1024 1)"
st_eq 'qssrt_scaled respects the floor' 512 "$(qssrt_scaled 1024 512)"
QSSRT_SCALE=1
st_eq 'qssrt_scaled at scale 1 is identity' 1024 "$(qssrt_scaled 1024 1)"

# --- text --------------------------------------------------------------

st_eq 'qssrt_oneline flattens' 'a b c' "$(qssrt_oneline $'a\tb\nc')"
st_eq 'qssrt_oneline collapses runs' 'a b' "$(qssrt_oneline 'a     b')"
QSSRT_EVIDENCE_MAX=8
st_eq 'qssrt_oneline clips' 'aaaaaaaa...' "$(qssrt_oneline aaaaaaaaaaaaaaaa)"
unset QSSRT_EVIDENCE_MAX

# --- the generator -----------------------------------------------------

QSSRT_SEED=selftest
a=$(gen_stream k1 4096 | sha256sum)
b=$(gen_stream k1 4096 | sha256sum)
c=$(gen_stream k2 4096 | sha256sum)
st_eq 'generator is deterministic' "$a" "$b"
if [ "$a" != "$c" ]; then st_ok 'generator varies with the key'; else
    st_no 'generator varies with the key' 'two keys produced the same bytes'
fi
QSSRT_SEED=selftest2
d=$(gen_stream k1 4096 | sha256sum)
QSSRT_SEED=selftest
if [ "$a" != "$d" ]; then st_ok 'generator varies with the seed'; else
    st_no 'generator varies with the seed' 'two seeds produced the same bytes'
fi
st_eq 'a short read is a prefix of a long one' \
    "$(gen_stream k1 1024 | sha256sum)" \
    "$(gen_stream k1 4096 | head -c 1024 | sha256sum)"
st_eq 'gen_md5 agrees with the stream' \
    "$(gen_stream k1 5000 | md5sum | cut -d' ' -f1)" \
    "$(gen_md5 k1 5000)"
st_eq 'gen_stream produces exactly the size asked for' 4096 \
    "$(gen_stream k1 4096 | wc -c)"

# The multipart ETag convention, computed by hand for a three-part object
# and compared against the library's one-pass version.
part=1024
total=2500
hand=$(
    tmp=$(mktemp)
    gen_stream k1 $total | head -c $part | openssl dgst -md5 -binary >>"$tmp"
    gen_stream k1 $((part * 2)) | tail -c $part | openssl dgst -md5 -binary >>"$tmp"
    gen_stream k1 $total | tail -c $((total - part * 2)) |
        openssl dgst -md5 -binary >>"$tmp"
    printf '%s-3' "$(md5sum "$tmp" | cut -d' ' -f1)"
    rm -f "$tmp"
)
st_eq 'multipart etag is md5-of-md5s with a part count' \
    "$hand" "$(gen_multipart_etag k1 $total $part)"

# --- the check vocabulary and the grading fold -------------------------

QSSRT_RUN_DIR=$(mktemp -d)
export QSSRT_RUN_DIR

phase_begin 90 selftest-pass >/dev/null
check_pass 'a passing check' >/dev/null
phase_end >/dev/null
st_eq 'a phase of passes exits 0' 0 "$?"

phase_begin 91 selftest-finding >/dev/null
check_pass 'fine' >/dev/null
check_find 'a deviation' 'evidence' >/dev/null
phase_end >/dev/null
st_eq 'a phase with a finding exits 1' 1 "$?"

phase_begin 92 selftest-fail >/dev/null
check_find 'a deviation' >/dev/null
check_fail 'a violation' 'evidence' >/dev/null
phase_end >/dev/null
st_eq 'a phase with a failure exits 2' 2 "$?"

phase_begin 93 selftest-skip >/dev/null
check_skip 'not run' 'no tool' >/dev/null
phase_end >/dev/null
st_eq 'a phase of skips exits 0' 0 "$?"

st_eq 'the run-level fold takes the worst phase' 2 "$(verdict_exit_code "$QSSRT_RUN_DIR")"
st_eq 'FAIL lines are counted across phases' 1 "$(verdict_count "$QSSRT_RUN_DIR" FAIL)"
st_eq 'FINDING lines are counted across phases' 2 "$(verdict_count "$QSSRT_RUN_DIR" FINDING)"
st_eq 'SKIP lines are counted across phases' 1 "$(verdict_count "$QSSRT_RUN_DIR" SKIP)"

verdict_render "$QSSRT_RUN_DIR" >/dev/null
if grep -q 'VERDICT: FAIL' "$QSSRT_RUN_DIR/verdict.md" &&
    grep -q 'a violation' "$QSSRT_RUN_DIR/verdict.md" &&
    grep -q 'no tool' "$QSSRT_RUN_DIR/verdict.md"; then
    st_ok 'verdict.md carries the verdict, the failures and the skips'
else
    st_no 'verdict.md carries the verdict, the failures and the skips' \
        "$(head -n 20 "$QSSRT_RUN_DIR/verdict.md")"
fi

# A findings-only run must not read as a pass.
rm -rf "${QSSRT_RUN_DIR:?}/phase-92-selftest-fail"
st_eq 'without the failure the fold drops to findings' 1 \
    "$(verdict_exit_code "$QSSRT_RUN_DIR")"

# --- assertions record rather than abort -------------------------------

phase_begin 94 selftest-asserts >/dev/null
assert_eq 'equal values pass' a a >/dev/null
assert_eq 'unequal values fail' a b >/dev/null
assert_ok 'a true command passes' true >/dev/null
assert_ok 'a false command fails' false >/dev/null
assert_err 'a command that fails with the right word passes' nonexistent-word-xyz \
    bash -c 'echo nonexistent-word-xyz; exit 1' >/dev/null
assert_err 'a command that succeeds fails the assertion' whatever true >/dev/null
st_eq 'assertions record instead of aborting: passes' 3 "$(phase_count PASS)"
st_eq 'assertions record instead of aborting: failures' 3 "$(phase_count FAIL)"

# --- block file accounting --------------------------------------------

hex64a=$(printf 'ab%062d' 0 | tr '0' 'a')
hex64b=$(printf 'ab%062d' 0 | tr '0' 'b')
hex64db=$(printf 'db%062d' 0 | tr '0' 'c')
fake=$(mktemp -d)
mkdir -p "$fake/blocks/ab" "$fake/blocks/db" "$fake/blocks/.db" \
    "$fake/blocks/.tmp" "$fake/blocks/.quarantine"
: >"$fake/blocks/ab/$hex64a"
: >"$fake/blocks/ab/$hex64b"
: >"$fake/blocks/.db/000001.sst"
: >"$fake/blocks/.db/lock"
: >"$fake/blocks/.db/version"
: >"$fake/blocks/.tmp/staged"
: >"$fake/blocks/.quarantine/$hex64a"
: >"$fake/blocks/store_header.bin"
st_eq 'block counting ignores the database and the staging areas' 2 \
    "$(qssrt_block_file_count "$fake")"

# The database lives at .db, so "db" is an ordinary fanout directory: a
# block whose hash starts with 0xdb counts like any other.
: >"$fake/blocks/db/$hex64db"
st_eq 'a 0xdb block in the db fanout directory is counted' 3 \
    "$(qssrt_block_file_count "$fake")"

# A hex-named file inside blocks/.db would mean the old collision is back
# in some new form: the detector phase 1 grades with must see it.
: >"$fake/blocks/.db/$hex64db"
st_eq 'a block file inside the database directory is reported' 1 \
    "$(qssrt_block_files_in_db_dir "$fake" | wc -l)"
st_eq 'the misplaced file is not counted as a live block' 3 \
    "$(qssrt_block_file_count "$fake")"
rm -rf "$fake"

# --- the --fresh guards ------------------------------------------------

QSSRT_MOUNT=$(mktemp -d)
QSSRT_STORE_ROOT="$QSSRT_MOUNT/store"
QSSRT_UNSAFE_ALLOW_ANY_PATH=1
phase_begin 95 selftest-fresh >/dev/null

mkdir -p "$QSSRT_STORE_ROOT"
rail_fresh >/dev/null
st_eq 'fresh is a no-op on an empty store root' 0 "$?"

: >"$QSSRT_STORE_ROOT/a-strangers-file"
rail_fresh >/dev/null
st_eq 'fresh refuses an entry it did not create' 1 "$?"
if [ -f "$QSSRT_STORE_ROOT/a-strangers-file" ]; then
    st_ok 'the refused entry is still there'
else
    st_no 'the refused entry is still there' 'fresh deleted it anyway'
fi
rm -f "$QSSRT_STORE_ROOT/a-strangers-file"

mkdir -p "$QSSRT_STORE_ROOT/s3/some-junk"
rail_fresh >/dev/null
st_eq 'fresh refuses a store root whose s3 dir is not a store' 1 "$?"
if [ -d "$QSSRT_STORE_ROOT/s3/some-junk" ]; then
    st_ok 'the not-a-store directory survives'
else
    st_no 'the not-a-store directory survives' 'fresh deleted it anyway'
fi

QSSRT_STORE_ROOT="$QSSRT_MOUNT"
rail_fresh >/dev/null
st_eq 'fresh refuses to wipe the mount itself' 1 "$?"
if [ -d "$QSSRT_MOUNT" ]; then
    st_ok 'the mount survives'
else
    st_no 'the mount survives' 'fresh removed the mount'
fi

QSSRT_STORE_ROOT="$QSSRT_MOUNT/store"
rail_check_store_root >/dev/null
st_eq 'a store root below the mount is accepted' 0 "$?"
QSSRT_STORE_ROOT="$QSSRT_MOUNT"
rail_check_store_root >/dev/null
st_eq 'a store root equal to the mount is refused' 1 "$?"

rm -rf "$QSSRT_MOUNT"
rm -rf "$QSSRT_RUN_DIR"

printf '\n%s passed, %s failed\n' "$st_pass" "$st_fail"
[ "$st_fail" = 0 ]
