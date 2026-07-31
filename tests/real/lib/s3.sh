#!/usr/bin/env bash
# aws-cli wrappers.
#
# Two rules shape this file.
#
# 1. The campaign owns its client configuration. AWS_CONFIG_FILE and
#    AWS_SHARED_CREDENTIALS_FILE are written into the run directory, so the
#    operator's ~/.aws is never read and never touched, and every knob the
#    test depends on -- multipart threshold, chunk size, concurrency,
#    retries, addressing style -- is pinned and logged rather than
#    inherited. ADR 0009's known unknown ("aws-cli multipart chunk sizing
#    defaults change across versions") is closed here.
#
# 2. Nothing retries silently. A retried PUT that finally works is not the
#    same statement as a PUT that worked, and this campaign exists to tell
#    those apart. max_attempts is 1.

# Writes the campaign's client configuration and exports the environment
# every aws invocation runs under.
s3_setup_client() {
    local dir="$QSSRT_RUN_DIR/aws"
    mkdir -p "$dir"

    cat >"$dir/config" <<EOF
[default]
region = ${QSSRT_S3_REGION}
output = json
retry_mode = standard
max_attempts = 1
s3 =
    addressing_style = path
    multipart_threshold = ${QSSRT_MULTIPART_THRESHOLD}
    multipart_chunksize = ${QSSRT_MULTIPART_CHUNKSIZE}
    max_concurrent_requests = ${QSSRT_S3_CONCURRENCY}
    max_queue_size = 1000
EOF

    cat >"$dir/credentials" <<EOF
[default]
aws_access_key_id = ${QSSRT_ACCESS_KEY}
aws_secret_access_key = ${QSSRT_SECRET_KEY}
EOF
    chmod 600 "$dir/credentials"

    export AWS_CONFIG_FILE="$dir/config"
    export AWS_SHARED_CREDENTIALS_FILE="$dir/credentials"
    export AWS_EC2_METADATA_DISABLED=true
    export AWS_PAGER=""
    QSSRT_S3_ENDPOINT="http://$QSSRT_S3_HOST:$QSSRT_S3_PORT"
    export QSSRT_S3_ENDPOINT
}

# The endpoint-pinned aws. Every S3 call in the campaign goes through it.
awsq() {
    "$QSSRT_AWS" --endpoint-url "$QSSRT_S3_ENDPOINT" "$@"
}

# `aws s3api ...` and `aws s3 ...`, spelled out so phases read as what they
# are testing rather than as argument plumbing.
s3api() { awsq s3api "$@"; }
s3cmd() { awsq s3 "$@"; }

# --- objects -----------------------------------------------------------

# Single-part PUT from a file. The only path whose ETag is the body's MD5.
s3_put_file() {
    s3api put-object --bucket "$1" --key "$2" --body "$3"
}

# PUT of generated content, materialising it first: a single-part PUT needs
# a seekable body, and the ETag check needs the size to be exact.
s3_put_generated() {
    local bucket="$1" key="$2" size="$3" tmp
    tmp="$(qssrt_scratch)/put-$$-$RANDOM"
    gen_file "$key" "$size" "$tmp"
    s3_put_file "$bucket" "$key" "$tmp"
    local status=$?
    rm -f "$tmp"
    return $status
}

# The object's bytes on stdout, streamed through a FIFO.
#
# The obvious spellings are both wrong. `s3api get-object <outfile>` writes
# its JSON summary to stdout as well as the body to the file, so "outfile =
# /dev/stdout" corrupts the stream. And the high-level `aws s3 cp s3://... -`
# switches to RANGED downloads above multipart_threshold -- which this
# server does not implement correctly, so every large object would come back
# empty and every byte-comparison in the campaign would fail for a reason
# that has nothing to do with what it is testing.
#
# So: one unranged GET, body into a FIFO, JSON to /dev/null. The ranged
# download path is not swept under the carpet -- it gets its own explicit
# checks in phase 1, which is where a finding about it belongs.
s3_get_stream() {
    local bucket="$1" key="$2" fifo status
    fifo="$(qssrt_scratch)/get-$$-${QSSRT_GET_SEQ:-0}"
    QSSRT_GET_SEQ=$((${QSSRT_GET_SEQ:-0} + 1))
    rm -f "$fifo"
    mkfifo "$fifo" || return 1
    (
        # On failure, open and close the FIFO so the reader sees EOF instead
        # of hanging forever on a GET that never started.
        s3api get-object --bucket "$bucket" --key "$key" "$fifo" >/dev/null 2>&1 ||
            : >"$fifo"
    ) &
    timeout "${QSSRT_GET_TIMEOUT:-3600}" cat "$fifo"
    status=$?
    wait
    rm -f "$fifo"
    return $status
}

# The object's bytes, fetched the way a client actually fetches a big one:
# the high-level command, which pages the object in with ranged GETs above
# the configured threshold.
s3_download_file() {
    s3cmd cp --quiet "s3://$1/$2" "$3"
}

# The object's ETag, unquoted, or the empty string when there is none.
s3_etag() {
    s3api head-object --bucket "$1" --key "$2" --query ETag --output text 2>/dev/null |
        tr -d '"' | sed 's/^None$//'
}

# The ETag as GET reports it: head-object omits it today (see the plan's
# component 0), so a phase that wants the value rather than the assertion
# reads it here.
s3_etag_via_get() {
    s3api get-object --bucket "$1" --key "$2" /dev/null \
        --query ETag --output text 2>/dev/null | tr -d '"'
}

s3_size() {
    s3api head-object --bucket "$1" --key "$2" \
        --query ContentLength --output text 2>/dev/null
}

s3_exists() {
    s3api head-object --bucket "$1" --key "$2" >/dev/null 2>&1
}

# --- listings ----------------------------------------------------------

# Every key under a prefix, as the V2 paginator enumerates it -- which is
# how most software lists a bucket, and therefore what a listing has to get
# right.
s3_list_v2_paginated() {
    s3api list-objects-v2 --bucket "$1" ${2:+--prefix "$2"} \
        --query 'Contents[].Key' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$'
}

# Every key under a prefix, driving the continuation token by hand. This is
# the server's contract; the paginator above is the client's experience of
# it. The campaign checks both because they can differ, and when they do,
# the difference is the finding.
s3_list_v2_manual() {
    local bucket="$1" prefix="${2:-}" token="" out page=0
    while :; do
        if [ -z "$token" ]; then
            out=$(s3api list-objects-v2 --bucket "$bucket" ${prefix:+--prefix "$prefix"} \
                --max-keys 1000 --no-paginate --output json 2>/dev/null)
        else
            out=$(s3api list-objects-v2 --bucket "$bucket" ${prefix:+--prefix "$prefix"} \
                --max-keys 1000 --no-paginate --continuation-token "$token" \
                --output json 2>/dev/null)
        fi
        [ -n "$out" ] || break
        printf '%s' "$out" | qssrt_json '(.Contents // [])[].Key'
        token=$(printf '%s' "$out" | qssrt_json '.NextContinuationToken // empty')
        page=$((page + 1))
        [ -n "$token" ] || break
        [ "$page" -ge "${QSSRT_LIST_MAX_PAGES:-10000}" ] && break
    done
}

# Every key under a prefix, as the V1 paginator enumerates it.
s3_list_v1_paginated() {
    s3api list-objects --bucket "$1" ${2:+--prefix "$2"} \
        --query 'Contents[].Key' --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$'
}

# Every key under a prefix, as `aws s3 ls` shows it: the command an operator
# types.
s3_list_ls() {
    s3cmd ls --recursive "s3://$1/${2:-}" 2>/dev/null | awk '{print $4}' | grep -v '^$'
}

# PUT of generated content, streamed: the generator feeds aws-cli's stdin
# and nothing lands on local disk, which is what makes a 300 GiB giant
# possible at all. --expected-size is load-bearing twice over: it lets the
# client choose a part size that stays under the 10,000-part ceiling (300
# GiB at the pinned 8 MB chunk would be 38,400 parts), and it makes the
# resulting part sizing reproducible instead of stream-guessed.
#
# s3_put_stream <bucket> <s3-key> <gen-key> <size>
#
# The generator key is a separate argument because the dedup band writes
# the SAME content under DIFFERENT S3 keys: content is keyed by gen-key,
# placement by s3-key.
s3_put_stream() {
    local bucket="$1" key="$2" genkey="$3" size="$4"
    gen_stream "$genkey" "$size" |
        s3cmd cp --quiet --expected-size "$size" - "s3://$bucket/$key"
}

# --- bulk paths --------------------------------------------------------

# Uploads a whole directory under a prefix in one aws invocation.
#
# The startup cost of aws-cli v2 is a few hundred milliseconds, which is
# nothing next to a 300 GiB object and everything next to three million
# 1 KiB ones. Bands that write many small objects stage a shard locally and
# push it with one `cp --recursive`.
s3_put_dir() {
    s3cmd cp --recursive --quiet "$1" "s3://$2/$3"
}

# Deletes every key under a prefix, through the client, in batches.
s3_rm_prefix() {
    s3cmd rm --recursive --quiet "s3://$1/${2:-}"
}

# --- scratch -----------------------------------------------------------

# Local staging space: the results disk, never the mount, never the store.
qssrt_scratch() {
    local dir="${QSSRT_SCRATCH_DIR:-$QSSRT_RUN_DIR/scratch}"
    mkdir -p "$dir"
    printf '%s' "$dir"
}

# --- json --------------------------------------------------------------

# jq when it is there. The campaign refuses at preflight when it is not, so
# this is a convenience rather than a fallback -- the fsck report has a
# documented text rendering and that is what lib/fsck.sh falls back to.
qssrt_json() {
    if qssrt_have jq; then
        jq -r "$1"
    else
        cat >/dev/null
        printf ''
    fi
}
