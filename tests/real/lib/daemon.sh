#!/usr/bin/env bash
# Daemon control, and the daemon-side error gate.
#
# ADR 0009: "Daemon logs captured per phase; any daemon-side error fails the
# phase regardless of client verdict." That is the whole point of this file.
# A flaky client tool can retry a real error into apparent success; the
# daemon's own log cannot.
#
# The log is ONE file for the whole run, sliced per phase by byte offset. A
# per-phase file would lose whatever the daemon wrote between phases --
# including, for the crash phases, whatever it wrote as it died.

QSSRT_S3_PID=""
QSSRT_RESPD_PID=""

qssrt_daemon_dir() { printf '%s/daemon' "$QSSRT_RUN_DIR"; }
qssrt_s3_log() { printf '%s/s3cas.log' "$(qssrt_daemon_dir)"; }
qssrt_respd_log() { printf '%s/respd.log' "$(qssrt_daemon_dir)"; }

# --- s3cas -------------------------------------------------------------

# Phases run as subprocesses of the driver, so a daemon one phase started is
# not a child of the next one. The pid file is what makes a daemon
# adoptable: a phase picks up the running daemon instead of starting a
# second one against a store whose fjall LOCK is already held.
#
# The comm check is not decoration. Pids are reused, and killing an
# unrelated process because a stale pid file named it is exactly the kind of
# damage a test harness must not be capable of.
s3d_adopt() {
    local pid_file="$(qssrt_daemon_dir)/s3cas.pid" pid
    [ -f "$pid_file" ] || return 1
    pid=$(cat "$pid_file" 2>/dev/null)
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    [ "$(cat "/proc/$pid/comm" 2>/dev/null)" = "s3cas" ] || return 1
    QSSRT_S3_PID="$pid"
    return 0
}

# The daemon this phase needs: the one already running, or a new one.
s3d_ensure_running() {
    s3d_running && return 0
    s3d_adopt && return 0
    s3d_start "$@"
}

# Starts the S3 daemon. An explicit durability level overrides the campaign
# config, which is how phase 7 runs the same crash cycle at buffer level.
s3d_start() {
    local durability="${1:-}" extra=()
    [ -n "$durability" ] && extra=(--durability "$durability")

    mkdir -p "$(qssrt_daemon_dir)" "$QSSRT_S3_STORE"

    # A daemon we did not start is already on the port. Refuse: a phase that
    # quietly talks to a stranger's daemon reports on a store nobody asked
    # about, and every measurement it takes is a fiction. (This is not
    # hypothetical -- it is how the first smoke run of this harness measured
    # an inline boundary that did not exist.)
    if s3d_tcp_ready; then
        check_fail "the S3 port is free before the daemon starts" \
            "something is already listening on $QSSRT_S3_HOST:$QSSRT_S3_PORT and it is not ours"
        return 1
    fi

    if [ "${QSSRT_DAEMON_LOG_FILTER:-0}" = "1" ]; then
        # Phase 10's log volume problem: the daemon's subscriber is pinned
        # at INFO and logs every request with its whole input, so a
        # three-million-object band writes on the order of a gigabyte. The
        # filter keeps everything the gate reads (WARN, ERROR, panics) plus
        # the startup and GC lines. Nothing that grades anything is dropped.
        "$QSSRT_BIN_DIR/s3cas" server \
            --config "$QSSRT_DAEMON_CONFIG" \
            --fs-root "$QSSRT_S3_STORE" \
            --meta-root "$QSSRT_S3_STORE" \
            "${extra[@]}" 2>&1 |
            grep -E --line-buffered \
                '(WARN|ERROR|panicked|stale-upload|^store:|server is|authentication)' \
                >>"$(qssrt_s3_log)" &
        QSSRT_S3_PID=$(pgrep -n -f "s3cas server --config $QSSRT_DAEMON_CONFIG" || true)
    else
        "$QSSRT_BIN_DIR/s3cas" server \
            --config "$QSSRT_DAEMON_CONFIG" \
            --fs-root "$QSSRT_S3_STORE" \
            --meta-root "$QSSRT_S3_STORE" \
            "${extra[@]}" >>"$(qssrt_s3_log)" 2>&1 &
        QSSRT_S3_PID=$!
    fi

    if [ -z "$QSSRT_S3_PID" ]; then
        # The filtered path launches through a pipeline, so the daemon's own
        # pid has to be looked up rather than taken from $!.
        sleep 1
        QSSRT_S3_PID=$(pgrep -n -f "s3cas server" || true)
    fi
    printf '%s' "$QSSRT_S3_PID" >"$(qssrt_daemon_dir)/s3cas.pid"

    if ! s3d_wait_ready; then
        check_fail "the daemon comes up" \
            "no answer on $QSSRT_S3_HOST:$QSSRT_S3_PORT: $(tail -n 2 "$(qssrt_s3_log)")"
        return 1
    fi
    # Ours, and still alive: a daemon that bound the port and then exited
    # would otherwise leave the phase talking to whoever takes it next.
    if ! s3d_running; then
        check_fail "the daemon that answers is the one we started" \
            "$(tail -n 2 "$(qssrt_s3_log)")"
        return 1
    fi
    return 0
}

# Is the S3 port accepting connections?
#
# A bare TCP connect, deliberately: an unsigned HTTP probe gets a perfectly
# correct AccessDenied, which the daemon logs at ERROR, which the error gate
# then reads as a daemon-side failure. The harness must not manufacture the
# evidence it grades.
s3d_tcp_ready() {
    (exec 3<>"/dev/tcp/$QSSRT_S3_HOST/$QSSRT_S3_PORT") 2>/dev/null
}

# Polls until the endpoint accepts connections, or gives up.
s3d_wait_ready() {
    qssrt_wait_for "${QSSRT_DAEMON_START_TIMEOUT:-60}" s3d_tcp_ready
}

s3d_running() {
    [ -n "$QSSRT_S3_PID" ] && kill -0 "$QSSRT_S3_PID" 2>/dev/null
}

# Graceful stop: the daemon breaks its accept loop on ctrl-c and shuts the
# GC sweeper down with it.
s3d_stop() {
    s3d_running || {
        QSSRT_S3_PID=""
        return 0
    }
    kill -INT "$QSSRT_S3_PID" 2>/dev/null
    local deadline=$((SECONDS + ${QSSRT_DAEMON_STOP_TIMEOUT:-60}))
    while [ "$SECONDS" -lt "$deadline" ]; do
        s3d_running || {
            QSSRT_S3_PID=""
            return 0
        }
        sleep 0.2
    done
    # A daemon that will not stop gracefully is a finding for whoever is
    # watching, but the phase still needs the store's lock released.
    kill -9 "$QSSRT_S3_PID" 2>/dev/null
    wait "$QSSRT_S3_PID" 2>/dev/null
    QSSRT_S3_PID=""
    return 1
}

# The campaign's crash grade, and the only one: process kill and restart.
# No power-off, no dm-flakey, no broken disks anywhere in this campaign.
s3d_kill9() {
    s3d_running || return 1
    local pid=$QSSRT_S3_PID
    kill -9 "$pid" 2>/dev/null
    while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
    QSSRT_S3_PID=""
    return 0
}

s3d_restart() {
    s3d_running && s3d_stop
    s3d_start "$@"
}

# Ensures the daemon is stopped, for the phases that need the fjall LOCK
# (every fsck run) and for the driver's own teardown.
s3d_ensure_stopped() {
    s3d_running || s3d_adopt || return 0
    s3d_stop
    return 0
}

# --- respd -------------------------------------------------------------

respd_tcp_ready() {
    (exec 3<>"/dev/tcp/$QSSRT_RESP_HOST/$QSSRT_RESP_PORT") 2>/dev/null
}

respd_start() {
    mkdir -p "$(qssrt_daemon_dir)" "$QSSRT_RESP_STORE"
    if respd_tcp_ready; then
        check_fail "the RESP port is free before respd starts" \
            "something is already listening on $QSSRT_RESP_HOST:$QSSRT_RESP_PORT"
        return 1
    fi
    "$QSSRT_BIN_DIR/respd" \
        --config "$QSSRT_DAEMON_CONFIG" \
        --data-dir "$QSSRT_RESP_STORE" \
        --host "$QSSRT_RESP_HOST" \
        --port "$QSSRT_RESP_PORT" \
        >>"$(qssrt_respd_log)" 2>&1 &
    QSSRT_RESPD_PID=$!
    printf '%s' "$QSSRT_RESPD_PID" >"$(qssrt_daemon_dir)/respd.pid"
    qssrt_wait_for "${QSSRT_DAEMON_START_TIMEOUT:-60}" \
        "$QSSRT_VALKEY_CLI" -h "$QSSRT_RESP_HOST" -p "$QSSRT_RESP_PORT" PING
}

respd_running() {
    [ -n "$QSSRT_RESPD_PID" ] && kill -0 "$QSSRT_RESPD_PID" 2>/dev/null
}

respd_adopt() {
    local pid_file="$(qssrt_daemon_dir)/respd.pid" pid
    [ -f "$pid_file" ] || return 1
    pid=$(cat "$pid_file" 2>/dev/null)
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    [ "$(cat "/proc/$pid/comm" 2>/dev/null)" = "respd" ] || return 1
    QSSRT_RESPD_PID="$pid"
    return 0
}

respd_ensure_running() {
    respd_running && return 0
    respd_adopt && return 0
    respd_start
}

respd_ensure_stopped() {
    respd_running || respd_adopt || return 0
    respd_stop
    return 0
}

respd_stop() {
    respd_running || {
        QSSRT_RESPD_PID=""
        return 0
    }
    kill -INT "$QSSRT_RESPD_PID" 2>/dev/null
    sleep 1
    kill -9 "$QSSRT_RESPD_PID" 2>/dev/null
    QSSRT_RESPD_PID=""
    return 0
}

# --- resource sampling -------------------------------------------------

# One sample of the daemon's memory and file descriptors, for the stress and
# terabyte phases. Empty when the daemon is not running, which is itself
# information at a crash boundary.
daemon_sample() {
    local pid=${1:-$QSSRT_S3_PID} rss fds
    [ -n "$pid" ] && [ -d "/proc/$pid" ] || {
        printf '\t\t'
        return 0
    }
    rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null)
    fds=$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)
    printf '%s\t%s' "${rss:-0}" "${fds:-0}"
}

# --- the log gate ------------------------------------------------------

# Remembers where each daemon log stands, so a phase's slice is exactly what
# was written while it ran.
daemon_log_mark() {
    local log
    for log in "$(qssrt_s3_log)" "$(qssrt_respd_log)"; do
        [ -f "$log" ] || continue
        stat -c %s "$log" >"${log}.mark"
    done
    return 0
}

# The bytes a daemon wrote during this phase.
daemon_log_slice() {
    local log="$1" mark=0
    [ -f "$log" ] || return 0
    [ -f "${log}.mark" ] && mark=$(cat "${log}.mark")
    tail -c "+$((mark + 1))" "$log" 2>/dev/null
}

# A phase declares an error it provokes on purpose. Phases 2, 3 and 6 do:
# a complete naming a missing part, an abort, a kill mid-write. Anything not
# declared fails the phase.
daemon_expect() {
    QSSRT_PHASE_EXPECTED_ERRORS+=("$1")
}

# The ADR's rule, applied at phase_end: any daemon-side error fails the
# phase regardless of the client verdict.
daemon_gate() {
    local log slice_file unexpected=0 total=0 line pattern matched logs=0
    for log in "$(qssrt_s3_log)" "$(qssrt_respd_log)"; do
        [ -f "$log" ] || continue
        logs=$((logs + 1))
        slice_file="$QSSRT_PHASE_DIR/$(basename "$log")"
        daemon_log_slice "$log" >"$slice_file"

        while IFS= read -r line; do
            total=$((total + 1))
            matched=0
            for pattern in "${QSSRT_PHASE_EXPECTED_ERRORS[@]:-}"; do
                [ -n "$pattern" ] || continue
                if printf '%s' "$line" | grep -qE "$pattern"; then
                    matched=1
                    break
                fi
            done
            if [ "$matched" = 0 ]; then
                unexpected=$((unexpected + 1))
                [ "$unexpected" -le 5 ] &&
                    check_fail "daemon-side error in $(basename "$log")" "$line"
            fi
        done < <(grep -E '(ERROR|panicked at)' "$slice_file" 2>/dev/null)
    done

    if [ "$unexpected" -gt 5 ]; then
        check_fail "daemon-side errors beyond the first five" \
            "$unexpected unexpected error lines in this phase"
    elif [ "$unexpected" = 0 ] && [ "$logs" -gt 0 ]; then
        # Silent when no daemon ran at all: preflight has nothing to gate.
        check_pass "no unexpected daemon-side errors" \
            "$total error line(s), all declared"
    fi
    return 0
}
