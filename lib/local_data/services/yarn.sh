#!/usr/bin/env bash
set -euo pipefail

# YARN service management.

# Expects: common.sh sourced.

ld_yarn_dirs() {
    local base_dir="$1"
    local sd
    sd="$(ld_state_dir "$base_dir")/yarn"
    echo "$sd"
}

ld_yarn_jps_pid() {
    local needle="$1"
    command -v jps > /dev/null 2>&1 || return 0
    jps -l 2> /dev/null | awk -v n="$needle" '$2 ~ n {print $1; exit}'
}

ld_yarn_start() {
    local base_dir="$1"

    local sd log_dir pid_dir
    sd="$(ld_yarn_dirs "$base_dir")"
    log_dir="$sd/logs"
    pid_dir="$sd/pids"
    ld_mkdirp "$log_dir" "$pid_dir"

    local rm_pid
    rm_pid=""
    if [ -f "$pid_dir/resourcemanager.pid" ] && kill -0 "$(cat "$pid_dir/resourcemanager.pid")" 2> /dev/null; then
        rm_pid="$(cat "$pid_dir/resourcemanager.pid")"
    else
        rm_pid="$(ld_yarn_jps_pid ResourceManager || true)"
    fi

    if [ -n "$rm_pid" ] && kill -0 "$rm_pid" 2> /dev/null; then
        echo "$rm_pid" > "$pid_dir/resourcemanager.pid"
        ld_log "YARN ResourceManager already running (pid $rm_pid)."
    else
        nohup yarn resourcemanager > "$log_dir/resourcemanager.log" 2>&1 &
        echo $! > "$pid_dir/resourcemanager.pid"
        sleep 1
        if ! kill -0 "$(cat "$pid_dir/resourcemanager.pid")" 2> /dev/null; then
            ld_die "YARN ResourceManager failed to stay running. Check: $log_dir/resourcemanager.log"
        fi
        ld_log "YARN ResourceManager started (pid $(cat "$pid_dir/resourcemanager.pid"))."
    fi

    local nm_pid
    nm_pid=""
    if [ -f "$pid_dir/nodemanager.pid" ] && kill -0 "$(cat "$pid_dir/nodemanager.pid")" 2> /dev/null; then
        nm_pid="$(cat "$pid_dir/nodemanager.pid")"
    else
        nm_pid="$(ld_yarn_jps_pid NodeManager || true)"
    fi

    if [ -n "$nm_pid" ] && kill -0 "$nm_pid" 2> /dev/null; then
        echo "$nm_pid" > "$pid_dir/nodemanager.pid"
        ld_log "YARN NodeManager already running (pid $nm_pid)."
    else
        nohup yarn nodemanager > "$log_dir/nodemanager.log" 2>&1 &
        echo $! > "$pid_dir/nodemanager.pid"
        sleep 1
        if ! kill -0 "$(cat "$pid_dir/nodemanager.pid")" 2> /dev/null; then
            ld_die "YARN NodeManager failed to stay running. Check: $log_dir/nodemanager.log"
        fi
        ld_log "YARN NodeManager started (pid $(cat "$pid_dir/nodemanager.pid"))."
    fi
}

ld_yarn_stop() {
    local base_dir="$1"

    local sd pid_dir
    sd="$(ld_yarn_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in nodemanager resourcemanager; do
        local pidfile="$pid_dir/${svc}.pid"
        if [ -f "$pidfile" ]; then
            local pid
            pid="$(cat "$pidfile")"
            if kill -0 "$pid" 2> /dev/null; then
                kill "$pid" || true
                ld_log "Stopped YARN $svc (pid $pid)."
            fi
            rm -f "$pidfile"
        fi
    done
}

ld_yarn_status() {
    local base_dir="$1"

    local sd pid_dir
    sd="$(ld_yarn_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in resourcemanager nodemanager; do
        local pidfile="$pid_dir/${svc}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2> /dev/null; then
            echo "$svc: running (pid $(cat "$pidfile"))"
        else
            local jps_pid=""
            if [ "$svc" = "resourcemanager" ]; then
                jps_pid="$(ld_yarn_jps_pid ResourceManager || true)"
            else
                jps_pid="$(ld_yarn_jps_pid NodeManager || true)"
            fi
            if [ -n "$jps_pid" ] && kill -0 "$jps_pid" 2> /dev/null; then
                echo "$svc: running (pid $jps_pid)"
            else
                echo "$svc: stopped"
            fi
        fi
    done
}

ld_yarn_logs() {
    local base_dir="$1"

    local sd log_dir
    sd="$(ld_yarn_dirs "$base_dir")"
    log_dir="$sd/logs"

    if [ ! -d "$log_dir" ]; then
        ld_die "No YARN logs directory found: $log_dir (have you started YARN?)"
    fi

    local -a files
    files=("$log_dir/resourcemanager.log" "$log_dir/nodemanager.log")

    for f in "${files[@]}"; do
        echo "==> $f"
        if [ -f "$f" ]; then
            tail -n 120 "$f" || true
        else
            echo "(missing)"
        fi
        echo
    done
}
