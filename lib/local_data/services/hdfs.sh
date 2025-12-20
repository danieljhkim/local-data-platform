#!/usr/bin/env bash
set -euo pipefail

# HDFS service management.

# Expects: common.sh sourced.

ld_hdfs_dirs() {
    local base_dir="$1"
    local sd
    sd="$(ld_state_dir "$base_dir")/hdfs"
    echo "$sd"
}

ld_hdfs_jps_pid() {
    local needle="$1"
    command -v jps > /dev/null 2>&1 || return 0
    jps -l 2> /dev/null | awk -v n="$needle" '$2 ~ n {print $1; exit}'
}

ld_hdfs_start() {
    local base_dir="$1"

    local sd log_dir pid_dir
    sd="$(ld_hdfs_dirs "$base_dir")"
    log_dir="$sd/logs"
    pid_dir="$sd/pids"
    ld_mkdirp "$log_dir" "$pid_dir"

    # NameNode
    local nn_pid
    nn_pid=""
    if [ -f "$pid_dir/namenode.pid" ] && kill -0 "$(cat "$pid_dir/namenode.pid")" 2> /dev/null; then
        nn_pid="$(cat "$pid_dir/namenode.pid")"
    else
        nn_pid="$(ld_hdfs_jps_pid NameNode || true)"
    fi

    if [ -n "$nn_pid" ] && kill -0 "$nn_pid" 2> /dev/null; then
        echo "$nn_pid" > "$pid_dir/namenode.pid"
        ld_log "HDFS NameNode already running (pid $nn_pid)."
    else
        nohup hdfs namenode > "$log_dir/namenode.log" 2>&1 &
        echo $! > "$pid_dir/namenode.pid"
        sleep 1
        if ! kill -0 "$(cat "$pid_dir/namenode.pid")" 2> /dev/null; then
            ld_die "HDFS NameNode failed to stay running. Check: $log_dir/namenode.log"
        fi
        ld_log "HDFS NameNode started (pid $(cat "$pid_dir/namenode.pid"))."
    fi

    # DataNode
    local dn_pid
    dn_pid=""
    if [ -f "$pid_dir/datanode.pid" ] && kill -0 "$(cat "$pid_dir/datanode.pid")" 2> /dev/null; then
        dn_pid="$(cat "$pid_dir/datanode.pid")"
    else
        dn_pid="$(ld_hdfs_jps_pid DataNode || true)"
    fi

    if [ -n "$dn_pid" ] && kill -0 "$dn_pid" 2> /dev/null; then
        echo "$dn_pid" > "$pid_dir/datanode.pid"
        ld_log "HDFS DataNode already running (pid $dn_pid)."
    else
        nohup hdfs datanode > "$log_dir/datanode.log" 2>&1 &
        echo $! > "$pid_dir/datanode.pid"
        sleep 1
        if ! kill -0 "$(cat "$pid_dir/datanode.pid")" 2> /dev/null; then
            ld_die "HDFS DataNode failed to stay running. Check: $log_dir/datanode.log"
        fi
        ld_log "HDFS DataNode started (pid $(cat "$pid_dir/datanode.pid"))."
    fi

    # Wait for NameNode to be ready
    ld_log "Waiting for NameNode to exit safe mode..."
    local retries=30
    while ! hdfs dfsadmin -safemode get 2> /dev/null | grep -q "OFF"; do
        retries=$((retries - 1))
        if [ "$retries" -le 0 ]; then
            ld_log "WARN: NameNode may still be in safe mode."
            break
        fi
        sleep 1
    done

    ld_log "Creating common HDFS directories..."
    hdfs dfs -mkdir -p /tmp || true
    hdfs dfs -chmod g+w /tmp || true
    hdfs dfs -mkdir -p "/user/$(whoami)" || true
    hdfs dfs -mkdir -p /user/hive/warehouse || true
    hdfs dfs -chmod g+w /user/hive/warehouse || true
}

ld_hdfs_stop() {
    local base_dir="$1"

    local sd pid_dir
    sd="$(ld_hdfs_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in datanode namenode; do
        local pidfile="$pid_dir/${svc}.pid"
        if [ -f "$pidfile" ]; then
            local pid
            pid="$(cat "$pidfile")"
            if kill -0 "$pid" 2> /dev/null; then
                kill "$pid" || true
                ld_log "Stopped HDFS $svc (pid $pid)."
            fi
            rm -f "$pidfile"
        fi
    done
}

ld_hdfs_status() {
    local base_dir="$1"

    local sd pid_dir
    sd="$(ld_hdfs_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in namenode datanode; do
        local pidfile="$pid_dir/${svc}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2> /dev/null; then
            echo "$svc: running (pid $(cat "$pidfile"))"
        else
            local jps_pid=""
            if [ "$svc" = "namenode" ]; then
                jps_pid="$(ld_hdfs_jps_pid NameNode || true)"
            else
                jps_pid="$(ld_hdfs_jps_pid DataNode || true)"
            fi
            if [ -n "$jps_pid" ] && kill -0 "$jps_pid" 2> /dev/null; then
                echo "$svc: running (pid $jps_pid)"
            else
                echo "$svc: stopped"
            fi
        fi
    done
}

ld_hdfs_logs() {
    local base_dir="$1"

    local sd log_dir
    sd="$(ld_hdfs_dirs "$base_dir")"
    log_dir="$sd/logs"

    if [ ! -d "$log_dir" ]; then
        ld_die "No HDFS logs directory found: $log_dir (have you started HDFS?)"
    fi

    local -a files
    files=("$log_dir/namenode.log" "$log_dir/datanode.log")

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
