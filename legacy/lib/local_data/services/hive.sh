#!/usr/bin/env bash
set -euo pipefail

# Hive service management.

# Expects: common.sh sourced.

ld_hive_dirs() {
    local base_dir="$1"
    local sd
    sd="$(ld_state_dir "$base_dir")/hive"
    echo "$sd"
}

ld_hive_start() {
    local base_dir="$1"

    local sd log_dir pid_dir
    sd="$(ld_hive_dirs "$base_dir")"
    log_dir="$sd/logs"
    pid_dir="$sd/pids"
    warehouse_dir="$sd/warehouse"
    ld_mkdirp "$log_dir" "$pid_dir" "$warehouse_dir"

    # If the active Hive config uses Postgres for the metastore, ensure the JDBC
    # driver is available before starting services.
    if [ -f "${HIVE_CONF_DIR:-}/hive-site.xml" ] &&
        grep -qE 'jdbc:postgresql:|org\.postgresql\.Driver' "${HIVE_CONF_DIR}/hive-site.xml"; then
        if declare -F ensure_postgres_jdbc_jar > /dev/null 2>&1; then
            ensure_postgres_jdbc_jar
        else
            ld_die "Postgres metastore detected but ensure_postgres_jdbc_jar is not available"
        fi
    fi

    if [ -f "$pid_dir/metastore.pid" ] && kill -0 "$(cat "$pid_dir/metastore.pid")" 2> /dev/null; then
        ld_log "Hive metastore already running (pid $(cat "$pid_dir/metastore.pid"))."
    else
        nohup hive --service metastore > "$log_dir/metastore.log" 2>&1 &
        echo $! > "$pid_dir/metastore.pid"
        ld_log "Hive metastore started (pid $(cat "$pid_dir/metastore.pid"))."
    fi

    if [ -f "$pid_dir/hiveserver2.pid" ] && kill -0 "$(cat "$pid_dir/hiveserver2.pid")" 2> /dev/null; then
        ld_log "HiveServer2 already running (pid $(cat "$pid_dir/hiveserver2.pid"))."
    else
        nohup hive --service hiveserver2 > "$log_dir/hiveserver2.log" 2>&1 &
        echo $! > "$pid_dir/hiveserver2.pid"
        ld_log "HiveServer2 started (pid $(cat "$pid_dir/hiveserver2.pid"))."
    fi
}

ld_hive_stop() {
    local base_dir="$1"

    local sd pid_dir
    sd="$(ld_hive_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in hiveserver2 metastore; do
        local pidfile="$pid_dir/${svc}.pid"
        if [ -f "$pidfile" ]; then
            local pid
            pid="$(cat "$pidfile")"
            if kill -0 "$pid" 2> /dev/null; then
                kill "$pid" || true
                ld_log "Stopped Hive $svc (pid $pid)."
            fi
            rm -f "$pidfile"
        fi
    done
}

ld_hive_stop_force() {
    local base_dir="$1"

    ld_log "Force-stopping Hive (pidfiles + listeners on 9083/10000)..."

    # First try graceful stop via pidfiles.
    ld_hive_stop "$base_dir" || true

    if ! command -v lsof > /dev/null 2>&1; then
        ld_log "WARN: lsof not found; cannot force-kill listener processes."
        return 0
    fi

    local -a ports
    ports=(9083 10000)

    for port in "${ports[@]}"; do
        local pids
        pids="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2> /dev/null | awk 'NR>1 {print $2}' | sort -u || true)"
        if [ -z "$pids" ]; then
            continue
        fi

        while IFS= read -r pid; do
            [ -n "$pid" ] || continue
            ld_hive_kill_if_hive "$pid" "port $port"
        done <<< "$pids"
    done

    # Cleanup any leftover pidfiles in our state dir.
    local sd pid_dir
    sd="$(ld_hive_dirs "$base_dir")"
    pid_dir="$sd/pids"
    rm -f "$pid_dir/metastore.pid" "$pid_dir/hiveserver2.pid" 2> /dev/null || true
}

ld_hive_kill_if_hive() {
    local pid="$1" reason="$2"

    if ! kill -0 "$pid" 2> /dev/null; then
        return 0
    fi

    local cmd
    cmd="$(ps -p "$pid" -o command= 2> /dev/null || true)"
    if [ -z "$cmd" ]; then
        ld_log "WARN: Could not inspect pid $pid; skipping."
        return 0
    fi

    # Safety: only kill if it looks like a Hive process.
    if echo "$cmd" | grep -Eq '(HiveMetaStore|HiveServer2|hiveserver2|org\.apache\.hadoop\.hive)'; then
        ld_log "Killing Hive process (pid $pid) from $reason"
        kill "$pid" 2> /dev/null || true

        # If still alive, escalate.
        local tries=10
        while kill -0 "$pid" 2> /dev/null && [ "$tries" -gt 0 ]; do
            sleep 0.2
            tries=$((tries - 1))
        done
        if kill -0 "$pid" 2> /dev/null; then
            ld_log "Escalating: kill -9 pid $pid"
            kill -9 "$pid" 2> /dev/null || true
        fi
    else
        ld_log "WARN: pid $pid is listening but doesn't look like Hive; not killing."
        ld_log "      cmd: $cmd"
    fi
}

ld_hive_status() {
    local base_dir="$1"

    local sd pid_dir
    sd="$(ld_hive_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in metastore hiveserver2; do
        local pidfile="$pid_dir/${svc}.pid"
        if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2> /dev/null; then
            echo "$svc: running (pid $(cat "$pidfile"))"
        else
            echo "$svc: stopped"
        fi
    done

    echo
    echo "listeners:"
    ld_hive_listeners_status
}

ld_hive_listeners_status() {
    if ! command -v lsof > /dev/null 2>&1; then
        echo "  WARN lsof not found; cannot check 9083/10000 listeners"
        return 0
    fi

    ld_hive_listener_line 9083 "metastore"
    ld_hive_listener_line 10000 "hiveserver2"
}

ld_hive_listener_line() {
    local port="$1" label="$2"

    # Example output:
    # COMMAND   PID USER   FD   TYPE             DEVICE SIZE/OFF NODE NAME
    # java    12345 ...
    local rows
    rows="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2> /dev/null | awk 'NR>1 {print $1":"$2}' || true)"

    if [ -z "$rows" ]; then
        echo "  $label:$port not listening"
        return 0
    fi

    while IFS= read -r row; do
        [ -n "$row" ] || continue
        local cmd pid
        cmd="${row%%:*}"
        pid="${row#*:}"
        echo "  $label:$port listening (pid $pid, cmd $cmd)"
    done <<< "$rows"
}

ld_hive_logs() {
    local base_dir="$1"

    local sd log_dir
    sd="$(ld_hive_dirs "$base_dir")"
    log_dir="$sd/logs"

    if [ ! -d "$log_dir" ]; then
        ld_die "No Hive logs directory found: $log_dir (have you started Hive?)"
    fi

    local -a files
    files=("$log_dir/metastore.log" "$log_dir/hiveserver2.log")

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
