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

ld_hdfs_ensure_local_storage_dirs() {
    # Ensure local filesystem paths referenced by our profiles exist so Hadoop
    # doesn't fail early with "storage directory does not exist".
    local base_dir="$1"

    # These match our templated profiles:
    # - dfs.namenode.name.dir = file:$BASE_DIR/state/hdfs/namenode
    # - dfs.datanode.data.dir = file:$BASE_DIR/state/hdfs/datanode
    # - hadoop.tmp.dir        = $BASE_DIR/state/hadoop/tmp
    ld_mkdirp \
        "$base_dir/state/hdfs/namenode" \
        "$base_dir/state/hdfs/datanode" \
        "$base_dir/state/hadoop/tmp"
}

ld_hdfs_apply_overlay_env() {
    # Ensure daemons use the runtime overlay config even if the caller didn't
    # `eval "$(local-data env print)"` (or if older daemons were started with a
    # different BASE_DIR).
    local base_dir="$1"
    local conf_dir
    conf_dir="$(ld_current_conf_dir "$base_dir")/hadoop"
    if [ -d "$conf_dir" ]; then
        export HADOOP_CONF_DIR="$conf_dir"
    fi
}

ld_hdfs_ps_env() {
    # Best-effort: return ps output including env vars (macOS supports `ps eww`).
    local pid="$1"
    ps eww -p "$pid" 2> /dev/null || ps -p "$pid" -o command= 2> /dev/null || true
}

ld_hdfs_pid_uses_current_conf() {
    local pid="$1" base_dir="$2"
    local conf_dir
    conf_dir="$(ld_current_conf_dir "$base_dir")/hadoop"
    [ -n "$conf_dir" ] || return 1
    local info
    info="$(ld_hdfs_ps_env "$pid")"
    echo "$info" | grep -Fq "HADOOP_CONF_DIR=$conf_dir"
}

ld_hdfs_jps_pid() {
    local needle="$1"
    command -v jps > /dev/null 2>&1 || return 0
    jps -l 2> /dev/null | awk -v n="$needle" '$2 ~ n {print $1; exit}'
}

ld_hdfs_pgrep_pid() {
    local needle="$1"
    command -v pgrep > /dev/null 2>&1 || return 0
    # Match common Java main class names.
    pgrep -f "$needle" 2> /dev/null | head -n 1 || true
}

ld_hdfs_find_pid() {
    local svc="$1"
    local jps_pid=""
    if [ "$svc" = "namenode" ]; then
        jps_pid="$(ld_hdfs_jps_pid NameNode || true)"
        [ -n "$jps_pid" ] || jps_pid="$(ld_hdfs_pgrep_pid 'org\\.apache\\.hadoop\\.hdfs\\.server\\.namenode\\.NameNode' || true)"
    else
        jps_pid="$(ld_hdfs_jps_pid DataNode || true)"
        [ -n "$jps_pid" ] || jps_pid="$(ld_hdfs_pgrep_pid 'org\\.apache\\.hadoop\\.hdfs\\.server\\.datanode\\.DataNode' || true)"
    fi
    printf '%s' "$jps_pid"
}

ld_hdfs_namenode_dirs_from_conf() {
    # Returns one local filesystem path per line, derived from dfs.namenode.name.dir.
    # Supports values like:
    #   file:/abs/path
    #   file:///abs/path
    #   file:/a,file:/b
    local conf="${HADOOP_CONF_DIR:-}/hdfs-site.xml"
    [ -f "$conf" ] || return 0

    local raw
    # Keep parsing logic intentionally simple/portable (BSD awk compatible).
    raw="$(
        awk '
          $0 ~ /dfs\\.namenode\\.name\\.dir/ {inprop=1; next}
          inprop && $0 ~ /<value>/ {
            line=$0
            sub(/.*<value>[[:space:]]*/, "", line)
            sub(/[[:space:]]*<\\/value>.*/, "", line)
            print line
            exit
          }
          inprop && $0 ~ /<\\/property>/ {inprop=0}
        ' "$conf" 2> /dev/null || true
    )"
    [ -n "$raw" ] || return 0

    # Split on commas
    printf '%s' "$raw" | tr ',' '\n' | while IFS= read -r uri; do
        uri="$(printf '%s' "$uri" | tr -d '[:space:]')"
        [ -n "$uri" ] || continue
        case "$uri" in
        file:*)
            local path="${uri#file:}"
            # Normalize file:/// -> /
            while [ "${path#///}" != "$path" ]; do
                path="/${path#///}"
            done
            while [ "${path#//}" != "$path" ]; do
                path="/${path#//}"
            done
            [ -n "$path" ] || continue
            printf '%s\n' "$path"
            ;;
        esac
    done
}

ld_hdfs_namenode_is_formatted() {
    local dir="$1"
    [ -f "$dir/current/VERSION" ]
}

ld_hdfs_ensure_namenode_formatted() {
    # Auto-format NameNode on first run when the configured storage dir is empty.
    # This avoids requiring users to run `hdfs namenode -format` manually, while
    # still refusing to blow away non-empty dirs automatically.
    local base_dir="$1"

    # If NameNode is already running, don't touch formatting.
    local nn_pid
    nn_pid="$(ld_hdfs_find_pid namenode || true)"
    if [ -n "$nn_pid" ] && kill -0 "$nn_pid" 2> /dev/null; then
        return 0
    fi

    local -a dirs
    dirs=()
    local dir
    while IFS= read -r dir; do
        [ -n "$dir" ] || continue
        dirs+=("$dir")
    done < <(ld_hdfs_namenode_dirs_from_conf)

    # Fallback: if we couldn't parse config, use our standard BASE_DIR-scoped layout.
    if [ "${#dirs[@]}" -eq 0 ]; then
        dirs+=("$base_dir/state/hdfs/namenode")
    fi

    for dir in "${dirs[@]}"; do
        # Ensure directory exists
        ld_mkdirp "$dir"

        if ld_hdfs_namenode_is_formatted "$dir"; then
            continue
        fi

        # Only auto-format when dir is empty (safe default).
        local count
        count="$(find "$dir" -mindepth 1 -maxdepth 1 -print 2> /dev/null | wc -l | tr -d ' ')"
        if [ "${count:-0}" -eq 0 ]; then
            ld_log "NameNode not formatted; auto-formatting (dir: $dir)"
            hdfs namenode -format -force -nonInteractive > /dev/null
        else
            ld_die "NameNode directory exists but is not formatted and is not empty: $dir. Refusing to auto-format. Run: local-data env exec -- hdfs namenode -format -force -nonInteractive"
        fi
    done
}

ld_hdfs_start() {
    local base_dir="$1"

    ld_hdfs_apply_overlay_env "$base_dir"
    ld_hdfs_ensure_local_storage_dirs "$base_dir"
    ld_hdfs_ensure_namenode_formatted "$base_dir"

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
        nn_pid="$(ld_hdfs_find_pid namenode || true)"
    fi

    if [ -n "$nn_pid" ] && kill -0 "$nn_pid" 2> /dev/null; then
        # If an existing daemon is running but not using our current overlay config,
        # restart it so it picks up the active profile.
        if ! ld_hdfs_pid_uses_current_conf "$nn_pid" "$base_dir"; then
            ld_log "HDFS NameNode running but not using current overlay config; restarting (pid $nn_pid)."
            kill "$nn_pid" 2> /dev/null || true
            sleep 0.5
            nn_pid=""
        fi
    fi

    if [ -n "$nn_pid" ] && kill -0 "$nn_pid" 2> /dev/null; then
        echo "$nn_pid" > "$pid_dir/namenode.pid"
        ld_log "HDFS NameNode already running (pid $nn_pid)."
    else
        nohup env HADOOP_CONF_DIR="$HADOOP_CONF_DIR" hdfs namenode > "$log_dir/namenode.log" 2>&1 &
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
        dn_pid="$(ld_hdfs_find_pid datanode || true)"
    fi

    if [ -n "$dn_pid" ] && kill -0 "$dn_pid" 2> /dev/null; then
        if ! ld_hdfs_pid_uses_current_conf "$dn_pid" "$base_dir"; then
            ld_log "HDFS DataNode running but not using current overlay config; restarting (pid $dn_pid)."
            kill "$dn_pid" 2> /dev/null || true
            sleep 0.5
            dn_pid=""
        fi
    fi

    if [ -n "$dn_pid" ] && kill -0 "$dn_pid" 2> /dev/null; then
        echo "$dn_pid" > "$pid_dir/datanode.pid"
        ld_log "HDFS DataNode already running (pid $dn_pid)."
    else
        nohup env HADOOP_CONF_DIR="$HADOOP_CONF_DIR" hdfs datanode > "$log_dir/datanode.log" 2>&1 &
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

    ld_hdfs_apply_overlay_env "$base_dir"

    local sd pid_dir
    sd="$(ld_hdfs_dirs "$base_dir")"
    pid_dir="$sd/pids"

    for svc in datanode namenode; do
        local pidfile="$pid_dir/${svc}.pid"
        local pid=""
        if [ -f "$pidfile" ]; then
            pid="$(cat "$pidfile" 2> /dev/null || true)"
        fi

        if [ -n "$pid" ] && kill -0 "$pid" 2> /dev/null; then
            kill "$pid" || true
            ld_log "Stopped HDFS $svc (pid $pid)."
            rm -f "$pidfile"
            continue
        fi

        # Fallback: if pidfile is missing/stale, try to find and stop via jps/pgrep.
        local found_pid
        found_pid="$(ld_hdfs_find_pid "$svc" || true)"
        if [ -n "$found_pid" ] && kill -0 "$found_pid" 2> /dev/null; then
            kill "$found_pid" || true
            ld_log "Stopped HDFS $svc (pid $found_pid) via process lookup."
        fi
        rm -f "$pidfile"
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
