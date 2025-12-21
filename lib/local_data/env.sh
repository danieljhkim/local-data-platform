#!/usr/bin/env bash
set -euo pipefail

# Environment computation + printing/exec.

# Expects: common.sh + overlay.sh sourced.

ld_brew_prefix() {
    brew --prefix "$1" 2> /dev/null || true
}

ld_emit_export() {
    local name="$1" value="$2"
    # Safe single-line export (bash).
    printf 'export %s=%q\n' "$name" "$value"
}

ld_env_print() {
    local repo_root="$1" base_dir="$2"

    # Ensure overlay exists before printing env; keeps wrappers hermetic.
    ld_conf_apply "$repo_root" "$base_dir" "$(ld_active_profile "$base_dir")" > /dev/null

    local current_conf
    current_conf="$(ld_current_conf_dir "$base_dir")"

    local active_profile
    active_profile="$(ld_active_profile "$base_dir")"

    # Prefer user overrides; otherwise compute from Homebrew.
    # Hadoop is optional (e.g. 'local' profile doesn't use it).
    local hadoop_home hive_home spark_home java_home
    hadoop_home="${HADOOP_HOME:-$(ld_brew_prefix hadoop)}"

    hive_home="${HIVE_HOME:-}"
    if [ -z "$hive_home" ]; then
        hive_home="$(ld_brew_prefix apache-hive)"
        [ -n "$hive_home" ] || hive_home="$(ld_brew_prefix hive)"
    fi

    spark_home="${SPARK_HOME:-}"
    if [ -z "$spark_home" ]; then
        local spark_brew
        spark_brew="$(ld_brew_prefix apache-spark)"
        [ -n "$spark_brew" ] || spark_brew="$(ld_brew_prefix spark)"
        if [ -n "$spark_brew" ]; then
            spark_home="$spark_brew/libexec"
        fi
    fi

    java_home="${JAVA_HOME:-}"
    if [ -z "$java_home" ] && command -v /usr/libexec/java_home > /dev/null 2>&1; then
        # Prefer a supported LTS JDK for Hadoop/Hive (Java 17), but fall back
        # to whatever macOS reports if 17 isn't installed.
        java_home="$(/usr/libexec/java_home -v 17 2> /dev/null || /usr/libexec/java_home 2> /dev/null || true)"
    fi

    [ -n "$hive_home" ] || ld_die "Could not determine HIVE_HOME (install Homebrew Hive or set HIVE_HOME)"

    local hadoop_conf hive_conf spark_conf
    hadoop_conf="$current_conf/hadoop"
    hive_conf="$current_conf/hive"
    spark_conf="$current_conf/spark"

    ld_emit_export BASE_DIR "$base_dir"
    ld_emit_export REPO_ROOT "$repo_root"
    ld_emit_export ACTIVE_PROFILE "$active_profile"

    # Hadoop env vars are optional (only set if hadoop_home is available)
    if [ -n "$hadoop_home" ]; then
        ld_emit_export HADOOP_HOME "$hadoop_home"
        ld_emit_export HADOOP_COMMON_HOME "${HADOOP_COMMON_HOME:-$hadoop_home}"
        ld_emit_export HADOOP_HDFS_HOME "${HADOOP_HDFS_HOME:-$hadoop_home}"
        ld_emit_export HADOOP_MAPRED_HOME "${HADOOP_MAPRED_HOME:-$hadoop_home}"
        ld_emit_export HADOOP_YARN_HOME "${HADOOP_YARN_HOME:-$hadoop_home}"
        if [ -d "$hadoop_conf" ]; then
            ld_emit_export HADOOP_CONF_DIR "$hadoop_conf"
        fi
    fi

    ld_emit_export HIVE_HOME "$hive_home"
    ld_emit_export HIVE_CONF_DIR "$hive_conf"

    if [ -n "$spark_home" ]; then
        ld_emit_export SPARK_HOME "$spark_home"
    fi

    if [ -n "$java_home" ]; then
        ld_emit_export JAVA_HOME "$java_home"
    fi

    if [ -d "$spark_conf" ]; then
        ld_emit_export SPARK_CONF_DIR "$spark_conf"
    fi

    # Build PATH with our directories first, then deduplicate
    local -a new_path_parts=()
    new_path_parts+=("$repo_root/bin")
    if [ -n "$java_home" ]; then
        new_path_parts+=("$java_home/bin")
    fi
    if [ -n "$hadoop_home" ]; then
        new_path_parts+=("$hadoop_home/bin")
        new_path_parts+=("$hadoop_home/sbin")
    fi
    new_path_parts+=("$hive_home/bin")
    if [ -n "$spark_home" ]; then
        new_path_parts+=("$spark_home/bin")
    fi

    # Deduplicate: keep first occurrence of each path component
    local seen="" final_path=""
    local IFS=':'
    for p in "${new_path_parts[@]}" $PATH; do
        [ -n "$p" ] || continue
        case ":$seen:" in
        *":$p:"*) ;; # already seen, skip
        *)
            seen="$seen:$p"
            final_path="${final_path:+$final_path:}$p"
            ;;
        esac
    done

    ld_emit_export PATH "$final_path"
}

ld_env_exec() {
    local repo_root="$1" base_dir="$2"
    shift 2

    # shellcheck disable=SC1090
    eval "$(ld_env_print "$repo_root" "$base_dir")"

    if [ "$#" -eq 0 ]; then
        ld_die "Usage: local-data env exec -- <cmd...>"
    fi

    exec "$@"
}
