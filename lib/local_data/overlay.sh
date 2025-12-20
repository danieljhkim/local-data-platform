#!/usr/bin/env bash
set -euo pipefail

# Profile + runtime config overlay management.

# Expects: common.sh sourced.

ld_profile_init() {
    local repo_root="$1" base_dir="$2"
    local src="$repo_root/conf/profiles"
    local dst="$(ld_conf_root_dir "$base_dir")/profiles"

    [ -d "$src" ] || ld_die "Missing repo profiles dir: $src"

    if [ -d "$dst" ]; then
        ld_log "Profiles already initialized: $dst"
        return 0
    fi

    ld_log "Initializing editable profiles under: $dst"
    ld_mkdirp "$(dirname "$dst")"
    cp -R "$src" "$dst"
}

ld_profile_list() {
    local repo_root="$1" base_dir="$2"
    local pdir
    pdir="$(ld_profiles_dir "$repo_root" "$base_dir")"

    [ -d "$pdir" ] || ld_die "Missing profiles directory: $pdir"

    (cd "$pdir" && find . -maxdepth 1 -mindepth 1 -type d -print | sed 's|^\./||' | sort)
}

ld_profile_set() {
    local repo_root="$1" base_dir="$2" profile="$3"
    [ -n "$profile" ] || ld_die "Profile name required"

    local pdir
    pdir="$(ld_profiles_dir "$repo_root" "$base_dir")"

    [ -d "$pdir/$profile" ] || ld_die "Unknown profile '$profile' (expected: $pdir/$profile)"

    ld_mkdirp "$(ld_conf_root_dir "$base_dir")"
    printf '%s' "$profile" > "$(ld_active_profile_file "$base_dir")"

    ld_log "Active profile set: $profile"
    ld_conf_apply "$repo_root" "$base_dir" "$profile"
}

ld_conf_apply() {
    local repo_root="$1" base_dir="$2" profile="${3:-}"
    if [ -z "$profile" ]; then
        profile="$(ld_active_profile "$base_dir")"
    fi

    local pdir
    pdir="$(ld_profiles_dir "$repo_root" "$base_dir")"

    local src_root="$pdir/$profile"
    [ -d "$src_root" ] || ld_die "Profile not found: $src_root"

    local dst_root
    dst_root="$(ld_current_conf_dir "$base_dir")"

    ld_log "Applying runtime config overlay for profile '$profile'"
    ld_log "  from: $src_root"
    ld_log "  to:   $dst_root"

    # Materialize as plain files (no symlinks into Homebrew dirs).
    ld_mkdirp "$dst_root/hadoop" "$dst_root/hive" "$dst_root/spark"

    # Hadoop XML
    for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
        [ -f "$src_root/hadoop/$f" ] || ld_die "Missing required Hadoop config in profile: $src_root/hadoop/$f"
        cp "$src_root/hadoop/$f" "$dst_root/hadoop/$f"
    done

    # Hive XML
    [ -f "$src_root/hive/hive-site.xml" ] || ld_die "Missing required Hive config in profile: $src_root/hive/hive-site.xml"
    cp "$src_root/hive/hive-site.xml" "$dst_root/hive/hive-site.xml"

    # Spark defaults (optional but strongly expected)
    if [ -f "$src_root/spark/spark-defaults.conf" ]; then
        cp "$src_root/spark/spark-defaults.conf" "$dst_root/spark/spark-defaults.conf"
    fi

    # Marker
    printf '%s' "$profile" > "$dst_root/.profile"
}

ld_conf_check() {
    local base_dir="$1"
    local cur
    cur="$(ld_current_conf_dir "$base_dir")"

    [ -d "$cur" ] || ld_die "Runtime conf overlay not found. Run: local-data conf apply"

    for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
        [ -f "$cur/hadoop/$f" ] || ld_die "Missing runtime Hadoop config: $cur/hadoop/$f"
    done

    [ -f "$cur/hive/hive-site.xml" ] || ld_die "Missing runtime Hive config: $cur/hive/hive-site.xml"

    ld_log "OK: runtime config overlay present at $cur"
}
