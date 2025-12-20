#!/usr/bin/env bash
set -euo pipefail

# Profile + runtime config overlay management.

# Expects: common.sh sourced.

ld_sed_escape_replacement() {
    # Escape characters that are special in sed replacement strings.
    # We use '|' as delimiter, so also escape '|'.
    # Note: we assume single-line values (no newlines).
    printf '%s' "$1" | sed -e 's/[\\&|]/\\&/g'
}

ld_render_template() {
    local src="$1" dst="$2" user="$3" home="$4" base_dir="$5"

    local user_e home_e base_e tmp
    user_e="$(ld_sed_escape_replacement "$user")"
    home_e="$(ld_sed_escape_replacement "$home")"
    base_e="$(ld_sed_escape_replacement "$base_dir")"

    tmp="$dst.tmp.$$"
    sed \
        -e "s|{{USER}}|$user_e|g" \
        -e "s|{{HOME}}|$home_e|g" \
        -e "s|{{BASE_DIR}}|$base_e|g" \
        < "$src" > "$tmp"
    mv -f "$tmp" "$dst"
}

ld_copy_or_render_profile_file() {
    # Prefer <filename>.tmpl, otherwise copy <filename>.
    # Usage: ld_copy_or_render_profile_file <src_dir> <dst_file> <filename>
    local src_dir="$1" dst_file="$2" filename="$3"

    local user home base_dir src_tmpl src_plain
    user="${USER:-$(whoami)}"
    home="${HOME:-}"
    base_dir="${BASE_DIR:-}"

    src_tmpl="$src_dir/$filename.tmpl"
    src_plain="$src_dir/$filename"

    if [ -f "$src_tmpl" ]; then
        ld_render_template "$src_tmpl" "$dst_file" "$user" "$home" "$base_dir"
        return 0
    fi

    [ -f "$src_plain" ] || ld_die "Missing required config in profile: $src_plain (or template: $src_tmpl)"
    cp "$src_plain" "$dst_file"
}

ld_profile_init() {
    local repo_root="$1" base_dir="$2" force="${3:-0}"
    local src="$repo_root/conf/profiles"
    local dst="$(ld_conf_root_dir "$base_dir")/profiles"

    [ -d "$src" ] || ld_die "Missing repo profiles dir: $src"

    if [ -d "$dst" ]; then
        if [ "$force" -eq 1 ]; then
            ld_log "Re-initializing profiles (overwriting): $dst"
            rm -rf "$dst"
        else
            ld_log "Profiles already initialized: $dst"
            ld_log "  (use: local-data profile init --force to overwrite from repo defaults)"
            return 0
        fi
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
    ld_log "Using profiles from: $pdir"
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
        ld_copy_or_render_profile_file "$src_root/hadoop" "$dst_root/hadoop/$f" "$f"
    done

    # Hadoop scheduler configs (optional, but required for some schedulers)
    for f in capacity-scheduler.xml fair-scheduler.xml; do
        if [ -f "$src_root/hadoop/$f.tmpl" ] || [ -f "$src_root/hadoop/$f" ]; then
            ld_copy_or_render_profile_file "$src_root/hadoop" "$dst_root/hadoop/$f" "$f"
        fi
    done

    # Hive XML
    ld_copy_or_render_profile_file "$src_root/hive" "$dst_root/hive/hive-site.xml" "hive-site.xml"

    # Spark defaults (optional but strongly expected)
    if [ -f "$src_root/spark/spark-defaults.conf.tmpl" ] || [ -f "$src_root/spark/spark-defaults.conf" ]; then
        ld_copy_or_render_profile_file "$src_root/spark" "$dst_root/spark/spark-defaults.conf" "spark-defaults.conf"
    fi

    # Marker
    printf '%s' "$profile" > "$dst_root/.profile"
}

ld_conf_check() {
    local base_dir="$1"
    local cur
    cur="$(ld_current_conf_dir "$base_dir")"

    [ -d "$cur" ] || ld_die "Runtime conf overlay not found. Run: local-data profile set <name>"

    for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do
        [ -f "$cur/hadoop/$f" ] || ld_die "Missing runtime Hadoop config: $cur/hadoop/$f"
    done

    [ -f "$cur/hive/hive-site.xml" ] || ld_die "Missing runtime Hive config: $cur/hive/hive-site.xml"

    ld_log "OK: runtime config overlay present at $cur"
}
