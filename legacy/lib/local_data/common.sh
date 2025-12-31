#!/usr/bin/env bash
set -euo pipefail

# Shared helpers for the local-data CLI.

ld_die() {
    echo "ERROR: $*" >&2
    exit 1
}
ld_log() { echo "==> $*"; }

ld_need_cmd() {
    command -v "$1" > /dev/null 2>&1 || ld_die "Missing command: $1"
}

ld_script_dir() {
    cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
}

ld_repo_root() {
    local start_dir="$1"
    if [ -d "$start_dir/conf" ]; then
        echo "$start_dir"
    elif [ -d "$start_dir/../conf" ]; then
        cd "$start_dir/.." && pwd
    else
        ld_die "Could not find 'conf/' directory next to bin/ or repo root."
    fi
}

ld_default_base_dir() {
    echo "${BASE_DIR:-$HOME/local-data-platform}"
}

ld_state_dir() {
    local base_dir="$1"
    echo "$base_dir/state"
}

ld_conf_root_dir() {
    local base_dir="$1"
    echo "$base_dir/conf"
}

ld_current_conf_dir() {
    local base_dir="$1"
    echo "$(ld_conf_root_dir "$base_dir")/current"
}

ld_active_profile_file() {
    local base_dir="$1"
    echo "$(ld_conf_root_dir "$base_dir")/active_profile"
}

ld_active_profile() {
    local base_dir="$1"
    local f
    f="$(ld_active_profile_file "$base_dir")"
    if [ -f "$f" ]; then
        cat "$f"
    else
        echo "local"
    fi
}

ld_profiles_dir() {
    local repo_root="$1"
    local base_dir="$2"

    # Prefer user-initialized profiles under BASE_DIR, fall back to repo defaults.
    if [ -d "$(ld_conf_root_dir "$base_dir")/profiles" ]; then
        echo "$(ld_conf_root_dir "$base_dir")/profiles"
    else
        echo "$repo_root/conf/profiles"
    fi
}

ld_mkdirp() {
    mkdir -p "$@"
}
