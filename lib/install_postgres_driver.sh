#!/usr/bin/env bash
set -euo pipefail

# Helpers for provisioning the Postgres JDBC driver.
#
# Expected to be sourced from `bin/local-data` (after `common.sh`) so helpers
# like `ld_log`, `ld_die`, and `ld_need_cmd` are available.

ensure_postgres_jdbc_jar() {
    local version="${1:-${PG_JDBC_VERSION:-42.7.4}}"

    [ -n "${HIVE_HOME:-}" ] || ld_die "HIVE_HOME is not set; cannot install Postgres JDBC driver"

    local dest_dir jar
    dest_dir="$HIVE_HOME/lib"
    jar="$dest_dir/postgresql-$version.jar"

    if [ -f "$jar" ]; then
        return 0
    fi

    # If Homebrew-managed dirs are not writable, fall back to a repo-owned
    # location and attach via HIVE_AUX_JARS_PATH.
    if [ ! -w "$dest_dir" ]; then
        [ -n "${BASE_DIR:-}" ] || BASE_DIR="${HOME}/local-data-platform"
        dest_dir="$BASE_DIR/lib/jars"
        jar="$dest_dir/postgresql-$version.jar"
        mkdir -p "$dest_dir"

        if [ -f "$jar" ]; then
            export HIVE_AUX_JARS_PATH="$jar${HIVE_AUX_JARS_PATH:+:$HIVE_AUX_JARS_PATH}"
            return 0
        fi

        ld_log "Hive lib not writable; will use HIVE_AUX_JARS_PATH=$jar"
        export HIVE_AUX_JARS_PATH="$jar${HIVE_AUX_JARS_PATH:+:$HIVE_AUX_JARS_PATH}"
    else
        mkdir -p "$dest_dir"
    fi

    ld_need_cmd curl

    local url tmp
    url="https://repo1.maven.org/maven2/org/postgresql/postgresql/$version/postgresql-$version.jar"
    tmp="$jar.tmp.$$"

    ld_log "Downloading Postgres JDBC driver v$version..."
    curl -fL -o "$tmp" "$url" || {
        rm -f "$tmp" 2> /dev/null || true
        ld_die "Failed to download JDBC jar: $url"
    }
    mv -f "$tmp" "$jar"
}
