#!/usr/bin/env bash
set -euo pipefail

# env doctor: required vs optional checks.

# Expects: common.sh sourced.

ld_java_major() {
    # Parse major version from `java -version` output.
    # Examples:
    #   openjdk version "17.0.10"  -> 17
    #   openjdk version "25.0.1"   -> 25
    local line major
    line="$(java -version 2>&1 | head -n 1 || true)"
    major="$(printf '%s' "$line" | sed -nE 's/.*\"([0-9]+)(\\..*)?\".*/\\1/p')"
    printf '%s' "$major"
}

ld_doctor() {
    local target="${1:-}"

    local -a required optional
    required=(java)
    optional=(curl)

    case "$target" in
    "")
        required+=(brew)
        optional+=(spark-sql beeline)
        ;;
    "start hdfs")
        required+=(hdfs)
        optional+=(jps)
        ;;
    "start yarn")
        required+=(yarn)
        optional+=(jps)
        ;;
    "start hive")
        required+=(hive)
        optional+=(beeline)
        ;;
    "profile init" | "profile set" | "profile list" | "profile check")
        required+=(cp sed)
        ;;
    "env exec" | "env print")
        required+=(awk)
        ;;
    *)
        # Unknown target: do a reasonable baseline.
        required+=(brew)
        optional+=(spark-sql beeline)
        ;;
    esac

    local missing=0

    ld_log "Doctor (${target:-general}):"

    for cmd in "${required[@]}"; do
        if command -v "$cmd" > /dev/null 2>&1; then
            echo "  OK   $cmd"
        else
            echo "  FAIL $cmd (required)"
            missing=1
        fi
    done

    # Extra: version sanity check for Java (Hadoop/Hive are sensitive to this).
    if command -v java > /dev/null 2>&1; then
        local major
        major="$(ld_java_major || true)"
        if [ -n "$major" ] && [ "$major" -ne 17 ]; then
            echo "  WARN java major version is $major (recommended: 17)"
            echo "       Fix: install Java 17 and export JAVA_HOME=\"$(/usr/libexec/java_home -v 17 2> /dev/null || echo '<path-to-jdk17>')\""
        fi
    fi

    for cmd in "${optional[@]}"; do
        if command -v "$cmd" > /dev/null 2>&1; then
            echo "  OK   $cmd"
        else
            echo "  WARN $cmd (optional)"
        fi
    done

    if [ "$missing" -ne 0 ]; then
        return 1
    fi
}
