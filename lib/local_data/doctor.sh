#!/usr/bin/env bash
set -euo pipefail

# env doctor: required vs optional checks.

# Expects: common.sh sourced.

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
    "conf apply" | "conf check" | "profile init" | "profile set" | "profile list")
        required+=(cp)
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
