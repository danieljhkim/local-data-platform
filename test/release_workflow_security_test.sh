#!/usr/bin/env bash

set -euo pipefail

workflow="${1:-.github/workflows/release.yml}"

fail() {
  echo "release workflow security test: $*" >&2
  exit 1
}

[[ -f "$workflow" ]] || fail "workflow not found: $workflow"

grep -Fq 'TAG_NAME: ${{ github.ref_name }}' "$workflow" || fail "tag name is not passed through step env"
grep -Fq '[[ ! "$TAG_NAME" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]' "$workflow" || fail "release tag allowlist is missing"

if awk '
  function indent(line, prefix) {
    prefix = line
    sub(/[^[:space:]].*$/, "", prefix)
    return length(prefix)
  }
  /^[[:space:]]*run:[[:space:]]*\|[[:space:]]*$/ {
    in_run = 1
    run_indent = indent($0)
    next
  }
  in_run && $0 !~ /^[[:space:]]*$/ && indent($0) <= run_indent { in_run = 0 }
  in_run && /\$\{\{/ { print FNR ": " $0; invalid = 1 }
  END { exit invalid }
' "$workflow"; then
  :
else
  fail "GitHub expressions must not be interpolated inside run scripts"
fi

if grep -q 'perl -' "$workflow"; then
  fail "formula update must not generate Perl source from dynamic values"
fi

probe_dir="$(mktemp -d)"
probe="$probe_dir/command-executed"
trap 'rm -rf "$probe_dir"' EXIT

payloads=(
  'v$(touch${IFS}${MARKER})'
  'v1.2.3";touch${IFS}${MARKER};#'
)

for tag in "${payloads[@]}"; do
  git check-ref-format "refs/tags/$tag" || fail "payload is not a valid tag name: $tag"
  if TAG_NAME="$tag" MARKER="$probe" bash -c '[[ "$TAG_NAME" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]'; then
    fail "malicious tag passed the release-version allowlist: $tag"
  fi
done

[[ ! -e "$probe" ]] || fail "malicious tag payload executed a command"

echo "release workflow security test passed"
