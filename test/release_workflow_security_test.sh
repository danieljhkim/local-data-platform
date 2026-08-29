#!/usr/bin/env bash

set -euo pipefail

workflow="${1:-.github/workflows/release.yml}"

fail() {
  echo "release workflow security test: $*" >&2
  exit 1
}

[[ -f "$workflow" ]] || fail "workflow not found: $workflow"

# --- Tag handling -----------------------------------------------------------

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

# --- Matrix metadata ---------------------------------------------------------

grep -Fq 'strategy:' "$workflow" || fail "build-release job is missing a build matrix"
grep -Fq 'matrix:' "$workflow" || fail "build-release job is missing a build matrix"
grep -Fq -- '- arch: arm64' "$workflow" || fail "release matrix is missing darwin/arm64"
grep -Fq -- '- arch: amd64' "$workflow" || fail "release matrix is missing darwin/amd64"

# --- Artifact naming + checksums --------------------------------------------

grep -Fq 'GOOS=darwin GOARCH="$GOARCH"' "$workflow" || fail "matrix leg does not build for GOOS=darwin/GOARCH=\$GOARCH"
grep -Fq 'ASSET="local-data_${VERSION}_darwin_${GOARCH}.tar.gz"' "$workflow" || fail "release asset name is not architecture-qualified"
grep -Fq 'ACTUAL_VERSION="$(./local-data version)"' "$workflow" || fail "workflow does not verify the built binary's version output"
grep -Fq 'if [[ "$ACTUAL_VERSION" != "$VERSION" ]]; then' "$workflow" || fail "workflow does not fail when local-data version output mismatches the tag"
grep -Fq 'echo "$SHA  $ASSET" > "${ASSET}.sha256"' "$workflow" || fail "workflow does not publish a per-artifact checksum file"
grep -Fq 'if-no-files-found: error' "$workflow" || fail "artifact upload does not fail on missing build output"
grep -Fq 'SHA256SUMS.txt' "$workflow" || fail "workflow does not publish a combined checksum manifest"
grep -Fq 'sha256sum -c SHA256SUMS.txt' "$workflow" || fail "workflow does not verify the checksum manifest before publishing"

for asset in \
  'ARM_ASSET="local-data_${VERSION}_darwin_arm64.tar.gz"' \
  'AMD_ASSET="local-data_${VERSION}_darwin_amd64.tar.gz"'; do
  grep -Fq "$asset" "$workflow" || fail "publish job is missing expected asset name: $asset"
done

grep -Fq '[[ -f "$asset" ]] || { echo "missing release artifact: $asset" >&2; exit 1; }' "$workflow" \
  || fail "publish job does not fail before release when an artifact is missing"
grep -Fq '[[ -f "$asset.sha256" ]] || { echo "missing checksum file: $asset.sha256" >&2; exit 1; }' "$workflow" \
  || fail "publish job does not fail before release when a checksum file is missing"

# --- Formula rewriting for both architectures (reproduced, not published) ---

formula_script="$(awk '
  /<<'"'"'RUBY'"'"'/ { flag = 1; next }
  flag && /^[[:space:]]*RUBY[[:space:]]*$/ { flag = 0; next }
  flag { print }
' "$workflow")"

[[ -n "$formula_script" ]] || fail "could not extract the Homebrew formula rewrite script from the workflow"

echo "$formula_script" | grep -Fq 'ENV.fetch("SHA_ARM64")' || fail "formula rewrite does not consume a per-arch arm64 checksum"
echo "$formula_script" | grep -Fq 'ENV.fetch("SHA_AMD64")' || fail "formula rewrite does not consume a per-arch amd64 checksum"
echo "$formula_script" | grep -Fq 'rewrite_stanza!(contents, formula, "arm"' || fail "formula rewrite does not update the on_arm stanza"
echo "$formula_script" | grep -Fq 'rewrite_stanza!(contents, formula, "intel"' || fail "formula rewrite does not update the on_intel stanza"

work_dir="$(mktemp -d)"
trap 'rm -rf "$probe_dir" "$work_dir"' EXIT

rewrite_rb="$work_dir/rewrite.rb"
printf '%s\n' "$formula_script" > "$rewrite_rb"

good_formula="$work_dir/good.rb"
cat > "$good_formula" <<'FORMULA'
class LocalData < Formula
  desc "Local Hadoop/Hive/Spark dev environment"
  homepage "https://github.com/danieljhkim/local-data-platform"
  version "0.0.0"

  on_arm do
    url "https://github.com/danieljhkim/local-data-platform/releases/download/v0.0.0/local-data_0.0.0_darwin_arm64.tar.gz"
    sha256 "0000000000000000000000000000000000000000000000000000000000000"
  end

  on_intel do
    url "https://github.com/danieljhkim/local-data-platform/releases/download/v0.0.0/local-data_0.0.0_darwin_amd64.tar.gz"
    sha256 "1111111111111111111111111111111111111111111111111111111111111"
  end
end
FORMULA

run_rewrite() {
  local formula="$1"
  VERSION="9.9.9" SHA_ARM64="aaaa" SHA_AMD64="bbbb" ruby "$rewrite_rb" "$formula"
}

if ! run_rewrite "$good_formula" >/dev/null 2>"$work_dir/good.err"; then
  cat "$work_dir/good.err" >&2
  fail "formula rewrite failed against a well-formed two-architecture formula"
fi

grep -Fq 'local-data_9.9.9_darwin_arm64.tar.gz' "$good_formula" || fail "formula rewrite did not set the arm64 url"
grep -Fq 'local-data_9.9.9_darwin_amd64.tar.gz' "$good_formula" || fail "formula rewrite did not set the amd64 url"
grep -Fq 'sha256 "aaaa"' "$good_formula" || fail "formula rewrite did not set the arm64 sha256"
grep -Fq 'sha256 "bbbb"' "$good_formula" || fail "formula rewrite did not set the amd64 sha256"
grep -Fq 'version "9.9.9"' "$good_formula" || fail "formula rewrite did not set the version"

missing_intel="$work_dir/missing_intel.rb"
cat > "$missing_intel" <<'FORMULA'
class LocalData < Formula
  version "0.0.0"

  on_arm do
    url "https://example.invalid/local-data_0.0.0_darwin_arm64.tar.gz"
    sha256 "0000000000000000000000000000000000000000000000000000000000000"
  end
end
FORMULA

if run_rewrite "$missing_intel" >/dev/null 2>"$work_dir/missing_intel.err"; then
  fail "formula rewrite must fail when the on_intel stanza is missing"
fi
grep -Fq 'on_intel' "$work_dir/missing_intel.err" || fail "formula rewrite failure did not name the missing on_intel stanza"
if grep -Fq '9.9.9' "$missing_intel"; then
  fail "formula rewrite must not commit a partial edit when a stanza is missing"
fi

missing_sha="$work_dir/missing_sha.rb"
cat > "$missing_sha" <<'FORMULA'
class LocalData < Formula
  version "0.0.0"

  on_arm do
    url "https://example.invalid/local-data_0.0.0_darwin_arm64.tar.gz"
  end

  on_intel do
    url "https://example.invalid/local-data_0.0.0_darwin_amd64.tar.gz"
    sha256 "1111111111111111111111111111111111111111111111111111111111111"
  end
end
FORMULA

if run_rewrite "$missing_sha" >/dev/null 2>"$work_dir/missing_sha.err"; then
  fail "formula rewrite must fail when a sha256 entry is missing from a stanza"
fi
grep -Fq 'sha256 entry' "$work_dir/missing_sha.err" || fail "formula rewrite failure did not name the missing sha256 entry"

echo "release workflow security test passed"
