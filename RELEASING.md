# Releasing local-data-platform

This runbook describes a human-approved release of `local-data-platform` from the `main` branch. It is grounded in the repository's `Makefile` and `.github/workflows/release.yml`.

## Versioning policy

Release tags are the version source of truth. They must be annotated, strict SemVer tags in the form `v<major>.<minor>.<patch>`; the release workflow rejects every other tag. There is no committed Cargo, npm, or other version-manifest file to bump. The tag version is injected into the Go binary by the release workflow and verified with `local-data version` before publishing.

The project is pre-1.0 (`0.<minor>.<patch>`):

- Non-breaking work is a patch bump, for example `0.3.3` to `0.3.4`.
- A confirmed breaking change is a minor bump, for example `0.3.3` to `0.4.0`.

Breaking classification is a human decision. Candidate examples include removing or renaming a CLI command or flag, changing a documented configuration format incompatibly, or changing service behavior that existing local environments depend on. New optional behavior, bug fixes, and internal refactors are not automatically breaking. Record the evidence and ask the release approver to confirm the classification before choosing the version.

## Release checklist

### 1. Survey the release range

Work only from `main`. Find the latest reachable strict release tag and survey the commits after it without merge commits:

```sh
git tag --merged HEAD --list 'v*' --sort=-version:refname | head -n 1
git log v<previous>..HEAD --no-merges --pretty='%h%x09%s'
git log v<previous>..HEAD --no-merges --pretty='%s' | grep -oE 'DANI-[0-9]+' | sort -u
```

Treat the tag as a valid baseline only when it matches the strict tag form. Read the referenced tasks and commit changes as needed. Do not release while unrelated backlog, in-progress, or review work remains unless the human has explicitly settled the release boundary.

### 2. Draft `CHANGELOG.md`

Add a new `## [X.Y.Z] - YYYY-MM-DD` section above the current latest release. Use the existing Keep a Changelog-style headings (`Added`, `Changed`, `Fixed`, and `Removed`) and summarize user-visible changes rather than reproducing the commit log. Include confirmed breaking changes prominently. Do not invent an `Unreleased` convention or modify old released sections.

### 3. Confirm the candidate with a human

Present the no-merge survey, referenced task IDs, proposed version, changelog draft, and every breaking-change candidate with its evidence. The human approval is required before editing `CHANGELOG.md`, committing, tagging, pushing, or publishing. The recurring `release-prep` probe only prepares this handoff; it never crosses this boundary.

### 4. Verify the release candidate

Run the repository checks before committing the approved release work:

```sh
make test
bash test/release_workflow_security_test.sh
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/release.yml
make build
```

`make build` derives a local development version from Git. The tag-triggered workflow is the release authority: it builds `darwin/arm64` and `darwin/amd64`, injects the tag version into the Go binary, and fails unless `local-data version` reports that exact version.

### 5. Commit, tag, and push

After approval and successful verification, commit the changelog change on `main`, create an annotated tag, then push the branch before the tag:

```sh
git commit -m "chore: prepare v<X.Y.Z> release [DANI-<task-id>]"
git tag -a v<X.Y.Z> -m "v<X.Y.Z>"
git push origin main
git push origin v<X.Y.Z>
```

Do not force-push a release commit or move an existing release tag. Pushing the branch first lets the tag-triggered workflow resolve the release commit already present on the remote.

### 6. Observe publication and Homebrew update

The release workflow performs the following after accepting the tag:

1. Builds `local-data_<version>_darwin_arm64.tar.gz` and `local-data_<version>_darwin_amd64.tar.gz`.
2. Creates a `.sha256` file for each archive, combines them in `SHA256SUMS.txt`, and verifies that manifest before publication.
3. Creates the GitHub Release and uploads both archives, both per-archive checksum files, and `SHA256SUMS.txt`.
4. Checks out `danieljhkim/homebrew-tap` and rewrites both architecture branches' URL/checksum entries plus the formula version, then commits and pushes that tap update. The updater supports Homebrew's `on_arm`/`on_intel` stanzas and its `Hardware::CPU.arm?` conditional form.

Check the GitHub Actions release run, the GitHub Release assets, and the Homebrew tap commit. The workflow fails rather than partially rewriting a formula when either architecture branch or checksum entry is missing.

## Failure and hotfix recovery

If a pre-tag check fails, fix the approved release task and rerun the checks; do not create a tag. If the tag workflow fails, preserve the failed run URL, logs, tag, commit, and produced assets in the release task, then determine the cause before taking any recovery action. Never silently retag or force-move a published tag. A corrective release needs a new human-approved patch version and a new annotated tag.

For an urgent production fix, branch from current `main`, make the smallest approved patch, run the same checks, merge it into `main`, and follow the same commit/tag/push order with the next patch tag. There is no Orbit-style branch-promotion or plugin/npm publishing phase in this repository.

## Canonical release task

The preparation probe creates or refreshes one open task named `Prepare v<X.Y.Z> release`, tagged `release`, only after its read-only gates pass. That task points its executor to this runbook and has the one ordinary release modification target:

```yaml
context_files:
  - file:CHANGELOG.md
```

The tag supplies the binary version, so no committed Cargo/npm manifest belongs in the task. The task must stop for human approval before making release edits or release-state changes.
