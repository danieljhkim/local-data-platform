# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] - 2026-08-30

### Added
- Native macOS release archives for both Apple Silicon and Intel Macs, with published checksums.
- An integration-test target for validating a local Hadoop, Hive, Spark, and Postgres environment.

### Changed
- Service startup now requires dependencies to become ready and rolls back services started by a failed attempt.
- HDFS readiness and initialization commands now use the active local-data profile overlay.
- Profile and settings updates are published atomically so commands do not observe partially applied configuration.
- Service lifecycle reporting now verifies process startup and shutdown before declaring success.

### Fixed
- NameNode formatting now validates every configured storage directory and refuses non-empty or inconsistent storage state.

## [0.3.1] - 2026-02-14

### Changed
- security improvements, SQL injection protection
- improved error handling
- bug fixes
- dead code removal

## [0.3.0] - 2026-02-14

### Added
- Support for Derby,Postgres and MySQL metastore
- New `local-data init` command to initialize the metastore and profiles
- new `local-data setting` command to manage the settings

### Changed
- `local-data profile init` command is removed
- improved output messages
- Color-coded CLI output (cyan info, green success, yellow warnings, red errors) with auto-detection for TTY and `NO_COLOR` support

## [0.2.0] - 2025-12-31

### Added
- Go-based `local-data` CLI replacing the legacy Bash implementation
- Profile-based configuration with typed, programmatic config generation
- Homebrew distribution support (`brew install danieljhkim/tap/local-data`)

### Notes
- macOS only
- Single-node (pseudo-distributed) local environment

## [0.1.0] - 2025-12-20

### First public release.

### Added
- Bash-based `local-data` CLI
- Profile-based configuration with hand-edited XML files
- Wrapper commands for HDFS, Hive, YARN, and Spark
- Service lifecycle management with PID tracking and logs

