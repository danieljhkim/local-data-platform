# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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


