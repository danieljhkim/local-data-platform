# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Local-data-platform (LDP) is a **Go-based** CLI tool for managing a local Hadoop (HDFS + YARN) + Hive + Spark development environment on macOS. It replaced a previous Bash implementation with a modern, testable Go codebase while maintaining the same profile-based configuration architecture.

**Core Architecture:**
- Go binary built with Cobra CLI framework (`cmd/local-data/main.go`)
- Profile-based configuration system with runtime overlays (`$BASE_DIR/conf/profiles/<name>/` → `$BASE_DIR/conf/current/`)
- **Programmatic config generation**: Configs defined as Go structs (`internal/config/schema/`) that serialize to XML/conf files
- Service lifecycle management via `ProcessManager` (PID files, log files, process discovery)
- Integrated wrapper commands that auto-inject runtime environment

**Key Design Patterns:**
- **Hermetic execution**: All commands automatically apply runtime overlay via environment computation
- **Fail-safe formatting**: HDFS NameNode auto-formats on first run with verification
- **Auto schema initialization**: Hive metastore schema auto-detected and initialized on first run (Postgres)
- **Process discovery**: Multiple fallback mechanisms (PID files → `jps` → `pgrep`) for robust service management
- **Silent overlay application**: Profile overlays applied automatically without user-facing output for seamless experience

## Commands

### Build & Test
```bash
make build                # Build Go binary → bin/local-data
make test                 # Run unit tests (no system dependencies)
make test-coverage        # Run tests with coverage report (coverage.html)
make test-integration     # Run integration tests (requires Hadoop/Hive/Postgres/Java 17)
make install              # Build and install to /usr/local/bin
make clean               # Remove build artifacts and coverage files
```

### Development
```bash
make format              # Format Go code with gofmt
make vet                 # Run go vet
make lint                # Run golangci-lint (requires: brew install golangci-lint)

# Run specific test
go test -v ./internal/service/hdfs -run TestEnsureNameNodeFormatted

# Run single test file
go test -v ./internal/config/overlay_test.go

# Build with version info
make build  # Uses git describe for version
```

### Service Management (Runtime)
```bash
local-data start              # Start all services: HDFS → YARN → Hive
local-data start hdfs         # Start HDFS only (NameNode + DataNode)
local-data stop               # Stop all services (reverse order)
local-data status             # Check service status

# Profile management
local-data profile init                    # Generate profiles with defaults
local-data profile init --force            # Regenerate, overwriting existing
local-data profile init --user daniel      # Custom username in configs
local-data profile init --db-url "jdbc:postgresql://host:5432/db" --db-password "secret"

local-data profile set hdfs   # Activate hdfs profile
local-data profile set local  # Activate local profile (Hive/Spark, no HDFS)

local-data hdfs dfs -ls /     # HDFS commands with auto-injected env
local-data hive -e "SHOW DATABASES"  # Hive/beeline with auto-injected env
```

## Architecture Details

### Directory Structure
```
internal/
├── cli/                   # Cobra command implementations
│   ├── service/           # start/stop/status commands
│   ├── profile/           # profile init/list/set/check commands
│   ├── env/               # env print/exec/doctor commands
│   ├── wrappers/          # Wrapper commands (hdfs, hive, yarn, etc.)
│   ├── logs.go            # Combined logs command
│   └── root.go            # Root Cobra command
├── config/                # Configuration & profile management
│   ├── overlay.go         # ProfileManager: init/list/set/apply profiles
│   ├── paths.go           # Path computation (BaseDir, profiles, overlays)
│   ├── generator/         # Config file generation
│   │   ├── generator.go   # ConfigGenerator: orchestrates profile generation
│   │   ├── writer.go      # XML/conf file serialization
│   │   └── merge.go       # User override merging from YAML
│   ├── schema/            # Typed config struct definitions
│   │   ├── common.go      # Property, TemplateContext, ConfigSet
│   │   ├── hadoop.go      # CoreSite, HDFSSite, YarnSite, MapredSite
│   │   ├── hive.go        # HiveConfig with metastore settings
│   │   └── spark.go       # SparkConfig
│   └── profiles/          # Built-in profile definitions
│       ├── registry.go    # Profile registry & lookup
│       ├── hdfs.go        # HDFS profile (Hadoop+Hive+Spark)
│       └── local.go       # Local profile (Hive+Spark only)
├── env/                   # Environment computation
│   ├── compute.go         # Compute(): Full environment for active profile
│   ├── detect.go          # Detect Hadoop/Hive/Spark/Java installations
│   └── doctor.go          # Dependency checking (env doctor)
├── service/               # Service lifecycle management
│   ├── process.go         # ProcessManager: Start/Stop/Status with PID files
│   ├── hdfs/              # HDFS NameNode/DataNode management
│   │   ├── hdfs.go        # HDFSService.Start/Stop/Status
│   │   ├── format.go      # Auto-formatting logic with verification
│   │   └── util.go        # Process discovery (jps/pgrep), safe mode checks
│   ├── yarn/              # YARN ResourceManager/NodeManager management
│   └── hive/              # Hive Metastore/HiveServer2 management
│       ├── hive.go        # HiveService.Start/Stop/Status
│       ├── schema.go      # Auto-detection and initialization of metastore schema
│       └── postgres.go    # Postgres JDBC driver auto-discovery
└── util/                  # Shared utilities
    ├── fs.go              # File operations (CopyDir, MkdirAll, etc.)
    ├── xml.go             # XML parsing (ParseNameNodeDirs from hdfs-site.xml)
    ├── shell.go           # Command execution helpers
    └── log.go             # Logging (Log/Warn/Error functions)
```

### Profile System Flow
1. **Initialization**: `profile init` generates profiles from Go struct definitions to `$BASE_DIR/conf/profiles/`
   - Supports `--user`, `--db-url`, `--db-password` flags for customization
   - Loads optional user overrides from `$BASE_DIR/conf/overrides.yaml`
2. **Activation**: `profile set <name>` triggers overlay application:
   - Generate config files from profile structs to `$BASE_DIR/conf/current/`
   - Substitute template variables: `{{USER}}`, `{{HOME}}`, `{{BASE_DIR}}`
   - Copy `hive-site.xml` to `spark/` for PySpark/Spark metastore access
   - Write active profile name to `$BASE_DIR/conf/active_profile`
3. **Execution**: All commands auto-apply overlay before running (hermetic execution)

### Config Generation Architecture
Profiles are defined as Go structs in `internal/config/profiles/` and serialized to XML/conf:
```go
// Profile definition (profiles/hdfs.go)
profile := &Profile{
    Name: "hdfs",
    ConfigSet: &schema.ConfigSet{
        Hadoop: &schema.HadoopConfig{...},
        Hive:   &schema.HiveConfig{...},
        Spark:  &schema.SparkConfig{...},
    },
}

// Generation flow (generator/generator.go)
gen := generator.NewConfigGenerator()
gen.Generate("hdfs", baseDir, destDir)  // Writes XML files
```

**Override precedence** (highest to lowest):
1. CLI flags (`--db-url`, `--db-password`, `--user`)
2. YAML overrides (`$BASE_DIR/conf/overrides.yaml`)
3. Built-in profile defaults

### Environment Computation (`env.Compute()`)
Called before every command execution to build hermetic environment:
1. Detect installations: `brew --prefix hadoop/hive/spark`, `/usr/libexec/java_home`
2. Set `*_HOME` variables: `HADOOP_HOME`, `HIVE_HOME`, `SPARK_HOME`, `JAVA_HOME`
   - **Homebrew note**: `HADOOP_HOME` and `HIVE_HOME` use `/libexec` suffix (e.g., `/opt/homebrew/opt/hadoop/libexec`)
3. Override `*_CONF_DIR`: Point to `$BASE_DIR/conf/current/{hadoop,hive,spark}`
4. Build PATH: Prepend Hadoop/Hive/Spark/Java bin directories
5. Return `Environment` struct with all computed variables

### Service Lifecycle (ProcessManager Pattern)
All services (HDFS, YARN, Hive) follow this pattern:
```go
// 1. Create service with paths
svc, err := hdfs.NewHDFSService(paths)

// 2. Start process via ProcessManager
cmd := exec.Command("hdfs", "namenode")
cmd.Env = environment.MergeWithCurrent()  // Inject computed environment
pid, err := procMgr.Start("namenode", cmd, "namenode.log")

// 3. PID file written to $BASE_DIR/state/<service>/pids/namenode.pid
// 4. Logs written to $BASE_DIR/state/<service>/logs/namenode.log

// 5. Stop uses PID file + SIGTERM
procMgr.Stop("namenode")
```

### HDFS NameNode Auto-Formatting
Critical logic in `internal/service/hdfs/format.go`:
- **Check VERSION file first** (not just running process) to avoid race conditions
- **Set HADOOP_CONF_DIR** when running format command so Hadoop knows where to write
- **Verify formatting succeeded** by checking VERSION file exists after format
- **Never format if directory is non-empty** (safety check)
- **Capture and show errors** instead of silencing them

Common pitfall avoided: Checking PID before VERSION file caused race condition where failing NameNode would block formatting.

### Hive Metastore Schema Auto-Initialization
Critical logic in `internal/service/hive/schema.go`:
- **Detect Postgres metastore**: Check `hive-site.xml` for `org.postgresql.Driver` or `jdbc:postgresql:`
- **Check schema status**: Run `schematool -dbType postgres -info` to detect if initialized
- **Auto-initialize if needed**: Run `schematool -dbType postgres -initSchema` on first use
- **Graceful degradation**: If schematool fails or is unavailable, log warning and continue

The schema check runs during `local-data start` and `local-data start hive` before starting the Metastore service.

### Wrapper Commands
All wrapper commands (`hdfs`, `hive`, `yarn`, `pyspark`, `spark-submit`, `hadoop`) use the same pattern:
```go
// 1. Compute environment for active profile
env, err := env.Compute(paths)

// 2. Build command with args
cmd := exec.Command("hdfs", args...)

// 3. Inject environment
cmd.Env = env.MergeWithCurrent()

// 4. Forward stdin/stdout/stderr for interactive use
cmd.Stdin = os.Stdin
cmd.Stdout = os.Stdout
cmd.Stderr = os.Stderr

// 5. Run and return exit code
cmd.Run()
```

## Important Constraints

- **macOS only**: Uses macOS-specific commands (`/usr/libexec/java_home`, `ps eww`, Homebrew paths)
- **Java 17 required**: Hadoop/Hive compatibility (detected via `/usr/libexec/java_home`)
- **Homebrew-centric**: Auto-discovery assumes `brew --prefix hadoop/hive/spark`
- **Single-node only**: Pseudo-distributed mode on localhost
- **Postgres metastore**: Default Hive profiles use Postgres (see `docs/METASTORE_SETUP.md`)
- **Postgres JDBC driver**: Auto-discovered in `HIVE_HOME/lib`, `SPARK_HOME/jars`, or `$BASE_DIR/lib/jars`; copied to Spark jars if missing

## Testing Philosophy

- **Unit tests**: No system dependencies, mock/stub external calls (`internal/**/*_test.go`)
- **Integration tests**: Require Hadoop/Hive/Spark/Postgres/Java 17 (`test/integration/`)
- **Test organization**: Mirror production structure (e.g., `format.go` → `format_test.go`)
- **Coverage**: Run `make test-coverage` to generate `coverage.html`

## Common Development Tasks

### Adding a New Service
1. Create service package: `internal/service/<name>/<name>.go`
2. Implement interface with `Start()`, `Stop()`, `Status()` methods
3. Use `ProcessManager` for process lifecycle
4. Add to `internal/cli/service/start.go`, `stop.go`, `status.go`
5. Add profile configs to `conf/profiles/*/` if needed

### Adding a New Profile
1. Create profile definition: `internal/config/profiles/<name>.go`
2. Define `ConfigSet` with Hadoop/Hive/Spark configurations using schema structs
3. Register profile in `internal/config/profiles/registry.go`
4. Test with: `local-data profile init --force && local-data profile set <name>`

### Modifying Profile Configurations
- Edit profile structs in `internal/config/profiles/*.go`
- Use template placeholders in string values: `{{USER}}`, `{{HOME}}`, `{{BASE_DIR}}`
- Run `make build && local-data profile init --force` to regenerate
- Verify: `cat $BASE_DIR/conf/profiles/<name>/hive/hive-site.xml`

### User Override via YAML
Create `$BASE_DIR/conf/overrides.yaml` to customize profiles without code changes:
```yaml
profiles:
  hdfs:
    hadoop:
      yarn-site:
        yarn.nodemanager.resource.memory-mb: 16384
    spark:
      spark.driver.memory: 8g
  local:
    hive:
      javax.jdo.option.ConnectionPassword: my-secret
```

### Debugging Service Startup Issues
1. Check logs: `local-data logs` or `tail -f $BASE_DIR/state/<service>/logs/*.log`
2. Verify overlay: `local-data profile check`
3. Check environment: `local-data env print`
4. Verify dependencies: `local-data env doctor`
5. Check process discovery: `jps -l` (Java processes), `ps aux | grep <service>`

## Critical Code Paths

### Startup Sequence (`local-data start`)
1. `cli/service/start.go:newStartCmd()` → Parse args, route to service
2. `service/hdfs/hdfs.go:Start()` → Start HDFS
   - `format.go:EnsureNameNodeFormatted()` → Auto-format if needed
   - `hdfs.go:startNameNode()` → Start NameNode via ProcessManager
   - `hdfs.go:startDataNode()` → Start DataNode via ProcessManager
   - `hdfs.go:WaitForSafeMode()` → Poll safe mode status
   - `format.go:CreateCommonHDFSDirs()` → Create /tmp, /user/*, /spark-history
3. `service/yarn/yarn.go:Start()` → Start YARN
4. `service/hive/hive.go:Start()` → Start Hive
   - `hive.go:ensurePostgresJDBC()` → Ensure Postgres JDBC driver is available
   - `schema.go:ensureMetastoreSchema()` → Check and init schema if needed
   - `hive.go:startMetastore()` → Start Metastore via ProcessManager
   - `hive.go:startHiveServer2()` → Start HiveServer2 via ProcessManager

### Profile Activation (`local-data profile set hdfs`)
1. `cli/profile/set.go:newSetCmd()` → Parse profile name
2. `config/overlay.go:ProfileManager.Set()` → Main logic
   - Validate profile exists in registry
   - `overlay.go:Apply()` → Generate config via `ConfigGenerator`
   - `generator.go:Generate()` → Write XML/conf files to `conf/current/`
   - Copy `hive-site.xml` to `spark/` directory
   - Write `.profile` marker file
3. All subsequent commands auto-apply this overlay via `env.Compute()`

### Environment Computation (Every Command)
1. `env/compute.go:Compute()` → Entry point
2. `config/overlay.go:Apply()` → Silent overlay application
3. `env/detect.go:DetectEnvironment()` → Find Hadoop/Hive/Spark/Java
4. Build `Environment` struct with all variables
5. `env.MergeWithCurrent()` → Merge with current environment for command execution

## Troubleshooting Common Issues

### NameNode fails with "NameNode is not formatted"
- **Cause**: Race condition between process check and VERSION file check (fixed)
- **Solution**: Code now checks VERSION file first, then process state
- **Manual fix**: `rm -rf $BASE_DIR/state/hdfs/namenode && local-data start hdfs`

### Safe mode warnings on startup
- **Expected behavior**: HDFS needs time to exit safe mode (30 sec timeout)
- **Check status**: `local-data hdfs dfsadmin -safemode get`
- **Force exit**: `local-data hdfs dfsadmin -safemode leave`

### Wrapper commands fail with "command not found"
- **Cause**: Hadoop/Hive/Spark not installed or not in PATH
- **Solution**: `brew install hadoop hive apache-spark`
- **Verify**: `local-data env doctor`

### Profile changes not taking effect
- **Cause**: Stale overlay in `conf/current/`
- **Solution**: `local-data profile set <name>` (re-applies overlay)
- **Verify**: `local-data profile check`

### Hive metastore "schema not initialized" errors
- **Expected behavior**: Schema is auto-initialized on first `local-data start` or `local-data start hive`
- **Manual check**: `schematool -dbType postgres -info`
- **Manual init**: `schematool -dbType postgres -initSchema`
- **Verify Postgres**: Ensure Postgres is running and credentials in `hive-site.xml` are correct

