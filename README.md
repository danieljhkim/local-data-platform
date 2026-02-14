# local-data-platform

- **macOS only** (Homebrew-first)
- Go-based CLI (`local-data`)

Personally, one of the most dreadful aspects of working on data pipelines is the "waiting" while spinning up a cluster on cloud, and the disconnect between the cloud and my local machine.

If you are in this unfortunate situation where you must run your (hdfs + hive + spark) pipelines in cloud everytime for validation, I have a solution for you that will save you both time and sanity.

Setting up a local environment is a pain, and I've had to do it many times. So I've created a tool that will help you set up a local environment quickly and easily.

Local-data-platform is a local, single-machine Hadoop (HDFS + YARN) + Hive + Spark environment manager and a wrapper around the Hadoop, Hive, and Spark commands with a modern Go CLI.

What you get:

- A modular `local-data` CLI to manage HDFS/YARN/Hive/Spark in one place
- **Profile overlays**: `$BASE_DIR/conf/profiles/<name>` → runtime overlay at `$BASE_DIR/conf/current`
- **Typed config generation**: configs are defined as Go structs (`internal/config/schema`) and serialized to XML/conf files
- **Hermetic execution**: wrapper commands auto-inject the active runtime overlay environment
- **Multiple metastore backends**: Derby (default, zero-config), Postgres, or MySQL
- Per-service logs + status + stop/start helpers
- Integrated wrapper commands for `hdfs`, `hive`, `yarn`, `pyspark`, and `spark-submit`
- 2 profile choices:
  1. **local**: local spark and hive (warehouse on local filesystem)
  2. **hdfs**: YARN + NameNode + DataNode + spark + hive (warehouse on HDFS)

**Prerequisites:**

- Homebrew
- Optional: Postgres/MySQL metastore setup (Derby is default): [METASTORE_SETUP.md](docs/METASTORE_SETUP.md)

---

## Installation

### Option 1: Install via Homebrew (Recommended)

Installing via Homebrew will install latest `local-data` CLI binary + required dependencies (Hadoop, Hive, Spark, jdk@17).

```bash
brew install danieljhkim/tap/local-data
```

### Option 2: Build from Source

```bash
git clone https://github.com/danieljhkim/local-data-platform.git
cd local-data-platform
make build

# Install to $HOME/bin or $HOME/.local/bin (optional)
make go-install

# Install dependencies
brew install go hadoop hive jdk@17 apache-spark

```

---

## Quick Start

By default, `local-data` uses Derby metastore, so no external DB setup is required.
If you prefer Postgres/MySQL, see [METASTORE_SETUP.md](docs/METASTORE_SETUP.md).

```bash
# Initialize profiles + metastore schema using defaults
local-data init

# Default values:
#   - user: $USER
#   - base-dir: $HOME/local-data-platform
#   - db-type: derby
#   - db-url: jdbc:derby:;databaseName=$BASE_DIR/state/hive/metastore_db;create=true
#   - db-password: (empty)

# initialize the metastore and profiles
local-data init

# Set active profile
local-data profile set hdfs    # HDFS + YARN + Hive + Spark
local-data profile set local   # Hive + Spark only (no HDFS/YARN)

# Start all services (HDFS → YARN → Hive) or (Hive only) depending on profile
local-data start

# Run a query
local-data hive -e "SHOW DATABASES"

# Start a PySpark shell
local-data pyspark

# Submit a Spark job
local-data spark-submit my_job.py

# Check service status
local-data status

# View logs
local-data logs

# Stop all services (reverse order: Hive → YARN → HDFS)
local-data stop
```

---

## Settings Management

User settings are persisted at `$BASE_DIR/settings/setting.json` and control profile generation.

```bash
# List current settings
local-data setting list

# Update individual settings
local-data setting set db-type postgres
local-data setting set db-url "jdbc:postgresql://localhost:5432/my_metastore"

# Show active profile config content
local-data setting show hive     # prints hive-site.xml
local-data setting show spark    # prints spark-defaults.conf + spark hive-site.xml
local-data setting show hadoop   # prints Hadoop config files
```

Setting precedence (highest to lowest):
1. CLI flags (`--db-url`, `--db-password`, `--user`, `--db-type`)
2. Persisted settings (`$BASE_DIR/settings/setting.json`)
3. Built-in defaults

---

## How It Works

- Profiles are generated programmatically from Go structs (no hand-edited XML required)
- `local-data init` generates profile templates under `$BASE_DIR/conf/profiles/` and bootstraps metastore schema
- `local-data profile set <name>` materializes the runtime overlay under `$BASE_DIR/conf/current/`
- `local-data setting set <key> <value>` updates persisted settings and relevant profile/current Hive XML values
- Every command computes and injects the environment for the active profile (hermetic execution)
- Profiles live in `$BASE_DIR/conf/profiles/<name>/{hadoop,hive,spark}`
- Wrapper commands (hdfs, hive, yarn, etc.) automatically use the overlay configuration
- `local-data env exec -- <cmd...>` runs commands with `HADOOP_CONF_DIR`, `HIVE_CONF_DIR`, and `PATH` set to use the overlay
- Services write logs to `$BASE_DIR/state/<service>/logs`
- PID files are managed in `$BASE_DIR/state/<service>/pids`

---

## Development

### Building

```bash
# Build the binary
make build

# Run unit tests (no system dependencies)
make test

# Integration tests (requires Hadoop/Hive/Spark/Postgres/Java 17)
make test-integration

# Run tests with coverage
make test-coverage

# Format code
make format

# Run linters
make vet
make lint  # Requires golangci-lint

# Clean build artifacts
make clean
```

### Project Structure

```
local-data-platform/
├── cmd/local-data/          # Main entry point
├── internal/
│   ├── cli/                 # Cobra CLI commands
│   │   ├── env/             # env print/exec/doctor
│   │   ├── profile/         # profile list/set/check
│   │   ├── setting/         # setting list/set/show
│   │   ├── service/         # start/stop/status
│   │   ├── wrappers/        # wrapper commands (hdfs, hive, yarn, etc.)
│   │   ├── logs.go          # combined logs
│   │   └── root.go          # root command wiring
│   ├── config/              # config + profile management
│   │   ├── generator/       # XML/conf generation + overrides merge
│   │   ├── profiles/        # built-in profile definitions
│   │   └── schema/          # typed config structs (Hadoop/Hive/Spark)
│   ├── env/                 # environment detection + computation
│   ├── metastore/           # metastore DB type detection + validation
│   ├── service/             # service lifecycle (ProcessManager)
│   │   ├── hdfs/
│   │   ├── yarn/
│   │   └── hive/
│   └── util/                # shared helpers (fs/xml/shell/log/color)
└── Makefile
```

---

## Configuration Profiles

### Local Profile

Uses local filesystem for Hive warehouse:
- No HDFS required
- Warehouse: `$BASE_DIR/state/hive/warehouse`
- Faster startup, simpler setup
- Good for Hive/Spark development

### HDFS Profile

Full Hadoop stack with HDFS:
- HDFS NameNode + DataNode
- YARN ResourceManager + NodeManager
- Hive Metastore + HiveServer2
- Warehouse on HDFS: `/user/hive/warehouse`
- Complete cluster simulation

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.


## Acknowledgments

Built with:
- Brew, Go, Cobra, and other great open-source softwares.

Special thanks to:
  - Claude Code (first time using it, and it's pretty good)

---

## License

See [LICENSE](LICENSE) for details.
