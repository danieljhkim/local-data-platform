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
- Per-service logs + status + stop/start helpers
- Integrated wrapper commands for `hdfs`, `hive`, `yarn`, `pyspark`, and `spark-submit`
- 2 profile choices:
  1. **local**: local spark and hive (warehouse on local filesystem)
  2. **hdfs**: YARN + NameNode + DataNode + spark + hive (warehouse on HDFS)

**Prerequisites:**

- Java 17
- Homebrew
- Hadoop + Hive + Spark (required)
- Postgres Hive metastore setup: [METASTORE_SETUP.md](docs/METASTORE_SETUP.md)
- Go 1.21+ (only if building from source)

Suggested installs:

```bash
brew install hadoop hive jdk@17 apache-spark postgresql@16
```
> If you plan to build from source: `brew install go`

---

## Installation

### Option 1: Install via Homebrew (Recommended)

```bash
brew install danieljhkim/tap/local-data
```

### Option 2: Build from Source

```bash
git clone https://github.com/danieljhkim/local-data-platform.git
cd local-data-platform
make build

# Install to /usr/local/bin (optional)
make install

```

---

## Quick Start

Before you start, you need to make sure that the postgres metastore is setup and running. See [METASTORE_SETUP.md](docs/METASTORE_SETUP.md) for more details.

```bash
# Initialize profiles (creates local and hdfs profiles in $BASE_DIR/conf/profiles/)
local-data profile init

# optional flags, if you'd like to customize the profiles
local-data profile init --user daniel --base-dir /Users/daniel/local-data-platform --db-url "jdbc:postgresql://localhost:5432/metastore" --db-password "secret"

# Set current profile to hdfs (hdfs + hive + spark)
local-data profile set hdfs

# Or set to local if you just want hive + spark without HDFS
local-data profile set local

# Start all services (HDFS → YARN → Hive) or (hive only) depending on the profile
local-data start

# Run a query
local-data hive -e "SHOW DATABASES"

# Start a PySpark shell
local-data pyspark

# Submit a Spark job (using the active profile's spark-submit)
local-data spark-submit my_job.py

# Check the status of the processes
local-data status

# View logs
local-data logs

# Stop all services (in reverse order: Hive → YARN → HDFS)
local-data stop
```

---

## How It Works

- Profiles are generated programmatically from Go structs (no hand-edited XML required)
- `local-data profile init` generates profile templates under `$BASE_DIR/conf/profiles/`
- `local-data profile set <name>` materializes the runtime overlay under `$BASE_DIR/conf/current/`
- Every command computes and injects the environment for the active profile (hermetic execution)
- Profiles live in `$BASE_DIR/conf/profiles/<name>/{hadoop,hive,spark}`
- `local-data profile set <name>` materializes a runtime overlay at `$BASE_DIR/conf/current/{hadoop,hive,spark}`
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
│   │   ├── profile/         # profile init/list/set/check
│   │   ├── service/         # start/stop/status
│   │   ├── wrappers/        # wrapper commands (hdfs, hive, yarn, etc.)
│   │   ├── logs.go          # combined logs
│   │   └── root.go          # root command wiring
│   ├── config/              # config + profile management
│   │   ├── generator/       # XML/conf generation + overrides merge
│   │   ├── profiles/        # built-in profile definitions
│   │   └── schema/          # typed config structs (Hadoop/Hive/Spark)
│   ├── env/                 # environment detection + computation
│   ├── service/             # service lifecycle (ProcessManager)
│   │   ├── hdfs/
│   │   ├── yarn/
│   │   └── hive/
│   └── util/                # shared helpers (fs/xml/shell/log)
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