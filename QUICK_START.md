# Local Data Engineering Environment (macOS) — Quick Start

This guide walks you through setting up a **local, pseudo-distributed** data engineering environment on macOS using the `local-data` CLI.

### Profile Types:
1. **local:** hive + spark on local file system warehouse
2. **hdfs:** hive + spark + name-node + data-node + yarn on hdfs warehouse

### Metastore Backends:
- **Derby** (default) — zero-config, embedded database
- **Postgres** — recommended for production-like usage
- **MySQL** — alternative external metastore

---

## Prereqs

- Homebrew
- Optional: Postgres/MySQL metastore (Derby is default)

***Note**: By default, `local-data` uses Derby metastore (no external DB setup required). For Postgres/MySQL, see [METASTORE_SETUP.md](docs/METASTORE_SETUP.md).*

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

# Optional
make install

# Install dependencies
brew install go hadoop hive jdk@17 apache-spark
```

---

## Initialize

```bash
# Initialize profiles + metastore schema using defaults
local-data init

# Default values:
#   - user: $USER
#   - base-dir: $HOME/local-data-platform
#   - db-type: derby
#   - db-url: jdbc:derby:;databaseName=$BASE_DIR/state/hive/metastore_db;create=true
#   - db-password: (empty)

# Optional: override settings during init
local-data init --user daniel --db-type postgres \
  --db-url "jdbc:postgresql://localhost:5432/metastore" \
  --db-password "secret"
```

---

## Settings Management

```bash
# List current settings
local-data setting list

# Update individual settings
local-data setting set db-type postgres
local-data setting set db-url "jdbc:postgresql://localhost:5432/my_metastore"
local-data setting set db-password "secret"
local-data setting set user daniel

# Show active profile config content
local-data setting show hive     # prints hive-site.xml
local-data setting show spark    # prints spark-defaults.conf + spark hive-site.xml
local-data setting show hadoop   # prints Hadoop config files
```

---

### Profile Management

```bash
# Set active profile
local-data profile set hdfs    # HDFS + YARN + Hive + Spark
local-data profile set local   # Hive + Spark only (no HDFS/YARN)

# List available profiles
local-data profile list

# Check overlay status
local-data profile check
```

---

## Service Management

```bash
# Start all services (HDFS → YARN → Hive) or (Hive only) depending on profile
local-data start

# Start individual services
local-data start hdfs
local-data start yarn
local-data start hive

# Check service status (table format with process + listener info)
local-data status

# View combined logs
local-data logs

# Stop all services (reverse order: Hive → YARN → HDFS)
local-data stop
```

---

## Hermetic environment helpers

Print exports (useful for debugging):

```bash
local-data env print

# Check dependencies
local-data env doctor
```

---

## CLI wrapper commands

These wrapper commands are built into the `local-data` CLI and automatically compute and inject the active profile's runtime environment before execution.

### Beeline wrapper

Note that that hive server2 takes a while to start, so you might need to wait a couple of minutes before the first command (takes around 3-5 minutes for me). If you get a connection refused error, wait a bit and try again.

```bash
local-data hive
local-data hive -e "SELECT 1"
```

### HDFS wrapper

```bash
# HDFS subcommands
local-data hdfs version
local-data hdfs dfs -ls /
local-data hdfs dfs -mkdir -p /spark-history
local-data hdfs dfs -put ./local_file.parquet /data/
```

### YARN wrapper

```bash
# Only relevant if you start YARN (local-data start yarn)
local-data yarn node -list
local-data yarn application -list
local-data yarn logs -applicationId <application_...>
```

### PySpark wrapper

```bash
# Interactive PySpark (uses spark-defaults.conf from the active profile)
local-data pyspark

# Override config at launch time
local-data pyspark --conf spark.sql.shuffle.partitions=4
```

### spark-submit wrapper

```bash
# Run a PySpark job with the profile's env + conf
local-data spark-submit ./jobs/etl_job.py --input hdfs:///data/raw --output hdfs:///data/curated

# Include additional Python deps
local-data spark-submit --py-files ./deps.zip ./jobs/etl_job.py
```
