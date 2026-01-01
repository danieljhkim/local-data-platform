# Local Data Engineering Environment (macOS) — Quick Start

This guide walks you through setting up a **local, pseudo-distributed** data engineering environment on macOS using the `local-data` CLI.

### Profile Types:
1. **local:** hive + spark on local file system warehouse
2. **hdfs:** hive + spark + name-node + data-node + yarn on hdfs warehouse

---

## Prereqs

- Homebrew
- Postgres Hive metastore (required)

***Note**: Before you start, you need to make sure that the postgres metastore is setup and running. See [METASTORE_SETUP.md](docs/METASTORE_SETUP.md) for more details.*

---

## Installation

### Option 1: Install via Homebrew (Recommended)

Installing via Homebrew will install latest `local-data` CLI binary + required dependencies (Hadoop, Hive, Spark, jdk@17). Postgres is not included, you need to install it manually.

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
brew install go hadoop hive jdk@17 apache-spark postgresql@16
```

---

### Profile Management

```bash
# Initialize profiles using default values (creates local and hdfs profiles in $BASE_DIR/conf/profiles/)
local-data profile init

# Note that the default values are:
# - user: $USER
# - base-dir: $HOME/local-data-platform
# - db-url: jdbc:postgresql://localhost:5432/metastore
# - db-password: password

# optional flags, if you'd like to customize the profiles
local-data profile init --user daniel --base-dir /Users/daniel/local-data-platform --db-url "jdbc:postgresql://localhost:5432/metastore" --db-password "secret"

# Set active profile
local-data profile set hdfs

# Or set to local if you just want hive + spark without HDFS
local-data profile set local
```

---

## Hermetic environment helpers

Print exports (useful for debugging):

```bash
local-data env print
```

---

## CLI wrapper commands

These wrapper commands are built into the `local-data` CLI and automatically compute and inject the active profile’s runtime environment before execution.

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
# Run a PySpark job with the profile’s env + conf
local-data spark-submit ./jobs/etl_job.py --input hdfs:///data/raw --output hdfs:///data/curated

# Include additional Python deps
local-data spark-submit --py-files ./deps.zip ./jobs/etl_job.py
```
