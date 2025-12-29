# Local Data Engineering Environment (macOS)

This guide sets up a **local, pseudo-distributed** environment on macOS for
learning and development:

- **Hadoop (HDFS + YARN)** running on your machine (single host)
- **Hive** using a local metastore (Postgres) and storing data in HDFS (hdfs profile) or local file system (local profile)
- **Spark** submitting jobs to YARN and reading/writing HDFS + Hive tables

### Profile Types:
1. **local:** hive + spark on local file system warehouse
2. **hdfs:** hive + spark + name-node + data-node + yarn on hdfs warehouse

---

## Prereqs

- Java 17 (required)
- Homebrew
- Hadoop + Hive (required)
- Spark
- Postgres (only required if your Hive profile uses Postgres metastore)

Suggested Homebrew installs:

```bash
brew install hadoop hive jdk@17 apache-spark
```

---

## Install / PATH

Make entrypoints executable:

```bash
make perms
```

Add this repo’s `bin/` to your PATH:

```bash
make path
```

Paste the printed export line into your shell profile (for zsh: `~/.zshrc`).

---

## Base directory

By default, the CLI writes state under:

```text
$HOME/local-data-platform
```

Override with:

```bash
export BASE_DIR="$HOME/some/other/path"
```

State layout:

```text
$BASE_DIR/
  conf/
    active_profile
    current/
      hadoop/
      hive/
      spark/
    profiles/          # only after `local-data profile init`
  state/
    hdfs/{logs,pids}/
    yarn/{logs,pids}/
    hive/{logs,pids,warehouse}/
```

---

## Profiles + runtime config overlay

Profiles are templates in `conf/profiles/<name>/{hadoop,hive,spark}`.

To copy repo profiles into `$BASE_DIR` for local edits:

```bash
local-data profile init
```

List profiles:

```bash
local-data profile list
```

Activate a profile (also applies the runtime overlay):

```bash
local-data profile set local
```

Check the overlay:

```bash
local-data profile check
```

Overlay output:

```text
$BASE_DIR/conf/current/{hadoop,hive,spark}
```

This avoids mutating Homebrew config directories.

---

## Start/stop/status/logs

Start everything (HDFS → YARN → Hive):

```bash
local-data start
```

Stop everything (Hive → YARN → HDFS):

```bash
local-data stop
```

Individual services:

```bash
local-data start hdfs
local-data start yarn
local-data start hive

local-data stop hive
```

Status:

```bash
local-data status
local-data status hive
```

Logs (Ctrl-C to stop):

```bash
local-data logs
local-data hive logs
```

If Hive ports are stuck (9083 metastore / 10000 hiveserver2):

```bash
local-data hive stop --force
```

---

## Hermetic environment helpers

Print exports (useful for debugging):

```bash
local-data env print
```

---

## CLI wrapper commands

These wrappers live under `bin/` and all run via `local-data env exec`, so they
automatically use the active profile’s runtime overlay in `$BASE_DIR/conf/current`.

### Beeline wrapper

```bash
hive-b
hive-b -e "SELECT 1"
```

### HDFS wrapper

```bash
# HDFS subcommands
hdfs-b version
hdfs-b dfs -ls /
hdfs-b dfs -mkdir -p /spark-history
hdfs-b dfs -put ./local_file.parquet /data/
```

### YARN wrapper

```bash
# Only relevant if you start YARN (local-data start yarn)
yarn-b node -list
yarn-b application -list
yarn-b logs -applicationId <application_...>
```

### PySpark wrapper

```bash
# Interactive PySpark (uses spark-defaults.conf from the active profile)
pyspark-b

# Override config at launch time
pyspark-b --conf spark.sql.shuffle.partitions=4
```

### spark-submit wrapper

```bash
# Run a PySpark job with the profile’s env + conf
spark-submit-b ./jobs/etl_job.py --input hdfs:///data/raw --output hdfs:///data/curated

# Include additional Python deps
spark-submit-b --py-files ./deps.zip ./jobs/etl_job.py
```


---


## Hive metastore notes

Step-by-step Postgres setup: see [docs/METASTORE_SETUP.md](docs/METASTORE_SETUP.md).

The default `local` profile’s `hive-site.xml` points at a Postgres metastore:

```text
jdbc:postgresql://localhost:5432/metastore
```

If you keep that configuration, make sure Postgres is running and the
DB/user/password match your profile.

If you use Spark with Postgres-backed Hive metastore, Spark may also need
the Postgres JDBC jar available on its classpath.