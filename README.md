
# local-data-platform

Local, single-machine Hadoop (HDFS + YARN) + Hive + spark environment manager
with a small Bash CLI for macOS. 

What you get:

- A modular `local-data` CLI to manage HDFS/YARN/Hive in one place.
- A runtime config overlay under `$BASE_DIR/conf/current` (hive-site.xml,
  core-site.xml, logs, etc all lives here) to easily change between profiles.
- Per-service logs + status + stop/start helpers

**Other Docs:**

- Detailed setup: [QUICK_START.md](QUICK_START.md)
- Postgres Hive metastore setup: [METASTORE_SETUP.md](docs/METASTORE_SETUP.md)

---

## Quick start

```bash
make perms
make path
local-data profile set local
local-data start
local-data status
local-data logs
```

Stop everything:

```bash
local-data stop
```

If Hive ports are stuck (9083 metastore / 10000 hiveserver2):

```bash
local-data hive stop --force
```

## Prereqs

- Java 17
- Homebrew
- Hadoop + Hive (required)
- Spark (recommended)
- Postgres (optional; only needed if your Hive profile uses it)

Suggested installs:

```bash
brew install hadoop hive jdk@17
brew install apache-spark   # optional
brew install postgresql     # optional
```

Sanity checks:

```bash
local-data env doctor
local-data env doctor start hive
```

## How it works

- Profiles live in `conf/profiles/<name>/{hadoop,hive,spark}`.
- `local-data profile set <name>` (or `local-data conf apply`) materializes a
  runtime overlay at `$BASE_DIR/conf/current/{hadoop,hive,spark}`.
- `local-data env exec -- <cmd...>` runs commands with `HADOOP_CONF_DIR`,
  `HIVE_CONF_DIR`, and `PATH` set to use the overlay.

## Common commands

```bash
local-data start [hdfs|yarn|hive]
local-data stop  [hdfs|yarn|hive]
local-data status [hdfs|yarn|hive]
local-data logs
local-data hive logs
local-data env exec -- hdfs dfs -ls /
```