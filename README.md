
# local-data-platform (LDP)

- *only macOS is supported at this time*

Personally, one of the most dreadful aspects of working on data pipelines is the "waiting" while spinning up a cluster on clound, and the disconnect between the cloud and my local machine. 

If you are in this unfortunate situation where you must run your (hdfs + hive + spark) pipelines in cloud everytime for validation, I have a solution for you that will save you both time and sanity.

Local-data-platform is a local, single-machine Hadoop (HDFS + YARN) + Hive + spark environment manager with a small Bash CLI. 

What you get:

- A modular `local-data` CLI to manage HDFS/YARN/Hive/Spark in one place.
- A runtime config overlay under `$BASE_DIR/conf/current` (hive-site.xml,
  core-site.xml, logs, data etc all lives here) to easily change between profiles and keep things organized.
- Per-service logs + status + stop/start helpers
- 2 profile choices:
  1. **local**: local spark and hive on local filesytem warehouse (in `$BASE_DIR/state/hive/warehouse`)
  2. **hdfs**: YARN + NameNode + DataNode + spark + hive on hdfs warehouse

> Note: *default value of `$BASE_DIR`=/Users/{whoami}/local-data-platform*

**Prequisite Setup:**

- Postgres Hive metastore setup: [METASTORE_SETUP.md](docs/METASTORE_SETUP.md)
- Java 17
- Homebrew
- Hadoop + Hive (required)
- Spark (recommended if you need spark to access tables via hive metastore)

Suggested installs:

```bash
brew install hadoop hive jdk@17 apache-spark postgresql@16
```

---

## Quick start

```bash
# makes scrips executable
make perms 
# add this output to PATH
make path

# instantiates local and hdfs profiles in $BASE_DIR/conf/profiles/
local-data profile init
# sets current profile to hdfs (hdfs + hive + spark);
local-data profile set hdfs
# set current profile to local, if you just want hive + spark
local-data profile set local

# starts YARN, nameNode, dataNode, hiveServer2, metastore
local-data start

# check the status of the processes
local-data status
local-data logs

# stop all
local-data stop
```

## Common CLI Usage

Once things are running, you can call hive, pyspark, hdfs, yarn like so:

```bash
# starts interactive beeline cli
hive-b
# run a query directly
hive-b -e "SHOW DATABASES"

# starts interactive pyspark
pyspark-b
# with custom config
pyspark-b --master yarn

# spark-submit job
spark-submit-b my_job.py

# hdfs commands
hdfs-b dfs -ls /
hdfs-b dfs -mkdir -p /user/hive/warehouse
hdfs-b dfs -put local_file.parquet /data/

# yarn commands
yarn-b top
```

## How it works

- Profiles live in `$BASE_DIR/conf/profiles/<name>/{hadoop,hive,spark}`.
- `local-data profile set <name>` materializes a runtime overlay at
  `$BASE_DIR/conf/current/{hadoop,hive,spark}`.
- `local-data env exec -- <cmd...>` runs commands with `HADOOP_CONF_DIR`,
  `HIVE_CONF_DIR`, and `PATH` set to use the overlay.
- `hive-b` invokes beeline cli
- `hdfs-b` invokes hdfs cli
- `yarn-b` invokes yarn cli
- `pyspark-b` invokes pyspark cli
- `spark-submit-b` invokes spark-submit cli

