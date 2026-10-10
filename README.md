# local-data-platform

A single-machine Hadoop (HDFS + YARN) + Hive + Spark environment for macOS, managed by one Go CLI: `local-data`.

Validating an HDFS + Hive + Spark pipeline usually means spinning up a cloud cluster and waiting. `local-data` gives you the same stack on your laptop in a few commands, so you can iterate locally and save the cloud for the real run.

- **macOS only**, Homebrew-first; native binaries for Apple Silicon (`darwin/arm64`) and Intel (`darwin/amd64`)
- **Two profiles**: `local` (Hive + Spark, warehouse on the local filesystem) or `hdfs` (full HDFS + YARN + Hive + Spark)
- **Zero-config metastore**: Derby by default; Postgres or MySQL optional
- **Hermetic wrappers**: `hdfs`, `hadoop`, `yarn`, `hive`, `pyspark`, and `spark-submit` always run against the active profile's config
- **Typed config generation**: Hadoop/Hive/Spark configs are Go structs serialized to XML/conf — no hand-edited XML
- **Service lifecycle**: start/stop/status, per-service logs with `--follow`, and a dependency doctor
- **Scriptable**: `status --json` and `env doctor --json` emit versioned JSON ([docs/JSON_OUTPUT.md](docs/JSON_OUTPUT.md))

---

## Install

### Homebrew (recommended)

Installs the `local-data` binary plus Hadoop, Hive, Spark, and `jdk@17`, picking the right architecture automatically.

```bash
brew install danieljhkim/tap/local-data
```

Each GitHub release also publishes standalone `darwin_arm64` and `darwin_amd64` tarballs with per-artifact and combined (`SHA256SUMS.txt`) checksums.

### From source

```bash
brew install go hadoop hive jdk@17 apache-spark
git clone https://github.com/danieljhkim/local-data-platform.git
cd local-data-platform
make build          # → bin/local-data
make install-user   # optional: copy to ~/bin or ~/.local/bin
```

Check your setup at any time:

```bash
local-data env doctor             # general preflight
local-data env doctor start hdfs  # preflight for a specific command
```

---

## Quick start

```bash
# 1. Generate profiles and bootstrap the metastore (Derby by default)
local-data init

# 2. Pick a profile
local-data profile set local   # Hive + Spark only
local-data profile set hdfs    # HDFS + YARN + Hive + Spark

# 3. Start services (hdfs: HDFS → YARN → Hive; local: Hive only)
local-data start

# 4. Use the stack
local-data hive -e "SHOW DATABASES"
local-data pyspark
local-data spark-submit my_job.py

# 5. Stop services (reverse order)
local-data stop
```

`init` defaults: user `$USER`, base dir `$HOME/local-data-platform`, Derby metastore at `$BASE_DIR/state/hive/metastore_db`. Re-running `init` on an initialized base dir resumes metastore setup without overwriting your profiles. For Postgres or MySQL, see [docs/METASTORE_SETUP.md](docs/METASTORE_SETUP.md).

---

## Profiles

| | `local` | `hdfs` |
|---|---|---|
| Services | Hive Metastore + HiveServer2 | NameNode, DataNode, ResourceManager, NodeManager, Hive Metastore + HiveServer2 |
| Warehouse | `$BASE_DIR/state/hive/warehouse` | `/user/hive/warehouse` on HDFS |
| Use when | Fast Hive/Spark development | You need a full cluster simulation |

```bash
local-data profile list
local-data profile check
local-data profile diff hdfs   # preview what `profile set hdfs` would change (secrets redacted)
```

---

## Everyday commands

### Status and logs

```bash
local-data status                 # all services for the active profile
local-data status --json          # versioned JSON; nonzero exit on collection errors

local-data logs                   # follows the active profile, like `status`
local-data logs hive              # one service, regardless of profile
local-data logs hive --lines 50   # trailing lines per file (default 120, max 100000)
local-data logs hive --lines 0    # list log files only
local-data logs hive --follow     # print the tail, then stream new output
```

`logs` reads files by fixed path, so it works even when services are stopped or Hadoop/Hive/Spark binaries are missing. `--follow` picks up files created later, restarts after truncation or replacement, and labels each chunk with its source file.

### Settings

Settings live in `$BASE_DIR/settings/setting.json` and drive profile generation.

```bash
local-data setting list
local-data setting set db-type postgres
local-data setting set db-url "jdbc:postgresql://localhost:5432/my_metastore"

# Passwords never go in argv: prompt (no echo), stdin, or file
local-data setting set db-password
printenv METASTORE_PASSWORD | local-data setting set db-password --stdin
local-data setting set db-password --from-file "$HOME/.config/local-data/db-password"

local-data setting show hive     # hive-site.xml
local-data setting show spark    # spark-defaults.conf + Spark's hive-site.xml
local-data setting show hadoop   # Hadoop config files
```

Precedence, highest first:

1. CLI flags (`--db-url`, `--user`, `--db-type`) and secret-safe password input (`--db-password-file`, `--from-file`, `--stdin`, `LOCAL_DATA_DB_PASSWORD`, or the prompt)
2. Persisted settings
3. Built-in defaults

### Running other tools against the profile

```bash
local-data env print               # show the computed environment
local-data env exec -- beeline ... # run any command with HADOOP_CONF_DIR, HIVE_CONF_DIR, and PATH set
```

---

## How it works

The CLI derives `BASE_DIR` from `HOME` and uses `$HOME/local-data-platform` by default. A caller-provided `BASE_DIR` environment variable is ignored.

```
$BASE_DIR/                         # runtime path derived from HOME
├── conf/
│   ├── profiles/<name>/{hadoop,hive,spark}   # generated by `init`
│   └── current/                              # active overlay, written by `profile set`
├── settings/setting.json
└── state/<service>/{logs,pids}
```

- `init` generates profile templates from typed Go structs and bootstraps the metastore schema.
- `profile set <name>` materializes the chosen profile into `conf/current/`.
- `setting set` updates persisted settings and the affected Hive XML in both the profile and the active overlay.
- Every command computes the active profile's environment and injects it, so wrappers and `env exec` never pick up stray system config.

---

## Development

Build targets, the hermetic and live test lanes, CI-equivalent checks, and the project layout are in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

---

## Contributing

Issues and pull requests are welcome. Run the [CI-equivalent checks](docs/DEVELOPMENT.md#ci-equivalent-checks) before opening a PR.

Built with Go, Cobra, Homebrew, and the Apache Hadoop, Hive, and Spark projects — and with help from Claude Code.

## License

MIT — see [LICENSE](LICENSE).
