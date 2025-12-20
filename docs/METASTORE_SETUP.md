
# Postgres Metastore Setup (Hive)

This repo’s default `local` and `hdfs` Hive profile is configured to use a
Postgres-backed Hive metastore.

This doc walks through:

- Installing + starting Postgres (Homebrew)
- Creating the metastore DB/user
- Verifying connectivity
- Matching settings in `hive-site.xml`

---

## 1) Install + start Postgres

Install:

```bash
brew install postgresql@16
```

Start as a background service:

```bash
brew services start postgresql@16
```

Add it to PATH:

```bash
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"
```

Confirm it’s up:

```bash
pg_isready
```

---

## 2) Create the metastore role + database

Choose values (examples shown):

- user: `daniel`
- password: `password`
- database: `metastore`

Create role + db:

```bash
createuser --superuser "$USER" 2>/dev/null || true

psql postgres <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'daniel') THEN
    CREATE ROLE daniel WITH LOGIN PASSWORD 'password';
  END IF;
END
$$;

CREATE DATABASE metastore OWNER daniel;
SQL
```

If the `CREATE DATABASE` fails because it already exists, that’s fine.

---

## 3) Verify connectivity

```bash
psql "postgresql://daniel:password@localhost:5432/metastore" -c 'SELECT 1;'
```

You should see a single row with `1`.

---

## 4) Ensure your Hive profile matches

The default local profile lives at:

- `conf/profiles/local/hive/hive-site.xml`

These properties must match what you created above:

```xml
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:postgresql://localhost:5432/metastore</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>org.postgresql.Driver</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>daniel</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionPassword</name>
  <value>password</value>
</property>
```

Apply the profile overlay after edits:

```bash
local-data profile set local
```

---

## 5) First run notes (schema initialization)

When you start the Hive metastore service for the first time, it will create
the metastore schema/tables in Postgres.

Start Hive (or the full stack):

```bash
local-data start hive
# or
local-data start
```

### Postgres JDBC driver jar

When your `hive-site.xml` is configured for Postgres (`jdbc:postgresql:`),
`local-data start hive` will try to ensure the Postgres JDBC jar is available.

Behavior:

- If a matching jar exists under `$HIVE_HOME/lib`, it uses it.
- If it’s missing, it downloads the jar from Maven Central.
- If `$HIVE_HOME/lib` is not writable (common with Homebrew-managed installs),
  it downloads to `$BASE_DIR/lib/jars/` and exports `HIVE_AUX_JARS_PATH` so Hive
  can still load the driver without mutating Homebrew directories.

Pick a specific driver version by setting:

```bash
export PG_JDBC_VERSION="42.7.4"
```

Then follow logs:

```bash
local-data hive logs
```

If you see authentication/connection failures, double-check:

- Postgres is running (`pg_isready`)
- The URL/username/password in your `hive-site.xml`
- You can connect using the `psql` command from step 3

---

## Optional: Spark + Postgres JDBC jar

If you use Spark with a Postgres-backed metastore, you may need the Postgres
JDBC jar available to Spark (depends on how you submit jobs).

Common options:

- Add `org.postgresql:postgresql` to your job dependencies
- Or place the jar on Spark’s classpath (for example under Spark’s `jars/`)

