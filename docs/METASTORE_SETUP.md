# Postgres Metastore Setup (Optional for Hive)

`local-data` defaults to Derby metastore.
Use this guide only if you want a Postgres-backed metastore.

This doc walks through:

- Installing + starting Postgres (Homebrew)
- Creating the metastore DB/user
- Initializing local-data with Postgres metastore
- Adding the postgres JDBC driver jar (for hive and spark)

---

## 0) Prerequisites

- local-data CLI + dependencies installed (via Homebrew)

```bash
# install local-data CLI + dependencies
brew install danieljhkim/tap/local-data

# or build from source
brew install hadoop hive jdk@17 apache-spark
```

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
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
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

## 4) Initialize local-data with Postgres metastore:

```bash
local-data init --user daniel --db-type postgres --db-url "jdbc:postgresql://localhost:5432/metastore" --db-password "password"
```

---


## 5) Add the postgres JDBC driver jar

For hive and spark to work with the postgres metastore, we need to ensure the Postgres JDBC jar is available.

### what to do:

- check if the jar is available in $HIVE_HOME/lib
- if not, download the jar from Maven Central and place it in `$HIVE_HOME/lib` or `$BASE_DIR/lib/jars/`
  - i.e. `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar`

Once the jar is available, just run `local-data start` to start the services and you're good to go. Schema will be initialized automatically. And the jar will be added to spark as well.
