# Development

Building, testing, and working on `local-data`. For installing and using it, see the [README](../README.md).

## Make targets

```bash
make build                   # build bin/local-data
make test                    # unit tests, no system dependencies
make test-integration        # hermetic black-box tests (real binary + deterministic shims)
make test-integration-live   # opt-in macOS smoke test against installed services
make test-coverage           # unit tests with coverage report
make format vet lint         # gofmt, go vet, golangci-lint
make help                    # all targets
```

The hermetic lane runs under a temporary `HOME` and needs none of Hadoop, Hive, Spark, Postgres, or MySQL. The live lane needs Homebrew Hadoop, Hive, Spark, and Java 17: it bootstraps Derby, exercises local Hive, round-trips an HDFS write/read, runs a small Spark job, and always attempts `local-data stop`. Don't run it while another local-data cluster holds the default ports.

## CI-equivalent checks

CI runs this on every pull request and push to `main`; run it locally before opening a PR:

```bash
gofmt -l $(git ls-files '*.go') | tee /tmp/gofmt.out && test ! -s /tmp/gofmt.out
go vet ./...
go test ./...
make test-integration
go test -race ./internal/...
go build -o bin/local-data ./cmd/local-data
bash test/release_workflow_security_test.sh
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml .github/workflows/release.yml
```

CI deliberately disables dependency and build caching, so pull-request code can't seed cache content later used by a trusted branch.

## Project layout

```
cmd/local-data/        # entry point
internal/
├── cli/               # Cobra commands: env, profile, setting, service, wrappers, logs, init
├── config/            # profile management
│   ├── generator/     # XML/conf generation + overrides merge
│   ├── profiles/      # built-in profile definitions
│   └── schema/        # typed Hadoop/Hive/Spark config structs
├── env/               # environment detection + computation
├── metastore/         # metastore DB type detection + validation
├── service/           # service lifecycle (ProcessManager): hdfs, yarn, hive
└── util/              # fs/xml/shell/log/color helpers
```
