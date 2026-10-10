# Machine-readable output

Two commands emit a stable, versioned JSON contract for scripts and CI.

## `local-data status --json`

Writes one JSON object to stdout. `schema_version` is currently `1`.

Stable fields: `profile`, `services[].processes`, optional `services[].listeners`,
and `errors`.

- A process with `running: false` or a listener with `listening: false` is an
  observed stopped state.
- Collection and configuration failures appear in `errors`; the observations
  that could be made are still reported, and the command exits nonzero.
- Process and listener observations do not establish that a dependency is ready.

```bash
local-data status --json | jq '.services[] | {name, processes}'
```

## `local-data env doctor [target...] --json`

Writes exactly one JSON object to stdout. `schema_version` is currently `1`.
It contains only dependency observations: `target`, `dependencies.required`,
`dependencies.optional`, `java`, and `result.healthy`.

- Required findings decide `result.healthy` and the exit status.
- Optional findings are warnings only.
- The report never includes environment values or credentials.

```bash
# Healthy preflight (exit 0)
local-data env doctor --json
# {"schema_version":1,"target":"general","dependencies":{"required":[{"command":"java","found":true},{"command":"brew","found":true}],"optional":[{"command":"curl","found":true},{"command":"spark-sql","found":true},{"command":"beeline","found":true}]},"java":{"major":17,"recommended_major":17,"is_recommended":true},"result":{"healthy":true}}

# Missing required dependency (exit 1)
local-data env doctor start hdfs --json
# {"schema_version":1,"target":"start hdfs","dependencies":{"required":[{"command":"java","found":true},{"command":"hdfs","found":false}],"optional":[{"command":"curl","found":true},{"command":"jps","found":true}]},"java":{"major":17,"recommended_major":17,"is_recommended":true},"result":{"healthy":false}}

# Missing optional dependency is still healthy (exit 0)
local-data env doctor --json
# {"schema_version":1,"target":"general","dependencies":{"required":[{"command":"java","found":true},{"command":"brew","found":true}],"optional":[{"command":"curl","found":true},{"command":"spark-sql","found":false},{"command":"beeline","found":true}]},"java":{"major":17,"recommended_major":17,"is_recommended":true},"result":{"healthy":true}}
```
