# Optional diagnostics

These tools support local investigation and performance analysis. Normal
workload execution uses the coordinator; setup and deployment are described in
[operations](../docs/operations.md).

| Tool | Purpose |
| --- | --- |
| `diagnostics/cmd/benchmark-report` | Evaluate completed inference outputs using isolated truth; optional GPU-profile comparison requires its own authorized evidence. |
| `diagnostics/compare_pilot.py` | Inspect local historical pilot results. |
| `worker-b/diagnostics/cmd/measure-cpu` | Measure bounded local fake-model analysis against validated metrics. |

Run Go tools from their owning module: root for `benchmark-report`, `worker-b`
for `measure-cpu`. Arguments require explicit local input/output paths. The
benchmark command also supports a GCS output; that cloud mutation requires a
concrete approved scope and is not part of offline checks.

Retained-artifact tests are excluded from default tests with the `diagnostics`
build tag. `RETAINED_REPORT_PREFIX` selects a local historical report fixture;
`MEASURE_REQUEST` and `MEASURE_BASELINE` select local measurement fixtures.
Use only verified compatible artifacts; the report checks target the archived
v1 run. Missing fixtures skip these optional checks. Default integration tests
create synthetic fixtures and require none of these inputs.

```sh
go -C worker-b test -tags diagnostics ./internal/analytics ./internal/pipeline ./diagnostics/internal/localmeasure
```

The original optional retained-artifact full-pipeline regression was removed.
Its index outage/recovery, API isolation, publication replay and delivery
idempotency are covered by default component tests and the self-contained
900-record integration suite.
