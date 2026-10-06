# Telemetry boundaries

Existing Go commands emit capped, versioned structured timing events. No new
exporter, monitoring service or dependency is required. Telemetry is best-effort
and does not change claims, completion markers, receipts or launch behavior.

| Boundary | Meaning |
| --- | --- |
| Application run | Entry through persistence/failure return. |
| Evaluation | Deterministic processing, including source object reads. |
| Model generation | Narrator invocation, adapter setup and final validation. |
| Model RPC | One bounded nonstreaming provider request; excludes downstream tool handling. |
| Storage | Bounded read/create with explicit missing/exists outcomes. |
| Index/query | Projection/receipt work and separate database-call interval. |
| Publish | Contract/persistence work and separate broker invocation. |
| Notify/email | Delivery/reconciliation and separate provider invocation. |
| Job lifecycle | Verified remote creation/start/completion timestamps. |

Elapsed spans use a monotonic clock. Timestamp pairs use the corresponding
remote/local UTC clock. Nested spans overlap. Broker-to-handler intervals include
transport/parsing and possible clock skew; they are not pure queue latency.

Process CPU is summed thread CPU, not allocated CPUs or billing. Process/cgroup
memory peaks are lifetime high-water marks, not per-span allocations. Missing
counters remain missing. CPU telemetry is not GPU utilization.

Events never include conversations, labels, model narratives, provider bodies,
URLs, headers, addresses or credentials. Correlation identities are constrained;
trace event counts are capped. Model-failure-v2 persists only an allowlisted
category, optional bounded HTTP status and allowed provider status symbol.
Historical failure markers and model-failure-v1 remain unchanged.
