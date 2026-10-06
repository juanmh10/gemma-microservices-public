# Validation evidence and acceptance

## Fresh coordinator validation — 2026-10-05

Run `pipeline-mvp-20261005-01`, analysis `mvp-acceptance-20261005-01`, completed
from fresh preparation through provider-accepted v2 email using one coordinator
invocation, without manual intermediate launches or recovery. The protected
weights and frozen compilation cache were reused; preparation outputs,
predictions, metrics and report were new.

| Check | Confirmed result |
| --- | --- |
| Preparation | 900 records, three 300-record shards and isolated truth. |
| Worker A | One execution, three successful RTX tasks, zero retries; completion in 6m19.14s. |
| Source integrity | All 14 source object hashes verified against the resolved request. |
| Quality | 892 valid, eight invalid and 891 correct outcomes; 99% accuracy and cause micro F1 90.2338%; quality-v1 passed. |
| Independent evaluation | Local deterministic reevaluation matched counts, rates and per-cause metrics without another model call. |
| Vertex | One request, 3,472 input and 568 output tokens, within approved bounds. |
| Indexing | Both analytical tables gained exactly one row, from four to five. |
| Private API | Authenticated health/status/report/history returned 200; report matched GCS and history included this analysis. Anonymous access returned 403. |
| Publication and notification | Immutable publication and delivery receipts; one provider-accepted stakeholder-email-v2 receipt. |
| Completed-plan replay | No additional Job/model/email; all 13 artifact generations, journal hash, execution lists and table metadata unchanged. |
| Terraform | Both refreshed component plans have no drift; ownership remains disjoint at 27 + 53 managed resources. |
| Inbox | The operator confirmed arrival on 2026-10-05 for this exact analysis. |

Execution-plan SHA-256:
`571a0c87c7a014024c19cda7636cc13781e1c8bf72955e1d9f20f8bd954c7426`.
Report SHA-256:
`74e1048dfe78dbad51b5ae021c51a6d469c0bb2bd9f9ccf37543f8fe7f180dcd`.

The execution and single replay approval are consumed. Evidence remains private;
no source text, addresses, credentials or provider identifier is published here.
Operator inbox confirmation on 2026-10-05 closes the fresh-run acceptance gate;
this validation does not authorize a further GPU/model/email run or establish
production availability.

Full CI passed again before Git publication, including dependency audits,
privacy/artifact checks, Go/Python tests and Terraform validation.

## MVP deployment — 2026-10-05

The operator approved publication of four immutable CPU images and the exact
saved deployment bundle. Foundation created two Job definitions; Application
updated five runtimes and replaced three conditional analysis reader bindings
for the new run. Deployment itself launched no pipeline workload.

Verified results:

- All four registry manifests match their local byte digests. Image contents
  are the static executable and CA bundle only, running as a non-root identity.
- Five Jobs and both private services are ready with the approved images and
  bounded configuration. The notifier no longer has `NOTIFIER_RETRY_FAULT` and
  references numeric secret version 1; no secret value was read.
- The two active private profiles match the applied configuration. Refreshed
  plans have no changes, and states retain disjoint 27 + 53 resource ownership.
- Protected models, data, history, queues and secret remain preserved.
- The final preparation-enabled plan passes offline validation and the actual
  coordinator's read-only preflight: all five Jobs, raw SHA-256 inputs, notifier
  revision/traffic/private configuration and secret reference match.

Deployment bundle SHA-256:
`4f08a401c8fce66c7f0ef9fe0f1c3cf6cbd9ca8973000443cb4f86d7cd6c0293`.

The separately approved fresh workload and replay subsequently passed the
technical checks recorded above. Deployment/preflight alone do not establish inference
quality, live model response or inbox arrival. Compute Editor review and
management-oriented Worker B narration remain deferred.

The first publication helper stopped before uploading a blob or manifest when
the registry returned a protocol upload location outside its repository path.
Read-only reconciliation confirmed all new manifests/blobs were absent. A
separately retained correction accepts only the exact returned upload URL on
the same fixed registry host, passed offline protocol checks, and published the
unchanged approved digests. The original helper, release manifest and binary
plans remain preserved; no remote resource scope or runtime image changed.

## Historical confirmed cloud evidence

| Scope | Confirmed result |
| --- | --- |
| Retained-source pipeline | Vertex report, BigQuery indexing, publication, notification and private API checks passed; the operator confirmed email arrival. |
| Completed-plan replay | No additional Job, model request or email in the accepted retained-source flow. |
| Fresh preparation | One successful new 900-record dataset preparation, three dialogue shards and isolated truth. |
| Fresh Worker A | One execution, three successful GPU tasks, no failure/cancellation/retry. |
| Fresh source integrity | All 14 pinned source objects remained unchanged through CPU recoveries. |
| Fresh mathematical evaluation | 892 valid and eight invalid predictions; 891 correct outcomes; 99.00% accuracy; cause micro F1 90.1704%. Independent and persisted metrics match. |
| Historical fresh Worker B failures | Initial narration and two CPU recoveries failed; the last historical failure had category adk_execution. |
| Corrected fresh-900 CPU recovery | One live Vertex request; validated report/provenance, indexing and messaging passed; operator confirmed email. |
| Corrected recovery replay | No new Job, model request or email; receipts unchanged and both tables remain at four rows. |
| Private API | Authenticated health/status/report/history returned 200; anonymous access returned 403. |
| Failed-run downstream isolation | No completed report, indexing, publication or email for the failed analyses; both analytical tables remained at three rows. |
| Historical CPU restoration | Exact 42-addition CPU restoration; two disjoint states managed 25 + 53 resources (including Power BI IAM). Three CPU Jobs and two private services; no preparation/GPU Job. Protected models/data/secret preserved; final plans showed no drift. |

Evidence is retained privately. The corrected CPU chain passed against unchanged
fresh GPU outputs; the original fresh run required manual recovery after failure.
See [architecture](architecture.md) for architecture definitions and contracts.

## Local finalization scope

The new correction removes unsupported length/pattern/collection constraints
from the Vertex function schema while preserving local report validation and
bounded tool selection. Provider failures now retain only safe HTTP/status
metadata in model-failure-v2; old diagnostic and report contracts stay readable.

The official function-calling contract supports a subset of schema fields;
this establishes a request compatibility issue, not the exact cause of the
historical RPC failure. That call did not preserve its provider status.
See [the official reference](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/function-calling).

Local verification includes schema-wire fixtures, actual SDK/ADK structured
response handling, no SDK retry, payload-free diagnostic rejection, 900-record
fake-ADK preparation-to-email integration and replay, partial/duplicate rejection,
GPU observer fault/deadline cases, concurrency/race tests and full CI.
These fixtures do not demonstrate live Vertex availability or email delivery.

## Completed live gate

All six steps passed for the recorded corrected CPU recovery:

1. Pass local CI and race tests for the exact final source.
2. Review and approve new immutable images and restoration/deployment plans.
3. Verify deployed images, private service revisions, limits and no drift.
4. Approve one CPU-only recovery with a new analysis identity and unchanged
   fresh 900-record GPU inputs; launch no Worker A task.
5. Verify completed report hash/provenance, population metrics, BigQuery
   projections, event/notification/provider receipts and private API status.
6. Verify completed-plan replay produces no duplicate work; obtain inbox
   confirmation and record final no-drift plans.

If any step fails, report the stage and preserve evidence. External services
can fail; acceptance is evidence for an exact bounded run, not a guarantee of
permanent availability or flawless future execution.
