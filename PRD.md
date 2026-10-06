# Product Requirements: gemma-microservices

Updated: 2026-10-05. This document consolidates the current approved scope.
Historical requirements and execution notes were preserved in the recoverable
cleanup archive; this English edition retains the implemented failure, cost,
isolation and acceptance boundaries.

## Objective

Process a bounded batch of sales conversations into structured outcomes,
cause codes and conversation evidence. Evaluate predictions against separately
stored truth, generate a factual narrative, persist history and notify the
approved stakeholder. The functional MVP is operator-initiated and intended for future production delivery. The final publication gate requires one fresh
900-record run from preparation through email, controlled by one coordinator
invocation without manual stage launches or reuse of prior outputs. High
availability remains outside acceptance.

## Components and ownership

| Component | Language/service | Responsibility |
| --- | --- | --- |
| Preparation | Go / Cloud Run Job | Separate dialogue from truth, shard data and persist a versioned manifest. |
| Worker A | Python / GPU Cloud Run Job | Stateless DiffusionGemma inference, resumable chunks and completion metadata. |
| Worker B | Go / ADK / Vertex AI | Verified deterministic evaluation followed by a bounded model narrative. |
| Indexer | Go / Cloud Run Job / BigQuery | Project immutable reports into analytical tables independently. |
| API | Go / private Cloud Run service | Read status, reports and history; no new-analysis submission. |
| Publisher | Go / Cloud Run Job / Pub/Sub | Publish an immutable event referencing a completed report. |
| Notifier | Go / private Cloud Run service / Resend | Validate delivery, persist receipts and send the configured aggregate email. |
| Coordinator | Go / local operator command | Review the exact plan, enforce limits and journal each authorized launch. |

Use GCS as the authoritative data plane. No additional workflow engine, Cloud
Function, database or scheduling service is needed for this scope. Use Terraform
for all Google Cloud resources, with the fixed project/region encoded in the
implementation; Vertex uses the approved US endpoint location.

## Frozen inference worker boundary

Worker A behavior, model/cache lineage, prompt and image remain frozen. L4 and
an L4/RTX comparison are excluded from MVP acceptance. Domain generalization
and optional definitions do not authorize another GPU experiment.

The approved fresh population was 900 records: three shards of 300, up to three
parallel RTX tasks, one GPU each, a 15-minute task bound and no retries. The
observer starts cancellation at 14 minutes and verifies termination within the
15-minute guard. Soft-stop and resumable-output behavior are preserved. That
execution approval was consumed; all three tasks completed. Any new GPU run
needs its own explicit scope and approval.

Worker A must never access ground truth. Keep source/truth out of its image and
IAM scope. Its chunks, success markers and model/image/prompt provenance must be
verified before downstream use. Incomplete, duplicate, corrupt or mismatched
sources cannot pass full-population acceptance.

## Analysis worker contract

Worker B is Go using pinned ADK and Gen AI SDK dependencies. The configured
model is Gemini 3.5 Flash-Lite through Vertex identity authentication. No API
key or credential-file fallback is allowed for this call.

Verify SHA-256 source references and all three task completions before model
use. Derive population metrics from exact counts across 900 unique records.
Invalid predictions remain in accuracy and coverage denominators. Missing truth
produces no fabricated supervised score. Apply quality-v1 thresholds: schema
validity at least 99%, outcome accuracy at least 98%, cause micro F1 at least 90%.

Support versioned single-shard and full-batch contracts without changing old
prompt contents. Current population-aware instructions are worker-b-v2. Reports
must validate their bounds and authoritative metric references. Send only the
provider-supported function schema; keep length, pattern and collection checks
in local validation. Never silently repair or invent a model narrative.

Bounds: at most three model requests, 24,000 input tokens and 6,000 output tokens
per analysis, 60 seconds per RPC, 480 seconds application timeout, 600 seconds
Job timeout, one task and no SDK/Job retries. Persist a create-only claim before
model calls. Failure retains metrics and the historical two-field failure
marker. Safe provider diagnostics are separately versioned and never contain
provider messages, payloads, headers, addresses or credentials.

## Persistence, delivery and recovery

GCS reports/completion markers are authoritative. BigQuery contains a compact
run projection and the report projection, indexed through a deterministic
attempt identity. Indexing failure must not corrupt the report; status must
expose it separately.

Publication requires a validated completed report. Preserve the report hash,
event identity, immutable intent and receipts. Pub/Sub is at-least-once;
notification processing and provider idempotency handle duplicate delivery.
Do not claim globally exactly-once transport. Provider acceptance and confirmed
inbox arrival are separate evidence.

One privately configured sender/recipient and a pinned secret version define
the bounded email delivery configuration. The notifier's deterministic Go template sends aggregate
metrics; it does not call an LLM. New deliveries use email template v2; existing
v1 delivery payloads/keys and the shared create-only ledger remain compatible.
Preserve the notification sink and receipts.

Journal launch intent before submitting each Job. Resume only a known execution;
never repeat Worker A after failure or an ambiguous launch. Use an approved new
CPU analysis identity against unchanged GPU outputs for narration recovery.
Exact completed-plan replay must launch no Job, model request or additional
email. Unknown send/launch outcomes require inspection, not blind retries.

## Infrastructure and preservation

Maintain two disjoint Terraform states for Foundation and Application. Their
roots are `terraform/components/foundation` and `terraform/components/application`. The full-profile wrapper creates no
third state. Private services use minimum zero, maximum one and concurrency
one. CPU Jobs use one task, parallelism one and zero retries. Limits do not
establish a billing guarantee.

Preserve the model bucket with PREVENT and force-destroy disabled. Never delete,
replace, rename or expire its objects. Retain Autoclass terminal NEARLINE with
its native 30-day no-read interval; object-age rules cannot emulate a shorter
inactivity interval. Retain analysis history, source/results evidence and the
email secret during runtime shutdown. Keep the worker network while its managed
serverless reservation prevents removal; never delete that reservation manually.

Use reviewed immutable-image plans before apply. Registry publication, apply,
GPU/model/email invocation and data deletion require their concrete authorized
scope. Local testing does not confer cloud execution approval.

## Acceptance and current evidence

The retained-source end-to-end run passed indexing, delivery, private API checks
and user-confirmed email; replay launched no extra Job. Fresh preparation and
all three GPU tasks completed once, and supervised quality passed. Worker B
failed in bounded narration, including a subsequent failed model RPC. Downstream
stages did not run for those failed analyses.

The provider-schema correction and safe HTTP/status diagnostics were published
and deployed through the exact reviewed CPU restoration plan. The approved
900-record recovery completed report/hash/provenance validation, indexing,
publication, notification, private API checks and duplicate-free replay. One
Vertex request succeeded; the operator confirmed inbox arrival. No new GPU task
was launched. Historical failed analyses remain preserved. This establishes
acceptance of the corrected CPU chain against fresh GPU outputs, rather than a
single uninterrupted fresh execution.

The MVP cleanup was published and deployed on 2026-10-05: two disjoint states
manage 27 + 53 resources, including read-only Power BI IAM. Five Job definitions
and two private services are ready. Preparation/GPU definitions were restored,
and the separately approved fresh workload subsequently completed. Protected
model policy/data remain intact;
refreshed plans show no drift. Read-only coordinator preflight passed against
the new images and ready notifier revision. Local CI and race checks validate
the source. The new 900-record coordinator run completed preparation, all three
GPU tasks, live Vertex analysis, indexing, private API checks, publication and
provider-accepted v2 email without manual stage recovery. All 14 source hashes
and independently recomputed metrics match: 892 valid, eight invalid, 891 correct
outcomes, 99% accuracy and 90.2338% cause micro F1. One model request used 3,472
input and 568 output tokens. Replay produced no additional Job/model/email and
preserved all 13 artifact generations; both tables gained exactly one row and
remain at five rows. The operator confirmed inbox arrival on 2026-10-05; the fresh-run acceptance gate is complete. Acceptance is evidence
for the recorded bounded runs, not permanent external-service availability.

## Private publication and final fresh workload

Publish the cleaned source and versioned contracts as gemma-microservices in the
operator's private repository after local CI passes. Remote CI runs secret/PII
checks, linters, dependency audits, Terraform validation and uncached race tests
without cloud credentials or execution. Keep private configuration, states,
plans, models and generated results outside Git. The MVP transition preserves
legacy resource identifiers and versioned event/receipt paths; it adds no
production environment or new workload approval.

The requested final workload uses a new preparation/run/analysis identity,
900 records in three shards and the frozen Worker A image. Preserve model
weights/compilation cache, but do not reuse prepared outputs, predictions,
metrics or reports. One coordinator command launches preparation, verifies its
outputs, launches Worker A once, resolves all output hashes, then runs analysis,
indexing and publication and observes email acceptance. Keep the existing
15-minute GPU guard, maximum three parallel GPUs and zero automatic retries.

Prepare and review immutable images, deployment plans and the exact execution
plan before cloud mutations. The 2026-10-05 fresh execution proposal was approved
and consumed by the successful run and one completed-plan replay. Further
GPU/model/email runs need their own concrete approval. On failure, stop and
preserve evidence; a separately approved recovery cannot count as uninterrupted
fresh acceptance.

## Repository data publication

Only pinned synthetic dataset files and reviewed dashboard assets are publication
exceptions. Imported Power BI binaries, private configuration, model weights and
generated results remain outside Git. Canonical reports and the IAM-private
report API deliberately include bounded evidence; they are sensitive surfaces
when input contains personal data. BigQuery omits direct evidence arrays but
retains narrative. See [data boundaries](docs/data-boundaries.md).
