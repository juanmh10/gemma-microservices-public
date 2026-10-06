# Repository hygiene and data boundaries

This review covers publication candidates, local report assets and the source
contracts that move data through the MVP. No cloud inspection, apply, model call,
GPU task or email delivery was performed. It does not certify future customer
input as anonymous or replace a production IAM review.

## Publication contents and checks

The repository includes exactly four pinned synthetic dataset files. Their hashes
are recorded in `repository-assets.json` and checked before CI scans. The source
contains fictional dialogue, persona/demographic fields and separate latent
labels; prepared input contains only `record_id` and dialogue messages. Truth
contains evaluation labels and reasoning. Dataset bytes and revision are unchanged.
Future real datasets are excluded from Git and require their own onboarding.

The artifact policy rejects visible private state, runtime tfvars, credentials,
model weights, imported datasets, build output, symlinks and unreviewed binaries.
It includes already tracked files even when ignore rules would hide new files.
The PII scanner now includes `data/` and flags email, valid CPF and local user-path
patterns. Gitleaks scans repository contents and Git history for secrets.
Pattern checks cannot identify every name, address or personal statement; pinned
synthetic provenance is distinct from automatic anonymization.

The one publishable image is the reviewed aggregate dashboard screenshot, pinned
by hash. Its visible content contains aggregate metrics/cause labels; it displays
no conversation excerpt, personal identity or credential. Changing it requires
renewed visual review and a deliberate pin update.

The local Power BI file contains a binary `DataModel` and `SecurityBindings`.
Readable report metadata yielded no email, local user-path or credential marker,
but the compressed model was not semantically decoded. The original PBIX remains
local, ignored and excluded from build contexts. Only report definition/static
resource JSON and the reviewed screenshot are publication candidates; they
contain layout, field bindings and formatting, not the imported semantic model,
connection credentials or a data cache. The exported definition is a design
snapshot, not a complete deployable Power BI project.

## Fields by boundary

| Boundary | Content | Exposure control |
| --- | --- | --- |
| Synthetic source and truth in Git | Dialogue, fictional persona/demographics, outcomes and hidden evaluation labels/reasoning. | Explicit dataset exception with exact hash pins. No new dataset accepted implicitly. |
| Preparation to inference | Record/turn IDs and dialogue messages. | Separate GCS/IAM and explicit Docker COPY; source/truth excluded from build context. |
| Inference output to evaluation | Predictions, cause codes, turn references, provenance and completion markers. | Verified source hashes, bounded chunks and isolated truth access in evaluation. |
| Analysis to Vertex | Verified aggregate metrics and up to ten prevalidated excerpts of at most 1024 bytes each. | Bounded model/tool access and request/token limits. Excerpts are not automatically anonymized. |
| GCS report | Metrics, bounded excerpt text, narrative, decision and source/model provenance. | Authoritative immutable artifacts; treated as sensitive when input is real. |
| Private API report endpoint | Full validated report, including `metrics.evidence[].text`. | Cloud Run IAM is mandatory in deployment. This endpoint is not an aggregate-only public view. |
| Private API status/history | Aggregate metrics/status and technical provenance. | Bounded filters, fixed query/object prefixes and no caller-supplied SQL/URLs. |
| BigQuery and Power BI | Aggregate counts/per-cause scores, technical provenance, narrative findings and recommendations. | Direct evidence arrays, dialogue rows, latent labels and credentials are omitted. Narrative can paraphrase or quote an excerpt; access remains private. |
| Pub/Sub and notification receipt | Stable identities, quality status, report reference/hash and aggregate notification. | Versioned contract, bounded payload and immutable receipts. No full report/conversation payload. |
| Email | Aggregate counts/scores, quality reasons and analysis reference. | No evidence text or model narrative; private sender/recipient configuration and hashed payload receipts. |
| Logs/diagnostics | Allowlisted identities, stages, counts, timing and safe provider status. | No conversations, truth, provider bodies/headers, secret values or email addresses. |

The full report API and model evidence access remain product requirements. This
cleanup preserves canonical report contents and hashes. Removing evidence or
redacting model input would change the analytical contract and needs a deliberate
versioned design before onboarding real conversations. A report containing bounded
source text must not be represented as fully anonymized.

## Names and compatibility

New emails use `stakeholder-email-v2` with neutral wording and aggregate fields.
Existing v1 accepted/pending receipts retain their payload and provider key.
Both versions use the existing delivery ledger path: its create-only intent
prevents template versions from creating independent sends for the same event.
A conflicting claim stops for reconciliation. Older binaries cannot process v2
receipts and must not be used to replay those deliveries after rollback.

`poc-gemma-*`, the existing analytics dataset, old IAM labels, `pilot-004`,
`gym-sales-manager-demo` and `test_sink` remain versioned/deployed compatibility
identifiers. The old POC email text exists only in the v1 compatibility renderer.
These values are not credentials. Conventional schema IDs and reserved test
fixtures are not executable deployment placeholders. The incomplete upstream
citation author field was removed without inventing attribution.

## Verification and remaining external work

Repository checks include artifact policy tests, dataset/image pins, secret/PII
checks, JSON/source documentation review, aggregate-output exclusion tests and
email v1/v2 recovery/race/replay tests. The MVP cleanup's immutable images were
subsequently published and deployed through the approved 2026-10-05 plans; see
[validation evidence](validation.md). Future source changes require new images
and their concrete reviewed deployment proposal before live use.

The approved deployment verified scoped runtime IAM, private service access and
image contents; the fresh execution verified source hashes, persisted reports
and delivery receipts. Future production work includes real-data access and
retention rules, inherited IAM review and analytics sharing/refresh ownership.
The default Compute Editor review remains deferred. Power BI is the saved
dashboard, with its connection managed externally; no dashboard change is part
of this validation. Local CI alone does not establish these cloud properties.
