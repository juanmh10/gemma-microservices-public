# Operation, recovery and shutdown

## Current status

The historical CPU restoration and 900-record recovery completed successfully.
The approved 2026-10-05 MVP deployment published four immutable CPU images,
restored preparation/GPU Job definitions and updated the three CPU Jobs and two
private services. Two independent states now manage 27 + 53 resources, including
Power BI IAM. Fresh historical GPU artifacts and failed analysis claims remain
preserved. Both refreshed plans show no drift; read-only coordinator preflight
passed. The separately approved fresh 900-record coordinator run completed
through provider-accepted v2 email without manual recovery; replay produced no
additional Job/model/email. The operator confirmed inbox arrival on 2026-10-05; the fresh-run acceptance gate is complete. Further execution
or shutdown requires its own reviewed scope. See [validation](validation.md).

## Components and local state

Foundation lives in `terraform/components/foundation`; Application lives in
`terraform/components/application`. These roots are independent ownership
boundaries within the fixed project and region, not separate environments.
Both use local state. Keep each component's state, backup and private tfvars
together; never initialize an existing deployment against a missing state.

The MVP migration backed up and relocated both states and verified their file
hashes, lineage, serial and disjoint resource ownership. Private backup evidence
is under the ignored `results/` directory. Existing cloud identifiers and model
objects remain stable. The provider-cache link was updated to the new root.

The full-plan Make target requires `TF_FOUNDATION_PROFILE` and
`TF_APPLICATION_PROFILE` to identify existing private deployment files; use
absolute paths. It rejects example profiles. Include the complete current
configuration in each file, not only partial profile overrides. Required run
and indexing identities must be supplied explicitly. Paused resources still
require their current private configuration when preserving desired state.

New bundles contain `foundation.tfplan`/`foundation.json` and
`application.tfplan`/`application.json`. Changing these filenames changes the
review digest. Generate and review new bundles; historical approvals cannot be
transferred by renaming files. `terraform-review-full` checks both roots without
launching work or accessing cloud resources.

The deployed notifier no longer contains the temporary fault switch and now
uses the verified messaging image with email template v2 compatibility. The
approved deployment is complete; it launched no pipeline workload. Future
source changes require new immutable images and reviewed plans.

## Restore and deploy

1. Confirm the authorized identity and the implementation's fixed target project.
2. Restore the privately backed-up runtime flags; keep protected data policies.
3. Build immutable CPU images from the final tested source and record their hashes.
4. Prepare private configuration and save plans for both Terraform roots.
5. Review exact additions/updates/deletions, disjoint ownership, bounded resources,
   private access, numeric secret version and protected model/data resources.
6. Obtain approval for image publication and the concrete deployment plans.
7. Publish/verify image digests, apply the exact saved plans, then verify ready
   revisions and refreshed no-drift plans. Deployment launches no pipeline work.

Cloud configuration belongs in ignored private files and attached runtime
identities. Operators do not copy credentials or personal account names into
this guide. The email secret is supplied through Secret Manager and readable
only by the notifier; inspection uses metadata, not secret payloads.

The CPU packaging helper is `scripts/build_cpu_oci.py`. The analysis command
requires explicit `--model vertex` for authorized remote execution or
`--model fake` for offline development. A missing model flag is rejected before
source access; remote fake mode is forbidden.

## Review and run

Prepare a versioned local plan containing pinned source objects, image digests,
analysis identity, notifier revision and secret version. Review it without launch:

```sh
go -C worker-b run ./cmd/pipeline --plan ../results/pipeline.plan.json
```

The command prints a digest for approval. Execution additionally requires the
execute flag, that exact digest and a private state directory. Authorization is
specific to the reviewed plan. Do not use a stale historical example directly.

For final fresh-run recovery, omit Worker A and reuse the unchanged outputs of
its completed 900-record execution. Use a new CPU analysis identity. Preserve
all preceding failed claims. A GPU retry is outside this recovery scope.

## Fresh workload from one command

Use a preparation-enabled plan with two immutable raw input hashes, the preparer
image/revision and a new run/analysis identity. Prospective output references
use zero hashes until preparation creates them; they never authorize retained
outputs. The coordinator verifies preparation provenance and all three shards,
journals the resolved hashes, then launches Worker A once. GPU output hashes are
resolved automatically before analysis. Existing prepared or prediction outputs
reject a new fresh launch. Failure stops the fresh acceptance attempt.

The exact approved plan is the only execution input. Cloud deployment and plan
preparation happen before launch; they are not manual workload stages. Existing
GPU deadline/no-retry bounds and private notification configuration remain.
The 2026-10-05 fresh cloud test and its single completed-plan replay passed
through provider acceptance. The recorded approval is consumed; any further
workload needs a new exact proposal. The operator confirmed inbox arrival on 2026-10-05; the fresh-run acceptance gate is complete.

## Failure handling

| Failure | Required action |
| --- | --- |
| Source missing, partial, duplicate or corrupt | Stop before model use; verify the original artifacts. |
| GPU deadline or launch ambiguity | Preserve journal/partial output and independently confirm termination; do not relaunch. |
| Model RPC/narrative failure | Preserve claim/metrics and safe diagnostic; inspect before a separately approved CPU recovery. |
| Index failure | Inspect the exact deterministic attempt; recover indexing independently without repeating inference. |
| Publication or send ambiguity | Reconcile intent, event identity and receipts; never blindly resend. |
| Completed-plan replay | Validate durable evidence; launch no extra Job/model/email. |

The historical failure marker retains only analysis identity and stage. Detailed
model diagnostics are separately versioned. Provider messages, request/response
payloads, headers, addresses and credentials must never be logged or persisted
as diagnostics. HTTP status does not by itself prove which request field failed.

Provider acceptance is distinct from inbox delivery. Ask the authorized recipient
for confirmation after a successful provider receipt; do not alter that receipt
to fabricate a delivery event.

## End-of-day shutdown

Use reviewed runtime-removal plans, not a full-root destroy. Set the Foundation Job
and preparer deployment switches off while retaining the worker network. Set
Application analysis/data/API/messaging compute and email enablement off; retain queues
and the provisioned secret. Check the push backlog before deleting its subscription.

Apply Application shutdown before Foundation. Verify zero pipeline Jobs/services, preserved
model policy/secret/history, independent state ownership and refreshed no-drift
plans. State backups and restoration settings remain private. Re-enabling runtime
requires fresh plans; previously applied binary plans cannot be reused.

Never delete, replace, rename or expire the models bucket/objects. Retain PREVENT,
force-destroy disabled and Autoclass terminal NEARLINE. Its native inactivity
interval is 30 days. Keep the worker network while the provider-managed serverless
reservation blocks removal; do not delete that reservation manually.

Shutdown is not complete project deletion. Persistent storage, images, retained
messaging, secrets and logs can still incur charges.

## Email template compatibility

New sends use `stakeholder-email-v2`. Existing v1 receipts and pending delivery
claims retain their payload and provider key. Both versions share the original
create-only delivery ledger path. New claim conflicts stop for reconciliation;
no template-specific second ledger is created. An older notifier image cannot
process a v2 receipt; review rollback before replaying a v2 delivery. See
[data boundaries](data-boundaries.md).
