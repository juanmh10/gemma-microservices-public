# Production readiness backlog

The current product is a functional bounded batch MVP. Production delivery
requires explicit acceptance of the following work; these items do not authorize
new infrastructure, datasets, workloads or cloud spending.

| Area | Required decision or evidence |
| --- | --- |
| Fresh acceptance | Completed on 2026-10-05: fresh 900-record run without manual recovery, duplicate-free replay and operator-confirmed inbox arrival. |
| Deployment and state | Define release ownership, private configuration management, state backup/locking and rollback. Decide remote state and environment isolation before provisioning them. |
| Operations | Assign an operator, recovery objectives, incident handling, cost ownership and alerts appropriate to the bounded runtime. Preserve no-blind-retry behavior. |
| Data onboarding | Define the customer dataset/domain, evaluation truth availability, access isolation, retention/deletion requirements and validated population sizes. |
| Contracts and delivery | Email template v2 is deployed; introduce neutral recipient/channel contracts for new records while retaining old event IDs, receipts and idempotency. Confirm the approved recipient and sender privately. |
| Analytics | Define supported report/schema changes, BigQuery query scope/costs and Power BI refresh ownership. Import refresh is distinct from pipeline execution. |
| Release acceptance | Publish tested immutable images, review concrete Terraform plans, verify private IAM and runtime bounds, and retain execution/replay evidence. |
| Later MVP improvement | After the current release and fresh acceptance pass, improve Worker B's narrative for management and decision makers: natural language, higher-level synthesis and actionable interpretation grounded in verified metrics. Prompt/model/output changes are outside the current release. |

The approved MVP deployment, fresh coordinator execution and completed-plan
replay are complete. The operator confirmed inbox arrival on 2026-10-05; the fresh-run acceptance gate is complete. These execution
approvals are consumed; further workloads need their own exact approval. Review
of the default Compute identity's
existing Editor role is deferred; this release does not change it. Power BI
remains the saved dashboard, with its connection managed externally.

Current limits, frozen inference behavior and protected model storage remain
requirements until the PRD is deliberately revised. The MVP transition alone
establishes neither production deployment nor permanent external-service
availability.
