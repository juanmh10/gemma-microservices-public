# Development and verification

## Toolchain

Use Go 1.26.6 for both modules, Python 3.12, Terraform 1.8 or later, Git,
gitleaks, govulncheck, Ruff, pip-audit and TFLint. Production dependency versions
are recorded in Go manifests/sums, the Python requirements lock and Terraform
provider locks. Worker B's separate module preserves root-module compatibility.

Create the local Python test environment:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r worker-a/requirements.lock
.venv/bin/python -m pip install pytest==9.1.1
```

Use your installed toolchain selector to activate the pinned Go version. Do not
copy another operator's home paths or account configuration into the repository.

## Verification commands

```sh
make ci
make test-pipeline
make test-worker-cpu
make test-terraform
```

CI checks Git integrity/whitespace, private/generated artifact policy, pinned
synthetic data and screenshot hashes, secret and personal-identifier patterns,
Go formatting/tests/vet/vulnerability audits for both modules, Python lint/tests
and locked dependency audit, Terraform format/validation/TFLint and mock Power BI
IAM tests. Documentation checks reject broken local links and deployment-specific identity/configuration
text. Scanner success is not proof that arbitrary data is anonymous.

The pipeline target runs uncached race tests for both Go modules, including the
self-contained 900-record synthetic fixtures. These exercise preparation,
evaluation, actual fake-model ADK handling, report validation, indexing,
publication, notification and idempotent replay. SDK transport fixtures cover
request shape, successful structured output, failure classification and no retry.
No GPU, cloud model, email provider or Terraform apply is part of these checks.

Additional retained-artifact tests require the `diagnostics` build tag and explicit
local inputs; see [diagnostics](../diagnostics/README.md). They are not
needed for default local pipeline coverage and are not evidence of a live cloud
execution. Tests must fail explicitly if required tools are unavailable.

## Source layout

| Path | Purpose |
| --- | --- |
| `cmd/` and `internal/` | Preparation, GPU observer, runtime evaluation and Terraform review. |
| `worker-a/` | Frozen Python GPU worker and CPU-only transfer/cache/contract tests. |
| `worker-b/` | Go ADK analysis, data/API, messaging and coordinator. |
| `schemas/` and `prompts/` | Versioned contracts, instructions and domain definitions. |
| `terraform/components/` | Two independent desired-state roots. |
| `terraform/stacks/full/` | Coordinated profile templates. |
| `data/` | Explicitly approved pinned synthetic dataset and isolated local truth. |
| `diagnostics/` and `worker-b/diagnostics/` | Optional evaluation and CPU measurement tools. |
| `docs/` | Architectural, operational, and data boundary documentation. |
| `results/` | Ignored private artifacts, evidence, saved plans and local journals. |

Keep models, state, plans, private runtime configuration and generated outputs
out of Git. Do not modify frozen Worker A behavior to fix a CPU-stage failure.

## Private repository CI

The private repository uses a read-only GitHub Actions workflow on push and pull
request. Actions are pinned to commit hashes; Go, Terraform, Python audit/lint
tools, Gitleaks, govulncheck and TFLint use explicit versions. The workflow
installs the Python test runner and locked CPU dependencies, runs full CI, then
runs uncached pipeline race tests. No deployment credentials or cloud execution
are configured. A clean checkout is checked before the first source push.

Version candidate releases until the final uninterrupted fresh workload passes.
The Go module paths follow the gemma-microservices repository; deployed resource
identifiers remain stable to preserve Terraform ownership and retained data.

The artifact and PII checks include visible dataset files; only the exact pinned
synthetic files are publication exceptions. PBIX/PBIT models remain local. See
[data boundaries](data-boundaries.md) for inspected fields and limits of pattern
scanning.
