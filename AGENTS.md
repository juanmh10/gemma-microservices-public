# Development Instructions

This repository contains an open-source batch pipeline that transforms conversation data into structured predictions, analyzes causes, indexes results into analytical storage, and delivers aggregate notifications. Go handles dataset preparation, orchestration, and backend microservices; Python handles GPU/CPU inference.

## Guiding Principles

- Keep changes minimal, modular, and well-tested. Write code, comments, commits, and documentation in English.
- Keep `README.md` as the main setup and usage guide, `docs/architecture.md` as the system design overview, and `CHANGELOG.md` as the record of notable releases.
- Ensure all public code remains free of hardcoded secrets, internal project IDs, private URIs, and PII.

## Implementation Architecture

- **Go**: Used for CLI utilities (`cmd/`), core orchestration libraries (`internal/`), and Worker B analytical pipelines (`worker-b/`). Follow standard Go idiomatic style and run `go test ./...`.
- **Python**: Used for Worker A (`worker-a/`) inference engine. Use pinned dependencies in `worker-a/requirements.lock` and execute tests via `pytest`.
- **Terraform**: Infrastructure definitions live under `terraform/components/foundation` and `terraform/components/application`. All infrastructure changes must be defined as declarative code and formatted via `terraform fmt`.
- **Workers**: Keep workers stateless, idempotent, and bounded. Ensure jobs support resumable outputs and write structured metadata.

## Security and Data Protection

- Never commit real credentials, service account keys, `.tfstate` files, or local `.env` files.
- Use placeholders and environment variables for cloud configurations (e.g. `your-gcp-project-id`, `gs://your-bucket-name`).
- Ground truth datasets must remain separated from inference workers to avoid evaluation bias.
- Maintain versioned schemas (`schemas/`), model versions, and container image tags.

## Quality and Verification

- Run `make ci` to verify code formatting, linting, security scanning, dependency audits, and test execution.
- Maintain automated unit and integration tests across Go, Python, and Terraform components.
- Do not commit heavy model weights, raw binary assets, generated logs, or run artifacts to version control.
