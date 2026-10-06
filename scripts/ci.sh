#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -d "$HOME/go/bin" ]; then
  export PATH="$HOME/go/bin:$PATH"
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required tool: $1" >&2
    exit 1
  fi
}

require git
require gitleaks
git fsck --no-reflogs --no-progress
git diff --check
git diff --cached --check
gitleaks git --no-banner --redact .
gitleaks dir --no-banner --redact .

# Scan repository text, including untracked work, for likely personal identifiers.
python3 scripts/check_repository_artifacts.py
python3 scripts/scan_pii.py
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_docs.py

if find cmd internal -type f -name '*.go' -print -quit | grep -q .; then
  require go
  require govulncheck
  if [ -n "$(gofmt -l cmd internal prompts schemas diagnostics)" ]; then
    echo 'Go files need gofmt' >&2
    exit 1
  fi
  go test ./...
  go vet ./...
  govulncheck ./...
fi

# Worker B has its own Go toolchain/dependency graph; keep inference worker pins intact.
if [ -f worker-b/go.mod ]; then
  if [ -n "$(gofmt -l worker-b)" ]; then
    echo 'Worker B files need gofmt' >&2
    exit 1
  fi
  go -C worker-b test ./...
  go -C worker-b vet ./...
  (cd worker-b && govulncheck ./...)
fi

if find worker-a -type f -name '*.py' -print -quit | grep -q .; then
  require ruff
  ruff check worker-a scripts diagnostics
  ruff format --check worker-a scripts diagnostics
  if [ -d worker-a/tests ]; then
    if [ -x .venv/bin/pytest ]; then
      .venv/bin/pytest worker-a/tests
    elif command -v pytest >/dev/null 2>&1; then
      pytest worker-a/tests
    else
      echo 'Missing required Python test runner: pytest' >&2
      exit 1
    fi
  fi
fi
if [ -f worker-a/requirements.lock ]; then
  require pip-audit
  pip-audit -r worker-a/requirements.lock
fi

if find terraform -type f -name '*.tf' -print -quit | grep -q .; then
  require terraform
  require tflint
  terraform fmt -check -recursive terraform
  while IFS= read -r directory; do
    terraform -chdir="$directory" init -backend=false -input=false >/dev/null
    terraform -chdir="$directory" validate
    tflint --chdir "$directory"
  done < <(find terraform -type f -name '*.tf' -printf '%h\n' | sort -u)
fi

make test-terraform

echo 'CI checks passed.'
