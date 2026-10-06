.PHONY: ci test-worker-cpu test-pipeline test-terraform

ci:
	bash scripts/ci.sh

# Self-contained synthetic fixtures; no cloud deployment, model call or email.
test-pipeline:
	go test -race -count=1 ./...
	go -C worker-b test -race -count=1 ./...

test-worker-cpu:
	.venv/bin/python -m pytest worker-a/tests

test-terraform:
	terraform -chdir=terraform/components/application init -backend=false -input=false
	terraform -chdir=terraform/components/application test -filter=tests/powerbi.tftest.hcl

# Run after identity/project confirmation. Saved plans may contain private inputs.
TF_PLAN_DIR ?= $(CURDIR)/results/terraform-full
TF_FOUNDATION_PROFILE ?=
TF_APPLICATION_PROFILE ?=
.PHONY: terraform-plan-full terraform-review-full
terraform-plan-full:
	@test -n "$(TF_FOUNDATION_PROFILE)" -a -f "$(TF_FOUNDATION_PROFILE)" || { echo 'Supply TF_FOUNDATION_PROFILE with a private deployment file.' >&2; exit 1; }
	@test -n "$(TF_APPLICATION_PROFILE)" -a -f "$(TF_APPLICATION_PROFILE)" || { echo 'Supply TF_APPLICATION_PROFILE with a private deployment file.' >&2; exit 1; }
	@case "$(TF_FOUNDATION_PROFILE)|$(TF_APPLICATION_PROFILE)" in *.example\|*|*\|*.example) echo 'Example profiles cannot be used for deployment planning.' >&2; exit 1;; esac
	@umask 077; mkdir -p "$(TF_PLAN_DIR)"
	chmod 700 "$(TF_PLAN_DIR)"
	umask 077; terraform -chdir=terraform/components/foundation plan -input=false -var-file="$(TF_FOUNDATION_PROFILE)" -out="$(TF_PLAN_DIR)/foundation.tfplan" > "$(TF_PLAN_DIR)/foundation.txt"
	umask 077; terraform -chdir=terraform/components/foundation show -json "$(TF_PLAN_DIR)/foundation.tfplan" > "$(TF_PLAN_DIR)/foundation.json"
	umask 077; terraform -chdir=terraform/components/application plan -input=false -var-file="$(TF_APPLICATION_PROFILE)" -out="$(TF_PLAN_DIR)/application.tfplan" > "$(TF_PLAN_DIR)/application.txt"
	umask 077; terraform -chdir=terraform/components/application show -json "$(TF_PLAN_DIR)/application.tfplan" > "$(TF_PLAN_DIR)/application.json"
	$(MAKE) terraform-review-full

terraform-review-full:
	go run ./cmd/terraform-review -full -directory="$(TF_PLAN_DIR)"
