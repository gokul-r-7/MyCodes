
.PHONY: infra/clean
infra/clean: terraform/clean

.PHONY: infra/init
infra/init:
	cd infra/ && terraform init

# TODO: include pre-commit/run once the security issues have been resolved
.PHONY: infra/lint
infra/lint:
	cd infra/ && terraform fmt && terraform validate

.PHONY: infra/plan
infra/plan: infra/lint
	cd infra/ && terraform plan -out terraform.plan

.PHONY: infra/apply
infra/apply: infra/lint infra/plan
	cd infra/ && terraform apply "terraform.plan"

.PHONY: infra/destroy
infra/destroy: infra/lint
	cd infra/ && terraform destroy -auto-approve
