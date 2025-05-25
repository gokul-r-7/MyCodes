
.PHONY: infra/clean
infra/clean: terraform/clean

.PHONY: infra/init
infra/init:
	cd infra/ && terraform init
	
.PHONY: infra/login
infra/login:
	cd infra/ && terraform login

# TODO: include pre-commit/run once the security issues have been resolved
.PHONY: infra/lint
infra/lint:
	cd infra/ && terraform fmt && terraform validate

.PHONY: infra/plan
infra/plan:
	cd infra/ && terraform plan

.PHONY: infra/security-scan
infra/security-scan:
	tfsec ./infra --format csv --out tfsec_report.csv
