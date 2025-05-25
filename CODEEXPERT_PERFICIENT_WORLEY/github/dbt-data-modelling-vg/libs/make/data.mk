#!/usr/bin/env make

SHELL := /bin/bash

# Targets

.PHONY: data/check
## Check required bins
data/check: data/_header
	$(foreach bin,$(REQUIRED_BINS),\
		$(if $(shell command -v $(bin) 2> /dev/null),$(info ✅ Found $(bin).),$(error ❌ Please install $(bin).)))

.PHONY: data/check-lint
## Checks Formatting and Linting
data/check-lint:
	@ black . --check  && ruff check .

.PHONY: data/format-lint
## Runs Code Formatter & Linter
data/format-lint:
	@ black . && ruff . --fix

.PHONY: data/_header
## Create header
data/_header:
	@ printf "${GREEN}$$HEADER\n${RESET}"
