#!/usr/bin/env make

.DEFAULT_GOAL := help

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RED    := $(shell tput -Txterm setaf 1)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

TARGET_MAX_CHAR_NUM := 20

# Define header
define HEADER
____    __    ____  ______   .______       __       ___________    ____
\   \  /  \  /   / /  __  \  |   _  \     |  |     |   ____\   \  /   /
 \   \/    \/   / |  |  |  | |  |_)  |    |  |     |  |__   \   \/   /
  \            /  |  |  |  | |      /     |  |     |   __|   \_    _/
   \    /\    /   |  `--'  | |  |\  \----.|  `----.|  |____    |  |
    \__/  \__/     \______/  | _| `._____||_______||_______|   |__|
endef
export HEADER

# Required tools
REQUIRED_BINS := make aws
EVAL := FALSE
