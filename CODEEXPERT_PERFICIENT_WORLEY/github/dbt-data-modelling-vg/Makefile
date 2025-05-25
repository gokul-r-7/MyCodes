export WORKSPACE=$(shell pwd)
export HABITS = $(WORKSPACE)/habits

-include $(WORKSPACE)/.env

include $(WORKSPACE)/libs/make/*.mk

include $(HABITS)/lib/make/*/*.mk
include $(HABITS)/lib/make/*.mk
