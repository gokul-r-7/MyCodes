# DevContainers

.PHONY: devcon/post-create
devcon/post-create:
	sudo apt update && sudo apt upgrade -y
	sudo apt install -y bash-completion make
	terraform -install-autocomplete

.PHONY: devcon/post-start
devcon/post-start:
	sudo apt update && devcontainer-info
