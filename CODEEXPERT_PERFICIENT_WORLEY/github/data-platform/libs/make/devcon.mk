# DevContainers

.PHONY: devcon/post-create
devcon/post-create:
	sudo apt update && sudo apt upgrade -y
	sudo apt install -y bash-completion make xsel
	terraform -install-autocomplete

.PHONY: devcon/post-start
devcon/post-start:
	sudo apt update && devcontainer-info
	mkdir -p "$(HOME)/.aws"
	cp -f "$(WORKSPACE)/.devcontainer/aws/config" "$(HOME)/.aws/config"
	chmod 400 "$(HOME)/.aws/config"
	chown -R "$(USER):$(USER)" "$(HOME)/.aws"
	mkdir -p "$(HOME)/.local/bin"
	cp -f "$(WORKSPACE)/.devcontainer/aws/aws-sso-login.sh" "$(HOME)/.local/bin/aws-sso-login"
	chmod 755 -R "$(HOME)/.local/bin"
