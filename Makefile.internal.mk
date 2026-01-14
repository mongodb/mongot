# Additional community specific make targets

#######################################
# Docker development environment rules
#######################################

# MODE can be 'local' (default) or 'latest'
# Examples:
#   make docker.up              # Use local build (default)
#   make docker.up MODE=local   # Use local build (explicit)
#   make docker.up MODE=latest  # Use Docker Hub image
MODE ?= local

.PHONY: docker.up
docker.up:
	@$(DIR)/community-quick-start/docker_up.sh $(MODE)

.PHONY: docker.down
docker.down:
	@$(DIR)/community-quick-start/docker_down.sh
