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


# Docker logs targeting a specific SERVICE
# Examples:
#   make docker.logs                  # mongot logs when using local build
#   make docker.logs SERVICE=mongod   # mongod logs
#   make docker.logs SERVICE=mongot   # mongot logs when using Docker Hub image
SERVICE ?= mongot-local

.PHONY: docker.logs
docker.logs:
	docker compose --project-directory community-quick-start logs $(SERVICE)

.PHONY: docker.ps
docker.ps:
	docker compose --project-directory community-quick-start ps

# Clear all docker resources including the data volumes
.PHONY: docker.clear
docker.clear:
	docker compose --project-directory community-quick-start down -v && docker network rm search-community

.PHONY: docker.connect
docker.connect:
	mongosh --tls --tlsCAFile ./community-quick-start/tls/ca.pem --tlsCertificateKeyFile ./community-quick-start/tls/client-combined.pem
