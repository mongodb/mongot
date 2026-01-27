#!/bin/bash
# Stop MongoDB Community Search Docker development environment
# This script stops and removes containers from both local and latest modes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/community-quick-start"

echo "MongoDB Community Search - Stopping Docker Services"
echo "=========================================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Nothing to stop."
    exit 0
fi

# Change to compose directory
cd "$COMPOSE_DIR"

# Check if any containers are running
if docker compose ps --quiet 2>/dev/null | grep -q .; then
    echo "Stopping Docker Compose services..."
    echo ""

    # Stop both profiles to handle both local and latest modes
    # This ensures we stop containers regardless of which mode was used
    docker compose --profile local down 2>/dev/null || true
    docker compose down 2>/dev/null || true

    echo ""
    echo -e "Services stopped successfully!"
else
    echo -e "No running containers found."
fi

# Clean up any temporary build artifacts
echo ""
echo "Cleaning up temporary build artifacts..."
rm -f "$COMPOSE_DIR/mongot-community-local-docker.tar"

echo ""
echo "Cleanup complete!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Next Steps:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Start services:          make docker.up"
echo "  Clear all data volumes:  make docker.clear"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

