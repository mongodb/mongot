#!/bin/bash
# Start MongoDB Community Search Docker development environment
# This script supports two modes:
#   1. Local mode (default): Builds and uses locally-built mongot image
#   2. Latest mode: Uses pre-built image from Docker Hub
#
# Usage:
#   ./docker-up.sh          # Use local build (default)
#   ./docker-up.sh local    # Use local build (explicit)
#   ./docker-up.sh latest   # Use Docker Hub image

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/community-quick-start"

# Parse mode argument (default to local)
MODE="${1:-local}"

if [ "$MODE" != "local" ] && [ "$MODE" != "latest" ]; then
    echo "Error: Invalid mode '$MODE'. Use 'local' or 'latest'."
    echo ""
    echo "Usage:"
    echo "  $0          # Use local build (default)"
    echo "  $0 local    # Use local build (explicit)"
    echo "  $0 latest   # Use Docker Hub image"
    exit 1
fi

echo "MongoDB Community Search - Docker Development Environment"
echo "=========================================================="
echo ""

if [ "$MODE" = "local" ]; then
    echo "Mode: Local Development Build"
    echo "Building mongot from local source code"
else
    echo "Mode: Docker Hub Latest"
    echo "Using pre-built image from Docker Hub"
fi
echo ""

# Detect platform (only needed for local builds)
PLATFORM=""
if [ "$MODE" = "local" ]; then
    ARCH=$(uname -m)
    OS=$(uname -s)

    if [ "$OS" = "Darwin" ]; then
        if [ "$ARCH" = "arm64" ]; then
            PLATFORM="arm64"
            echo "Detected platform: macOS Apple Silicon (ARM64)"
        else
            PLATFORM="amd64"
            echo "Detected platform: macOS Intel (AMD64)"
        fi
    elif [ "$OS" = "Linux" ]; then
        if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
            PLATFORM="arm64"
            echo "Detected platform: Linux ARM64"
        else
            PLATFORM="amd64"
            echo "Detected platform: Linux AMD64"
        fi
    else
        echo "Unknown platform, defaulting to AMD64"
        PLATFORM="amd64"
    fi
    echo ""
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Create the search-community network if it doesn't exist
echo "Checking Docker network..."
if ! docker network inspect search-community > /dev/null 2>&1; then
    echo "Creating search-community network..."
    docker network create search-community
    echo "Network created successfully!"
else
    echo "Network search-community already exists."
fi
echo ""

# Set correct permissions on pwfile
echo "Setting permissions on password file..."
chmod 400 "$COMPOSE_DIR/pwfile"
echo "Password file permissions set to read-only."
echo ""

# Build the Docker image (only for local mode)
if [ "$MODE" = "local" ]; then
    echo "Building mongot-community Docker image..."
    echo ""
    cd "$REPO_ROOT"

    # Set platform-specific variables
    if [ "$PLATFORM" = "arm64" ]; then
        BAZEL_PLATFORM="linux_aarch64"
        DOCKER_PLATFORM="linux/arm64"
    else
        BAZEL_PLATFORM="linux_x86_64"
        DOCKER_PLATFORM="linux/amd64"
    fi

    # Define BAZEL command
    BAZEL="$REPO_ROOT/scripts/tools/bazelisk/run.sh"

    # Step 1: Build the mongot-community tarball with Bazel
    echo "Building mongot-community tarball with Bazel..."
    if ! "$BAZEL" build --platforms="//bazel/platforms:$BAZEL_PLATFORM" //deploy:mongot-community-local-docker; then
        echo ""
        echo "Error: Bazel build failed"
        exit 1
    fi

    # Step 2: Copy the tarball to community-quick-start directory
    echo "Copying tarball to community-quick-start..."
    BAZEL_BIN=$("$BAZEL" info bazel-bin)
    cp -f "$BAZEL_BIN/deploy/mongot-community-local-docker.tar" "$COMPOSE_DIR/"

    # Step 3: Build Docker image
    echo "Building Docker image..."
    if ! docker build \
        -f "$COMPOSE_DIR/Dockerfile.local" \
        -t mongot-community-local:latest \
        --platform "$DOCKER_PLATFORM" \
        "$COMPOSE_DIR/"; then
        echo ""
        echo "Error: Docker build failed"
        rm -f "$COMPOSE_DIR/mongot-community-local-docker.tar"
        exit 1
    fi

    # Step 4: Clean up tarball
    echo "Cleaning up..."
    rm -f "$COMPOSE_DIR/mongot-community-local-docker.tar"

    echo ""
    echo "Build completed successfully!"
    echo ""
else
    echo "Pulling latest image from Docker Hub..."
    echo ""
    docker pull mongodb/mongodb-community-search:latest
    if [ $? -ne 0 ]; then
        echo ""
        echo "Warning: Failed to pull latest image. Will use cached version if available."
    fi
    echo ""
fi

echo "Generating TLS certificates..."
echo ""

# Create TLS directory if it doesn't exist
TLS_DIR="$COMPOSE_DIR/tls"
mkdir -p "$TLS_DIR"

# Generate CA certificate if it doesn't exist
if [ ! -f "$TLS_DIR/ca.pem" ]; then
    echo "Generating CA certificate..."
    openssl req -x509 -nodes -sha256 -days 3650 -newkey rsa:4096 \
        -keyout "$TLS_DIR/ca-key.pem" \
        -out "$TLS_DIR/ca.pem" \
        -subj "/CN=ca.mongodb-dev.local"
fi

generate_combined_cert() {
    local service=$1
    local cn=$2

    echo "Generating $service certificate..."

    openssl req -newkey rsa:4096 -keyout "$TLS_DIR/$service-key.pem" -nodes \
        -out "$TLS_DIR/$service.csr" \
        -subj "/CN=$cn"

    openssl req -x509 -nodes \
        -CA "$TLS_DIR/ca.pem" \
        -CAkey "$TLS_DIR/ca-key.pem" \
        -in "$TLS_DIR/$service.csr" \
        -out "$TLS_DIR/$service.pem" \
        -days 3650 \
        -subj "/CN=$cn" \
        -extensions san \
        -config <(echo '[req]'; \
                  echo 'distinguished_name=req'; \
                  echo '[san]'; \
                  echo "subjectAltName=DNS:localhost,DNS:$cn,IP:127.0.0.1,IP:::1")


    rm -f "$TLS_DIR/$service.csr"

    cat "$TLS_DIR/$service.pem" "$TLS_DIR/$service-key.pem" > "$TLS_DIR/$service-combined.pem"
    echo ""
}

# Generate service certificates if they don't exist
[ ! -f "$TLS_DIR/mongod-combined.pem" ] && generate_combined_cert "mongod" "mongod.search-community"
[ ! -f "$TLS_DIR/mongot-combined.pem" ] && generate_combined_cert "mongot" "mongot.search-community"

# Generate client certificate for mongosh/mongorestore connections
[ ! -f "$TLS_DIR/client-combined.pem" ] && generate_combined_cert "client" "mongodb-client"

echo "TLS certificates ready!"
echo ""

echo "Starting Docker Compose services..."
echo ""

# Change to compose directory
cd "$COMPOSE_DIR"

# Stop any existing containers first
if [ "$MODE" = "local" ]; then
    docker compose --profile local down 2>/dev/null || true
    docker compose down 2>/dev/null || true  # Also stop default profile
else
    docker compose down 2>/dev/null || true
    docker compose --profile local down 2>/dev/null || true  # Also stop local profile
fi

# Start the services with appropriate profile
if [ "$MODE" = "local" ]; then
    docker compose --profile local up -d
else
    docker compose up -d
fi

if [ $? -ne 0 ]; then
    echo ""
    echo "Error: Failed to start Docker Compose services"
    exit 1
fi

echo ""
echo -e "Services started successfully!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Service Information:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
if [ "$MODE" = "local" ]; then
    echo "  MongoDB Search Mode:     Local Build"
    echo "  Image:                   mongot-community-local:latest"
else
    echo "  MongoDB Search Mode:     Docker Hub Latest"
    echo "  Image:                   mongodb/mongodb-community-search:latest"
fi
echo ""
echo "  MongoDB (mongod):        localhost:27017"
echo "  MongoDB Search (mongot): localhost:27028"
echo "  Metrics endpoint:        http://localhost:9946/metrics"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Useful Commands:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Connect to MongoDB:   make docker.connect"
echo ""
echo "  View mongod logs:     make docker.logs SERVICE=mongod"
if [ "$MODE" = "local" ]; then
    echo "  View mongot logs:     make docker.logs"
    echo "  Switch to latest:     make docker.up MODE=latest"
else
    echo "  View logs:            make docker.logs SERVICE=mongot"
    echo "  Switch to local:      make docker.up MODE=local"
fi
echo ""
echo "  Check status:         make docker.ps"
echo "  Stop services:        make docker.down"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Wait a moment and check if containers are still running
sleep 5
if ! docker compose ps | grep -q "mongot-community"; then
    echo "Warning: mongot container may have crashed. Check logs:"
    if [ "$MODE" = "local" ]; then
        echo "   make docker.logs"
    else
        echo "   make docker.logs SERVICE=mongot"
    fi
    echo ""
fi

if ! docker compose ps | grep -q "mongod"; then
    echo "Warning: mongod container may have crashed. Check logs:"
    echo "   make docker.logs SERVICE=mongod"
    echo ""
fi
