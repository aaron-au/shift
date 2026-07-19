#!/bin/bash
# Build connector plugins as .so files for the runner using Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNNER_DIR="$PROJECT_ROOT/runner"
PLUGINS_DIR="$RUNNER_DIR/plugins"
OUTPUT_DIR="$PROJECT_ROOT/hub/connectors"

echo "Building connector plugins using Docker..."

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Use Docker to build plugins (CGO cross-compilation is problematic)
# Build in a container that matches the runner's environment
# Use linux/amd64 platform for compatibility with Docker containers
# Build plugins with exact same settings as runner (CGO_ENABLED=1, -tags sqlite3)
# This is critical - plugins must match the runner's build configuration
docker run --rm --platform linux/amd64 \
  -v "$PROJECT_ROOT:/workspace" \
  -w /workspace/runner/plugins/http \
  golang:1.25-alpine3.22 \
  sh -c "apk add --no-cache gcc musl-dev sqlite-dev binutils binutils-gold && \
         cd /workspace/runner && \
         CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin -tags sqlite3 -o /workspace/hub/connectors/http-1.0.0-linux.so ./plugins/http"

docker run --rm --platform linux/amd64 \
  -v "$PROJECT_ROOT:/workspace" \
  -w /workspace/runner/plugins/sleep \
  golang:1.25-alpine3.22 \
  sh -c "apk add --no-cache gcc musl-dev sqlite-dev binutils binutils-gold && \
         cd /workspace/runner && \
         CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -buildmode=plugin -tags sqlite3 -o /workspace/hub/connectors/sleep-1.0.0-linux.so ./plugins/sleep"

echo "Connector plugins built successfully!"
echo "Output directory: $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"

