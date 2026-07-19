.PHONY: build build-hub build-runner clean test run-hub run-runner docker-build docker-up docker-down

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Directories
HUB_DIR=hub
RUNNER_DIR=runner

# Build flags
BUILD_FLAGS=-ldflags="-s -w"

# Build all services
build: build-hub build-runner

# Build Hub services
build-hub:
	@echo "Building Hub services..."
	cd $(HUB_DIR) && $(GOBUILD) $(BUILD_FLAGS) -o bin/account-service ./cmd/account-service
	cd $(HUB_DIR) && $(GOBUILD) $(BUILD_FLAGS) -o bin/runner-service ./cmd/runner-service
	cd $(HUB_DIR) && $(GOBUILD) $(BUILD_FLAGS) -o bin/billing-service ./cmd/billing-service
	cd $(HUB_DIR) && $(GOBUILD) $(BUILD_FLAGS) -o bin/api-gateway ./cmd/api-gateway
	@echo "Hub services built successfully"

# Build Runner
build-runner:
	@echo "Building Runner..."
	cd $(RUNNER_DIR) && $(GOBUILD) $(BUILD_FLAGS) -o bin/runner ./cmd/runner
	@echo "Runner built successfully"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(HUB_DIR)/bin
	rm -rf $(RUNNER_DIR)/bin
	rm -f $(HUB_DIR)/account-service $(HUB_DIR)/runner-service $(HUB_DIR)/billing-service $(HUB_DIR)/api-gateway
	rm -f $(RUNNER_DIR)/runner
	@echo "Clean complete"

# Run tests
test:
	@echo "Running tests..."
	cd $(HUB_DIR) && $(GOTEST) ./...
	cd $(RUNNER_DIR) && $(GOTEST) ./...

# Run Hub services locally (requires PostgreSQL)
run-hub:
	@echo "Starting Hub services..."
	@echo "Make sure PostgreSQL is running and configured"

# Run Runner locally
run-runner:
	@echo "Starting Runner..."
	cd $(RUNNER_DIR) && ./bin/runner

# Docker commands
docker-build:
	@echo "Building Docker images..."
	docker-compose build

docker-up:
	@echo "Starting Docker containers..."
	docker-compose up -d

docker-down:
	@echo "Stopping Docker containers..."
	docker-compose down

docker-logs:
	docker-compose logs -f

# Development helpers
tidy:
	cd $(HUB_DIR) && $(GOMOD) tidy
	cd $(RUNNER_DIR) && $(GOMOD) tidy

fmt:
	$(GOCMD) fmt ./...

vet:
	$(GOCMD) vet ./...

