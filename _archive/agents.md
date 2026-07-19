# Cursor Agent Instructions: Project SHIFT

## Core Mission & Philosophy

Your primary goal is to build **SHIFT**, a resilient, secure, and performant Hub-and-Spoke Integration Platform as a Service (iPaaS). The target audience is small to medium businesses, with a focus on ease of use, no-code capabilities, and extreme reliability.

The core philosophy is **"Encrypted by Default"** and **"Resilient by Design"**. Every component must be built with the assumption that network connections can fail and that security is non-negotiable. The user should always be in control of their data and deployments.

## Key Architectural Pillars (Non-Negotiable)

1.  **Hub-and-Spoke Model:** The architecture is strictly divided.
    *   **The Hub:** A global, multi-tenant, cloud-native application (Kubernetes) acting as the central management and orchestration plane. It does **not** execute customer integration flows.
    *   **The Spoke (Runner):** A lightweight, self-contained, deployable Go application that executes integration flows. It must be capable of offline operation and horizontal clustering.

2.  **Go (Golang) as the Primary Language:** All backend services for both the Hub and the Runner will be written in Go. Adhere to idiomatic Go practices, including effective use of goroutines, channels, and strong error handling.

3.  **RPC-based External Plugins for Connectors:** Connectors are **not** compiled into the runner. They are separate Go executables that the runner manages and communicates with via a lightweight RPC mechanism. This ensures fault isolation and allows for dynamic, independent updates.

4.  **SQLite for Runner State:** Each runner uses its own embedded SQLite database for all local state, including flow definitions, configurations, license details, and the distributed task queue. This is critical for offline capabilities and avoids shared storage dependencies.

5.  **Secure WebSocket Communication:**
    *   **Hub-to-Runner:** Long-lived, secure WebSockets are used for command, control, configuration deployment, and telemetry reporting.
    *   **Runner-to-Runner:** Secure WebSockets are used for peer-to-peer communication within a cluster for task distribution, heartbeats, and status sharing.

## Agent Behaviors & Best Practices

*   **Security First:**
    *   All communication must be encrypted (TLS/SSH-style keys).
    *   Runner-exposed APIs are HTTPS only.
    *   Validate all inputs. Sanitize outputs.
    *   Hub authentication is OAuth/OIDC only.
    *   Sensitive data in SQLite should be encrypted.

*   **Modularity and Microservices:**
    *   The Hub should be composed of distinct microservices (e.g., Accounts, Billing, Runner Management).
    *   The Runner itself should be modular, with clear separation between the core, the API gateway, the P2P layer, and the execution engine.

*   **Avoid Disk I/O Bottlenecks:**
    *   Do not create many small files for logging or data. Use structured logging to a single stream and use SQLite for all operational data.
    *   Process data in-memory wherever feasible.

*   **Embrace Asynchronicity:** Use Go's concurrency primitives and message queues (where appropriate) to build non-blocking, responsive systems.

*   **Test-Driven Development (TDD):** Prioritize writing robust unit and integration tests for all components. Pay special attention to testing the distributed task queue's race conditions and the security middleware.

*   **Clear Documentation:** Generate clear, concise comments and docstrings. All public functions and complex logic should be well-documented.

## Key Libraries & Technologies to Use

*   **WebSockets:** `github.com/gorilla/websocket`
*   **Database (Runner):** `github.com/mattn/go-sqlite3`
*   **Database (Hub):** `github.com/lib/pq` or `github.com/jackc/pgx` for PostgreSQL
*   **HTTP/Web Framework:** Go's standard `net/http` library. Avoid heavy frameworks.
*   **Scheduling:** `github.com/robfig/cron/v3`
*   **Let's Encrypt:** `golang.org/x/crypto/acme/autocert`
*   **SSH/Crypto:** `golang.org/x/crypto/ssh` and its sub-packages for P2P security.
*   **OAuth/OIDC:** `golang.org/x/oauth2`, `github.com/coreos/go-oidc/v3`
*   **OpenAPI Parsing:** `github.com/go-openapi/loads`

## Container Standards

*   **Docker Base Images:** All Go-based Dockerfiles must use official Go Docker images based on Alpine 3.22:
    *   **Build stage:** `FROM golang:<version>-alpine3.22` (e.g., `golang:1.23-alpine3.22` or latest available)
    *   **Runtime stage:** `FROM alpine:3.22` (for multi-stage builds)
    *   **Never use:** `golang:latest`, `golang:alpine`, or older Alpine versions
    *   Always use explicit Go version tags compatible with Alpine 3.22
*   **Version Tagging:** All Docker images must use explicit version tags. Never use `latest` tags in production builds.
*   This standard applies to all Go-based services: Hub microservices, Runner, and Connectors.

## Prohibited Patterns

*   **No Shared File Systems:** Runners must not rely on NFS, SMB, or any shared directory for clustering or state.
*   **No Unencrypted Communication:** No plaintext HTTP or WebSocket traffic is permitted between any components.
*   **No Monolithic Designs:** Adhere to the microservice and modular runner architecture.