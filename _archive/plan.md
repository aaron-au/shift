# Project SHIFT: Implementation Plan

This plan outlines the phased development of the SHIFT iPaaS, a resilient Hub-and-Spoke integration platform built in Go.

### Phase 1: Core Foundation & Hub Services

The goal of this phase is to establish the central Hub, its core services, and the database schema.

1.  **Project Setup & Initial Scaffolding:**
    *   Initialize Go modules for the Hub microservices.
    *   Set up Dockerfiles for each service and a Docker Compose file for local development.
        *   All Dockerfiles must use `golang:<version>-alpine3.22` for build stage and `alpine:3.22` for runtime stage (multi-stage builds).
        *   Use explicit Go version tags (e.g., `golang:1.23-alpine3.22`), never use `latest` tags.
    *   Establish a CI/CD pipeline foundation.

2.  **Hub Database Schema (PostgreSQL):**
    *   Design and create initial schemas for:
        *   `users`, `accounts`, `roles`
        *   `runners`, `runner_groups`
        *   `integration_flows` (with versioning)
        *   `connectors` (catalog and versioning)
        *   `billing_profiles`, `usage_metrics`

3.  **User & Account Service (Go):**
    *   Implement OAuth/OIDC-only login flow using Auth0 (or another provider) as the identity platform.
    *   Create services for managing users, accounts, and role-based access control (RBAC).
    *   Develop a secure session management system (e.g., signed JWTs in cookies).

4.  **Hub API Gateway & Runner Management Service (Go):**
    *   Build the primary API gateway for the Hub UI.
    *   Implement a WebSocket server for Hub-to-Runner communication.
    *   Create services to register new runners, assign them to groups, and track their status (online/offline).

### Phase 2: The Standalone Runner

This phase focuses on building a functional, single-node runner capable of executing flows.

1.  **Runner Core Application (Go):**
    *   Create a Go application for the runner with a clear startup and shutdown lifecycle.
    *   Implement structured logging and configuration management (e.g., from environment variables or a config file).

2.  **SQLite Database Layer:**
    *   Integrate `go-sqlite3`.
    *   Define schemas and repositories for local storage of:
        *   `flows`, `configurations`, `secrets`
        *   `license_info`
        *   `local_usage_cache`
        *   `task_queue`

3.  **Hub WebSocket Client:**
    *   Implement a resilient WebSocket client to connect to the Hub.
    *   Handle registration, configuration syncing (pulling flows from the Hub), and telemetry reporting (pushing usage).
    *   Implement automatic reconnect and message buffering for offline operation.

4.  **RPC Plugin System for Connectors:**
    *   Define the standard Go interface that all connector plugins must implement (e.g., `Connect`, `Execute`, `Shutdown`).
    *   Implement the runner-side logic to manage the lifecycle of these external connector processes.

### Phase 3: Runner Clustering (Peer-to-Peer)

This phase introduces horizontal scaling and high availability for runners.

1.  **P2P WebSocket Layer:**
    *   Implement both WebSocket server and client logic within each runner for peer communication.
    *   Develop a secure handshake protocol using SSH-style keypairs provisioned by the Hub. Encrypt all P2P traffic.

2.  **Distributed Task Queue using SQLite:**
    *   Implement the task queue logic on top of the local SQLite database.
    *   Crucially, implement the **atomic task claiming mechanism** using a transactional `UPDATE ... WHERE status = 'PENDING'` statement to eliminate race conditions.
    *   Implement logic for broadcasting task notifications to peers.

3.  **Peer Status & Failure Detection:**
    *   Implement a heartbeat mechanism over the P2P WebSockets.
    *   Develop logic to detect failed peers and requeue their "stale" claimed tasks after a timeout.

### Phase 4: Integration Execution & Custom APIs

This phase enables the core functionality of running integrations and exposing custom endpoints.

1.  **Runner Integration Execution Engine:**
    *   Build the engine that processes the steps of a deployed integration flow.
    *   It should pull tasks from the distributed queue and use the RPC plugin system to interact with connectors.

2.  **Runner API Gateway:**
    *   Implement a configurable `net/http` server within the runner.
    *   Dynamically register API endpoints based on `APIDefinition`s deployed from the Hub.
    *   Handle request parsing and response generation.

3.  **Security Middleware for Custom APIs:**
    *   Implement a chain of Go middleware for:
        *   Basic Authentication
        *   Client Certificate (mTLS) Authentication
        *   SSO (OAuth2/OIDC Bearer Token) Validation
        *   Role-based authorization checks.

4.  **Certificate Management (Let's Encrypt):**
    *   Integrate `autocert` to automate certificate generation and renewal for customer domains and `*.shift.cloud` subdomains.
    *   Implement logic to allow customers to upload their own custom certificates.
    *   Enforce "Encrypted by Default" (HTTPS only).

### Phase 5: Connectors & Versioning

This phase focuses on building out the connector ecosystem and providing user control.

1.  **Build Initial Connectors:**
    *   **OpenAPI Connector:** The meta-connector to import any spec.
    *   **Core Connectors:** Custom HTTP, Database (PostgreSQL/MySQL), SFTP, Local Disk (self-hosted only).
    *   **Business Connectors:** Xero, QuickBooks Online, Shopify.

2.  **Hub Version Control System:**
    *   Implement a versioned schema and logic in the Hub for all user-created components (flows, APIs).
    *   Expose a "publish" or "deploy" workflow in the Hub UI.

3.  **Connector Catalog & Runner Update Mechanism:**
    *   Build the Hub service to manage the catalog of available connector versions.
    *   Implement the runner logic to check its required connector versions against its local cache and download new versions from the Hub's repository as needed.

### Phase 6: Billing & Telemetry

This phase implements the monetization and resource management aspects.

1.  **Telemetry Collector Service (Hub):**
    *   Create a Hub service to receive, aggregate, and store granular usage metrics from runners.

2.  **Resource Estimation Service (Hub):**
    *   Develop the logic to estimate resource usage (CPU/memory) of flows before execution, based on historical metadata.

3.  **Billing Processor (Hub):**
    *   Implement a scheduled Go service to process aggregated usage data against customer billing profiles (Self-Hosted, Cloud Managed, Hosted Arrears).
    *   Generate billing records.