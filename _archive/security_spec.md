# SHIFT: Security Specification

## 1. Authentication

*   **Hub UI:** OAuth 2.0 / OIDC only. No password-based logins.
*   **Hub APIs:** Require a valid session JWT obtained via OIDC flow.
*   **Runner Registration:** Runners authenticate to the Hub using a provisioned, single-use registration token, then receive a long-lived API key/secret.
*   **Runner P2P:** Mutual authentication via a shared symmetric key or SSH-style keypair challenge-response over a TLS-encrypted WebSocket.
*   **Custom APIs:** Support Basic Auth, mTLS, and OIDC Bearer Token validation.

## 2. Encryption

*   **In Transit:** All communication (Hub-Runner, Runner-Runner, external APIs) MUST use TLS 1.2+.
*   **At Rest:** Sensitive data in the Runner's SQLite database (e.g., credentials, API keys) MUST be encrypted using AES-256-GCM with a key managed by the runner.

## 3. Authorization

*   **Hub:** Implements RBAC. Users can only see/manage runners and flows associated with their account.
*   **Custom APIs:** Each API endpoint definition includes an authorization policy (e.g., "requires 'admin' role", "requires scope 'read:invoices'").