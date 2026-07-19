# Version Requirements

## Go Version
- **Required Go Version**: `1.25`
- **Docker Base Image**: `golang:1.25-alpine3.22`

### Consistency Rules
1. All `go.mod` files MUST specify `go 1.25` (not `1.24.0` or any other version)
2. All Dockerfiles MUST use `golang:1.25-alpine3.22` as the base image
3. When updating Go version, update BOTH:
   - All `go.mod` files (`go 1.XX`)
   - All Dockerfiles (`FROM golang:1.XX-alpine3.22`)

### Current Docker Images
- Hub services: `golang:1.25-alpine3.22`
- Runner: `golang:1.25-alpine3.22`

### Alpine Version
- **Alpine Version**: `3.22`
- This is the latest stable Alpine version as of project start

## Checking Consistency
To verify all versions are consistent:
```bash
# Check go.mod files
grep "^go " */go.mod hub/go.mod runner/go.mod

# Check Dockerfiles
grep "FROM golang" */Dockerfile hub/cmd/*/Dockerfile runner/cmd/*/Dockerfile
```

