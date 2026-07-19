# Build stage
FROM golang:1.25-alpine3.22 AS builder

# Install build dependencies for CGO (required for SQLite and plugins)
# Note: binutils-gold provides the ld linker needed for plugin builds
RUN apk add --no-cache gcc musl-dev sqlite-dev binutils binutils-gold

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application (CGO required for SQLite)
# Note: Building for native architecture (CGO doesn't cross-compile easily)
RUN CGO_ENABLED=1 go build -tags sqlite3 -o runner ./cmd/runner

# Runtime stage
FROM alpine:3.22

# Install SQLite libraries required for CGO
RUN apk --no-cache add ca-certificates tzdata sqlite

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/runner /app/runner

# Copy UI files
COPY ui /app/ui

EXPOSE 8000 8001

CMD ["/app/runner"]

