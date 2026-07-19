# Build stage
FROM golang:1.25-alpine3.22 AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o runner-service ./cmd/runner-service

# Runtime stage
FROM alpine:3.22

# Install Docker CLI for log streaming
RUN apk --no-cache add ca-certificates tzdata docker-cli

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/runner-service /app/runner-service

# Copy connector plugins (if they exist)
COPY connectors /app/connectors

# Copy UI files
COPY ui /app/ui

EXPOSE 2000 2001

CMD ["/app/runner-service"]

