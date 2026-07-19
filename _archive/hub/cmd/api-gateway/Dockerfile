# Build stage
FROM golang:1.25-alpine3.22 AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o api-gateway ./cmd/api-gateway

# Runtime stage
FROM alpine:3.22

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/api-gateway /app/api-gateway

EXPOSE 8080

CMD ["/app/api-gateway"]

