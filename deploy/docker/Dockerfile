# SHIFT bundle images (M4b "just runs"). One multi-stage file, three
# targets: hubd, runnerd, tools (bootstrap + connectors + consign).
#   docker build -f deploy/docker/Dockerfile --target hubd -t shift/hubd .
FROM golang:1.26-alpine AS build
WORKDIR /src
# The whole workspace: go.work resolves cross-module replaces.
COPY . .
ARG VERSION=dev
ENV CGO_ENABLED=0
RUN go build -ldflags "-X github.com/aaron-au/shift/pkg/buildinfo.Version=${VERSION}" \
      -o /out/hubd            github.com/aaron-au/shift/hub/cmd/hubd && \
    go build -ldflags "-X github.com/aaron-au/shift/pkg/buildinfo.Version=${VERSION}" \
      -o /out/runnerd         github.com/aaron-au/shift/runner/cmd/runnerd && \
    go build -o /out/shift-bootstrap     github.com/aaron-au/shift/hub/cmd/shift-bootstrap && \
    go build -o /out/shift-consign       github.com/aaron-au/shift/connectors/cmd/shift-consign && \
    go build -o /out/shift-connector-gen  github.com/aaron-au/shift/connectors/cmd/shift-connector-gen && \
    go build -o /out/shift-connector-http github.com/aaron-au/shift/connectors/cmd/shift-connector-http && \
    mkdir /out/empty

FROM gcr.io/distroless/static-debian12:nonroot AS hubd
COPY --from=build /out/hubd /usr/local/bin/hubd
ENTRYPOINT ["/usr/local/bin/hubd"]

# runnerd needs a writable connector cache + scratch; distroless static,
# nonroot (uid 65532). The cache dir ships in the image owned by nonroot
# so the named volume inherits that ownership on first mount.
FROM gcr.io/distroless/static-debian12:nonroot AS runnerd
COPY --from=build /out/runnerd /usr/local/bin/runnerd
COPY --from=build --chown=nonroot:nonroot /out/empty /var/lib/shift
VOLUME ["/var/lib/shift"]
ENTRYPOINT ["/usr/local/bin/runnerd"]

# tools: the bootstrap one-shot plus everything it publishes. Runs as
# the same uid as the distroless services (65532) so the material it
# writes into the bootstrap volume (0600 KEK, certs, tokens) is readable
# by hubd/runnerd and by nothing else.
FROM alpine:3.22 AS tools
COPY --from=build /out/shift-bootstrap /out/shift-consign \
     /out/shift-connector-gen /out/shift-connector-http /usr/local/bin/
RUN mkdir -p /bootstrap && chown 65532:65532 /bootstrap
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/shift-bootstrap"]
