.PHONY: setup build test bench bench-report cover cover-bump fmt fmt-check vet lint vuln leaks check tidy tidy-check clean fuzz

MODULES := engine sdk pkg runner hub connectors
VERSION ?= dev
LDFLAGS := -s -w -X github.com/aaron-au/shift/pkg/buildinfo.Version=$(VERSION)

## setup: one-time clone setup (enables the pre-push security gate)
setup:
	git config core.hooksPath .githooks
	@echo "pre-push gate enabled (.githooks). Run 'make check' to test it."

## build: compile all binaries into bin/
build:
	@mkdir -p bin
	cd runner && go build -ldflags="$(LDFLAGS)" -o ../bin/runnerd ./cmd/runnerd
	cd hub && go build -ldflags="$(LDFLAGS)" -o ../bin/hubd ./cmd/hubd
	cd connectors && go build -ldflags="$(LDFLAGS)" -o ../bin/shift-connector-gen ./cmd/shift-connector-gen
	cd connectors && go build -ldflags="$(LDFLAGS)" -o ../bin/shift-connector-http ./cmd/shift-connector-http
	cd connectors && go build -ldflags="$(LDFLAGS)" -o ../bin/shift-consign ./cmd/shift-consign
	cd hub && go build -ldflags="$(LDFLAGS)" -o ../bin/shift-bootstrap ./cmd/shift-bootstrap

## proto: regenerate gRPC code from proto/ (requires protoc + Go plugins)
proto:
	protoc --go_out=. --go_opt=module=github.com/aaron-au/shift \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/aaron-au/shift \
	       proto/connector/v1/connector.proto

## test: run all tests with the race detector (always on — ADR-0006).
## -shuffle=on surfaces order-dependent / shared-state tests; -count=1 defeats
## the test cache so a gate never trusts a stale pass.
test:
	@for m in $(MODULES); do echo "--- test $$m"; (cd $$m && go test -race -shuffle=on -count=1 ./...) || exit 1; done

## cover: per-package coverage gate (coverage.thresholds) + coverage/ artifacts
## (coverage.html browsable, coverage.md job summary, coverage.json badge). Runs
## full -race but sets SHIFT_COVERAGE=1 so the timing-flaky connector-subprocess
## + e2e tests skip — deterministic coverage. Those run in `make test`.
cover:
	./scripts/coverage.sh

## cover-bump: ratchet coverage.thresholds up to achieved-minus-epsilon (floors
## only rise). Run after adding tests; review the diff; commit the thresholds.
cover-bump:
	./scripts/cover-bump.sh

## fuzz: mutation-fuzz the untrusted-input parsers/verifiers (ADR-0022).
## The seed corpus already runs under `make test`; this is the discovery
## pass. FUZZTIME overridable (default 30s per target).
FUZZTIME ?= 30s
fuzz:
	@echo "--- fuzz flowdoc.Parse";   cd pkg    && go test ./flowdoc/       -run='^$$' -fuzz='^FuzzParse$$'  -fuzztime=$(FUZZTIME)
	@echo "--- fuzz consign.Verify";  cd pkg    && go test ./consign/       -run='^$$' -fuzz='^FuzzVerify$$' -fuzztime=$(FUZZTIME)
	@echo "--- fuzz ndjson.Reader";   cd engine && go test ./format/ndjson/ -run='^$$' -fuzz='^FuzzReader$$' -fuzztime=$(FUZZTIME)
	@echo "--- fuzz spill.Decode";    cd engine && go test ./spill/         -run='^$$' -fuzz='^FuzzDecode$$' -fuzztime=$(FUZZTIME)

## bench: micro-benchmarks + shift-bench RSS regression checks (ADR-0003)
bench:
	@for m in $(MODULES); do echo "--- bench $$m"; (cd $$m && go test -bench=. -benchmem -run='^$$' ./...) || exit 1; done
	@mkdir -p bin && cd engine && go build -o ../bin/shift-bench ./cmd/shift-bench
	@echo "--- shift-bench transform (RSS must stay bounded)"
	bin/shift-bench -scenario transform -bytes 64MiB -max-rss 100MiB
	@echo "--- shift-bench aggregate with spill"
	bin/shift-bench -scenario aggregate -bytes 64MiB -watermark 8MiB -groups 100000 -max-rss 120MiB
	@echo "--- connector transport parity (ADR-0007)"
	@cd connectors && go build -o ../bin/shift-connector-gen ./cmd/shift-connector-gen && go build -o ../bin/shift-bench-remote ./cmd/shift-bench-remote
	bin/shift-bench-remote -records 500000 -connector bin/shift-connector-gen -max-ratio 3.0

## bench-report: run the shift-bench scenario matrix and render the visible
## results table (docs/bench-M7/results.md). RSS ceilings stay hard gates.
bench-report:
	./scripts/bench.sh

fmt:
	@for m in $(MODULES); do (cd $$m && gofmt -w .); done

fmt-check:
	@out=$$(for m in $(MODULES); do (cd $$m && gofmt -l .); done); \
	if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	@for m in $(MODULES); do echo "--- vet $$m"; (cd $$m && go vet ./...) || exit 1; done

## lint: staticcheck, gosec, errcheck and friends via golangci-lint (.golangci.yml)
lint:
	@for m in $(MODULES); do echo "--- lint $$m"; (cd $$m && golangci-lint run ./...) || exit 1; done

## vuln: known-CVE scan with reachability analysis
vuln:
	@for m in $(MODULES); do echo "--- govulncheck $$m"; (cd $$m && govulncheck ./...) || exit 1; done

## leaks: committed-secret scan (whole repo)
leaks:
	gitleaks git --no-banner --redact . 2>/dev/null || gitleaks detect --no-banner --redact -s .

## check: THE gate (ADR-0006) — identical locally, pre-push, and in CI.
## `test` = full `-race` suite incl. subprocess integration tests (behavior);
## `cover` = deterministic per-package coverage gate (SHIFT_COVERAGE).
## Supply-chain scans that cannot run in pre-push (image/Dockerfile/SAST) live
## in the separate release/scheduled tier (.github/workflows/supply-chain.yml),
## an explicit ADR-0006 extension — not smuggled in as CI-only correctness.
check: fmt-check tidy-check vet lint vuln leaks test cover
	@echo "check: all gates green"

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy); done

## tidy-check: prove go.mod/go.sum are already tidy (part of the gate). Runs
## `go mod tidy` per module then fails on any diff — on a clean CI checkout the
## mutation is discarded; locally the diff IS the fix, ready to commit.
tidy-check:
	@for m in $(MODULES); do echo "--- tidy-check $$m"; (cd $$m && go mod tidy) || exit 1; done
	@git diff --exit-code -- $(foreach m,$(MODULES),$(m)/go.mod $(m)/go.sum) \
	  || { echo "go.mod/go.sum not tidy — commit the changes shown above"; exit 1; }

## images: OCI images for the compose bundle (hubd, runnerd, tools)
images:
	docker build -f deploy/docker/Dockerfile --build-arg VERSION=$(VERSION) --target hubd    -t shift/hubd:$(VERSION) .
	docker build -f deploy/docker/Dockerfile --build-arg VERSION=$(VERSION) --target runnerd -t shift/runnerd:$(VERSION) .
	docker build -f deploy/docker/Dockerfile --build-arg VERSION=$(VERSION) --target tools   -t shift/tools:$(VERSION) .

## up: the "just runs" bundle (M4b exit criterion) — see deploy/README.md
up: images
	VERSION=$(VERSION) docker compose -f deploy/compose.yml up

clean:
	rm -rf bin
