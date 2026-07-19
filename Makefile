.PHONY: setup build test bench fmt fmt-check vet lint vuln leaks check tidy clean

MODULES := engine sdk pkg runner hub
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

## test: run all tests with the race detector (always on — ADR-0006)
test:
	@for m in $(MODULES); do echo "--- test $$m"; (cd $$m && go test -race ./...) || exit 1; done

## bench: run benchmarks (non-blocking job; perf regressions gate from M1)
bench:
	@for m in $(MODULES); do echo "--- bench $$m"; (cd $$m && go test -bench=. -benchmem -run='^$$' ./...) || exit 1; done

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

## check: THE gate (ADR-0006) — identical locally, pre-push, and in CI
check: fmt-check vet lint vuln leaks test
	@echo "check: all gates green"

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy); done

clean:
	rm -rf bin
