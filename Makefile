.PHONY: help install-tools-gen install-tools-lint proto mocks gen lint-proto-check fmt vet test test-race test-integration tidy-check lint check docker-build

.DEFAULT_GOAL := help

# Read out of go.mod so the toolchain is pinned in one place.
GEN_TOOLCHAIN := go$(shell sed -n 's/^go //p' go.mod | head -1)
ifeq ($(GEN_TOOLCHAIN),go)
$(error could not read the Go version from go.mod; refusing to run unpinned)
endif
TOOL_PATH := PATH="$(CURDIR)/.bin:$$PATH"

GOLANGCI_LINT_VERSION := v2.14.0
TWF_VERSION := 0.16.0
WORKERS := agent sandboxes infra sidecar

help:
	@awk -F' — ' '/^## /{ sub(/^## /, ""); printf "  make %-22s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

## install-tools-gen — install buf and protoc plugins into .bin
install-tools-gen:
	@mkdir -p .bin
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) GOBIN=$(CURDIR)/.bin go install tool

## install-tools-lint — install golangci-lint into .bin
# Rebuilt when the version or Go changes: a build with older Go refuses the module.
install-tools-lint: install-tools-gen
	@$(CURDIR)/.bin/golangci-lint version 2>/dev/null | grep -qF "version $(GOLANGCI_LINT_VERSION:v%=%) built with $(GEN_TOOLCHAIN) " || \
		GOTOOLCHAIN=$(GEN_TOOLCHAIN) GOBIN=$(CURDIR)/.bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

## proto — regenerate protobuf code
proto: install-tools-gen
	@$(TOOL_PATH) buf generate

## mocks — regenerate mocks
mocks: install-tools-gen
	@GOWORK=off GOTOOLCHAIN=$(GEN_TOOLCHAIN) go tool mockery

## gen — proto + mocks
gen: proto mocks

## lint-proto-check — buf lint and format check
lint-proto-check: install-tools-gen
	@$(TOOL_PATH) buf lint
	@$(TOOL_PATH) buf format -d --exit-code

## fmt — rewrite with gofmt (skip gen/)
fmt:
	@gofmt -w $$(git ls-files '*.go' | grep -v '^gen/')

## vet — go vet, integration-tagged code too
vet:
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) go vet -tags=integration ./...

## test — unit tests
test:
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) go test ./...

## test-race — unit tests under the race detector
test-race:
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) go test -race -count=1 ./...

## test-integration — tests that start their own dependencies in containers, against the sidecar image built here
# Tagged per checkout, so worktrees testing at once never load each other's sidecar.
test-integration: SIDECAR_IMAGE = temporal-agent-sandbox/worker-sidecar:test-$(shell printf %s '$(CURDIR)' | cksum | cut -d' ' -f1)
test-integration:
	@docker build -f cmd/workers/sidecar/Dockerfile -t $(SIDECAR_IMAGE) .
	@SIDECAR_IMAGE=$(SIDECAR_IMAGE) GOTOOLCHAIN=$(GEN_TOOLCHAIN) go test -tags=integration -count=1 -timeout 15m ./...

## tidy-check — go mod tidy leaves go.mod and go.sum unchanged
tidy-check:
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) go mod tidy
	@git diff --exit-code -- go.mod go.sum || (echo 'go.mod dirty; run go mod tidy' && exit 1)

## lint — proto lint, fmt check, vet, golangci-lint
lint: lint-proto-check install-tools-lint
	@test -z "$$(gofmt -l $$(git ls-files '*.go' | grep -v '^gen/') | tee /dev/stderr)"
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) go vet -tags=integration ./...
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) $(TOOL_PATH) golangci-lint run ./...

## check — proto and mock freshness, twf, helm lint, tidy, lint, unit tests
check: lint tidy-check
	@$(TOOL_PATH) buf generate
	@git diff --exit-code -- gen proto || (echo 'proto gen dirty; run make proto' && exit 1)
	@GOWORK=off GOTOOLCHAIN=$(GEN_TOOLCHAIN) go tool mockery
	@git diff --exit-code -- mocks || (echo 'mocks dirty; run make mocks' && exit 1)
	@npx -y @temporal-architect/twf@$(TWF_VERSION) check twf
	@helm lint helm/sandbox --set sandboxId=check --set taskQueue=sandbox-check
	@GOTOOLCHAIN=$(GEN_TOOLCHAIN) go test ./...

## docker-build — build the worker images for this machine
docker-build:
	@for w in $(WORKERS); do docker build -f cmd/workers/$$w/Dockerfile -t temporal-agent-sandbox/worker-$$w:local . || exit 1; done

