SHELL := bash
.DEFAULT_GOAL := help

# Pin these once the team settles on versions.
LEFTHOOK_VERSION ?= latest
GOLANGCI_LINT_VERSION ?= latest

# Every .NET service has its own solution; every Go service has its own module.
DOTNET_SOLUTIONS := $(shell find . \( -name '*.sln' -o -name '*.slnx' \) -not -path '*/node_modules/*')
GO_MODULES := $(shell find . -name go.mod -not -path '*/node_modules/*' -exec dirname {} \;)

.PHONY: help setup prereqs tools hooks fmt lint test

help: ## Show available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

setup: prereqs tools hooks ## One-time developer environment setup

prereqs: ## Verify git, .NET SDK and Go are installed (reports only, never installs)
	@missing=0; \
	for tool in git dotnet go; do \
		if command -v $$tool >/dev/null 2>&1; then echo "ok       $$tool"; \
		else echo "MISSING  $$tool (see docs/get-started.md, Prerequisites)"; missing=1; fi; \
	done; \
	exit $$missing
	@dotnet --version >/dev/null || { echo "The .NET SDK required by global.json is not installed"; exit 1; }

tools: ## Install lefthook and golangci-lint if they are not already on PATH
	@command -v lefthook >/dev/null 2>&1 || { echo "Installing lefthook..."; go install github.com/evilmartians/lefthook@$(LEFTHOOK_VERSION); }
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	@for tool in lefthook golangci-lint; do \
		command -v $$tool >/dev/null 2>&1 || { echo "$$tool was installed but is not on PATH. Add $$(go env GOPATH)/bin to PATH and re-run."; exit 1; }; \
	done

hooks: ## Install git hooks defined in lefthook.yml
	lefthook install

fmt: ## Auto-fix formatting and style
	@for s in $(DOTNET_SOLUTIONS); do dotnet format "$$s" || exit 1; done
	@for m in $(GO_MODULES); do (cd $$m && golangci-lint fmt ./...) || exit 1; done

lint: ## Check everything without fixing (same as CI)
	@for s in $(DOTNET_SOLUTIONS); do \
		dotnet format "$$s" --verify-no-changes && dotnet build "$$s" -c Release || exit 1; \
	done
	@for m in $(GO_MODULES); do (cd $$m && golangci-lint run ./...) || exit 1; done

test: ## Run all tests
	@for s in $(DOTNET_SOLUTIONS); do dotnet test "$$s" || exit 1; done
	@for m in $(GO_MODULES); do (cd $$m && go test -race ./...) || exit 1; done
