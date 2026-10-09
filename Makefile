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
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

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

# ---------- MVP: contracts and local infrastructure ----------
# PROTO_INCLUDE: directory holding google/protobuf/*.proto (shipped next to protoc on most installs).
PROTO_INCLUDE ?= $(shell dirname "$$(command -v protoc)")/../include

.PHONY: proto up down logs

proto: ## Regenerate Go code from api/proto (.NET generates at build time via Grpc.Tools)
	@rm -rf gen/go/taakht
	protoc -I api/proto -I third_party -I "$(PROTO_INCLUDE)" \
		--go_out=gen/go --go_opt=paths=source_relative \
		--go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
		$$(find api/proto -name '*.proto')

# Local dev keeps the dev-only /v1/dev/ edge route (the demo needs it); set ENVOY_ENABLE_DEV_ROUTES=false to harden.
export ENVOY_ENABLE_DEV_ROUTES ?= true

up: ## Start local infrastructure (Postgres, Kafka, Envoy gateway on :8080); renders the Envoy config first
	@changed=$$(scripts/gen-envoy-config.sh) || exit 1; \
	docker compose -f deploy/docker-compose.yml up -d --wait || exit 1; \
	if [ "$$changed" = updated ]; then echo "Envoy config changed: restarting envoy"; docker compose -f deploy/docker-compose.yml restart envoy; fi

down: ## Stop local infrastructure and delete its data
	docker compose -f deploy/docker-compose.yml down -v

logs: ## Follow infrastructure logs
	docker compose -f deploy/docker-compose.yml logs -f

# ---------- MVP: run the whole system locally ----------
.PHONY: dev dev-stop dev-status e2e scenario reset

dev: ## Start infra and all services in the background (scripts/dev.sh start)
	@scripts/dev.sh start

dev-stop: ## Stop the locally running services (infra keeps running)
	@scripts/dev.sh stop

dev-status: ## Show which services are up
	@scripts/dev.sh status

e2e: ## Run the end-to-end tests in tests/e2e (needs make dev)
	@cd tests/e2e && E2E=1 go test -count=1 ./...

scenario: ## Run the scripted demo scenario (needs make dev)
	@cd tests/e2e && go run ./cmd/scenario

reset: ## Stop services and wipe all data (infra down -v, then up)
	@scripts/dev.sh stop
	@$(MAKE) --no-print-directory down
	@$(MAKE) --no-print-directory up

# ---------- MVP: REST edge (Envoy) ----------
.PHONY: gateway-descriptor

gateway-descriptor: ## Rebuild gateway/descriptor.binpb (Envoy transcoder) from api/proto
	protoc -I api/proto -I third_party -I "$(PROTO_INCLUDE)" \
		--include_imports --include_source_info \
		-o gateway/descriptor.binpb \
		api/proto/taakht/ad/v1/ad.proto \
		api/proto/taakht/matching/v1/matching.proto \
		api/proto/taakht/negotiation/v1/negotiation.proto \
		api/proto/taakht/swap/v1/swap.proto

.PHONY: gateway-test

gateway-test: ## Edge tests (auth, CORS, limits, 404, dev route, retries) against the running gateway; needs make dev
	@scripts/test-gateway.sh

# ---------- MVP: REST demo ----------
.PHONY: demo

demo: ## Run the narrated REST demo through Envoy (needs make up + scripts/dev.sh start); DEMO_ARGS="--pause" or "timeout"
	@scripts/demo.sh $(DEMO_ARGS)

# ---------- MVP: fault-injection (chaos) tests ----------
.PHONY: chaos

chaos: ## Fault-injection tests (kill services, stop Kafka/Postgres); needs make dev, takes ~10 min, stops/starts the stack
	@cd tests/e2e && CHAOS=1 E2E=1 go test -run TestChaos -v -count=1 -timeout 60m ./...

# ---------- MVP: load and stress tests (k6) ----------
.PHONY: load

load: ## k6 load tests in tests/load (browse, negotiate, contention) with modest defaults; needs make dev and k6; results in tests/load/results
	@tests/load/gen-tokens.sh
	@cd tests/load && k6 run -q -e VUS=25 -e DURATION=30s browse.js; \
	 k6 run -q -e VUS=10 -e DURATION=30s negotiate.js; \
	 k6 run -q -e REQUESTERS=40 contention.js; true
