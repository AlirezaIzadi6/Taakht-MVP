# Get started

Everything you need to go from a fresh clone to a first commit that passes CI. The root [README](../README.md) has the three-command version; this document explains what those commands do and what to do when they fail.

## Prerequisites

Install these yourself. `make setup` only checks that they exist, it never installs them.

| Tool | Notes |
|---|---|
| Git | |
| .NET SDK | Version pinned in [`global.json`](../global.json). If it is missing, `dotnet` commands fail with a message naming the required version. |
| Go | Version comes from each module's `go.mod`. Go downloads the matching toolchain automatically, so any recent Go is enough to bootstrap. |
| Docker | Needed by `make up` / `make dev` (Postgres, Kafka, Envoy). |
| jq | Needed by `make demo` (also `curl`). |
| grpcurl | Optional, for manual gRPC calls. |
| make | Linux/macOS: preinstalled or via the package manager. Windows: `scoop install make` or `choco install make`, and run it from Git Bash because the Makefile uses `bash` and `find`. |

## Setup

```bash
git clone <repo-url>
cd Taakht
make setup
```

`make setup` runs three targets in order. Each is safe to re-run and skips work that is already done.

| Target | What it does |
|---|---|
| `make prereqs` | Checks that `git`, `dotnet` and `go` are on `PATH` and that the SDK required by `global.json` is installed. Reports only. |
| `make tools` | Installs `lefthook` and `golangci-lint` with `go install` if they are not already on `PATH`. |
| `make hooks` | Runs `lefthook install`, which registers the git hooks defined in [`lefthook.yml`](../lefthook.yml). |

`go install` places binaries in `$(go env GOPATH)/bin`. That directory must be on your `PATH`; if it is not, `make tools` stops with a message telling you so.

## Everyday commands

| Command | What it does |
|---|---|
| `make fmt` | Auto-fixes formatting and style for .NET and Go. |
| `make lint` | Checks everything without fixing anything. This is exactly what CI runs. |
| `make test` | Runs all .NET and Go tests (Go with `-race`). |

Run `make` with no arguments to list all targets.

Each .NET service has its own solution file and each Go service its own module. The targets above discover them automatically (every `*.sln`/`*.slnx` and every `go.mod` in the repo) and run the command once per solution or module, so adding a service needs no Makefile change.

## What happens when you commit

[`lefthook.yml`](../lefthook.yml) defines the git hooks. Only the definition is committed; the installed hooks live in `.git/hooks/` and are created by `make hooks`.

**pre-commit** works on staged files only and fixes what can be fixed mechanically, re-staging the result:

1. `dotnet format whitespace` on staged C# and project files (folder mode, no solution needed)
2. `golangci-lint fmt` in every Go module
3. `golangci-lint run --fix --new-from-rev=HEAD` in every Go module, so only issues you introduced block the commit

Style fixes for C# (usings, braces, `var`, and so on) need a loaded project, so the hook does not apply them. Use format-on-save in your IDE or run `make fmt`; CI verifies them.

**commit-msg** requires a [Conventional Commits](https://www.conventionalcommits.org) message such as `feat(reputation): add score projection`.

Anything the tools cannot fix on their own, such as nullable warnings, unchecked errors, missing `CancellationToken` or `context.Context` propagation, and naming violations, fails the commit and must be fixed by hand.

To skip the hooks for one commit, use `git commit --no-verify`. CI runs the same checks without fixing anything, so a skipped hook only moves the failure to the pull request. To change hook behavior on your machine only, create `lefthook-local.yml` (git-ignored).

## Continuous integration

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every pull request and on pushes to `main`.

- A change-detection job decides whether .NET or Go files changed, so unrelated stacks are skipped.
- The .NET job runs once per solution, discovered from the `*.sln`/`*.slnx` files: restore, format check, Release build (analyzers run as errors) and tests.
- The Go job runs once per module, discovered from the `go.mod` files: `golangci-lint` and `go test -race`.
- If any .NET or Go file or shared config changes, all solutions or modules of that stack run. Narrowing this to affected services only is a possible later optimization.
- The .NET and Go test jobs get a Postgres 17 service container; a single-node Kafka (`apache/kafka:3.9.0`) is started for the modules that use it (`libs/goplatform`, `src/ad`, `src/matching`, `libs/dotnet`). `TEST_DATABASE_URL` and `KAFKA_BROKERS` are set for the test step, so the DB- and Kafka-backed lock/concurrency tests run in CI instead of skipping.
- A separate `e2e` job runs on pushes to `main` and on pull requests touching `src/`, `libs/`, `api/`, `tests/`, `config/` or `deploy/`: it starts Postgres, Kafka and the four services, then runs `tests/e2e` with `E2E=1`. Service logs are printed and uploaded on failure.
- `e2e` is informational (`continue-on-error`) and is not part of `ci-ok`, so a red e2e run does not block merging.
- Pull request titles must follow Conventional Commits, because squash merge uses the title as the commit message.
- `ci-ok` is the single required status check for branch protection.

## Code quality configuration

| Concern | Where it is configured |
|---|---|
| Editor formatting (charset, indentation, line endings) | [`.editorconfig`](../.editorconfig) |
| .NET analyzers, nullable, warnings as errors | [`Directory.Build.props`](../Directory.Build.props) and `.editorconfig` |
| Central NuGet package versions | [`Directory.Packages.props`](../Directory.Packages.props) |
| Go linters and formatters | [`.golangci.yml`](../.golangci.yml) |
| Line endings in git | [`.gitattributes`](../.gitattributes) |
| Ignored files | [`.gitignore`](../.gitignore) |

Package versions are declared only in `Directory.Packages.props`; project files reference packages without a `Version` attribute.

## Conventions

- All repository content is English: code, comments, docs, configs, commit messages.
- Persian appears only as localized user-facing messages, kept in resource files (UTF-8).
- Never commit secrets. Keep local configuration in `.env` or `appsettings.Local.json`, both git-ignored.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Commits go through without any checks | Hooks are not installed. Run `make hooks`. |
| `make tools` says a tool is not on `PATH` | Add `$(go env GOPATH)/bin` to `PATH` and open a new shell. |
| `dotnet` complains about a missing SDK | Install the version pinned in `global.json`. |
| `make: command not found` on Windows | Install make (see Prerequisites) and use Git Bash. |
| `make lint` passes locally but CI fails | Make sure you ran it from the repo root with the same SDK version, and that you did not commit with `--no-verify` after a failed hook. |
