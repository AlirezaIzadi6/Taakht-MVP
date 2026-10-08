# Documentation

Entry point for all Taakht design documents. The root [README](../README.md) is the showcase; this folder is the detail behind it.

## Layout

| Folder / file | What goes here | Not here |
|---|---|---|
| `product/` | The business case: business summary, business decisions log, and later PRD (vision, target users, KPIs), risk assessment (business risks and conscious tech debt), roadmap | Technical design |
| `domain/` | Ubiquitous language (glossary), Event Storming output, bounded contexts, context map | Technology choices |
| `architecture/` | Architecture overview (services, flows, events), the raw chronological decisions log, diagrams | The final "why" of a single decision (that is an ADR) |
| `adr/` | One file per architecture decision, in English. See [adr/README.md](adr/README.md) | Conventions, remaining work |
| `evidence/` | Experiments and document reviews behind ADRs, with results and what was not verified | The decision itself |
| `guidelines/` | Conventions that follow from decisions: topic naming, retry/DLQ patterns, client libraries, deploy config, code style | Decisions with trade-offs |
| `api/` | API documentation (OpenAPI, Postman) and event contracts | Internal design |
| `testing/` | Test strategy, load and stress test scenarios and recorded results (the scripts themselves live with the code) | Unit test code |
| `process/` | How the team works: Scrum, GitHub workflow, meetings | Architecture |
| `get-started.md` | Developer onboarding: prerequisites, setup, everyday commands, git hooks, CI, troubleshooting | Architecture, conventions that follow from decisions |
| `open-items.md` | Undecided questions and remaining steps, the working list that feeds new ADRs | Decided material |

## How the pieces relate

```
Event Storming (domain/)  ->  decisions log (architecture/)  ->  ADR (adr/)  ->  guideline (guidelines/)
      discover                    think out loud                  the "why"        the "how"
```

- `open-items.md` holds a question until it is decided. Then it moves to the decisions log and, if it passes the ADR test, becomes an ADR.
- `architecture/overview` is the current state; the decisions log is the history. When they disagree, the overview wins.

## Language

All documentation is written in English, including file names and identifiers.
