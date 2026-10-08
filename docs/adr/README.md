# Architecture Decision Records

ADRs are written in English.

## Rules

- One decision per ADR.
- An accepted ADR is immutable. If a decision changes, write a new ADR and mark the old one `Superseded by`.
- Numbers are sequential, four digits, and never reused.
- File name: `NNNN-short-imperative-slug.md`.
- Start from [template.md](template.md). Keep an ADR under about one page; if it grows, it probably holds more than one decision.
- Always list the rejected options with the reason.

## What is not an ADR

| Not an ADR | Where it goes |
|---|---|
| Domain discovery: events, aggregates, context boundaries | Event Storming / context map docs |
| How the system works | Architecture overview |
| Cheap, reversible choices (variable names, library versions, log format) | Code, PR description, guidelines |
| Coding style and conventions (topic names, retry/DLQ patterns, client libraries, deploy config) | Convention / guideline docs |
| Remaining work and open questions | `open-items.md` |
| Operational procedures | Runbooks |
| Product decisions with no architectural impact | Product docs |
| Decisions not yet made | `open-items.md` until decided |

**Rule of thumb:** if reversing it later is expensive or painful, if it touches several contexts or teams, or if a newcomer would ask "why did you build it this way?", write an ADR. Otherwise do not.

## Status values

- **Proposed** - under discussion.
- **Accepted** - decided; the team follows it.
- **Deprecated** - no longer applies, with no replacement.
- **Superseded by NNNN** - replaced by a newer ADR.

## Index

| # | Title | Status | Context |
|---|---|---|---|
