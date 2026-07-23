---
name: write-adr
description: Use when recording a new architecture decision (ADR) anywhere in the perfectory monorepo — root global numbering, frozen area sequences and the GZ-ADR-NNNN citation style, Accepted-immutability with supersession chains, and the template skeleton.
---

# Write an ADR

## Numbering — one global sequence (root ADR-0002 ⑤)

- ALL new ADRs go to root `docs/adr/` with the next number in the root
  sequence. Next number = highest existing `NNNN-*.md` in `docs/adr/` + 1
  (check with `ls docs/adr/`). Filename: `NNNN-<kebab-case-title>.md`.
- Add an index line to `docs/adr/README.md` in the same change.
- Area `docs/adr/` sequences are FROZEN at their last numbers
  (gongzzang 0050, foundation 0027, identity 0001, intelligence 0001). Never
  add a new number there — even for an area-local decision, use the root
  sequence and name the area in the title.

## Citing frozen area ADRs

Use the area-prefixed ID so `ADR-0021` is never ambiguous across areas:

- `GZ-ADR-NNNN` — `products/gongzzang/docs/adr/`
- `FP-ADR-NNNN` — `platforms/foundation-platform/docs/adr/`
- `IDP-ADR-NNNN` — `platforms/identity-platform/docs/adr/`
- `ITP-ADR-NNNN` — `platforms/intelligence-platform/docs/adr/`

Example: `GZ-ADR-0050` = gongzzang's 0050-dawneer-workbench ADR. Bare
`ADR-NNNN` refers to the root sequence.

## Immutability + supersession

- Once Status is Accepted, the decision text is immutable. To change a
  decision, write a NEW root ADR and mark the old one
  `Superseded by ADR-NNNN` — this forms the supersession chain.
- Allowed edits to an Accepted ADR: fixing dead relative links and appending a
  dated revision footnote (e.g., crate-rename notes). Rewriting the decision
  is not.
- Frozen area ADRs keep the same rule: supersede them with a ROOT-numbered ADR
  citing the area ID (e.g., "Supersedes GZ-ADR-0034").

## Template skeleton (root style — see docs/adr/0001)

```markdown
# ADR NNNN: <title>

- Status: Accepted            <!-- or: Superseded by ADR-NNNN -->
- Date: YYYY-MM-DD

## Context

<why this decision is needed; measured facts, not speculation>

## Decision

<numbered, testable statements; name the guard/script if one enforces it>

## Consequences

<what changes, what it costs, follow-up work>
```

ADRs carry their own `Status:` line — they are exempt from the
`status: current|archived` frontmatter rule for ordinary docs (root ADR-0002 ④).

## Before writing

- One decision = one file. Decision-first flow: gongzzang routing says "새 결정
  필요 → ADR 작성 후 코드".
- If a guard/CI check is part of the decision, ADR-0001's guard policy applies:
  it must answer "what real incident does failing prevent?" in one sentence.
