# ADR-0044: Native build and verification SSOT

| | |
|---|---|
| Date | 2026-06-20 |
| Status | **Accepted — Bazel adoption is superseded** |
| Decision owner | perfectoryinc (platform owner) |
| Supersedes | ADR-0040, ADR-0041, ADR-0042, ADR-0043 |
| Reaffirms | ADR-0002 and foundation-platform ADR-0010 |

## Context

The repository previously carried two competing build models: native Cargo/pnpm tasks and a Bazel
graph wrapped by repository-specific registry, projection, and ratchet machinery. Duplicating the
same build and verification knowledge across those models made the documentation and executable
gates disagree.

Native tools already provide the required scoped execution: Cargo selects Rust packages and
Turborepo filters frontend packages. Bazel would add another dependency graph, specialized
maintenance, and infrastructure without removing a demonstrated bottleneck at this project's scale.

The root cause is not one failed build. It is multiple writable definitions of how the repository is
built and verified. The repository therefore needs one build path and one verification entrypoint.

## Decision

1. **Cargo is the Rust build tool. pnpm/Turborepo is the frontend package and task runner.** Bazel is
   not part of the supported build, test, lint, or release path.
2. **`cargo xtask verify <area>` is the verification SSOT.** It may invoke Cargo, pnpm, and standard
   tools, but CI and local verification use the same area entrypoint.
3. Scoped work uses native selection: `cargo build|test|check -p <crate>` for Rust and
   `pnpm turbo <task> --filter <pkg>` for frontend packages.
4. Repository-owned automation is Rust or a standard tool. PowerShell registry/projection/ratchet
   meta-machines are not an accepted verification architecture.
5. Generated runtime policy remains legitimate only when application code consumes it, one canonical
   input owns it, and a mechanical drift guard proves the generated output matches that input.

## Alternatives

- **Native Cargo and pnpm/Turborepo** — adopted. They cover the required languages and scoped builds
  without a second graph.
- **Bazel as the terminal SSOT** — rejected. Its additional graph and operating model are not
  justified by a measured repository bottleneck.
- **PowerShell wrappers as a permanent control plane** — rejected. They duplicate task knowledge and
  make the wrapper hierarchy verify itself.
- **Buck2** — rejected for the same second-graph reason and consistent with ADR-0042.

## Consequences

- Build and verification ownership is unambiguous: native language tools execute work and xtask owns
  the public verification contract.
- Partial builds remain available without remote execution or a repository-specific build framework.
- Repository-specific guards stay small and test real invariants; off-the-shelf tools remain the
  default for generic concerns.
- A change that introduces Bazel or a PowerShell verification control plane contradicts this ADR and
  must fail repository governance checks.

## Re-adoption bar

Re-adopting Bazel requires a new ADR that demonstrates both:

1. reproducible builds on every supported development and CI platform; and
2. a concrete, measured build or test bottleneck that native package selection and caching cannot
   solve at acceptable cost.

The ADR must also define migration ownership, hermetic toolchains, cache trust boundaries, and how it
replaces rather than duplicates the existing SSOT.

## References

- ADR-0002, ADR-0040, ADR-0041, ADR-0042, ADR-0043
- foundation-platform ADR-0010 and ADR-0011
- root ADR-0004, verification SSOT
