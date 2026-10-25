# Serval authority reference model

This directory holds the Phase 0 executable contracts for Serval: the deterministic reference model
(`reference-model.ts`) and the seeded adversarial history generator (`histories.ts`).

It is internal test support only. It is deliberately Redis-free and dependency-free, it never runs
in production, and it is not reachable from any package export. Production coordination will live in
`src/coordination/serval/` from Phase 1 onward.

The model is the specification's proof obligation, not the implementation:

- Each class in `reference-model.ts` covers one of the eleven mandatory decisions in
  [`docs/serval.md`](../../../docs/serval.md) and records a *violation* when a contract invariant
  breaks, so a test fails with the invariant's name.
- `test/unit/serval-authority.test.ts` drives one deterministic acceptance group per decision,
  including the worked examples from [`docs/serval-authority.md`](../../../docs/serval-authority.md).
- `test/property/serval-authority.property.test.ts` replays seeded adversarial histories through the
  same model, honouring `CARACAL_TEST_SEED` for replay.

Phase 1 ports these cases to the real session and retires this module as the oracle.
