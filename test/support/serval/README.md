# Serval authority reference model and session test support

This directory holds Serval's executable test infrastructure. It is internal test support only: it
is never reachable from a package export, and production coordination lives in
`src/coordination/serval/`.

| File | Role |
|---|---|
| `reference-model.ts` | The Phase 0 deterministic reference model: one small model per mandatory decision, each recording a violation when a contract invariant breaks |
| `histories.ts` | Seeded adversarial histories for the reference model (grants, the rate envelope, the bulkhead handoff) with independent oracles |
| `deterministic-scheduler.ts` | A virtual clock and task queue for the session: time moves only when a test advances it |
| `fake-authority.ts` | A scriptable authority: versioned records stamped with the server's clock, plus read failures, a held read, a rewound ledger and a failover |
| `notification-transport.ts` | A transport whose delivery, duplication, disconnect and failed open a test controls |
| `session-harness.ts` | The session wired to all of the above, plus the identity most tests use |
| `session-history.ts` | The generated session history, with its own oracle for deadlines, versions and bounds |

The model is the specification's proof obligation, not the implementation. The unit suites
(`test/unit/serval-*.test.ts`) drive one deterministic acceptance group per decision and per session
concern, and the generated suites (`test/property/serval-authority.property.test.ts`) replay the
same models and the session over adversarial histories, honouring `CARACAL_TEST_SEED`.

