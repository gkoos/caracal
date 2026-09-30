import { randomInt } from "node:crypto"
import * as fc from "fast-check"
import { it } from "vitest"

export const testSeedEnvironmentVariable = "CARACAL_TEST_SEED"

export function resolveTestSeed(): number {
  const configured = process.env[testSeedEnvironmentVariable]
  if (configured === undefined || configured === "") {
    return randomInt(1, 2 ** 31 - 1)
  }

  const seed = Number(configured)
  if (!Number.isSafeInteger(seed) || seed < 0) {
    throw new Error(
      `${testSeedEnvironmentVariable} must be a non-negative integer`,
    )
  }

  return seed
}

export function replayInstruction(command: string, seed: number): string {
  return `${testSeedEnvironmentVariable}=${seed} ${command}`
}

/** The case-count multiplier applied to every generated suite. */
export const testRunsEnvironmentVariable = "CARACAL_TEST_RUNS"

/**
 * Resolves the run depth for generated suites: 1 for a normal run, higher for
 * `npm run test:generated:deep` and for the nightly workflow.
 */
export function resolveTestRuns(): number {
  const configured = process.env[testRunsEnvironmentVariable]
  if (configured === undefined || configured === "") {
    return 1
  }

  const runs = Number(configured)
  if (!Number.isSafeInteger(runs) || runs < 1) {
    throw new Error(`${testRunsEnvironmentVariable} must be a positive integer`)
  }

  return runs
}

// ---------------------------------------------------------------------------
// Seeded PRNG - mulberry32, period 2^32, reproducible from any 32-bit seed.
// Every loop-based generated suite draws from this one implementation, so a
// reported seed reproduces the same cases in any suite.
// ---------------------------------------------------------------------------

export function mulberry32(seed: number): () => number {
  let s = seed >>> 0
  return () => {
    s = (s + 0x6d2b79f5) | 0
    let t = Math.imul(s ^ (s >>> 15), 1 | s)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export function randInt(rng: () => number, lo: number, hi: number): number {
  return lo + Math.floor(rng() * (hi - lo + 1))
}

export function randFloat(rng: () => number, lo: number, hi: number): number {
  return lo + rng() * (hi - lo)
}

// ---------------------------------------------------------------------------
// Generated-suite protocol
//
// A generated suite is one file of loop-based fuzz cases or fast-check
// properties. It resolves one random seed per run (`CARACAL_TEST_SEED` replays
// it), scales every case count by the run depth (`CARACAL_TEST_RUNS`), and
// reports both on failure. `npm run test:generated` runs every generated suite
// at the default depth; `npm run test:generated:deep` runs the same suites
// deeper with a fixed seed, which is what the nightly workflow does.
// ---------------------------------------------------------------------------

export interface GeneratedSuiteOptions {
  /** Names the suite in failure messages. */
  readonly name: string
  /** The command that replays this suite. */
  readonly command: string
  /** Ceiling for a depth-scaled case count, so a deep run stays bounded. */
  readonly maxCases?: number
}

export interface GeneratedSuite {
  readonly name: string
  readonly seed: number
  /** The resolved run depth: 1 unless `CARACAL_TEST_RUNS` asks for more. */
  readonly depth: number
  /** Replays this suite's seed, plus its depth when the depth is above 1. */
  readonly replay: string
  /**
   * This suite's own case count, multiplied by the run depth. `cap` (or the
   * suite's `maxCases`) is a ceiling for the scaled count - useful for loops
   * that are far slower per case than the rest of the suite. A cap never
   * reduces a count below the suite's own value, so a default run is unaffected.
   */
  cases(count: number, cap?: number): number
  /** fast-check options carrying this suite's seed and depth-scaled runs. */
  assertOptions(count: number, cap?: number): { seed: number; numRuns: number }
  /** A PRNG from this suite's seed; `offset` selects an independent stream. */
  rng(offset?: number): () => number
  /** Registers a fast-check property that fails with this suite's replay hint. */
  itProperty<Value>(
    title: string,
    arbitrary: fc.Arbitrary<Value>,
    predicate: (value: Value) => Promise<void> | void,
    options: { readonly runs: number },
  ): void
}

export function createGeneratedSuite(
  options: GeneratedSuiteOptions,
): GeneratedSuite {
  const { name, command, maxCases } = options
  const seed = resolveTestSeed()
  const depth = resolveTestRuns()

  const cases = (count: number, cap?: number): number => {
    if (!Number.isSafeInteger(count) || count < 1) {
      throw new RangeError(
        `${name}: a case count must be a positive integer, received ${count}`,
      )
    }

    const ceiling = cap ?? maxCases
    const scaled = count * depth
    if (ceiling === undefined) {
      return scaled
    }

    // A cap bounds the depth multiplier, never the suite's own case count.
    return Math.max(count, Math.min(scaled, ceiling))
  }

  const replay = `${testSeedEnvironmentVariable}=${seed}${
    depth === 1 ? "" : ` ${testRunsEnvironmentVariable}=${depth}`
  } ${command}`

  return Object.freeze({
    name,
    seed,
    depth,
    replay,
    cases,
    assertOptions: (count: number, cap?: number) => ({
      seed,
      numRuns: cases(count, cap),
    }),
    rng: (offset = 0) => mulberry32(seed ^ offset),
    itProperty: <Value>(
      title: string,
      arbitrary: fc.Arbitrary<Value>,
      predicate: (value: Value) => Promise<void> | void,
      propertyOptions: { readonly runs: number },
    ) => {
      it(title, async () => {
        try {
          await fc.assert(
            fc.asyncProperty(arbitrary, async (value: Value) => {
              await predicate(value)
            }),
            { seed, numRuns: cases(propertyOptions.runs) },
          )
        } catch (error) {
          // The counterexample first, then the exact command that replays this
          // suite at this depth - so a failure is reproducible without reading
          // the test file.
          throw new Error(
            `${error instanceof Error ? error.message : String(error)}\n${name}: ${replay}`,
            { cause: error },
          )
        }
      })
    },
  })
}
