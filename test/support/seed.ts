import { randomInt } from "node:crypto"

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
