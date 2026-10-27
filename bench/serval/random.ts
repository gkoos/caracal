/**
 * A deterministic PRNG for the lab's offered load and its fault schedules.
 *
 * The lab pairs profiles by seed: the same seed has to produce the same load and the same faults
 * in every profile, so that a difference between profiles is attributable to the coordination
 * strategy rather than to the work offered or the failures it met. Wall-clock scheduling still
 * varies - these are the waits the worker asks for, not the waits it gets - and the digest the
 * worker reports is what makes the intended schedule visible.
 */
export function mulberry32(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let value = Math.imul(state ^ (state >>> 15), 1 | state)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}
