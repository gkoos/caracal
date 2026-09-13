// ---------------------------------------------------------------------------
// Versioned atomic operations, internal to the Redis policy capabilities.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Bulkhead scripts
// ---------------------------------------------------------------------------

export const leaseV1 = `
local action = ARGV[1]
local token = ARGV[2]
local ttl = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local existing = redis.call('ZSCORE', KEYS[1], token)
if action == 'release' then
  return redis.call('ZREM', KEYS[1], token)
end
if action == 'renew' and not existing then return 0 end
if action == 'acquire' and existing then return 1 end
if action == 'acquire' and redis.call('ZCARD', KEYS[1]) >= limit then return 0 end
redis.call('ZADD', KEYS[1], now + ttl, token)
local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
redis.call('PEXPIREAT', KEYS[1], math.ceil(tonumber(latest[2])))
return 1
`

/** Returns the decision and occupancy from the same atomic transition. */
export const bulkheadLeaseV1 = `local function transition()\n${leaseV1}\nend\nlocal allowed = transition()\nreturn {allowed, redis.call('ZCARD', KEYS[1])}`

// ---------------------------------------------------------------------------
// Circuit-breaker scripts
//
// Return arrays use fixed positions so the TypeScript caller can validate
// and decode without field names (which Lua/Redis cannot return).
//
// stateCode encoding:  0=closed  1=open  2=half-open
// ---------------------------------------------------------------------------

/**
 * Record one attempt outcome into the distributed sliding window.
 * Atomically opens the breaker when the failure ratio meets the threshold.
 *
 * KEYS[1] = breaker state hash         (:breaker)
 * KEYS[2] = observations sorted set    (:observations)
 *
 * ARGV[1] = outcome               "success" | "failure"
 * ARGV[2] = expectedGeneration    integer
 * ARGV[3] = windowTtlMs           observation retention window in ms
 * ARGV[4] = minimumThroughput     min observations before opening
 * ARGV[5] = failureThresholdNum   failure threshold × 1000 (e.g. 500 = 0.5).
 *                                The policy rejects thresholds that would round
 *                                to 0 or to 1000 before they reach this script
 *                                (see assertResolvableThreshold in core).
 * ARGV[6] = windowSize            max observations retained by count
 * ARGV[7] = openMs                how long to stay OPEN; closed hash TTL = openMs×2
 * ARGV[8] = uuid                  unique string for member deduplication
 *
 * Returns: {status, stateCode, generation, windowTotal, windowFailures}
 *   status 0 = stale  (dropped; generation or state mismatch)
 *   status 1 = observed, no transition  (remained closed)
 *   status 2 = observed, breaker opened (stateCode=1, generation incremented)
 *
 * TTL policy:
 *   OPEN state  → PERSIST (no expiry).  A missing key is treated as closed by
 *                 all scripts, so expiring an open breaker would silently admit
 *                 unrestricted traffic and reset the generation counter.
 *   CLOSED state → max(openMs×2, windowTtlMs) TTL for eventual cleanup of idle
 *                 scopes.  Expiring a closed key is safe *only* because the TTL
 *                 outlives the observation window: the window is scoped by an
 *                 epoch, and an epoch that outlives its members is what keeps
 *                 cleanup from resurrecting them.
 *
 * Epochs:
 *   `generation` increments on every transition that moves the window's epoch -
 *   CLOSED→OPEN, and anything leaving HALF_OPEN - and a brand new value is
 *   minted whenever the state hash has to be recreated while observation members
 *   from a previous epoch are still present (state lost to eviction or admin
 *   cleanup).  OPEN→HALF_OPEN deliberately keeps the generation: it stays the
 *   same epoch, and the superseded probe tokens are cleared with a DEL on the
 *   probe set instead, so the membership key of the window does not move while
 *   the recovery window changes.
 *   Window membership is decided by comparing the stored epoch, so members from
 *   a superseded epoch can never be counted again, and an attempt holding a
 *   pre-loss generation can never pass the staleness check.  A scope that has
 *   never been observed keeps generation 0.
 */
export const breakerObserveV1 = `
local outcome    = ARGV[1]
local expectGen  = tonumber(ARGV[2])
local windowTtl  = tonumber(ARGV[3])
local minTP      = tonumber(ARGV[4])
local threshNum  = tonumber(ARGV[5])
local windowSize = tonumber(ARGV[6])
local openMs     = tonumber(ARGV[7])
local uuid       = ARGV[8]
local t   = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local f   = redis.call('HMGET', KEYS[1], 'state', 'generation')
local state = f[1] or 'closed'
local gen   = tonumber(f[2]) or 0
if state ~= 'closed' then
  local sc = (state == 'open' and 1) or (state == 'half-open' and 2) or 0
  return {0, sc, gen, 0, 0}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  -- No live state hash: a scope we have never seen, or a hash that was lost
  -- while its window survived.
  if expectGen ~= 0 then
    -- The caller holds a generation this key cannot confirm.
    return {0, 0, 0, 0, 0}
  end
  if redis.call('EXISTS', KEYS[2]) == 1 then
    -- Members from a superseded epoch are still here.  Mint an epoch that has
    -- never been used for this key (derived from the observation's own uuid, so
    -- no clock is involved) instead of restarting at a value those members
    -- would match.
    --
    -- Bounded to 11 hex digits (44 bits, <= 14 decimal digits) so the value
    -- prints exactly with stock Lua number formatting: the epoch is read back
    -- from the hash as a decimal string by both this script and the client, and
    -- a larger value could be written in scientific notation and break the
    -- round-trip.
    gen = tonumber(string.sub(redis.sha1hex(uuid), 1, 11), 16)
    if not gen or gen == 0 then gen = 1 end
  else
    gen = 0
  end
  redis.call('HSET', KEYS[1], 'state', 'closed', 'generation', gen,
             'openedAt', 0, 'probeCount', 0, 'probeSuccesses', 0)
elseif gen ~= expectGen then
  return {0, 0, gen, 0, 0}
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - windowTtl)
local member = tostring(gen) .. ':' .. uuid .. ':' .. outcome
redis.call('ZADD', KEYS[2], 'NX', now, member)
local cnt = redis.call('ZCARD', KEYS[2])
if cnt > windowSize then
  redis.call('ZREMRANGEBYRANK', KEYS[2], 0, cnt - windowSize - 1)
end
redis.call('PEXPIREAT', KEYS[2], now + windowTtl)
local all     = redis.call('ZRANGE', KEYS[2], 0, -1)
local prefix  = tostring(gen) .. ':'
local wTotal  = 0
local wFail   = 0
for _, m in ipairs(all) do
  if string.sub(m, 1, #prefix) == prefix then
    wTotal = wTotal + 1
    if string.sub(m, -8) == ':failure' then wFail = wFail + 1 end
  end
end
if wTotal >= minTP and wFail * 1000 >= threshNum * wTotal then
  gen = gen + 1
  redis.call('HSET', KEYS[1], 'state', 'open', 'generation', gen,
             'openedAt', now, 'probeCount', 0, 'probeSuccesses', 0)
  -- OPEN state must never expire: a missing key is treated as closed,
  -- which would silently admit unrestricted traffic and reset the generation.
  redis.call('PERSIST', KEYS[1])
  return {2, 1, gen, wTotal, wFail}
end
if redis.call('EXISTS', KEYS[1]) == 1 then
  -- Still CLOSED.  The cleanup TTL must not expire the state before the window
  -- it governs, otherwise retained members would survive their epoch.
  redis.call('PEXPIRE', KEYS[1], math.max(openMs * 2, windowTtl))
end
return {1, 0, gen, wTotal, wFail}
`

/**
 * Admit a probe attempt.  Transitions OPEN→HALF_OPEN when openMs has elapsed,
 * then atomically allocates a probe token slot.
 *
 * KEYS[1] = breaker state hash         (:breaker)
 * KEYS[2] = probe tokens sorted set    (:probes)
 *
 * ARGV[1] = probeToken        unique UUID for this probe attempt
 * ARGV[2] = openMs            determines OPEN→HALF_OPEN transition and TTL
 * ARGV[3] = halfOpenProbes    maximum concurrent probe tokens
 * ARGV[4] = probeLeaseTtlMs   probe token TTL in ms
 *
 * Returns: {status, stateCode, generation, probeCount, transitioned}
 *   status 0 = rejected
 *     stateCode 0 = breaker already CLOSED (no probe needed)
 *     stateCode 1 = still OPEN (openMs not yet elapsed)
 *     stateCode 2 = HALF_OPEN but probe limit reached
 *   status 1 = admitted (probe token stored)
 *     transitioned 1 = OPEN→HALF_OPEN transition happened in this call
 *     transitioned 0 = was already HALF_OPEN
 */
export const breakerAdmitProbeV1 = `
local probeToken    = ARGV[1]
local openMs        = tonumber(ARGV[2])
local maxProbes     = tonumber(ARGV[3])
local probeLeaseTtl = tonumber(ARGV[4])
local t   = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local f = redis.call('HMGET', KEYS[1], 'state', 'generation', 'openedAt',
                     'probeCount', 'probeSuccesses')
local state     = f[1] or 'closed'
local gen       = tonumber(f[2]) or 0
local openedAt  = tonumber(f[3]) or 0
local transitioned = 0
if state == 'closed' then
  return {0, 0, gen, 0, 0}
end
if state == 'open' then
  if now - openedAt < openMs then
    return {0, 1, gen, 0, 0}
  end
  state = 'half-open'
  transitioned = 1
  redis.call('HSET', KEYS[1], 'state', 'half-open', 'probeSuccesses', 0, 'probeCount', 0)
  -- OPEN/HALF_OPEN must never expire; PERSIST removes any prior cleanup TTL.
  redis.call('PERSIST', KEYS[1])
  -- Tokens from the superseded recovery window must not consume slots in this
  -- one; their settle finds no token and is dropped, and observations from the
  -- same window are dropped by the epoch-scoped window instead.
  redis.call('DEL', KEYS[2])
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local activeProbes = redis.call('ZCARD', KEYS[2])
if activeProbes >= maxProbes then
  return {0, 2, gen, activeProbes, transitioned}
end
redis.call('ZADD', KEYS[2], 'NX', now + probeLeaseTtl, probeToken)
activeProbes = activeProbes + 1
redis.call('HSET', KEYS[1], 'probeCount', activeProbes)
-- State hash must not expire while recovery probes are in flight.
redis.call('PERSIST', KEYS[1])
-- Probes sorted set expires naturally once the last lease elapses.
redis.call('PEXPIREAT', KEYS[2], now + probeLeaseTtl + 1000)
return {1, 2, gen, activeProbes, transitioned}
`

/**
 * Record a probe attempt outcome.  Removes the probe token and applies the
 * result: failure re-opens; sufficient successes close the breaker.
 *
 * KEYS[1] = breaker state hash         (:breaker)
 * KEYS[2] = probe tokens sorted set    (:probes)
 *
 * ARGV[1] = probeToken          the token issued by admitProbe
 * ARGV[2] = outcome             "success" | "failure" | "ignored", where
 *                               "ignored" releases the slot without recording
 *                               an outcome or advancing recovery
 * ARGV[3] = expectedGeneration  generation captured at admission time
 * ARGV[4] = halfOpenSuccesses   consecutive probe successes needed to close
 * ARGV[5] = openMs              how long the breaker stays OPEN (TTL floor)
 * ARGV[6] = windowTtlMs         observation retention (TTL floor): the CLOSED
 *                               hash must outlive the window it governs
 *
 * Returns: {status, stateCode, generation}
 *   status 0 = stale  (token missing, lease elapsed, or generation mismatch;
 *              the result is dropped and any consumed token is released)
 *   status 1 = settled, no state transition  (still HALF_OPEN; an "ignored"
 *              outcome only releases the probe slot)
 *   status 2 = settled, state transition occurred
 *     stateCode 0 = transitioned to CLOSED
 *     stateCode 1 = transitioned back to OPEN  (probe failure)
 *
 * A token carries its own deadline as its sorted-set score.  When the lease has
 * elapsed the slot is already recoverable - admitProbe prunes expired tokens
 * and re-issues the slot - so the result must not count even while the member is
 * still present.  The deadline is therefore checked under the same atomic call.
 */
export const breakerSettleProbeV1 = `
local probeToken  = ARGV[1]
local outcome     = ARGV[2]
local expectGen   = tonumber(ARGV[3])
local halfOpenSucc = tonumber(ARGV[4])
local openMs      = tonumber(ARGV[5])
local windowTtl   = tonumber(ARGV[6])
local t   = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local f = redis.call('HMGET', KEYS[1], 'state', 'generation', 'probeSuccesses', 'probeCount')
local state     = f[1] or 'closed'
local gen       = tonumber(f[2]) or 0
local probeSucc = tonumber(f[3]) or 0
local probeCnt  = tonumber(f[4]) or 0
local sc = (state == 'open' and 1) or (state == 'half-open' and 2) or 0
local deadline = redis.call('ZSCORE', KEYS[2], probeToken)
if not deadline then
  -- Never admitted, already settled, or dropped with its recovery window.
  return {0, sc, gen}
end
redis.call('ZREM', KEYS[2], probeToken)
if tonumber(deadline) <= now then
  -- The lease elapsed while the probe was still running: its slot was
  -- recoverable, so the result cannot be counted.  Release the slot it still
  -- occupied so the probe count keeps matching the token set.
  probeCnt = math.max(0, probeCnt - 1)
  redis.call('HSET', KEYS[1], 'probeCount', probeCnt)
  return {0, sc, gen}
end
if state ~= 'half-open' or gen ~= expectGen then
  return {0, sc, gen}
end
probeCnt = math.max(0, probeCnt - 1)
if outcome == 'ignored' then
  -- The result was not recorded (the classifier ignored it), but the slot the
  -- probe held must still be released: otherwise the recovery window stalls
  -- until the probe lease elapses instead of letting the next probe through.
  -- The local breaker releases its slot immediately, and this matches it: no
  -- success counter advances and the state does not transition.
  redis.call('HSET', KEYS[1], 'probeCount', probeCnt)
  -- Still HALF_OPEN: must not expire while recovery is in progress.
  redis.call('PERSIST', KEYS[1])
  return {1, 2, gen}
end
if outcome == 'failure' then
  gen = gen + 1
  redis.call('HSET', KEYS[1], 'state', 'open', 'generation', gen,
             'openedAt', now, 'probeCount', 0, 'probeSuccesses', 0)
  -- Re-opened: OPEN must not expire (see breakerObserveV1 TTL policy).
  redis.call('PERSIST', KEYS[1])
  -- In-flight probes belong to the dead generation; free their slots so the
  -- next recovery window starts with full capacity.
  redis.call('DEL', KEYS[2])
  return {2, 1, gen}
end
probeSucc = probeSucc + 1
if probeSucc >= halfOpenSucc then
  gen = gen + 1
  redis.call('HSET', KEYS[1], 'state', 'closed', 'generation', gen,
             'probeCount', 0, 'probeSuccesses', 0)
  -- Newly CLOSED: expire for cleanup, but never before the window it governs.
  redis.call('PEXPIRE', KEYS[1], math.max(openMs * 2, windowTtl))
  redis.call('DEL', KEYS[2])
  return {2, 0, gen}
end
redis.call('HSET', KEYS[1], 'probeSuccesses', probeSucc, 'probeCount', probeCnt)
-- Still HALF_OPEN: must not expire while recovery is in progress.
redis.call('PERSIST', KEYS[1])
return {1, 2, gen}
`
