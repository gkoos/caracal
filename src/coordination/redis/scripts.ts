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

/**
 * Returns the decision and occupancy from the same atomic transition.
 *
 * KEYS[2] is the configuration record. When it exists, its effective limit is
 * authoritative; the constructor limit in ARGV[4] is only the baseline. The
 * reply is {envelopeVersion, allowed, occupancy, revision, changedAt, body?},
 * where body is the record JSON only when the caller knows an older revision.
 */
export const bulkheadLeaseV1 = `
local knownRevision = tonumber(ARGV[5])
local revision = 0
local changedAt = 0
local body = ''
local record = redis.call('GET', KEYS[2])
if record then
  local ok, decoded = pcall(cjson.decode, record)
  if not ok or type(decoded) ~= 'table' or decoded['schemaVersion'] ~= 1 then
    return redis.error_reply('unsupported configuration record')
  end
  revision = decoded['revision']
  changedAt = decoded['changedAt']
  local effective = decoded['effective']
  if type(revision) ~= 'number' or type(changedAt) ~= 'number'
     or type(effective) ~= 'table' or effective['policy'] ~= 'bulkhead' then
    return redis.error_reply('unsupported configuration record')
  end
  local override = tonumber(effective['limit'])
  if not override or override < 1 or override ~= math.floor(override) then
    return redis.error_reply('unsupported configuration record')
  end
  -- leaseV1 reads the limit from ARGV[4]; the record wins over the baseline.
  ARGV[4] = tostring(override)
  if knownRevision ~= revision then body = record end
end
local function transition()
${leaseV1}
end
local allowed = transition()
local occupancy = redis.call('ZCARD', KEYS[1])
if body == '' then
  return {1, allowed, occupancy, revision, changedAt}
end
return {1, allowed, occupancy, revision, changedAt, body}
`

/**
 * Delegated slot allocation for one replica (decisions 7, 9 and 11).
 *
 * The fleet-wide limit is a **sum of allocations**, so the server keeps one
 * counter of granted slots and updates it atomically with every commit. An
 * allocation reserves its slots for its validity plus a grace period, which is what
 * lets a replica reconcile after a lost reply before the capacity is handed to
 * someone else: within the grace window the reservation exists and cannot be
 * reallocated.
 *
 * Renewal is not a grant. It extends the deadline and advances `leaseRevision`
 * without touching the counter. A resize-up reserves only the delta, a resize-down
 * returns slots the caller has already frozen locally, and both are validated
 * against the expected `leaseRevision` so a stale reply cannot undo a newer one.
 *
 * KEYS[1] = allocation hash   (:allocation)        replicaId -> grantId|revision|slots|expiresAt
 * KEYS[2] = reservation expiry(:allocation-expiry) "a|replicaId|grantId|slots" and "o|operationId"
 * KEYS[3] = granted counter   (:allocated)         slots granted across the fleet
 * KEYS[4] = operation ledger  (:allocation-ops)    operationId -> status|reason|slots|revision|expiresAt
 * KEYS[5] = configuration record
 * ARGV[1] = action            allocate | renew | resize | read
 * ARGV[2] = replicaId
 * ARGV[3] = operationId       one modification, including all its retries
 * ARGV[4] = slots             requested (allocate) or signed delta (resize)
 * ARGV[5] = validityMs
 * ARGV[6] = expectedRevision  -1 to skip the compare-and-swap
 * ARGV[7] = limit             baseline fleet-wide ceiling
 * ARGV[8] = graceMs           reservation grace after the validity deadline
 * ARGV[9] = knownRevision     the configuration revision this caller holds
 *
 * Returns: {envelopeVersion, status, reason, allocated, revision, expiresAt,
 *           serverTime, duplicate, configRevision, changedAt, body?}
 *   status 1 = committed, 0 = refused
 *   reason 0 none, 1 limit, 2 revision, 3 no live allocation, 4 one already exists
 *   expiresAt is a server-clock reading; a client derives its own deadline from it
 */
export const bulkheadAllocationV1 = `
local action = ARGV[1]
local replicaId = ARGV[2]
local operationId = ARGV[3]
local amount = tonumber(ARGV[4])
local validityMs = tonumber(ARGV[5])
local expectedRevision = tonumber(ARGV[6])
local limit = tonumber(ARGV[7])
local graceMs = tonumber(ARGV[8])
local knownRevision = tonumber(ARGV[9])
local revision = 0
local changedAt = 0
local body = ''
local record = redis.call('GET', KEYS[5])
if record then
  local ok, decoded = pcall(cjson.decode, record)
  if not ok or type(decoded) ~= 'table' or decoded['schemaVersion'] ~= 1 then
    return redis.error_reply('unsupported configuration record')
  end
  revision = decoded['revision']
  changedAt = decoded['changedAt']
  local effective = decoded['effective']
  if type(revision) ~= 'number' or type(changedAt) ~= 'number'
     or type(effective) ~= 'table' or effective['policy'] ~= 'bulkhead' then
    return redis.error_reply('unsupported configuration record')
  end
  local override = tonumber(effective['limit'])
  if not override or override < 1 or override ~= math.floor(override) then
    return redis.error_reply('unsupported configuration record')
  end
  limit = override
  if knownRevision ~= revision then body = record end
end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

-- A reservation ends at its expiry plus the grace period. Until then the slots are
-- held for the replica that owns them, which is what makes a lost reply
-- reconcilable instead of immediately reallocated.
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
for _, member in ipairs(expired) do
  local kind, rest = string.match(member, '^(%a)|(.*)$')
  if kind == 'a' then
    local replica, grantId, slots = string.match(rest, '^([^|]*)|([^|]*)|(%d+)$')
    if replica and grantId and slots then
      redis.call('DECRBY', KEYS[3], tonumber(slots))
      local held = redis.call('HGET', KEYS[1], replica)
      local heldGrant = held and string.match(held, '^([^|]*)|')
      -- Only the allocation that owned this reservation is forgotten: a replica
      -- that has since allocated again keeps its newer one.
      if heldGrant == grantId then redis.call('HDEL', KEYS[1], replica) end
    end
  elseif kind == 'o' then
    redis.call('HDEL', KEYS[4], rest)
  end
end
if #expired > 0 then redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now) end

local function reply(status, reason, allocated, rev, expiresAt, duplicate)
  if body == '' then
    return {1, status, reason, allocated, rev, expiresAt, now, duplicate, revision, changedAt}
  end
  return {1, status, reason, allocated, rev, expiresAt, now, duplicate, revision, changedAt, body}
end

local function grantedTotal()
  return tonumber(redis.call('GET', KEYS[3]) or '0')
end

local function remember(status, reason, allocated, rev, expiresAt)
  local reservationEnd = math.max(expiresAt, now) + graceMs
  redis.call('HSET', KEYS[4], operationId, status .. '|' .. reason .. '|' .. allocated .. '|' .. rev .. '|' .. expiresAt)
  redis.call('ZADD', KEYS[2], reservationEnd, 'o|' .. operationId)
  local ledgerTtl = redis.call('PTTL', KEYS[4])
  if ledgerTtl < reservationEnd - now then
    redis.call('PEXPIREAT', KEYS[4], reservationEnd)
  end
  -- The reservation set is deliberately not given a TTL. It is the granted counter's
  -- source of truth: if it expired before the next call, that call could not decrement
  -- the counter, and the fleet would lose the capacity for good. The prune below
  -- removes members as they lapse, and Redis drops the key when the last one goes.
  if grantedTotal() <= 0 then redis.call('DEL', KEYS[3]) end
end

local current = redis.call('HGET', KEYS[1], replicaId)
local grantId, curRevision, curAllocated, curExpiresAt = '', 0, 0, 0
if current then
  local heldGrant, heldRevision, heldSlots, heldExpiresAt =
    string.match(current, '^([^|]*)|(%d+)|(%d+)|(%d+)$')
  if heldGrant then
    grantId = heldGrant
    curRevision = tonumber(heldRevision)
    curAllocated = tonumber(heldSlots)
    curExpiresAt = tonumber(heldExpiresAt)
  end
end

-- A committed modification is idempotent: a retry returns its recorded result and
-- changes nothing, so an ambiguous reply cannot be applied twice.
local recorded = redis.call('HGET', KEYS[4], operationId)
if recorded and action ~= 'read' then
  local status, reason, allocated, rev, expiresAt =
    string.match(recorded, '^(%d+)|(%d+)|(%d+)|(%d+)|(%d+)$')
  return reply(tonumber(status), tonumber(reason), tonumber(allocated), tonumber(rev), tonumber(expiresAt), 1)
end

if action == 'read' then
  return reply(1, 0, curAllocated, curRevision, curExpiresAt, 0)
end

if action == 'allocate' then
  if grantId ~= '' then return reply(0, 4, curAllocated, curRevision, curExpiresAt, 0) end
  local granted = math.min(amount, limit - grantedTotal())
  if granted <= 0 then return reply(0, 1, 0, 0, 0, 0) end
  local expiresAt = now + validityMs
  -- The allocation is identified by the operation that created it, so there is no
  -- way to create a second allocation for one replica without retiring the first.
  redis.call('HSET', KEYS[1], replicaId, operationId .. '|1|' .. granted .. '|' .. expiresAt)
  redis.call('ZADD', KEYS[2], expiresAt + graceMs, 'a|' .. replicaId .. '|' .. operationId .. '|' .. granted)
  redis.call('INCRBY', KEYS[3], granted)
  remember(1, 0, granted, 1, expiresAt)
  return reply(1, 0, granted, 1, expiresAt, 0)
end

if grantId == '' or now > curExpiresAt then
  return reply(0, 3, curAllocated, curRevision, curExpiresAt, 0)
end
if expectedRevision >= 0 and expectedRevision ~= curRevision then
  return reply(0, 2, curAllocated, curRevision, curExpiresAt, 0)
end

if action == 'renew' then
  local expiresAt = now + validityMs
  local rev = curRevision + 1
  redis.call('HSET', KEYS[1], replicaId, grantId .. '|' .. rev .. '|' .. curAllocated .. '|' .. expiresAt)
  redis.call('ZREM', KEYS[2], 'a|' .. replicaId .. '|' .. grantId .. '|' .. curAllocated)
  redis.call('ZADD', KEYS[2], expiresAt + graceMs, 'a|' .. replicaId .. '|' .. grantId .. '|' .. curAllocated)
  remember(1, 0, curAllocated, rev, expiresAt)
  return reply(1, 0, curAllocated, rev, expiresAt, 0)
end

if action == 'resize' then
  local nextSlots = curAllocated
  if amount > 0 then
    local granted = math.min(amount, limit - grantedTotal())
    if granted <= 0 then return reply(0, 1, curAllocated, curRevision, curExpiresAt, 0) end
    nextSlots = curAllocated + granted
  elseif amount < 0 then
    nextSlots = curAllocated - math.min(-amount, curAllocated)
  end
  local rev = curRevision + 1
  redis.call('ZREM', KEYS[2], 'a|' .. replicaId .. '|' .. grantId .. '|' .. curAllocated)
  redis.call('INCRBY', KEYS[3], nextSlots - curAllocated)
  if nextSlots == 0 then
    -- Returned in full: the allocation is retired and its reservation is gone, so
    -- the capacity is reallocatable from this commit onwards.
    redis.call('HDEL', KEYS[1], replicaId)
  else
    redis.call('HSET', KEYS[1], replicaId, grantId .. '|' .. rev .. '|' .. nextSlots .. '|' .. curExpiresAt)
    redis.call('ZADD', KEYS[2], curExpiresAt + graceMs, 'a|' .. replicaId .. '|' .. grantId .. '|' .. nextSlots)
  end
  remember(1, 0, nextSlots, rev, curExpiresAt)
  return reply(1, 0, nextSlots, rev, curExpiresAt, 0)
end

return redis.error_reply('unsupported allocation action')
`

// ---------------------------------------------------------------------------
// Rate-limit script
//
// Generic Cell Rate Algorithm (GCRA) over a single state key. The stored value
// is the theoretical arrival time (TAT) of the next cell, an absolute ms
// timestamp on the server clock. A TAT in the past is reset to `now`, so an
// idle scope always admits immediately and the key can be expired the moment
// its value stops influencing future admissions.
//
// KEYS[1] = rate state key  (:rate)
// KEYS[2] = configuration record
// ARGV[1] = emissionIntervalMs  min time between cells at the sustained rate
// ARGV[2] = burstDelayMs        how far ahead of schedule an arrival may be
// ARGV[3] = knownRevision       the configuration revision this caller holds
//
// Returns: {envelopeVersion, allowed, retryAfterMs, revision, changedAt, body?}
//   allowed 1 = admitted; the TAT advanced and was stored
//   allowed 0 = rejected; retryAfterMs is when the next arrival would be
//               admissible (always > 0 on rejection)
//   body is the configuration record, only when ARGV[3] differs from it
// ---------------------------------------------------------------------------

export const rateLimitV1 = `
local emission = tonumber(ARGV[1])
local burstDelay = tonumber(ARGV[2])
local knownRevision = tonumber(ARGV[3])
local revision = 0
local changedAt = 0
local body = ''
local record = redis.call('GET', KEYS[2])
if record then
  local ok, decoded = pcall(cjson.decode, record)
  if not ok or type(decoded) ~= 'table' or decoded['schemaVersion'] ~= 1 then
    return redis.error_reply('unsupported configuration record')
  end
  revision = decoded['revision']
  changedAt = decoded['changedAt']
  local effective = decoded['effective']
  if type(revision) ~= 'number' or type(changedAt) ~= 'number'
     or type(effective) ~= 'table' or effective['policy'] ~= 'rateLimit' then
    return redis.error_reply('unsupported configuration record')
  end
  local overrideEmission = tonumber(effective['emissionIntervalMs'])
  local overrideBurst = tonumber(effective['burstDelayMs'])
  if not overrideEmission or overrideEmission < 1
     or overrideEmission ~= math.floor(overrideEmission)
     or not overrideBurst or overrideBurst < 0
     or overrideBurst ~= math.floor(overrideBurst) then
    return redis.error_reply('unsupported configuration record')
  end
  emission = overrideEmission
  burstDelay = overrideBurst
  if knownRevision ~= revision then body = record end
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
local anchored = math.max(tat, now)
local allowed = 0
local retryAfterMs = 0
if anchored - now > burstDelay then
  retryAfterMs = anchored - burstDelay - now
else
  local nextTat = anchored + emission
  redis.call('SET', KEYS[1], nextTat)
  redis.call('PEXPIREAT', KEYS[1], nextTat)
  allowed = 1
end
if body == '' then
  return {1, allowed, retryAfterMs, revision, changedAt}
end
return {1, allowed, retryAfterMs, revision, changedAt, body}
`

/**
 * Issues a bounded quota grant against the same GCRA state `rateLimitV1` uses.
 *
 * A grant of `n` permits is `n` arrivals taken at once, so it is admissible
 * exactly when the bucket holds `n` tokens: the TAT advances by `n * emission`
 * and the burst condition is unchanged. That is what makes the delegated path
 * and the strict path share one bucket instead of counting the same budget
 * twice.
 *
 * Issuance is idempotent on `operationId` for as long as the grant it produced is
 * live, so a retry after an ambiguous reply returns the recorded grant instead of
 * deducting the bucket again. A refusal is not recorded: it deducted nothing, and
 * re-evaluating it is what the caller asked for.
 *
 * KEYS[1] = rate state key          (:rate)         shared with rateLimitV1
 * KEYS[2] = grant ledger            (:grants)       hash: operationId -> granted|validForMs
 * KEYS[3] = live grant expiry       (:grant-expiry) zset: operationId, score = server expiry
 * KEYS[4] = configuration record
 * ARGV[1] = emissionIntervalMs  min time between cells at the sustained rate
 * ARGV[2] = burstDelayMs        how far ahead of schedule an arrival may be
 * ARGV[3] = knownRevision       the configuration revision this caller holds
 * ARGV[4] = operationId         one issuance operation, including all its retries
 * ARGV[5] = requestedPermits
 * ARGV[6] = validForMs          the grant's spend horizon, V
 * ARGV[7] = maximumGrants       live grants allowed, -1 for uncapped
 *
 * Returns: {envelopeVersion, granted, retryAfterMs, validForMs, duplicate,
 *           revision, changedAt, body?}
 *   granted 0 = refused; retryAfterMs is then always > 0
 *   duplicate 1 = a recorded grant was returned, nothing was deducted
 *   body is the configuration record, only when ARGV[3] differs from it
 */
export const rateGrantV1 = `
local emission = tonumber(ARGV[1])
local burstDelay = tonumber(ARGV[2])
local knownRevision = tonumber(ARGV[3])
local operationId = ARGV[4]
local requested = tonumber(ARGV[5])
local validForMs = tonumber(ARGV[6])
local maximumGrants = tonumber(ARGV[7])
local revision = 0
local changedAt = 0
local body = ''
local record = redis.call('GET', KEYS[4])
if record then
  local ok, decoded = pcall(cjson.decode, record)
  if not ok or type(decoded) ~= 'table' or decoded['schemaVersion'] ~= 1 then
    return redis.error_reply('unsupported configuration record')
  end
  revision = decoded['revision']
  changedAt = decoded['changedAt']
  local effective = decoded['effective']
  if type(revision) ~= 'number' or type(changedAt) ~= 'number'
     or type(effective) ~= 'table' or effective['policy'] ~= 'rateLimit' then
    return redis.error_reply('unsupported configuration record')
  end
  local overrideEmission = tonumber(effective['emissionIntervalMs'])
  local overrideBurst = tonumber(effective['burstDelayMs'])
  if not overrideEmission or overrideEmission < 1
     or overrideEmission ~= math.floor(overrideEmission)
     or not overrideBurst or overrideBurst < 0
     or overrideBurst ~= math.floor(overrideBurst) then
    return redis.error_reply('unsupported configuration record')
  end
  emission = overrideEmission
  burstDelay = overrideBurst
  if knownRevision ~= revision then body = record end
end

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

-- A grant is live while now <= its server expiry. Expired grants are neither
-- deduplicated nor outstanding, so they are forgotten here rather than lingering.
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now - 1)
if #expired > 0 then
  redis.call('HDEL', KEYS[2], unpack(expired))
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now - 1)
end

local function reply(granted, retryAfterMs, duplicate)
  if body == '' then
    return {1, granted, retryAfterMs, validForMs, duplicate, revision, changedAt}
  end
  return {1, granted, retryAfterMs, validForMs, duplicate, revision, changedAt, body}
end

local existing = redis.call('HGET', KEYS[2], operationId)
if existing then
  local granted = string.match(existing, '^(%d+)|')
  return reply(tonumber(granted), 0, 1)
end

local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
local anchored = math.max(tat, now)
local available = math.floor((now + burstDelay - anchored) / emission) + 1
local granted = math.min(requested, available)

if granted > 0 and maximumGrants >= 0
   and redis.call('ZCARD', KEYS[3]) >= maximumGrants then
  -- The configured maximum outstanding stock is reached. Refuse, and hint at the
  -- earliest expiry that frees capacity.
  granted = 0
  local earliest = redis.call('ZRANGE', KEYS[3], 0, 0, 'WITHSCORES')
  local retryAfterMs = validForMs
  if earliest[2] then retryAfterMs = tonumber(earliest[2]) - now end
  if retryAfterMs < 1 then retryAfterMs = 1 end
  return reply(0, retryAfterMs, 0)
end

if granted <= 0 then
  return reply(0, anchored - burstDelay - now, 0)
end

local nextTat = anchored + granted * emission
redis.call('SET', KEYS[1], nextTat)
redis.call('PEXPIREAT', KEYS[1], nextTat)

-- Retained until the grant can no longer be adopted: a client's send-time-anchored
-- deadline is at or before the server expiry it was derived from.
local expiresAt = now + validForMs
redis.call('HSET', KEYS[2], operationId, granted .. '|' .. validForMs)
redis.call('ZADD', KEYS[3], expiresAt, operationId)
local ledgerTtl = redis.call('PTTL', KEYS[2])
if ledgerTtl < expiresAt - now then
  redis.call('PEXPIREAT', KEYS[2], expiresAt)
end
local expiryTtl = redis.call('PTTL', KEYS[3])
if expiryTtl < expiresAt - now then
  redis.call('PEXPIREAT', KEYS[3], expiresAt)
end
return reply(granted, 0, 0)
`

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
 * Returns: {status, stateCode, generation, windowTotal, windowFailures, openedAt}
 *   status 0 = stale  (dropped; generation or state mismatch)
 *   status 1 = observed, no transition  (remained closed)
 *   status 2 = observed, breaker opened (stateCode=1, generation incremented)
 *
 * `openedAt` is the server's clock reading at which the breaker opened, or 0 when
 * the reply is not about an open breaker.  It is the authoritative origin of the
 * recovery cooldown, so a replica that saw the transition can compute its own
 * cooldown from the server's time instead of guessing from the receipt time.
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
/**
 * Shared configuration read for the breaker scripts.
 *
 * KEYS[3] is the configuration record. `config` is nil when no record exists;
 * `override` returns nil for a value the script cannot use, so each call site
 * turns that into an error reply.
 */
export const breakerConfigurationV1 = `
local revision = 0
local changedAt = 0
local body = ''
local record = redis.call('GET', KEYS[3])
local config = nil
if record then
  local ok, decoded = pcall(cjson.decode, record)
  if not ok or type(decoded) ~= 'table' or decoded['schemaVersion'] ~= 1
     or type(decoded['effective']) ~= 'table'
     or decoded['effective']['policy'] ~= 'circuitBreaker' then
    return redis.error_reply('unsupported configuration record')
  end
  revision = decoded['revision']
  changedAt = decoded['changedAt']
  if type(revision) ~= 'number' or type(changedAt) ~= 'number' then
    return redis.error_reply('unsupported configuration record')
  end
  config = decoded['effective']
  if knownRevision ~= revision then body = record end
end
local function override(name, minimum, maximum)
  local value = tonumber(config[name])
  if not value or value ~= math.floor(value) or value < minimum then
    return nil
  end
  if maximum and value > maximum then
    return nil
  end
  return value
end
`

export const breakerObserveV1 = `
local outcome    = ARGV[1]
local expectGen  = tonumber(ARGV[2])
local windowTtl  = tonumber(ARGV[3])
local minTP      = tonumber(ARGV[4])
local threshNum  = tonumber(ARGV[5])
local windowSize = tonumber(ARGV[6])
local closedCleanup = tonumber(ARGV[7])
local uuid       = ARGV[8]
local knownRevision = tonumber(ARGV[9])
${breakerConfigurationV1}
if config then
  local value = override('minimumThroughput', 1)
  if not value then return redis.error_reply('unsupported configuration record') end
  minTP = value
  value = override('failureThresholdNumerator', 1, 999)
  if not value then return redis.error_reply('unsupported configuration record') end
  threshNum = value
  value = override('windowSize', 1, 10000)
  if not value then return redis.error_reply('unsupported configuration record') end
  windowSize = value
end
local function decide()
local t   = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local f   = redis.call('HMGET', KEYS[1], 'state', 'generation', 'openedAt')
local state = f[1] or 'closed'
local gen   = tonumber(f[2]) or 0
local openedAt = tonumber(f[3]) or 0
if state ~= 'closed' then
  local sc = (state == 'open' and 1) or (state == 'half-open' and 2) or 0
  return {0, sc, gen, 0, 0, openedAt}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  -- No live state hash: a scope we have never seen, or a hash that was lost
  -- while its window survived.
  if expectGen ~= 0 then
    -- The caller holds a generation this key cannot confirm.
    return {0, 0, 0, 0, 0, 0}
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
  return {0, 0, gen, 0, 0, 0}
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
  return {2, 1, gen, wTotal, wFail, now}
end
if redis.call('EXISTS', KEYS[1]) == 1 then
  -- Still CLOSED.  The cleanup TTL must not expire the state before the window
  -- it governs, otherwise retained members would survive their epoch.
  redis.call('PEXPIRE', KEYS[1], math.max(closedCleanup, windowTtl))
end
return {1, 0, gen, wTotal, wFail, 0}
end
local result = decide()
local out = {1, revision, changedAt}
for i = 1, #result do out[#out+1] = result[i] end
if body ~= '' then out[#out+1] = body end
return out
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
local knownRevision = tonumber(ARGV[5])
${breakerConfigurationV1}
if config then
  local value = override('openMs', 1)
  if not value then return redis.error_reply('unsupported configuration record') end
  openMs = value
  value = override('halfOpenProbes', 1)
  if not value then return redis.error_reply('unsupported configuration record') end
  maxProbes = value
end
local function decide()
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
end
local result = decide()
local out = {1, revision, changedAt}
for i = 1, #result do out[#out+1] = result[i] end
if body ~= '' then out[#out+1] = body end
return out
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
 * Returns: {status, stateCode, generation, openedAt}
 *   status 0 = stale  (token missing, lease elapsed, or generation mismatch;
 *              the result is dropped and any consumed token is released)
 *   status 1 = settled, no state transition  (still HALF_OPEN; an "ignored"
 *              outcome only releases the probe slot)
 *   status 2 = settled, state transition occurred
 *     stateCode 0 = transitioned to CLOSED
 *     stateCode 1 = transitioned back to OPEN  (probe failure)
 *
 * `openedAt` is the server's clock reading at which the breaker re-opened, or 0
 * when this settle did not open it.  It is the authoritative origin of the
 * recovery cooldown for the replica that observed the transition.
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
local closedCleanup = tonumber(ARGV[5])
local windowTtl   = tonumber(ARGV[6])
local knownRevision = tonumber(ARGV[7])
${breakerConfigurationV1}
if config then
  local value = override('halfOpenSuccesses', 1)
  if not value then return redis.error_reply('unsupported configuration record') end
  halfOpenSucc = value
end
local function decide()
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
  return {0, sc, gen, 0}
end
redis.call('ZREM', KEYS[2], probeToken)
if tonumber(deadline) <= now then
  -- The lease elapsed while the probe was still running: its slot was
  -- recoverable, so the result cannot be counted.  Release the slot it still
  -- occupied so the probe count keeps matching the token set.
  probeCnt = math.max(0, probeCnt - 1)
  redis.call('HSET', KEYS[1], 'probeCount', probeCnt)
  return {0, sc, gen, 0}
end
if state ~= 'half-open' or gen ~= expectGen then
  -- Unreachable while every generation change also discards the probe set: a
  -- token from another generation is already gone by the time it is looked up
  -- above. Kept as a guard in case that invariant is ever broken.
  return {0, sc, gen, 0}
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
  return {1, 2, gen, 0}
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
  return {2, 1, gen, now}
end
probeSucc = probeSucc + 1
if probeSucc >= halfOpenSucc then
  gen = gen + 1
  redis.call('HSET', KEYS[1], 'state', 'closed', 'generation', gen,
             'probeCount', 0, 'probeSuccesses', 0)
  -- Newly CLOSED: expire for cleanup, but never before the window it governs.
  redis.call('PEXPIRE', KEYS[1], math.max(closedCleanup, windowTtl))
  redis.call('DEL', KEYS[2])
  return {2, 0, gen, 0}
end
redis.call('HSET', KEYS[1], 'probeSuccesses', probeSucc, 'probeCount', probeCnt)
-- Still HALF_OPEN: must not expire while recovery is in progress.
redis.call('PERSIST', KEYS[1])
return {1, 2, gen, 0}
end
local result = decide()
local out = {1, revision, changedAt}
for i = 1, #result do out[#out+1] = result[i] end
if body ~= '' then out[#out+1] = body end
return out
`

/**
 * Read breaker state and configuration metadata in one command.
 *
 * KEYS[1] = breaker state hash
 * KEYS[2] = configuration record
 * ARGV[1] = knownRevision
 *
 * Returns {envelope, revision, changedAt, present, stateCode, generation, openedAt, body?}.
 * `present` is 0 when the state hash does not exist, so a missing runtime hash
 * still reports configuration metadata instead of a bare null.
 */
export const breakerReadStateV1 = `
local knownRevision = tonumber(ARGV[1])
local revision = 0
local changedAt = 0
local body = ''
local record = redis.call('GET', KEYS[2])
if record then
  local ok, decoded = pcall(cjson.decode, record)
  if not ok or type(decoded) ~= 'table' or decoded['schemaVersion'] ~= 1
     or type(decoded['effective']) ~= 'table'
     or decoded['effective']['policy'] ~= 'circuitBreaker' then
    return redis.error_reply('unsupported configuration record')
  end
  revision = decoded['revision']
  changedAt = decoded['changedAt']
  if type(revision) ~= 'number' or type(changedAt) ~= 'number' then
    return redis.error_reply('unsupported configuration record')
  end
  if knownRevision ~= revision then body = record end
end
local f = redis.call('HMGET', KEYS[1], 'state', 'generation', 'openedAt')
local state = f[1]
if not state then
  return {1, revision, changedAt, 0, 0, 0, 0}
end
local code = (state == 'open' and 1) or (state == 'half-open' and 2) or 0
local out = {1, revision, changedAt, 1, code, tonumber(f[2]) or 0, tonumber(f[3]) or 0}
if body ~= '' then out[#out+1] = body end
return out
`

/**
 * Read the configuration record for one identity.
 * KEYS[1] = configuration record
 * Returns the stored JSON string, or false when absent.
 */
export const configurationReadV1 = `
return redis.call('GET', KEYS[1])
`

/**
 * Apply one configuration update.
 * KEYS[1] = configuration record
 * ARGV[1] = expected revision (0 when the caller believes no record exists)
 * ARGV[2] = record template with revision 0 and changedAt 0
 * ARGV[3] = fingerprint of the normalized settings
 * Returns {status, revision, changedAt, body}: 0 unchanged, 1 conflict, 2 applied.
 */
export const configurationUpdateV1 = `
local expected = tonumber(ARGV[1])
local template = ARGV[2]
local fingerprint = ARGV[3]
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local current = redis.call('GET', KEYS[1])
if current then
  local record = cjson.decode(current)
  if type(record) ~= 'table' or record['schemaVersion'] ~= 1 then
    return redis.error_reply('unsupported configuration record')
  end
  if record['fingerprint'] == fingerprint then
    return {0, record['revision'], record['changedAt'], current}
  end
  if expected ~= record['revision'] then
    return {1, record['revision'], record['changedAt'], current}
  end
  local nextRecord = cjson.decode(template)
  nextRecord['revision'] = record['revision'] + 1
  nextRecord['changedAt'] = now
  local body = cjson.encode(nextRecord)
  redis.call('SET', KEYS[1], body)
  return {2, nextRecord['revision'], now, body}
end
if expected ~= 0 then
  return {1, 0, 0, ''}
end
local nextRecord = cjson.decode(template)
nextRecord['revision'] = 1
nextRecord['changedAt'] = now
local body = cjson.encode(nextRecord)
redis.call('SET', KEYS[1], body)
return {2, 1, now, body}
`
