import 'server-only'

import {
  apiKeyHash,
  getModelUsage,
  setModelExhausted,
  isModelDailyQuotaExhausted,
  geminiUsageDay,
  checkDailyReset,
} from './store'
import { pacingIntervalMs, RATE_COOLDOWN_MS, CHUNK_COOLDOWN_MS, displayModelName } from './models'

export interface CandidateLane {
  apiKey: string
  keyIdx: number
  modelId: string
  slot?: number
  rpd?: number
}

interface LaneWaiter {
  scanId: string
  scanTitle: string
  operation: string
  resolve: (releaseFn: (actualVideoSec?: number, cooldownOverrideMs?: number) => void) => void
  reject: (err: Error) => void
  isStopping?: () => boolean
}

interface GlobalLaneState {
  laneKey: string
  keyHash: string
  keyIdx: number
  modelId: string
  slot: number
  // ATOMIC RESERVATION LEDGER (guarantees zero collision before upload and execution)
  reservedByScanId: string | null
  reservedScanTitle: string | null
  reservedOperation: string | null
  reservedAt: number | null
  activeScanId: string | null
  activeScanTitle: string | null
  activeOperation: string | null
  activeSince: number | null
  lastCompletedAt?: number | null
  lastOperation: string | null
  lastOperationVideoSec: number | null
  nextFreeAt: number
  cooldownUntil: number
  consecutiveQuotaErrors?: number
  isExhausted: boolean
  waiters: LaneWaiter[]
}

interface PendingAssignmentRequest {
  reqId: string
  scanId: string
  scanTitle: string
  candidates: CandidateLane[]
  operation: string
  videoSeconds: number
  onWait?: (msg: string, waitSec: number, candidateSummary: string) => void
  isStopping?: () => boolean
  resolve: (res: {
    selected: CandidateLane
    release: (actualVideoSec?: number, cooldownOverrideMs?: number) => void
  }) => void
  reject: (err: Error) => void
  createdAt: number
  lastLoggedWaitMsg?: string
}

class GlobalGeminiCoordinator {
  private lanes = new Map<string, GlobalLaneState>()
  private currentActiveDay = geminiUsageDay()
  private pendingAssignmentQueue: PendingAssignmentRequest[] = []
  private assignmentDispatchTimer: NodeJS.Timeout | null = null
  private isDispatchingAssignments = false

  /**
   * Checks if the date has rolled over (midnight Pacific Time).
   * Automatically clears all exhaustion flags across all lanes so the new day's quota is instantly active!
   */
  public checkDayRollover(): boolean {
    const today = geminiUsageDay()
    if (today !== this.currentActiveDay) {
      console.log(`[Global Coordinator] Daily quota rollover detected (${this.currentActiveDay} -> ${today}). Resetting all lane exhaustion flags!`)
      this.currentActiveDay = today
      for (const lane of this.lanes.values()) {
        lane.isExhausted = false
        lane.cooldownUntil = 0
      }
      checkDailyReset()
      return true
    }
    return false
  }

  /**
   * Instant, zero-wait quota check:
   * Verifies if a model on a given API key has exhausted its daily quota (RPD)
   * using coordinator in-memory lane state and cached counters.json.
   */
  public isModelExhausted(apiKey: string, modelId: string, rpdCap: number = 500): boolean {
    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, 0)
    const exhaustedInStore = isModelDailyQuotaExhausted(modelId, apiKey, rpdCap)
    if (!exhaustedInStore) {
      lane.isExhausted = false
      return false
    }
    lane.isExhausted = true
    return true
  }

  private getLaneKey(apiKey: string, modelId: string, slot: number = 0): string {
    return `${apiKeyHash(apiKey)}:${modelId}:${slot}`
  }

  private getOrCreateLane(apiKey: string, modelId: string, slot: number = 0, keyIdx: number = 1): GlobalLaneState {
    const key = this.getLaneKey(apiKey, modelId, slot)
    let lane = this.lanes.get(key)
    if (!lane) {
      lane = {
        laneKey: key,
        keyHash: apiKeyHash(apiKey),
        keyIdx,
        modelId,
        slot,
        reservedByScanId: null,
        reservedScanTitle: null,
        reservedOperation: null,
        reservedAt: null,
        activeScanId: null,
        activeScanTitle: null,
        activeOperation: null,
        activeSince: null,
        lastCompletedAt: null,
        lastOperation: null,
        lastOperationVideoSec: null,
        nextFreeAt: 0,
        cooldownUntil: 0,
        isExhausted: false,
        waiters: [],
      }
      this.lanes.set(key, lane)
    }
    if (keyIdx > 0) lane.keyIdx = keyIdx
    return lane
  }

  /**
   * Strict Invariant Assertion:
   * Guarantees zero overlap/collision. If two scans simultaneously try to reserve or activate the same laneKey,
   * this immediately throws an invariant violation error!
   */
  public assertLaneInvariant(lane: GlobalLaneState, scanId: string, action: 'reserve' | 'activate'): void {
    if (action === 'reserve') {
      if (lane.reservedByScanId !== null && lane.reservedByScanId !== scanId) {
        throw new Error(
          `[Coordinator Invariant Breach] Lane "${lane.laneKey}" already reserved by scan "${lane.reservedByScanId}", cannot be reserved by "${scanId}"`
        )
      }
    } else if (action === 'activate') {
      if (lane.reservedByScanId !== null && lane.reservedByScanId !== scanId) {
        throw new Error(
          `[Coordinator Invariant Breach] Lane "${lane.laneKey}" is reserved by scan "${lane.reservedByScanId}", cannot be activated by "${scanId}"`
        )
      }
      if (lane.activeScanId !== null && lane.activeScanId !== scanId) {
        throw new Error(
          `[Coordinator Invariant Breach] Lane "${lane.laneKey}" is already active in scan "${lane.activeScanId}", cannot be activated by "${scanId}"`
        )
      }
    }
  }

  /** Check if a lane is currently in use by ANY scan, reserved, in cooldown/pacing, or exhausted */
  public isLaneBusy(apiKey: string, modelId: string, slot: number = 0, rpdCap: number = 500, currentScanId?: string): {
    busy: boolean
    exhausted?: boolean
    reserved?: boolean
    activeScanId?: string
    activeScanTitle?: string
    activeOperation?: string
    waitSec?: number
    cooling?: boolean
  } {
    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    const now = Date.now()

    if (lane.isExhausted || isModelDailyQuotaExhausted(modelId, apiKey, rpdCap)) {
      lane.isExhausted = true
      return {
        busy: true,
        exhausted: true,
        activeOperation: 'Exhausted for today',
      }
    }

    // Check if reserved by another scan
    if (lane.reservedByScanId && (!currentScanId || lane.reservedByScanId !== currentScanId)) {
      // Auto-expire stale reservation if held > 5 minutes without activating
      if (!lane.activeScanId && lane.reservedAt && now - lane.reservedAt > 300_000) {
        lane.reservedByScanId = null
        lane.reservedScanTitle = null
        lane.reservedOperation = null
        lane.reservedAt = null
      } else {
        return {
          busy: true,
          reserved: true,
          activeScanId: lane.reservedByScanId,
          activeScanTitle: lane.reservedScanTitle || undefined,
          activeOperation: lane.reservedOperation || 'Reserved by another scan',
          waitSec: 5,
        }
      }
    }

    if (lane.activeScanId && (!currentScanId || lane.activeScanId !== currentScanId)) {
      return {
        busy: true,
        activeScanId: lane.activeScanId,
        activeScanTitle: lane.activeScanTitle || undefined,
        activeOperation: lane.activeOperation || undefined,
        waitSec: 5,
      }
    }

    if (lane.cooldownUntil > now) {
      return {
        busy: true,
        cooling: true,
        waitSec: Math.ceil((lane.cooldownUntil - now) / 1000),
      }
    }

    if (lane.nextFreeAt > now) {
      return {
        busy: true,
        waitSec: Math.ceil((lane.nextFreeAt - now) / 1000),
      }
    }

    return { busy: false }
  }

  /**
   * Check if an API key has any active or reserved lanes currently executing in another scan.
   * Useful for load-balancing parallel scans so that each scan prefers idle keys.
   */
  public isKeyActiveInOtherScan(apiKey: string, currentScanId: string): boolean {
    const hash = apiKeyHash(apiKey)
    for (const lane of this.lanes.values()) {
      if (lane.keyHash === hash) {
        if (lane.activeScanId !== null && lane.activeScanId !== currentScanId) {
          return true
        }
        if (lane.reservedByScanId !== null && lane.reservedByScanId !== currentScanId) {
          return true
        }
      }
    }
    return false
  }

  /**
   * Reset all in-memory lane exhaustion, cooldown, and reservation flags.
   * Called when user manually resets daily counters via Settings.
   */
  public resetAllLanes(): void {
    for (const lane of this.lanes.values()) {
      lane.isExhausted = false
      lane.cooldownUntil = 0
      lane.nextFreeAt = 0
      lane.reservedByScanId = null
      lane.reservedScanTitle = null
      lane.reservedOperation = null
      lane.reservedAt = null
    }
    this.scheduleAssignmentDispatch(0)
    console.log('[Global Coordinator] All lane exhaustion, cooldown, and reservation states reset.')
  }

  /**
   * Clear an active or held reservation for a scan.
   */
  public clearReservation(lane: GlobalLaneState, scanId: string): void {
    if (lane.reservedByScanId === scanId) {
      lane.reservedByScanId = null
      lane.reservedScanTitle = null
      lane.reservedOperation = null
      lane.reservedAt = null
      this.scheduleAssignmentDispatch(0)
    }
  }

  /**
   * Schedule next Central Assignment Dispatcher tick.
   * Runs in the single-threaded event loop so that all lane assignments
   * are serialized through a single authority — eliminating any race conditions.
   */
  private scheduleAssignmentDispatch(delayMs = 0): void {
    if (this.assignmentDispatchTimer) {
      clearTimeout(this.assignmentDispatchTimer)
      this.assignmentDispatchTimer = null
    }

    if (delayMs <= 0) {
      queueMicrotask(() => this.dispatchAssignments())
    } else {
      this.assignmentDispatchTimer = setTimeout(() => {
        this.assignmentDispatchTimer = null
        this.dispatchAssignments()
      }, delayMs)
    }
  }

  /**
   * Central Scheduler Tick:
   * Processes all pending multi-candidate lane assignment requests in strict FIFO order.
   * Every assignment atomically verifies the zero-collision invariant and locks the lane.
   */
  private dispatchAssignments(): void {
    if (this.isDispatchingAssignments) return
    this.isDispatchingAssignments = true

    try {
      const now = Date.now()
      this.checkDayRollover()

      // 1. Remove stopped/cancelled requests
      for (let i = this.pendingAssignmentQueue.length - 1; i >= 0; i--) {
        const req = this.pendingAssignmentQueue[i]
        if (req.isStopping && req.isStopping()) {
          this.pendingAssignmentQueue.splice(i, 1)
          req.reject(new Error('Stop requested — lane assignment cancelled'))
        }
      }

      if (this.pendingAssignmentQueue.length === 0) return

      let shortestWaitAcrossAllMs = 1000

      // 2. Iterate in strict FIFO order across pending assignment requests
      for (let i = 0; i < this.pendingAssignmentQueue.length; i++) {
        const req = this.pendingAssignmentQueue[i]

        // Clear stale reservations (TTL = 5 minutes)
        for (const lane of this.lanes.values()) {
          if (lane.reservedByScanId && !lane.activeScanId && lane.reservedAt && now - lane.reservedAt > 300_000) {
            console.warn(`[Global Coordinator] Auto-clearing stale reservation on lane ${lane.laneKey} (held by ${lane.reservedByScanId} for >5m)`)
            lane.reservedByScanId = null
            lane.reservedScanTitle = null
            lane.reservedOperation = null
            lane.reservedAt = null
          }
        }

        // Filter available candidates (not permanently exhausted)
        const availableCandidates = req.candidates.filter((c) => {
          return !this.isModelExhausted(c.apiKey, c.modelId, c.rpd || 500)
        })

        if (availableCandidates.length === 0) {
          this.pendingAssignmentQueue.splice(i, 1)
          i--
          req.reject(new Error('All candidate keys/models have reached their daily quota or are exhausted'))
          continue
        }

        // Step 2 — Deterministic, non-overlapping candidate sorting across scans:
        // Priority 1: Lanes that are NOT reserved by another scan AND NOT active in another scan
        // Priority 2: Keys that are NOT active in another scan (idle keys first)
        // Priority 3: Immediately ready lanes (no cooldown, no pacing wait, no waiters)
        // Priority 4: Lower usage
        // Priority 5: Original candidate order
        const sortedCandidates = [...availableCandidates].sort((a, b) => {
          const laneA = this.getOrCreateLane(a.apiKey, a.modelId, a.slot || 0, a.keyIdx)
          const laneB = this.getOrCreateLane(b.apiKey, b.modelId, b.slot || 0, b.keyIdx)

          const aReservedOther = laneA.reservedByScanId !== null && laneA.reservedByScanId !== req.scanId ? 1 : 0
          const bReservedOther = laneB.reservedByScanId !== null && laneB.reservedByScanId !== req.scanId ? 1 : 0
          if (aReservedOther !== bReservedOther) return aReservedOther - bReservedOther

          const aActiveOther = laneA.activeScanId !== null && laneA.activeScanId !== req.scanId ? 1 : 0
          const bActiveOther = laneB.activeScanId !== null && laneB.activeScanId !== req.scanId ? 1 : 0
          if (aActiveOther !== bActiveOther) return aActiveOther - bActiveOther

          const aKeyOther = this.isKeyActiveInOtherScan(a.apiKey, req.scanId) ? 1 : 0
          const bKeyOther = this.isKeyActiveInOtherScan(b.apiKey, req.scanId) ? 1 : 0
          if (aKeyOther !== bKeyOther) return aKeyOther - bKeyOther

          const aWait = Math.max(0, laneA.cooldownUntil - now, laneA.nextFreeAt - now)
          const bWait = Math.max(0, laneB.cooldownUntil - now, laneB.nextFreeAt - now)
          const aReady = aWait === 0 && laneA.activeScanId === null && laneA.waiters.length === 0 ? 0 : 1
          const bReady = bWait === 0 && laneB.activeScanId === null && laneB.waiters.length === 0 ? 0 : 1
          if (aReady !== bReady) return aReady - bReady

          const aUsage = getModelUsage(a.modelId, a.apiKey)
          const bUsage = getModelUsage(b.modelId, b.apiKey)
          if (Math.abs(aUsage - bUsage) >= 5) {
            return aUsage - bUsage
          }

          return req.candidates.indexOf(a) - req.candidates.indexOf(b)
        })

        // Check if any candidate is completely ready right now
        let assignedCandidate: CandidateLane | null = null
        let assignedLane: GlobalLaneState | null = null
        let reqShortestWaitMs = 1000

        for (const cand of sortedCandidates) {
          const lane = this.getOrCreateLane(cand.apiKey, cand.modelId, cand.slot || 0, cand.keyIdx)

          // Skip if lane is reserved by another scan
          if (lane.reservedByScanId !== null && lane.reservedByScanId !== req.scanId) {
            continue
          }

          // Skip if lane is active in another scan
          if (lane.activeScanId !== null && lane.activeScanId !== req.scanId) {
            continue
          }

          const isFree =
            lane.cooldownUntil <= now &&
            lane.nextFreeAt <= now &&
            lane.waiters.length === 0

          if (isFree) {
            assignedCandidate = cand
            assignedLane = lane
            break
          } else {
            const cdWait = Math.max(0, lane.cooldownUntil - now)
            const paceWait = Math.max(0, lane.nextFreeAt - now)
            const wait = Math.max(cdWait, paceWait)
            if (wait > 0 && wait < reqShortestWaitMs) {
              reqShortestWaitMs = wait
            }
          }
        }

        if (assignedCandidate && assignedLane) {
          // ATOMIC ASSIGNMENT & INVARIANT ASSERTION
          this.assertLaneInvariant(assignedLane, req.scanId, 'activate')

          assignedLane.reservedByScanId = req.scanId
          assignedLane.reservedScanTitle = req.scanTitle
          assignedLane.reservedOperation = req.operation
          assignedLane.reservedAt = now

          assignedLane.activeScanId = req.scanId
          assignedLane.activeScanTitle = req.scanTitle
          assignedLane.activeOperation = req.operation
          assignedLane.activeSince = now

          console.log(`[Global Coordinator] [Invariant Validated] Assigned exclusive lane Key ${assignedLane.keyIdx} · ${assignedLane.modelId} to Scan "${req.scanTitle}" (${req.scanId}) for "${req.operation}"`)

          this.pendingAssignmentQueue.splice(i, 1)
          i--

          const targetLane = assignedLane
          const releaseFn = (actualVideoSec?: number, cooldownOverrideMs?: number) => {
            this.releaseLane(targetLane, actualVideoSec ?? req.videoSeconds, cooldownOverrideMs)
          }

          req.resolve({
            selected: assignedCandidate,
            release: releaseFn,
          })
        } else {
          if (reqShortestWaitMs > 0 && reqShortestWaitMs < shortestWaitAcrossAllMs) {
            shortestWaitAcrossAllMs = reqShortestWaitMs
          }

          const waitSec = Math.max(1, Math.ceil(reqShortestWaitMs / 1000))
          const candidateSummary = availableCandidates
            .map((c) => `Key ${c.keyIdx} (${c.modelId})`)
            .slice(0, 4)
            .join(', ')

          const waitMsg = `[Global Coordinator] All candidate lanes busy (${candidateSummary}${availableCandidates.length > 4 ? '...' : ''}). Central coordinator queued scan "${req.scanTitle}" (next free ~${waitSec}s)...`

          if (waitMsg !== req.lastLoggedWaitMsg) {
            req.lastLoggedWaitMsg = waitMsg
            req.onWait?.(waitMsg, waitSec, candidateSummary)
          }
        }
      }

      if (this.pendingAssignmentQueue.length > 0) {
        this.scheduleAssignmentDispatch(Math.min(shortestWaitAcrossAllMs + 20, 1000))
      }
    } finally {
      this.isDispatchingAssignments = false
    }
  }

  /**
   * Acquire an exclusive lock on a (Key × Model × Slot) lane across ALL scans in the entire application.
   * If another scan is using the lane or if the lane is in TPM pacing / 429 cooldown,
   * this will wait and yield gracefully without triggering duplicate requests or 429 collisions.
   */
  public async acquireLane(opts: {
    scanId: string
    scanTitle?: string
    apiKey: string
    keyIdx?: number
    modelId: string
    slot?: number
    operation: string
    videoSeconds?: number
    rpd?: number
    onWait?: (msg: string, waitSec: number) => void
    isStopping?: () => boolean
  }): Promise<(actualVideoSec?: number) => void> {
    const {
      scanId,
      scanTitle = scanId,
      apiKey,
      keyIdx = 1,
      modelId,
      slot = 0,
      operation,
      videoSeconds = 60,
      rpd = 500,
      onWait,
      isStopping,
    } = opts

    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, slot, keyIdx)

    // Pre-flight quota check: if daily quota is already exhausted, abort immediately without waiting or uploading!
    if (this.isModelExhausted(apiKey, modelId, rpd)) {
      lane.isExhausted = true
      throw new Error(`[Global Coordinator] Key ${keyIdx} (${modelId}) daily quota (${rpd} RPD) is exhausted for today. Skipping immediately.`)
    }

    return new Promise<(actualVideoSec?: number, cooldownOverrideMs?: number) => void>((resolve, reject) => {
      const tryAcquireOrQueue = async () => {
        if (isStopping && isStopping()) {
          reject(new Error('Stop requested — lane acquisition cancelled'))
          return
        }

        if (this.isModelExhausted(apiKey, modelId, rpd)) {
          lane.isExhausted = true
          reject(new Error(`[Global Coordinator] Key ${keyIdx} (${modelId}) daily quota (${rpd} RPD) is exhausted for today. Skipping immediately.`))
          return
        }

        const now = Date.now()

        // If lane is currently active in another scan OR reserved by another scan OR there are earlier waiters queued
        const hasOtherActive = lane.activeScanId !== null && lane.activeScanId !== scanId
        const hasOtherReservation = lane.reservedByScanId !== null && lane.reservedByScanId !== scanId
        const isQueuedBehindOthers = lane.waiters.length > 0 && lane.waiters[0]?.scanId !== scanId

        if (hasOtherActive || hasOtherReservation || isQueuedBehindOthers) {
          const waitMsg = hasOtherReservation
            ? `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} is reserved by Scan "${lane.reservedScanTitle || lane.reservedByScanId}". Waiting for lane to become free...`
            : hasOtherActive
            ? `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} is busy in Scan "${lane.activeScanTitle || lane.activeScanId}" (${lane.activeOperation || 'working'}). Waiting for lane to become free...`
            : `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} is queued behind other scans. Waiting turn...`

          onWait?.(waitMsg, 5)

          lane.waiters.push({
            scanId,
            scanTitle,
            operation,
            resolve: (releaseFn) => resolve(releaseFn),
            reject,
            isStopping,
          })
          return
        }

        // Check 429 Cooldown
        if (lane.cooldownUntil > now) {
          const waitMs = lane.cooldownUntil - now
          const waitSec = Math.ceil(waitMs / 1000)
          onWait?.(
            `[Global Coordinator] Key ${lane.keyIdx} · ${displayModelName(modelId)} is in 429 rate cooldown (${waitSec}s remaining). Waiting for rate limit reset...`,
            waitSec,
          )

          setTimeout(() => {
            if (isStopping && isStopping()) {
              reject(new Error('Stop requested during cooldown'))
              return
            }
            void this.processNext(lane)
          }, waitMs + 50)

          lane.waiters.push({
            scanId,
            scanTitle,
            operation,
            resolve: (releaseFn) => resolve(releaseFn),
            reject,
            isStopping,
          })
          return
        }

        // Check TPM pacing interval
        if (lane.nextFreeAt > now) {
          const waitMs = lane.nextFreeAt - now
          const waitSec = Math.ceil(waitMs / 1000)
          onWait?.(
            `[Global Coordinator] Key ${lane.keyIdx} · ${modelId} pacing wait (${waitSec}s for TPM quota). Pacing request...`,
            waitSec,
          )

          setTimeout(() => {
            if (isStopping && isStopping()) {
              reject(new Error('Stop requested during pacing wait'))
              return
            }
            void this.processNext(lane)
          }, waitMs + 20)

          lane.waiters.push({
            scanId,
            scanTitle,
            operation,
            resolve: (releaseFn) => resolve(releaseFn),
            reject,
            isStopping,
          })
          return
        }

        // Lock is free! Assert invariant and acquire exclusively now.
        this.assertLaneInvariant(lane, scanId, 'activate')
        lane.reservedByScanId = scanId
        lane.reservedScanTitle = scanTitle
        lane.reservedOperation = operation
        lane.reservedAt = now

        lane.activeScanId = scanId
        lane.activeScanTitle = scanTitle
        lane.activeOperation = operation
        lane.activeSince = now

        const releaseFn = (actualVideoSec?: number, cooldownOverrideMs?: number) => {
          this.releaseLane(lane, actualVideoSec ?? videoSeconds, cooldownOverrideMs)
        }

        resolve(releaseFn)
      }

      void tryAcquireOrQueue()
    })
  }

  /**
   * PHASE 1 (RESERVATION-FIRST ARCHITECTURE):
   * Atomically reserves a candidate lane for this scan BEFORE uploading clips.
   * Once reserved:
   * - No other scan can target or reserve this lane.
   * - Caller can immediately start uploading clips specifically for this lane's API key.
   * - Caller calls `await waitUntilReady()` when uploads are complete to wait for any remaining cooldown/pacing before sending.
   * - Caller calls `release()` when request completes or fails.
   */
  public async reserveLane(opts: {
    scanId: string
    scanTitle?: string
    candidates: CandidateLane[]
    operation: string
    videoSeconds?: number
    onWait?: (msg: string, waitSec: number, candidateSummary: string) => void
    isStopping?: () => boolean
  }): Promise<{
    selected: CandidateLane
    waitUntilReady: () => Promise<void>
    release: (actualVideoSec?: number, cooldownOverrideMs?: number) => void
  }> {
    const {
      scanId,
      scanTitle = scanId,
      candidates,
      operation,
      videoSeconds = 60,
      onWait,
      isStopping,
    } = opts

    if (!candidates || candidates.length === 0) {
      throw new Error('No candidate lanes provided for execution')
    }

    this.checkDayRollover()

    // Filter available candidates
    const availableCandidates = candidates.filter((c) => {
      return !this.isModelExhausted(c.apiKey, c.modelId, c.rpd || 500)
    })

    if (availableCandidates.length === 0) {
      throw new Error('All candidate keys/models have reached their daily quota or are exhausted')
    }

    // Sort candidates prioritizing:
    // Priority 1: Lanes not reserved or active in another scan
    // Priority 2: Idle keys
    // Priority 3: Shortest remaining wait time
    // Priority 4: Lower usage
    const now = Date.now()
    const sortedCandidates = [...availableCandidates].sort((a, b) => {
      const laneA = this.getOrCreateLane(a.apiKey, a.modelId, a.slot || 0, a.keyIdx)
      const laneB = this.getOrCreateLane(b.apiKey, b.modelId, b.slot || 0, b.keyIdx)

      const aResOther = laneA.reservedByScanId !== null && laneA.reservedByScanId !== scanId ? 1 : 0
      const bResOther = laneB.reservedByScanId !== null && laneB.reservedByScanId !== scanId ? 1 : 0
      if (aResOther !== bResOther) return aResOther - bResOther

      const aActOther = laneA.activeScanId !== null && laneA.activeScanId !== scanId ? 1 : 0
      const bActOther = laneB.activeScanId !== null && laneB.activeScanId !== scanId ? 1 : 0
      if (aActOther !== bActOther) return aActOther - bActOther

      const aKeyOther = this.isKeyActiveInOtherScan(a.apiKey, scanId) ? 1 : 0
      const bKeyOther = this.isKeyActiveInOtherScan(b.apiKey, scanId) ? 1 : 0
      if (aKeyOther !== bKeyOther) return aKeyOther - bKeyOther

      const aWait = Math.max(0, laneA.cooldownUntil - now, laneA.nextFreeAt - now)
      const bWait = Math.max(0, laneB.cooldownUntil - now, laneB.nextFreeAt - now)
      if (aWait !== bWait) return aWait - bWait

      const aUsage = getModelUsage(a.modelId, a.apiKey)
      const bUsage = getModelUsage(b.modelId, b.apiKey)
      return aUsage - bUsage
    })

    // Pick top candidate that is not reserved/active in another scan
    const cand = sortedCandidates.find((c) => {
      const lane = this.getOrCreateLane(c.apiKey, c.modelId, c.slot || 0, c.keyIdx)
      return (
        (lane.reservedByScanId === null || lane.reservedByScanId === scanId) &&
        (lane.activeScanId === null || lane.activeScanId === scanId)
      )
    })

    if (!cand) {
      // If all lanes are currently occupied by other scans, route via central FIFO queue
      const acq = await this.acquireFirstAvailableLane(opts)
      return {
        selected: acq.selected,
        waitUntilReady: async () => {},
        release: acq.release,
      }
    }

    const lane = this.getOrCreateLane(cand.apiKey, cand.modelId, cand.slot || 0, cand.keyIdx)
    this.assertLaneInvariant(lane, scanId, 'reserve')
    lane.reservedByScanId = scanId
    lane.reservedScanTitle = scanTitle
    lane.reservedOperation = operation
    lane.reservedAt = Date.now()

    console.log(`[Global Coordinator] [Reserved Before Upload] Lane Key ${lane.keyIdx} · ${lane.modelId} atomically reserved for Scan "${scanTitle}" (${scanId})`)

    const waitUntilReady = async () => {
      while (true) {
        if (isStopping && isStopping()) {
          this.clearReservation(lane, scanId)
          throw new Error('Stop requested — lane reservation cancelled')
        }

        const tickNow = Date.now()
        const cdWait = Math.max(0, lane.cooldownUntil - tickNow)
        const paceWait = Math.max(0, lane.nextFreeAt - tickNow)
        const totalWait = Math.max(cdWait, paceWait)

        if (totalWait <= 0 && lane.waiters.length === 0 && (lane.activeScanId === null || lane.activeScanId === scanId)) {
          this.assertLaneInvariant(lane, scanId, 'activate')
          lane.activeScanId = scanId
          lane.activeScanTitle = scanTitle
          lane.activeOperation = operation
          lane.activeSince = Date.now()
          return
        }

        onWait?.(
          `[Global Coordinator] Reserved lane Key ${lane.keyIdx} · ${lane.modelId} cooling down (${Math.ceil(totalWait / 1000)}s remaining)...`,
          Math.ceil(totalWait / 1000),
          `Key ${lane.keyIdx} · ${lane.modelId}`
        )

        await new Promise((r) => setTimeout(r, Math.min(totalWait + 20, 1000)))
      }
    }

    const release = (actualVideoSec?: number, cooldownOverrideMs?: number) => {
      if (lane.activeScanId === scanId) {
        this.releaseLane(lane, actualVideoSec ?? videoSeconds, cooldownOverrideMs)
      } else {
        this.clearReservation(lane, scanId)
      }
    }

    return {
      selected: cand,
      waitUntilReady,
      release,
    }
  }

  /**
   * Dynamically search across multiple candidate lanes (different API keys and/or models).
   * Central Assignment Model (Step 3):
   * All requests are serialized through a central coordinator queue.
   * When any lane frees up, it is atomically assigned to the waiting scan in FIFO order,
   * completely eliminating duplicate race conditions and parallel scan collisions!
   */
  public async acquireFirstAvailableLane(opts: {
    scanId: string
    scanTitle?: string
    candidates: CandidateLane[]
    operation: string
    videoSeconds?: number
    onWait?: (msg: string, waitSec: number, candidateSummary: string) => void
    isStopping?: () => boolean
  }): Promise<{
    selected: CandidateLane
    release: (actualVideoSec?: number, cooldownOverrideMs?: number) => void
  }> {
    if (!opts.candidates || opts.candidates.length === 0) {
      throw new Error('No candidate lanes provided for execution')
    }

    return new Promise((resolve, reject) => {
      const req: PendingAssignmentRequest = {
        reqId: `req-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        scanId: opts.scanId,
        scanTitle: opts.scanTitle || opts.scanId,
        candidates: opts.candidates,
        operation: opts.operation,
        videoSeconds: opts.videoSeconds || 60,
        onWait: opts.onWait,
        isStopping: opts.isStopping,
        createdAt: Date.now(),
        resolve,
        reject,
      }

      this.pendingAssignmentQueue.push(req)
      this.dispatchAssignments()
    })
  }

  private releaseLane(lane: GlobalLaneState, videoSeconds: number, cooldownOverrideMs?: number) {
    const paceMs = cooldownOverrideMs !== undefined
      ? cooldownOverrideMs
      : (videoSeconds >= 50 ? CHUNK_COOLDOWN_MS : pacingIntervalMs(videoSeconds))
    const now = Date.now()
    lane.lastCompletedAt = now
    lane.nextFreeAt = now + paceMs
    lane.cooldownUntil = Math.max(lane.cooldownUntil, now + paceMs)
    lane.lastOperation = lane.activeOperation
    lane.lastOperationVideoSec = videoSeconds

    // Atomically release active and reservation locks
    lane.activeScanId = null
    lane.activeScanTitle = null
    lane.activeOperation = null
    lane.activeSince = null
    lane.reservedByScanId = null
    lane.reservedScanTitle = null
    lane.reservedOperation = null
    lane.reservedAt = null

    // Process next waiter in per-lane queue after pacing expires
    if (lane.waiters.length > 0) {
      setTimeout(() => {
        void this.processNext(lane)
      }, paceMs + 20)
    }

    // Immediately trigger central assignment dispatch for pending cross-candidate scans
    this.scheduleAssignmentDispatch(0)

    // Schedule dispatch for when this lane's pacing/cooldown expires so waiting scans take it immediately
    this.scheduleAssignmentDispatch(paceMs + 20)
  }

  private async processNext(lane: GlobalLaneState) {
    if (lane.activeScanId !== null) return // still busy

    while (lane.waiters.length > 0) {
      const next = lane.waiters.shift()
      if (!next) break

      if (next.isStopping && next.isStopping()) {
        next.reject(new Error('Stop requested while queued'))
        continue
      }

      // If lane is reserved by another scan, do not process waiter yet
      if (lane.reservedByScanId !== null && lane.reservedByScanId !== next.scanId) {
        lane.waiters.unshift(next)
        return
      }

      const now = Date.now()
      if (lane.cooldownUntil > now) {
        // Still in cooldown, put back and wait
        lane.waiters.unshift(next)
        const waitMs = lane.cooldownUntil - now
        setTimeout(() => {
          void this.processNext(lane)
        }, waitMs + 50)
        return
      }

      if (lane.nextFreeAt > now) {
        // Still in pacing interval, put back and wait
        lane.waiters.unshift(next)
        const waitMs = lane.nextFreeAt - now
        setTimeout(() => {
          void this.processNext(lane)
        }, waitMs + 20)
        return
      }

      // Lane is free to take. Assert invariant and activate!
      this.assertLaneInvariant(lane, next.scanId, 'activate')
      lane.reservedByScanId = next.scanId
      lane.reservedScanTitle = next.scanTitle
      lane.reservedOperation = next.operation
      lane.reservedAt = Date.now()

      lane.activeScanId = next.scanId
      lane.activeScanTitle = next.scanTitle
      lane.activeOperation = next.operation
      lane.activeSince = Date.now()

      const releaseFn = (actualVideoSec?: number, cooldownOverrideMs?: number) => {
        this.releaseLane(lane, actualVideoSec ?? 60, cooldownOverrideMs)
      }

      next.resolve(releaseFn)
      return
    }
  }

  /** Record successful request on this lane — resets consecutive error counters */
  public recordSuccess(apiKey: string, modelId: string, slot: number = 0) {
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    lane.consecutiveQuotaErrors = 0
  }

  /** Report a 429 Rate Limit error on a lane across the entire app */
  public reportRateLimit(apiKey: string, modelId: string, cooldownMs: number = RATE_COOLDOWN_MS, slot: number = 0) {
    const kh = apiKeyHash(apiKey)
    const now = Date.now()
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    lane.cooldownUntil = Math.max(lane.cooldownUntil, now + cooldownMs)

    // Cooldown only slots for THIS specific model on this API key.
    // Each model has its own independent 250k TPM and 15 RPM quota!
    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.cooldownUntil = Math.max(other.cooldownUntil, now + cooldownMs)
      }
    }

    // Immediately trigger central dispatcher to re-route any pending multi-candidate requests to other idle lanes
    this.scheduleAssignmentDispatch(0)
  }

  /**
   * Smart Quota/Rate Limit handler:
   * 1. Never disable the entire API key! Only isolate this specific model on this key.
   * 2. RPD (daily quota) is the ONLY thing that causes "exhausted for today" (when used >= rpdCap).
   * 3. RPM/TPM 429 rate limits are transient — apply a 1m 10s cooldown (70s) and retry.
   *    consecutiveQuotaErrors is tracked for telemetry, but never marks a lane as exhausted on its own.
   */
  public handleQuotaOrRateError(
    apiKey: string,
    modelId: string,
    slot: number = 0,
    rpdCap: number = 20,
    isExplicitDailyMsg: boolean = false,
  ): {
    action: 'cooldown' | 'exhausted'
    waitSec: number
    reason: string
  } {
    this.checkDayRollover()
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    const used = getModelUsage(modelId, apiKey)

    // The ONLY path to exhausted: actual recorded usage has reached or exceeded the daily cap
    if (used >= rpdCap) {
      this.reportExhausted(apiKey, modelId, slot, rpdCap)
      return {
        action: 'exhausted',
        waitSec: 0,
        reason: `Daily quota limit reached (${used}/${rpdCap} RPD) on ${modelId} (Key ${lane.keyIdx})`,
      }
    }

    lane.consecutiveQuotaErrors = (lane.consecutiveQuotaErrors || 0) + 1

    // Transient rate limit (RPM/TPM 429). Always put ONLY this model in 70s cooldown (CHUNK_COOLDOWN_MS) and retry.
    this.reportRateLimit(apiKey, modelId, CHUNK_COOLDOWN_MS, slot)
    const reason = isExplicitDailyMsg
      ? `Rate/quota spike on ${modelId} (Key ${lane.keyIdx}, used ${used}/${rpdCap} RPD, error: ${lane.consecutiveQuotaErrors}x) — cooling down for 1m 10s before retry`
      : `Temporary rate limit on ${modelId} (Key ${lane.keyIdx}, used ${used}/${rpdCap} RPD, error: ${lane.consecutiveQuotaErrors}x) — cooling down for 1m 10s before retry`

    return {
      action: 'cooldown',
      waitSec: Math.ceil(CHUNK_COOLDOWN_MS / 1000),
      reason,
    }
  }

  /** Report that a model's daily quota has been exhausted across the entire app */
  public reportExhausted(apiKey: string, modelId: string, slot: number = 0, rpdCap: number = 20) {
    const kh = apiKeyHash(apiKey)
    const lane = this.getOrCreateLane(apiKey, modelId, slot)
    lane.isExhausted = true

    // Mark ALL slots for this model on this API key as exhausted!
    for (const other of this.lanes.values()) {
      if (other.keyHash === kh && other.modelId === modelId) {
        other.isExhausted = true
        // Reject all queued waiters on this exhausted lane immediately with an error so they don't wait forever!
        while (other.waiters.length > 0) {
          const waiter = other.waiters.shift()
          if (waiter) {
            waiter.reject(new Error(`[Global Coordinator] Key ${other.keyIdx} (${modelId}) daily quota (${rpdCap} RPD) exhausted`))
          }
        }
      }
    }

    // Persist to counters.json so subsequent workers/processes know this model is quota-capped today
    try {
      setModelExhausted(modelId, apiKey)
    } catch {}

    // Immediately trigger central dispatcher so queued multi-candidate scans switch to other models
    this.scheduleAssignmentDispatch(0)
  }

  /** Get snapshot summary of all active/busy/reserved lanes across the application */
  public getSnapshot(): Array<{
    laneKey: string
    keyIdx: number
    modelId: string
    reservedByScanId: string | null
    reservedScanTitle: string | null
    reservedOperation: string | null
    activeScanId: string | null
    activeScanTitle: string | null
    activeOperation: string | null
    waitingCount: number
    cooling: boolean
    pacingWaitSec: number
  }> {
    const now = Date.now()
    return Array.from(this.lanes.values()).map((l) => ({
      laneKey: l.laneKey,
      keyIdx: l.keyIdx,
      modelId: l.modelId,
      reservedByScanId: l.reservedByScanId,
      reservedScanTitle: l.reservedScanTitle,
      reservedOperation: l.reservedOperation,
      activeScanId: l.activeScanId,
      activeScanTitle: l.activeScanTitle,
      activeOperation: l.activeOperation,
      waitingCount: l.waiters.length,
      cooling: l.cooldownUntil > now,
      pacingWaitSec: Math.max(0, Math.ceil((Math.max(l.nextFreeAt, l.cooldownUntil) - now) / 1000)),
    }))
  }
}

// Global Singleton instance shared across the entire Node.js server process
const globalCoordinatorKey = Symbol.for('__global_gemini_coordinator__')
const globalObj = globalThis as unknown as { [globalCoordinatorKey]?: GlobalGeminiCoordinator }

if (!globalObj[globalCoordinatorKey]) {
  globalObj[globalCoordinatorKey] = new GlobalGeminiCoordinator()
}

export const globalGeminiCoordinator = globalObj[globalCoordinatorKey]!
