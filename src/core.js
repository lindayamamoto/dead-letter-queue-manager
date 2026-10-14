/**
 * Core implementation for dead-letter-queue-manager.
 *
 * Design decisions made explicit (the brief left these open):
 *
 * - Capacity applies to total entries across all topics. This keeps memory bounded
 *   without forcing the caller to reason per-topic. If you need per-topic caps,
 *   instantiate one manager per topic.
 * - Retry happens on the ORIGINAL topic the event was rejected from. Cross-topic
 *   re-routing is deliberately out of scope; it hides failure causes from consumers
 *   who subscribed to a specific topic.
 * - backoffMs receives (attempt, enqueueMs). Growing delays are the caller's
 *   responsibility; the default doubles the previous delay, capped at 5 minutes.
 * - When a handler runs (manual replay), success removes the entry permanently.
 *   Failure updates retry metadata in place rather than creating a new entry,
 *   preserving the chronological failure history of the original event.
 */

/**
 * @typedef {(attempt: number, enqueueMs: number) => number} BackoffFn
 * @typedef {(topic: string, event: unknown) => boolean} HandlerFn
 * @typedef {{ topic: string, event: unknown, error: string, attempt: number, enqueueMs: number, nextAttemptMs: number }} QueueEntry
 * @typedef {{ topic: string, id: string, event: unknown, error: string, attempt: number, enqueueMs: number, nextAttemptMs: number, ageMs: number }} InspectEntry
 * @typedef {{ dead: InspectEntry[] } & ({ success: false, error: string } | { success: true, replays: number })} ReplayResult
 */

export const defaultBackoff = (attempt, enqueueMs) => {
  // Cap exponential growth so a perpetually-failing event does not starve
  // later attempts forever. 5 minutes is arbitrary but clearly documented.
  const capMs = 5 * 60 * 1000;
  const baseMs = 1000;
  // Math.pow can overflow to Infinity for large attempt counts; guard it.
  const raw = baseMs * Math.pow(2, Math.max(0, attempt - 1));
  const ms = Number.isFinite(raw) ? raw : capMs;
  return Math.min(ms, capMs) + enqueueMs;
};

let _idCounter = 0;
const newId = () => {
  // Monotonic, collision-free within a single process. No randomness so tests
  // are fully deterministic; we never rely on Date.now() for identity.
  _idCounter = (_idCounter + 1) | 0;
  return `dlq-${_idCounter}`;
};

export class DeadLetterQueueManager {
  /**
   * @param {{
   *   capacity?: number,
   *   backoffMs?: BackoffFn,
   *   clock?: () => number
   * }} [opts]
   */
  constructor(opts = {}) {
    const capacity = opts.capacity ?? 1000;
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new TypeError('capacity must be a positive integer');
    }
    this.capacity = capacity;
    this.backoffMs = opts.backoffMs ?? defaultBackoff;
    // clock defaults to a real wall clock for production use; tests inject
    // a deterministic fake so no assertion ever depends on real time.
    this.clock = opts.clock ?? (() => Date.now());
    /** @type {QueueEntry[]} */
    this.entries = [];
  }

  /** @returns {number} */
  size() {
    return this.entries.length;
  }

  /**
   * Rejects (enqueues) an event that failed processing. Increments attempt
   * if an entry for the same topic+event already exists, preserving history.
   *
   * @param {string} topic
   * @param {unknown} event
   * @param {string} error
   * @returns {string} the entry id
   */
  reject(topic, event, error) {
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new TypeError('topic must be a non-empty string');
    }
    if (typeof error !== 'string') {
      throw new TypeError('error must be a string');
    }
    const now = this.clock();
    const existing = this.entries.find(e => e.topic === topic && Object.is(e.event, event));
    if (existing) {
      existing.attempt += 1;
      existing.error = error;
      existing.enqueueMs = now;
      existing.nextAttemptMs = this.backoffMs(existing.attempt, now);
      return existing.id;
    }
    if (this.entries.length >= this.capacity) {
      // Evict oldest to make room. Head-of-line eviction is simpler and more
      // predictable than LRU for a debugging-oriented queue: callers should
      // drain the queue rather than rely on retention under saturation.
      this.entries.shift();
    }
    const entry = {
      id: newId(),
      topic,
      event,
      error,
      attempt: 1,
      enqueueMs: now,
      nextAttemptMs: this.backoffMs(1, now)
    };
    this.entries.push(entry);
    return entry.id;
  }

  /**
   * Removes and returns entries whose nextAttemptMs is <= now, oldest first.
   * The caller is responsible for attempting actual processing of these events.
   *
   * @returns {InspectEntry[]}
   */
  drainDue() {
    const now = this.clock();
    const due = this.entries.filter(e => e.nextAttemptMs <= now);
    for (const e of due) {
      const idx = this.entries.indexOf(e);
      if (idx >= 0) this.entries.splice(idx, 1);
    }
    return due.map(e => ({
      topic: e.topic,
      id: e.id,
      event: e.event,
      error: e.error,
      attempt: e.attempt,
      enqueueMs: e.enqueueMs,
      nextAttemptMs: e.nextAttemptMs,
      ageMs: now - e.enqueueMs
    }));
  }

  /**
   * Inspects the queue, optionally filtered to a topic. Read-only snapshot.
   *
   * @param {{ topic?: string }} [filter]
   * @returns {InspectEntry[]}
   */
  inspect(filter = {}) {
    const now = this.clock();
    let list = this.entries;
    if (filter.topic !== undefined) {
      list = list.filter(e => e.topic === filter.topic);
    }
    return list.map(e => ({
      topic: e.topic,
      id: e.id,
      event: e.event,
      error: e.error,
      attempt: e.attempt,
      enqueueMs: e.enqueueMs,
      nextAttemptMs: e.nextAttemptMs,
      ageMs: now - e.enqueueMs
    }));
  }

  /**
   * Manually re-invoke a handler for a specific entry. On failure the entry
   * is retained with updated retry metadata rather than duplicated.
   *
   * @param {string} id
   * @param {HandlerFn} handler
   * @returns {ReplayResult}
   */
  replay(id, handler) {
    if (typeof id !== 'string' || id.length === 0) {
      throw new TypeError('id must be a non-empty string');
    }
    if (typeof handler !== 'function') {
      throw new TypeError('handler must be a function');
    }
    const now = this.clock();
    const idx = this.entries.findIndex(e => e.id === id);
    if (idx < 0) {
      return {
        dead: this.inspect(),
        success: false,
        error: 'entry not found'
      };
    }
    const entry = this.entries[idx];
    try {
      const ok = handler(entry.topic, entry.event);
      if (!ok) {
        entry.attempt += 1;
        entry.error = 'handler returned false';
        entry.enqueueMs = now;
        entry.nextAttemptMs = this.backoffMs(entry.attempt, now);
        return {
          dead: this.inspect(),
          success: false,
          error: 'handler returned false'
        };
      }
      this.entries.splice(idx, 1);
      return { dead: this.inspect(), success: true, replays: 1 };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      entry.attempt += 1;
      entry.error = msg;
      entry.enqueueMs = now;
      entry.nextAttemptMs = this.backoffMs(entry.attempt, now);
      return {
        dead: this.inspect(),
        success: false,
        error: msg
      };
    }
  }

  /**
   * Deletes an entry outright (acknowledge as permanently failed, etc.).
   *
   * @param {string} id
   * @returns {boolean} whether an entry was removed
   */
  remove(id) {
    if (typeof id !== 'string' || id.length === 0) {
      throw new TypeError('id must be a non-empty string');
    }
    const idx = this.entries.findIndex(e => e.id === id);
    if (idx < 0) return false;
    this.entries.splice(idx, 1);
    return true;
  }

  /** Removes all entries. */
  clear() {
    this.entries.length = 0;
  }
}
