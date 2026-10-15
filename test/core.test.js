import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DeadLetterQueueManager, defaultBackoff } from '../src/index.js';

describe('DeadLetterQueueManager construction', () => {
  it('uses default capacity 1000 when not given', () => {
    const m = new DeadLetterQueueManager();
    assert.equal(m.capacity, 1000);
    assert.equal(m.size(), 0);
  });
  it('respects a custom capacity', () => {
    const m = new DeadLetterQueueManager({ capacity: 3 });
    assert.equal(m.capacity, 3);
  });
  it('rejects non-integer capacity', () => {
    assert.throws(() => new DeadLetterQueueManager({ capacity: 2.5 }), TypeError);
  });
  it('rejects capacity below 1', () => {
    assert.throws(() => new DeadLetterQueueManager({ capacity: 0 }), TypeError);
  });
});

describe('reject', () => {
  it('enqueues a new event and returns an id', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const id = m.reject('orders', { id: 1 }, 'boom');
    assert.equal(typeof id, 'string');
    assert.equal(m.size(), 1);
    const [entry] = m.inspect();
    assert.equal(entry.topic, 'orders');
    assert.equal(entry.error, 'boom');
    assert.equal(entry.attempt, 1);
    assert.equal(entry.enqueueMs, 100);
  });
  it('increments attempt for a repeated identical event instead of duplicating', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const event = { id: 1 };
    const id1 = m.reject('orders', event, 'boom');
    const id2 = m.reject('orders', event, 'bang');
    assert.equal(id1, id2);
    assert.equal(m.size(), 1);
    const [entry] = m.inspect();
    assert.equal(entry.attempt, 2);
    assert.equal(entry.error, 'bang');
  });
  it('treats same-shaped but distinct object events as separate entries', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    m.reject('orders', { id: 1 }, 'x');
    m.reject('orders', { id: 1 }, 'y'); // different reference
    assert.equal(m.size(), 2);
  });
  it('treats NaN event as the same identity across rejects', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const id1 = m.reject('t', NaN, 'a');
    const id2 = m.reject('t', NaN, 'b');
    assert.equal(id1, id2);
  });
  it('throws on empty topic', () => {
    const m = new DeadLetterQueueManager();
    assert.throws(() => m.reject('', {}, 'x'), TypeError);
  });
  it('throws on non-string error', () => {
    const m = new DeadLetterQueueManager();
    assert.throws(() => m.reject('t', {}, 123), TypeError);
  });
  it('evicts oldest when at capacity', () => {
    const m = new DeadLetterQueueManager({ capacity: 2, clock: () => 100 });
    m.reject('t', { n: 1 }, 'a');
    m.reject('t', { n: 2 }, 'b');
    m.reject('t', { n: 3 }, 'c');
    const seen = m.inspect().map(e => e.event.n);
    assert.deepEqual(seen.sort((a, b) => a - b), [2, 3]);
  });
});

describe('inspect', () => {
  it('returns empty array for empty queue', () => {
    const m = new DeadLetterQueueManager();
    assert.deepEqual(m.inspect(), []);
  });
  it('filters by topic', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    m.reject('a', {}, 'x');
    m.reject('b', {}, 'y');
    const onlyA = m.inspect({ topic: 'a' });
    assert.equal(onlyA.length, 1);
    assert.equal(onlyA[0].topic, 'a');
  });
  it('computes ageMs from the injected clock', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    m.reject('t', {}, 'x');
    const [e] = m.inspect();
    assert.equal(e.ageMs, 0);
  });
  it('returns a shallow snapshot: mutating the result does not affect queue', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    m.reject('t', { id: 1 }, 'x');
    const [snap] = m.inspect();
    snap.error = 'mutated';
    const [real] = m.inspect();
    assert.equal(real.error, 'x');
  });
});

describe('drainDue', () => {
  it('returns entries whose nextAttemptMs is due and removes them from the queue', () => {
    const m = new DeadLetterQueueManager({
      clock: () => 100,
      backoffMs: () => 50
    });
    m.reject('t', { n: 1 }, 'x');
    assert.equal(m.size(), 1);
    const due = m.drainDue();
    assert.equal(due.length, 1);
    assert.equal(m.size(), 0);
  });
  it('returns nothing when no entries are due', () => {
    const m = new DeadLetterQueueManager({
      clock: () => 100,
      backoffMs: () => 200
    });
    m.reject('t', { n: 1 }, 'x');
    assert.equal(m.drainDue().length, 0);
  });
});

describe('replay', () => {
  it('removes the entry when the handler returns true', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const id = m.reject('orders', { id: 1 }, 'boom');
    const r = m.replay(id, () => true);
    assert.equal(r.success, true);
    assert.equal(r.replays, 1);
    assert.equal(m.size(), 0);
  });
  it('keeps the entry with updated attempt when the handler returns false', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const id = m.reject('orders', { id: 1 }, 'boom');
    const r = m.replay(id, () => false);
    assert.equal(r.success, false);
    assert.equal(r.error, 'handler returned false');
    assert.equal(m.size(), 1);
    const [entry] = m.inspect();
    assert.equal(entry.attempt, 2);
    assert.equal(entry.error, 'handler returned false');
  });
  it('keeps the entry with updated error when the handler throws', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const id = m.reject('orders', { id: 1 }, 'boom');
    const r = m.replay(id, () => { throw new Error('network down'); });
    assert.equal(r.success, false);
    assert.equal(r.error, 'network down');
    assert.equal(m.size(), 1);
  });
  it('reports not found for an unknown id', () => {
    const m = new DeadLetterQueueManager();
    const r = m.replay('missing', () => true);
    assert.equal(r.success, false);
    assert.equal(r.error, 'entry not found');
  });
  it('throws on non-function handler', () => {
    const m = new DeadLetterQueueManager();
    assert.throws(() => m.replay('x', null), TypeError);
  });
  it('throws on empty id', () => {
    const m = new DeadLetterQueueManager();
    assert.throws(() => m.replay('', () => true), TypeError);
  });
});

describe('remove', () => {
  it('removes a known id and returns true', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    const id = m.reject('t', {}, 'x');
    assert.equal(m.remove(id), true);
    assert.equal(m.size(), 0);
  });
  it('returns false for an unknown id', () => {
    const m = new DeadLetterQueueManager();
    assert.equal(m.remove('nope'), false);
  });
  it('throws on empty id', () => {
    const m = new DeadLetterQueueManager();
    assert.throws(() => m.remove(''), TypeError);
  });
});

describe('clear', () => {
  it('empties the queue', () => {
    const m = new DeadLetterQueueManager({ clock: () => 100 });
    m.reject('a', {}, 'x');
    m.reject('b', {}, 'y');
    m.clear();
    assert.equal(m.size(), 0);
    assert.deepEqual(m.inspect(), []);
  });
});

describe('defaultBackoff', () => {
  it('returns enqueueMs plus an exponentially growing delay', () => {
    const a1 = defaultBackoff(1, 1000);
    const a2 = defaultBackoff(2, 1000);
    const a3 = defaultBackoff(3, 1000);
    assert.ok(a2 > a1);
    assert.ok(a3 > a2);
  });
  it('caps the delay at 5 minutes', () => {
    const big = defaultBackoff(50, 1000);
    assert.equal(big, 5 * 60 * 1000 + 1000);
  });
});
