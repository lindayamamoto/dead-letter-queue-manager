# dead-letter-queue-manager

Accumulates failed events with retry metadata, exposes inspection and manual replay APIs.

```js
import { DeadLetterQueueManager } from 'dead-letter-queue-manager';

const dlq = new DeadLetterQueueManager({ capacity: 1000 });

// On processing failure:
dlq.reject('orders', { id: 42, amount: 99 }, 'db connection refused');

// Later, inspect failures before retrying:
for (const entry of dlq.inspect({ topic: 'orders' })) {
  console.log(entry.id, entry.attempt, entry.error);
}

// Manually replay a specific entry with a handler. Success removes it;
// failure updates retry metadata in place.
const result = dlq.replay('dlq-1', (topic, event) => {
  return true;
});

// Remove entries you have decided to give up on:
dlq.remove('dlq-1');

// Wipe everything:
dlq.clear();
```

## Why this exists

Most message systems have a built-in dead-letter mechanism, but the wiring is often opaque: events land in a bucket, you lose the retry history, and manual replay requires out-of-band tooling. This library keeps the failure record co-located with the event so an operator can inspect *why* and *how many times* something failed before deciding to replay or discard. The trade-off is that the queue lives in the same process as the application — it is not durable across restarts. Use it for tactical retry bookkeeping, not as your system of record.

## The awkward edge you will hit

Event identity is determined by `Object.is` reference equality on the event payload. Two calls to `reject` with the same *shape* but different object references create two queue entries. This is deliberate: deep-cloning or structural comparison would silently coalesce events that are genuinely distinct, and `Object.is` is the only comparison that is both fast and free of surprises for primitive payloads. If you need deduplication, keep one canonical reference per event on your side and reject that.
