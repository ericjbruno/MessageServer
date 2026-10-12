# Defects to Fix

## Queue messages can be lost before delivery

**Where:** `src/main/java/messageserver/QueueController.java`, `index()` (lines 59–64)

**Problem:** A queue message is deleted from Oracle NoSQL as soon as `emitter.onNext()`
returns. `onNext()` only hands the event to Micronaut/Netty; it does not mean the
receiver got it. If the receiver disconnects, or the write fails after that point, the
message is gone from both the in-memory queue and the store, and is never delivered.

**Seen:** 2026-10-12. Before the SSE delivery fix (`.subscribeOn(Schedulers.io())`), a
test message was removed from the queue and deleted from the store, but never reached
the receiver. The delivery fix makes this rare, but the window still exists.

**Possible fixes:**
- Delete only after the receiver acknowledges the message, for example with a separate
  ack endpoint that takes the message ID. Redeliver messages that aren't acknowledged
  within a timeout.
- At minimum, if the stream ends with an error or cancellation, put the message ID
  back on the queue (`Queue.messageIds`) instead of deleting the message.
