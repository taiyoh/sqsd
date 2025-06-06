# Deep Dive

This document covers some of the more complex internal mechanisms of `sqsd`.

## Locking Lifecycle

The locking mechanism is critical for preventing the same message from being processed by multiple `sqsd` instances simultaneously, especially in a distributed environment.

### Success Case

In the success case, the flow is straightforward:

1.  `Gateway` receives a message from SQS.
2.  `Gateway` acquires a lock on the message ID using the configured `QueueLocker`.
3.  `Gateway` sends the message to the `Consumer`'s channel.
4.  A `Consumer` worker picks up the message and executes the job via the `Invoker`.
5.  The `Invoker` returns a `nil` error, indicating success.
6.  The `Consumer` instructs the `Gateway` to delete the message from SQS.
7.  The lock is implicitly gone as the message is no longer in flight. The `Unlocker` will eventually clean up the lock key after the `LOCK_EXPIRE` duration, but it has no practical effect as the message is already processed.

```mermaid
sequenceDiagram
    participant SQS
    participant Gateway
    participant Consumer
    participant Invoker
    participant Locker
    participant Unlocker

    Gateway->>+SQS: ReceiveMessage
    SQS-->>-Gateway: Message(ID: M1)
    Gateway->>+Locker: Lock(M1)
    Locker-->>-Gateway: OK
    Gateway->>Consumer: Push to channel
    Consumer->>+Invoker: Invoke(M1)
    Invoker-->>-Consumer: Success (nil error)
    Consumer->>+Gateway: remove(M1)
    Gateway->>+SQS: DeleteMessage(M1)
    SQS-->>-Gateway: OK
    deactivate Gateway
    deactivate Consumer

    loop Every UnlockInterval
        Unlocker->>Locker: Clean up expired locks
    end
```

### Failure Case (and Retry)

When a job fails, the message is intentionally not deleted from SQS, allowing it to be re-processed after the SQS visibility timeout expires. The lock prevents other instances from picking it up until the lock itself expires.

1.  `Gateway` receives and locks a message (M1), then passes it to the `Consumer`.
2.  The `Consumer`'s `Invoker` fails to process the job and returns an error (e.g., a network error, or any error other than `ErrRetainMessage`).
3.  The `Consumer` logs the error but **does not** call `Gateway.remove()`.
4.  The message M1 remains locked by the `QueueLocker`.
5.  After SQS's visibility timeout expires, M1 becomes visible in the SQS queue again.
6.  Another `Gateway` instance might try to receive M1, but the `QueueLocker.Lock(M1)` call will fail because the lock is still held, preventing duplicate processing.
7.  Eventually, the `Unlocker` process, running in the background, finds that the lock for M1 has exceeded its `LOCK_EXPIRE` duration.
8.  The `Unlocker` removes the lock for M1.
9.  Now that the lock is released, a `Gateway` instance can successfully receive and lock message M1 again, triggering a retry of the job.

This ensures that a failed message is retried only after a configured cool-down period (`LOCK_EXPIRE`), preventing failing jobs from overwhelming the system.

## Error Handling Philosophy

The error handling in `sqsd` is designed to be simple and predictable, centering on the error returned by the `Invoker`.

The `Consumer`'s worker (`wrappedProcess` function) has three main branches for handling errors from `invoker.Invoke`:

1.  **`nil` error (Success)**: The job is considered successful. The `Consumer` proceeds to call `Gateway.remove()` to delete the message from SQS.

2.  **`locker.ErrQueueExists`**: This specific error indicates that the message was a duplicate (another worker locked it first). The `Consumer` simply logs this as a warning and takes no further action. The message is not deleted, as it's being handled by another worker.

3.  **`ErrRetainMessage`**: This is a special sentinel error that an `Invoker` implementation can return. It signals that the job should not be retried and the message should not be deleted immediately, but kept in the queue. This could be useful for scenarios where a job needs to be manually inspected or depends on a condition that is not yet met. The message will become visible again after the SQS visibility timeout.

4.  **All Other Errors**: Any other non-nil error is treated as a transient failure. The `Consumer` logs the error and takes no further action. Crucially, it **does not** delete the message from SQS. This allows the message to become visible again after the visibility timeout and be retried. The `QueueLocker` mechanism described above prevents this message from being picked up by another worker until the lock expires, effectively creating a cool-down period for retries.

In summary, the `Invoker`'s responsibility is to perform the job and return an appropriate error. The `sqsd` core then uses this error to drive the lifecycle of the message (delete, ignore, or retry).

## Circuit Breaker Implementation

The circuit breaker functionality in `sqsd` is not an explicit, state-machine-based implementation (like some libraries provide), but rather an emergent property of its concurrent design, specifically the interaction between the `Consumer`'s semaphore and the SQS long polling mechanism.

Here's how it works:

1.  **Limited Concurrency**: The `Consumer` has a fixed number of worker goroutines, controlled by a `semaphore.Weighted` instance. The size of this semaphore is determined by the `INVOKER_PARALLEL_COUNT` setting.

2.  **Workers Occupied**: When all worker goroutines are busy processing jobs, they are all holding a lock on the semaphore.

3.  **Channel Fills Up**: The `Gateway` continues to fetch messages from SQS and attempts to send them to the `msgsCh` channel, which is consumed by the `Consumer` workers.

4.  **Backpressure**: The `msgsCh` channel has a buffer size equal to `INVOKER_PARALLEL_COUNT`. Once all workers are busy, this channel quickly fills up.

5.  **Gateway Blocks**: When the `Gateway` tries to send a new message to the full channel (`broker <- Message`), it will block. The `Gateway`'s `runForFetch` goroutine will pause at this line, unable to proceed.

6.  **Fetching Stops**: Because the `Gateway`'s goroutine is blocked, it cannot make any further `ReceiveMessage` calls to SQS.

7.  **The "Circuit Opens"**: At this point, message fetching from SQS has effectively stopped, simply because there is no capacity to process new messages. This acts as a natural circuit breaker, preventing the system from pulling in more work than it can handle.

8.  **The "Circuit Closes"**: As soon as a `Consumer` worker finishes its job and releases its lock on the semaphore, it becomes available to take a new message from the `msgsCh` channel. This frees up space in the channel buffer. The `Gateway`'s blocked goroutine can now successfully send its message and unblock, allowing it to loop around and make another `ReceiveMessage` call to SQS.

This implicit, backpressure-based mechanism provides a simple yet effective way to regulate the flow of messages from SQS without needing a complex, explicit circuit breaker implementation.
