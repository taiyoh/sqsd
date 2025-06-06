# Architecture

## Overview

This system is built on a standard Go concurrency model using goroutines and channels, not the Actor Model. It is designed with a clear separation of concerns, where each core component runs in its own goroutine(s) and communicates via channels. This approach keeps the coupling between components low, enhancing the system's reliability and scalability.

The system operates based on a pipeline model: the `Gateway` fetches messages and puts them into a channel, and the `Consumer`'s workers pick them up from the channel to process them.

## Core Components

The main components are as follows:

-   **Gateway**: Fetches messages from SQS in parallel, using long polling. It uses a `QueueLocker` to prevent duplicate processing of messages before sending them to a channel for the Consumer. It is also responsible for deleting the message from SQS after successful processing.
-   **Consumer**: A pool of workers that receive messages from the Gateway's channel. The number of concurrent workers is controlled by a semaphore. Each worker passes the message to the job process via the `Invoker`. It tracks the currently processing tasks, which are exposed via the monitoring service. Based on the result of the invocation, it either instructs the Gateway to delete the message or leaves it to be re-processed.
-   **Invoker**: An interface responsible for executing the job logic with a given message. `sqsd` provides a default `HTTPInvoker` implementation, but users can provide their own to integrate with any Go-based logic.
-   **Monitor**: Starts a gRPC server to provide the system's operational status (scoreboard) to external systems.
-   **QueueLocker**: Prevents multiple `sqsd` instances from processing the same message simultaneously. It can be implemented with Redis for distributed environments or in-memory for single-instance setups.
-   **Unlocker**: A background process that periodically releases expired message locks, ensuring that failed jobs can be retried.

## Design Patterns

-   **Circuit Breaker**: When all worker processes become busy, a circuit breaker pattern is employed to automatically stop fetching messages from SQS. When worker processes become available, it automatically resumes fetching. This prevents excessive load on the system.
-   **Interface-based Abstraction**: The `sqsd.Invoker` interface is the key to the system's flexibility. It abstracts the job execution logic, allowing `sqsd` to be used in two primary ways:

    1.  **Standalone Mode (via `HTTPInvoker`)**: When running `sqsd` as a standalone binary, it uses the built-in `HTTPInvoker`. This invoker sends an HTTP POST request with the message payload to a configured worker URL (`INVOKER_URL`). This is ideal for polyglot environments where the job processor is a separate application written in any language (e.g., PHP, Python, Ruby).

    2.  **Library Mode (via custom `Invoker`)**: When `sqsd` is used as a library within a Go application, developers can implement their own `Invoker`. This allows `sqsd` to act as a background job processor that triggers any Go function directly, without the overhead of an HTTP call. This provides a powerful way to build robust, self-contained worker services entirely in Go.
