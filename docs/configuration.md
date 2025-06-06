# Configuration

`sqsd` can be configured using environment variables. These can be provided directly or loaded from a `.env` file.

## Core Settings

-   `INVOKER_URL`
    -   **Description**: The endpoint URL of the worker server to which job messages will be POSTed.
    -   **Required**: Yes, when using HTTP invocation.
    -   **Example**: `http://localhost:8080/worker`

-   `QUEUE_URL`
    -   **Description**: The URL of the Amazon SQS queue to fetch messages from.
    -   **Required**: Yes.
    -   **Example**: `https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue`

-   `SQS_ENDPOINT_URL`
    -   **Description**: A custom endpoint URL for the SQS API. Useful for local testing with tools like ElasticMQ.
    -   **Required**: No.

-   `AWS_REGION`
    -   **Description**: The AWS region for the SQS client.
    -   **Default**: `ap-northeast-1`

## Fine-tuning

-   `INVOKER_TIMEOUT`
    -   **Description**: The timeout duration for a single job invocation. If the worker server does not respond within this duration, the job is considered failed.
    -   **Default**: `60s`
    -   **Format**: Go's `time.Duration` string format (e.g., `30s`, `5m`, `1h`).

-   `FETCHER_WAIT_TIME`
    -   **Description**: The duration for which the SQS `ReceiveMessage` call will wait for a message to arrive in the queue (long polling).
    -   **Default**: `1s`
    -   **Format**: Go's `time.Duration` string format.

-   `UNLOCK_INTERVAL`
    -   **Description**: The interval at which the lock is checked and potentially released. Used by the locker mechanism.
    -   **Default**: `1m`
    -   **Format**: Go's `time.Duration` string format.

-   `LOCK_EXPIRE`
    -   **Description**: The expiration time for a message lock. This prevents a message from being processed indefinitely if a worker fails.
    -   **Default**: `24h`
    -   **Format**: Go's `time.Duration` string format.

-   `FETCHER_PARALLEL_COUNT`
    -   **Description**: The number of parallel goroutines fetching messages from SQS.
    -   **Default**: `1`

-   `INVOKER_PARALLEL_COUNT`
    -   **Description**: The number of parallel goroutines invoking jobs. This effectively sets the maximum number of concurrent jobs.
    -   **Default**: `1`

## Locker Settings

If the following `REDIS_LOCKER_*` variables are set, a Redis-based distributed lock is used. Otherwise, a simple in-memory lock is used.

-   `REDIS_LOCKER_HOST`
    -   **Description**: The host and port of the Redis server.
    -   **Required**: Yes, for Redis locker.
    -   **Example**: `localhost:6379`

-   `REDIS_LOCKER_DBNAME`
    -   **Description**: The Redis database number to use.
    -   **Default**: `0`

-   `REDIS_LOCKER_KEYNAME`
    -   **Description**: The key name to use for the lock in Redis.
    -   **Required**: Yes, for Redis locker.

## Monitoring

-   `MONITORING_PORT`
    -   **Description**: The port on which the gRPC monitoring server will listen.
    -   **Default**: `6969`

## Logging

-   `LOG_LEVEL`
    -   **Description**: The logging level.
    -   **Default**: `info`
    -   **Available values**: `debug`, `info`, `warn`, `error`
