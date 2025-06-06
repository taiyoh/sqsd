# gRPC API Reference

`sqsd` provides a gRPC service for monitoring the current status of worker processes.

-   **Service**: `MonitoringService`
-   **Default Port**: `6969` (configurable via `MONITORING_PORT` env var)

## RPC Methods

### `CurrentWorkings`

Retrieves a list of tasks currently being processed by the workers. This is useful for understanding the current workload of the system.

-   **Request**: `CurrentWorkingsRequest` (empty message)
-   **Response**: `CurrentWorkingsResponse`

#### `CurrentWorkingsRequest`

This message has no fields.

```proto
message CurrentWorkingsRequest {}
```

#### `CurrentWorkingsResponse`

Contains a list of `Task` messages.

-   `tasks`: `repeated Task` - A list of currently executing tasks.

```proto
message CurrentWorkingsResponse {
  repeated Task tasks = 1;
}
```

#### `Task`

Represents a single task currently being processed.

-   `id`: `string` - The message ID from SQS.
-   `receipt`: `string` - The receipt handle of the message from SQS.
-   `started_at`: `google.protobuf.Timestamp` - The timestamp when the processing of this task started.

```proto
message Task {
  string id = 1;
  string receipt = 2;
  google.protobuf.Timestamp started_at = 3;
}
```

## Example Usage

You can use a gRPC client like `grpcurl` to query the monitoring service:

```shell
# Assuming the service is running on localhost:6969
grpcurl -plaintext localhost:6969 sqsd.MonitoringService/CurrentWorkings
