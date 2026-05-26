---
id: handlers
title: Built-in handlers
sidebar_position: 5
---

# Built-in transport handlers

Two subpackages skip the boilerplate when the handler's only job is to forward the message to a downstream service. Each lives in its own Go module so its transport-specific dependencies stay out of the core `go.mod`.

## HTTP webhook — `handlers/http`

```bash
go get github.com/ndmt1at21/tickr/handlers/http
```

```go
import httphandler "github.com/ndmt1at21/tickr/handlers/http"

_ = tickr.On(reg, "email.send",
    httphandler.PostJSON[Email](http.DefaultClient, httphandler.Config[Email]{
        URLFunc: func(_ context.Context, _ *tickr.InboundMessage, e Email) string {
            return e.HookURL
        },
    }),
    tickr.WithMaxAttempts(8),
    tickr.WithAttemptTimeout(15*time.Second),
)
```

### Default classification

| Response | Outcome |
|---|---|
| 2xx | `success` |
| 408 Request Timeout, 425 Too Early, 429 Too Many Requests | retry (Retry-After honored) |
| Other 4xx | `DeadLetter` — client error won't fix on retry |
| 5xx | retry (Retry-After honored) |
| Transport / context error | retry |

### Config[T]

| Field | Default | Purpose |
|---|---|---|
| `Method` | `POST` | HTTP verb |
| `URL` | — | Static destination URL |
| `URLFunc` | — | Derive URL from payload (e.g. per-tenant webhook). Overrides `URL` |
| `StaticHeaders` | — | Headers added to every request |
| `HeaderFunc` | — | Per-message headers merged on top of `StaticHeaders` |
| `IdempotencyHeader` | `Idempotency-Key` | Auto-forward `msg.IdempotencyKey`. Set `""` to disable |
| `Classifier` | `DefaultClassifier` | Maps `(resp, body, transportErr)` → tickr outcome |

### Trace propagation

All headers in `tickr.InboundMessage.Headers` are forwarded as HTTP headers — including the W3C `traceparent` / `tracestate` that the OpenTelemetry tracer injects at enqueue time. **Trace-context headers always override user-supplied `StaticHeaders` / `HeaderFunc` entries** so spans stay coherent.

### Custom classification

Wrap `DefaultClassifier` to special-case a specific status code:

```go
cfg.Classifier = func(resp *http.Response, body []byte, transportErr error) error {
    if resp != nil && resp.StatusCode == 422 {
        // Business validation error — give up.
        return tickr.DeadLetter(fmt.Errorf("validation: %s", body))
    }
    return httphandler.DefaultClassifier(resp, body, transportErr)
}
```

## gRPC unary — `handlers/grpc`

```bash
go get github.com/ndmt1at21/tickr/handlers/grpc
```

```go
import grpchandler "github.com/ndmt1at21/tickr/handlers/grpc"

client := userpb.NewUserServiceClient(conn)

_ = tickr.On(reg, "user.signup",
    grpchandler.Unary(client.Notify, grpchandler.Config[*userpb.NotifyRequest]{}),
    tickr.WithMaxAttempts(5),
    tickr.WithAttemptTimeout(10*time.Second),
)
```

Pass the generated client method directly — `grpchandler.Unary` infers `Req` / `Resp` from its signature.

### Default classification

| gRPC code | Outcome |
|---|---|
| `OK` | `success` |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED` | retry |
| `INVALID_ARGUMENT`, `NOT_FOUND`, `PERMISSION_DENIED`, `UNAUTHENTICATED`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `UNIMPLEMENTED` | `DeadLetter` |
| `INTERNAL`, `UNKNOWN`, `DATA_LOSS`, `CANCELLED` | retry — override via `Config.Classifier` if your service uses these for permanent failures |
| Non-status error (transport / context) | retry |

### Config[Req]

| Field | Default | Purpose |
|---|---|---|
| `StaticMetadata` | — | Outgoing gRPC metadata added to every call |
| `MetadataFunc` | — | Per-message metadata merged on top of `StaticMetadata` |
| `IdempotencyMetadata` | `x-idempotency-key` | Auto-forward `msg.IdempotencyKey`. Set `""` to disable |
| `CallOpts` | — | Forwarded to the underlying `grpc.CallOption` chain |
| `Classifier` | `DefaultClassifier` | Maps gRPC status error → tickr outcome |

### Trace propagation

The message's W3C trace context (`InboundMessage.Headers`) and `IdempotencyKey` are attached as outgoing gRPC metadata. Trace-context keys override any user-supplied `StaticMetadata` entries.

### Custom classification

```go
cfg.Classifier = func(err error) error {
    if st, ok := status.FromError(err); ok && st.Code() == codes.Internal {
        // This service uses INTERNAL only for unrecoverable bugs.
        return tickr.DeadLetter(err)
    }
    return grpchandler.DefaultClassifier(err)
}
```

## When not to use these

If your handler does anything beyond "forward the payload" — e.g. mixes a DB write, queries another service, or transforms the body — write a plain `tickr.On[T]` instead. These wrappers are syntactic sugar for the pure-forwarding case.
