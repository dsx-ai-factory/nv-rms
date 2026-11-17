# Rack Management Service Rust crate

This crate provides shared RMS protobuf wire types plus optional generated client
and server support for the NVIDIA Rack Management Service (RMS).

## Overview

The NVIDIA Rack Management Service provides functionality for managing NVIDIA
multi-node NVLINK rack-scale hardware.

## Features

- `client`: generated client APIs plus the TLS, retry, and transport stack.
- `server`: generated `rack_manager_server` service definitions.
- `serde`: serde derives for generated protobuf models.

The default feature set is `client` and `serde` to preserve existing client
consumer behavior. Consumers that only need shared types or server definitions
can use `default-features = false` and enable the smaller feature set they need.

## Testing

Run the feature matrix before publishing changes that touch generated protobuf,
client, server, or serde support:

```bash
cargo test --no-default-features
cargo test --no-default-features --features client
cargo test --no-default-features --features server
cargo test --all-features
```

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for
details.

## Caller-owned observability

Transport layers are trusted, caller-supplied code that sits above TLS and sees plaintext
requests and responses. Production deployments are expected to use mTLS.

`RmsClientConfig::transport_layer` accepts an optional `Arc<dyn RmsTransportLayer>`.
Its `layer` method wraps the cloneable HTTP transport before the V1 or V2 tonic client
is constructed. The layer is applied again on connection rebuilds and covers readiness
probes as well as application RPCs. TLS, timeout, pooling, and retry policy remain owned
by librms. With `None` (the default), transport behavior is unchanged.

`RmsClientConfig::rpc_observer` accepts an optional `Arc<dyn RpcObserver>` for decoded
unary calls through generated API wrappers and `RackManagerApi`, including its V2 method.
Observers receive protobuf method/type names, encoded message bodies, client-side UTC
call boundary times as `SystemTime`, and the final tonic status code. Call start precedes
lazy connection setup and readiness retries. Readiness probes do not create separate
observations. The observer's span is entered while polling the call, so a transport layer
can propagate that span's context. No encoding or observer callback occurs with `None` or
when `RpcObserver::enabled` returns false.

Observers receive unredacted payloads; redaction is the observer's responsibility, and
observers must not block. librms does not log the payloads itself. `RpcObserver::start` and
`RpcObservation::finish` return `Result`; an `Err` is logged at `warn` level and never
affects the RPC, which is then unobserved (for `start`) or simply not recorded (for
`finish`). Error status text/details and metadata are deliberately excluded from the
observer API. Implement `Drop` on the returned `RpcObservation` to record calls
cancelled before completion. `FILE_DESCRIPTOR_SET` contains the V1/V2 descriptors for
policy implementations that decode payloads dynamically. Raw tonic clients and streaming
RPCs do not use the decoded observer.
