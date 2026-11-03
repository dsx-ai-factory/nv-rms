# Getting started with the RMS gRPC API

This guide shows you how to connect to RMS, call your first RPC, and follow three common flows:
control power, update firmware, and operate on nodes that RMS doesn't store.

## Prerequisites

- A running RMS instance. To start one for development, refer to [Running RMS](../getting-started/running.md).
- The address of the RMS gRPC listener. The default port is `8801`.
- For a production deployment, a client certificate, client key, and CA certificate. RMS requires
  mutual TLS (mTLS) over TLS 1.3 unless an operator explicitly enables plaintext for development.
  For the settings, refer to [Configuring RMS](../configuration/configuring-rms.md#tls).
- [`grpcurl`](https://github.com/fullstorydev/grpcurl) to call RPCs from a shell.
- The protobuf files for the services. RMS doesn't enable gRPC server reflection, so `grpcurl`
  needs the files on disk.

## Get the protobuf files

Clone the [`nv-rms-client`](https://github.com/dsx-ai-factory/nv-rms-client) repository and check
out the revision that your RMS release pins as `librms` in its `Cargo.toml`. The files are in the
`proto` directory:

- `rack_manager.proto` defines the `RackManager` service.
- `rack_manager_v2.proto` defines the `RackManagerV2` service and imports `rack_manager.proto`.

## Call an RPC with grpcurl

Set the address and the common `grpcurl` flags once. The examples in this reference use these two
variables. They assume a Bash shell.

```bash
export RMS_ADDR=rms.example.com:8801
export RMS_GRPCURL_FLAGS="-import-path ./proto -proto rack_manager.proto -proto rack_manager_v2.proto \
  -cacert ca.pem -cert client.pem -key client.key"
```

For a development instance that serves plaintext, replace the three certificate flags with
`-plaintext`.

Check connectivity with `GetVersion`:

```bash
grpcurl $RMS_GRPCURL_FLAGS -d '{}' "$RMS_ADDR" rack_manager.RackManager/GetVersion
```

The response carries the server version:

```json
{
  "version": "<version>"
}
```

If you prefer a purpose-built client, the `rack_manager_client` binary in the RMS repository
wraps many of the RPCs. Each RPC page lists the equivalent command where one exists.

## Control power on a registered node

Registered RPCs act on nodes that you store in RMS inventory.

1. Register the node with [`CreateNodes`](generated/inventory.md#createnodes). Set `type` to a
   [`NodeType`](generated/types-rack-manager.md#nodetype) value that matches the hardware. The node
   needs a BMC endpoint with credentials.

   ```bash
   grpcurl $RMS_GRPCURL_FLAGS -d '{
     "nodes": {
       "nodes": [
         {
           "node_id": "compute-01",
           "rack_id": "rack-1",
           "type": "<node_type>",
           "bmc_endpoint": {
             "interface": { "ip_address": "<bmc_ip>", "mac_address": "<bmc_mac>" },
             "port": 443,
             "credentials": {
               "user_pass": { "username": "<username>", "password": "<password>" }
             }
           }
         }
       ]
     }
   }' "$RMS_ADDR" rack_manager.RackManager/CreateNodes
   ```

1. Read the power state with [`GetPowerState`](generated/power-control.md#getpowerstate):

   ```bash
   grpcurl $RMS_GRPCURL_FLAGS -d '{"rack_id": "rack-1", "node_id": "compute-01"}' \
     "$RMS_ADDR" rack_manager.RackManager/GetPowerState
   ```

1. Change it with [`SetPowerState`](generated/power-control.md#setpowerstate). Both calls are
   synchronous, so the response carries the result.

   ```bash
   grpcurl $RMS_GRPCURL_FLAGS -d '{
     "rack_id": "rack-1",
     "node_id": "compute-01",
     "operation": "POWER_OPERATION_ON"
   }' "$RMS_ADDR" rack_manager.RackManager/SetPowerState
   ```

## Update firmware and poll the job

Firmware updates are asynchronous. The RPC starts the work and returns a job ID.

1. Start the update with [`UpdateFirmware`](generated/firmware-updates.md#updatefirmware). The
   `filename` is relative to the RMS firmware directory, or an absolute path.

   ```bash
   grpcurl $RMS_GRPCURL_FLAGS -d '{
     "rack_id": "rack-1",
     "node_id": "compute-01",
     "firmware_targets": [
       { "target": "<target>", "filename": "<firmware_file>" }
     ],
     "activate": true
   }' "$RMS_ADDR" rack_manager.RackManager/UpdateFirmware
   ```

   The response includes `job_id`.

1. Poll [`GetFirmwareJobStatus`](generated/utility-jobs.md#getfirmwarejobstatus) with that ID until
   `job_state` is `FIRMWARE_JOB_STATE_COMPLETED` or `FIRMWARE_JOB_STATE_FAILED`:

   ```bash
   grpcurl $RMS_GRPCURL_FLAGS -d '{"job_id": "<job_id>"}' \
     "$RMS_ADDR" rack_manager.RackManager/GetFirmwareJobStatus
   ```

   If the job fails, `error_code` and `error_message` explain why. On completion, `result_json`
   holds the detailed result.

For the job model, including parent and child jobs for batch RPCs, refer to
[Async jobs](async-jobs.md).

## Operate on nodes that RMS doesn't store

Batch RPCs act on caller-supplied nodes. The request carries each node's endpoints and credentials,
and RMS doesn't store them. For example, read power state for a set of nodes with
[`BatchGetPowerState`](generated/power-control.md#batchgetpowerstate):

```bash
grpcurl $RMS_GRPCURL_FLAGS -d '{
  "nodes": {
    "nodes": [
      {
        "node_id": "compute-01",
        "rack_id": "rack-1",
        "type": "<node_type>",
        "bmc_endpoint": {
          "interface": { "ip_address": "<bmc_ip>", "mac_address": "<bmc_mac>" },
          "port": 443,
          "credentials": {
            "user_pass": { "username": "<username>", "password": "<password>" }
          }
        }
      }
    ]
  }
}' "$RMS_ADDR" rack_manager.RackManager/BatchGetPowerState
```

The response reports a result for each node in `response.node_results` and the power states of the
nodes that RMS queried in `node_power_states`. A batch can partly succeed, so check each node's
result. For details, refer to [Conventions](conventions.md#error-handling).

## Next steps

- Browse the [RPC catalog](generated/rpc-catalog.md) to find the RPC for your task.
- Read [Conventions](conventions.md) to understand node identity and error handling.
- Read [Async jobs](async-jobs.md) before you build a client that starts long-running work.
