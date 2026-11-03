# NMX-C transport

The `Nmxc` client supports controller observations and complete static
configuration file writes. These operations use NMX-C's protocol definitions
within RMS rather than the northbound RMS API.

## Observations

The transport can read controller state, topology, switch nodes, and compute
nodes. Every response requires a server header with a successful NMX-C return
code. A successful gRPC exchange alone does not establish operation success.

The wire models preserve physical identities, device and node health, port
states, and unknown enum values. The transport does not aggregate health or
compare observations against a requested fabric configuration.

Clients that do not implement an observation operation return `Unimplemented`.

## Static configuration files

`set_static_config_file` sends the complete caller-supplied file content through
`SetStaticConfig`, including empty content. The transport preserves comments,
line endings, and text without interpreting or merging configuration keys.

The caller owns file content and any readback or controller restart required by
the workflow. The existing key/value write operation remains available. Clients
without file-content support return `Unimplemented`.
