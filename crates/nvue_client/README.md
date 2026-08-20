# nvue_client

`nvue_client` contains the NVUE HTTP client and the small set of request and
response types that RMS and nvfwupd need today.

The crate is intentionally narrow. It follows the NVOS API version listed in
[VERSION.md](VERSION.md), but the OpenAPI document is not vendored here. Add
only the shapes needed by RMS or nvfwupd work, and keep broader NVUE migration
separate.

The client owns one immutable HTTP transport generation at a time. Client TLS
rotation builds a complete candidate, verifies it with a read-only NVUE request,
then swaps it into use. Failed or cancelled verification leaves the last-known-good
transport active, and in-flight requests finish on their original generation.

Helpers normalize switch behavior seen in the field, including action polling,
image state checks, and platform firmware response shapes.
