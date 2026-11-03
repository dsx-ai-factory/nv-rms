# Test fixtures

## Dell GB200 AMI

`dell_gb200_ami.json` contains selected response bodies from the read-only
Redfish capture of October 1, 2026. Hostnames, credentials, addresses, serial
numbers, UUIDs, timestamps, and HTTP headers are omitted. The fixture preserves
firmware inventory, action discovery, background-copy status, and upload URI
shapes from the AMI host BMC and NVIDIA HMC.
`HttpPushUriOptions.ForceUpdate` is included from the October 7 diagnostic
capture; live PATCH and read-back confirmed that this option is writable.

Firmware uploads, task results, and power-operation responses are synthesized by
the tests. They are not evidence of successful firmware updates on physical
hardware. The capture did not include processor topology or firmware-update tasks.
