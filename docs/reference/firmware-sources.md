# Firmware Sources and Manifests

This page covers where RMS retrieves firmware artifacts and the JSON manifest
format that describes them.

## Firmware sources

RMS needs access to firmware artifacts at runtime. When you register a firmware
object with `AddFirmwareObject`, RMS downloads every artifact referenced in the
manifest into a local staging directory (the `--firmware-dir` path) before
marking the object `available`. Subsequent `ApplyStoredFirmwareObject` calls
read from that cache. RMS must be able to reach every artifact URL or path at
the time of download.

### Supported endpoints

RMS supports three source types, selected by the `LocationType` field in each
`Locations` entry of the manifest.

| `LocationType` | Accepted URL or path | Authentication |
| --- | --- | --- |
| `http` or `https` | `http://…` or `https://…` | None (plain GET) |
| `artifactory` or `jfrog` | `http://…` or `https://…` | Optional API key via `access_token` in `AddFirmwareObjectRequest`, sent as `X-JFrog-Art-Api` |
| `file` | Bare filesystem path (`/data/fw/artifact.fwpkg`) or `file:///…` URL | None |

For HTTP and HTTPS downloads, RMS streams the response body directly to the
staging directory. When you supply an optional `Sha256` digest in a `Locations`
entry, RMS accumulates the hash chunk-by-chunk as bytes arrive, then compares
the digest after the stream completes. If the digest does not match, RMS
discards the temp file and returns an error without promoting anything to cache.

#### Local filesystem paths

A `file` location can be any path that the RMS process can read. In
containerized deployments, make the firmware files available inside the
container through a volume mount. RMS copies the file into the staging
directory and applies the same optional SHA-256 check.

#### Artifactory / JFrog

Artifactory and JFrog use the same HTTP/HTTPS download path as a plain file
server. When you pass a non-empty `access_token` in `AddFirmwareObjectRequest`,
RMS attaches it as the `X-JFrog-Art-Api` header. When `access_token` is absent
or empty, RMS performs an unauthenticated GET.

### Unsupported endpoints

The following protocols are not supported and will cause `AddFirmwareObject` to
return an error if referenced in the manifest:

- **S3** (`s3://`) - object-storage URLs are not supported. Stage artifacts on
  an HTTP/HTTPS file server or copy them to a local path accessible to RMS.
- **FTP / FTPS / SFTP / TFTP** - file-transfer protocols are not supported.
  Use an HTTP file server or a local path instead.
- **HTTP Basic/Digest authentication and Bearer tokens** - only Artifactory
  API-key authentication (`X-JFrog-Art-Api`) is supported for authenticated
  downloads.

---

## Firmware manifest

The firmware manifest is a JSON file distributed per firmware milestone. It
describes the board SKUs, firmware components, expected versions, and the
download locations for every artifact. You pass the manifest content as the
`config_json` field in `AddFirmwareObjectRequest` or `ApplyFirmwareObjectRequest`.

RMS parses the manifest, builds an internal lookup table of components and
targets, and downloads all referenced artifacts. The object becomes `available`
once all downloads complete successfully.

### Top-level structure

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `ProductName` | string | Yes | Product identifier for the manifest, such as `DGXGB200` or `VR-NVL72`. |
| `Milestones` | array | Yes | Exactly one milestone entry. RMS rejects manifests with zero or more than one element. |

### Milestone fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Name` | string | Yes | Milestone label, such as `1.3.6`. Combined with `ProductName` to form the firmware object ID; RMS returns `INVALID_ARGUMENT` when absent. |
| `State` | string | No | Release state, such as `Released` or `Beta`. Informational only. |
| `BoardSKUs` | array | Yes | One entry per board SKU included in this release. |

### BoardSKU fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Name` | string | No | Human-readable SKU name. |
| `SKUID` | string | No | Hardware SKU identifier used for lookup-table keying. |
| `Type` | string | No | SKU category: `Compute Node`, `Switch Tray`, `Power Shelf`, or `VRNVL72 Compute Node`. RMS uses this to route artifacts to the correct node type. |
| `Components` | object | Yes | Contains `Firmware` and `Software` arrays. |

### Component (Firmware entry) fields

RMS reads the `Firmware` array inside `Components`. It ignores the `Software`
array for firmware-update workflows, except when collecting switch system images
from entries whose `Component` is `NVOS`.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Component` | string | Yes | Logical firmware component name, such as `BMC+FPGA+EROT`, `SBIOS+EROT`, or `CPLD`. Maps to device update targets. |
| `Version` | string | No | Expected version string. RMS uses this for post-update version verification when present. |
| `Type` | string | No | Firmware channel, such as `Prod` or `Beta`. Used for filtering during apply. |
| `Bundle` | string | No | Bundle group name. When set, RMS associates the component with a named bundle for selection. |
| `Locations` | array | Yes | Download locations for this component's artifacts. |
| `SubComponents` | array | No | Individual firmware version information for the sub-components bundled in this package. |

### Location fields

Each `Locations` entry identifies one downloadable artifact.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Location` | string | No | Download URL or filesystem path. Can be a bare path (`/data/fw/artifact.fwpkg`), a `file:///…` URL, an `http://…` URL, or an `https://…` URL. Empty when the artifact is not downloadable by RMS. |
| `LocationType` | string | No | Source type: `http`, `https`, `artifactory`, `jfrog`, or `file`. Determines the download strategy and authentication. |
| `FileName` | string | No | Filename used during manifest parsing to classify the entry as a firmware payload (via extension and component checks). Does not affect the on-disk cache name, which is always derived from `Location`. |
| `Type` | string | No | Artifact role: `Firmware`, `Firmware-Recovery`, `Certificate`, `Binary`, or `Misc`. RMS only downloads and stages entries with firmware-payload types. |
| `Sha256` | string | No | Optional lowercase or uppercase hex-encoded SHA-256 digest. When present, RMS verifies the artifact after download and rejects mismatches. |

### SubComponent fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Component` | string | Yes | Sub-component name, such as `BMC`, `EROT`, or `SBIOS`. |
| `Version` | string | Yes | Version string for this sub-component. |
| `SKUID` | string | No | Optional SKU qualifier for sub-component lookup. |

### Abbreviated example

The following example shows the essential structure for a manifest with one
compute and one switch SKU. Fields not relevant to RMS processing are omitted.

```json
{
  "ProductName": "DGXGB200",
  "Milestones": [
    {
      "Name": "1.3.6",
      "State": "Released",
      "BoardSKUs": [
        {
          "Name": "GB200 Switch",
          "SKUID": "P4978",
          "Type": "Switch Tray",
          "Components": {
            "Firmware": [
              {
                "Component": "BMC+FPGA+EROT",
                "Version": "GB200-P4978_0004_260127.1.3",
                "Type": "Prod",
                "Locations": [
                  {
                    "Location": "https://artifact-server.example.com/fw/nvfw_GB200-P4978_0004.fwpkg",
                    "LocationType": "https",
                    "FileName": "nvfw_GB200-P4978_0004.fwpkg",
                    "Type": "Firmware",
                    "Sha256": "a3f1..."
                  }
                ],
                "SubComponents": [
                  { "Component": "BMC", "Version": "88.0002.1336" },
                  { "Component": "EROT", "Version": "01.04.0026.0000_n04" }
                ]
              }
            ],
            "Software": []
          }
        },
        {
          "Name": "GB200 Compute Tray",
          "SKUID": "P4975",
          "Type": "Compute Node",
          "Components": {
            "Firmware": [
              {
                "Component": "BMC",
                "Version": "23.08.09",
                "Type": "Prod",
                "Locations": [
                  {
                    "Location": "/mnt/firmware/bmc-23.08.09.fwpkg",
                    "LocationType": "file",
                    "FileName": "bmc-23.08.09.fwpkg",
                    "Type": "Firmware"
                  }
                ],
                "SubComponents": []
              }
            ],
            "Software": []
          }
        }
      ]
    }
  ]
}
```

For a complete, real-world example, refer to the
[sample manifest](https://github.com/dsx-ai-factory/nv-rms/tree/main/sample_firmware_manifest)
in the repository.
