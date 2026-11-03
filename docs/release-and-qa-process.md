# Release and QA Process

NVIDIA targets one public minor release of Rack Management Service (RMS) each
month, with a rolling three-month roadmap refreshed monthly. RMS follows
Semantic Versioning, and guaranteed public interfaces remain backward-compatible
within an RMS application major version. Each public release follows QA approval.

## RMS Versioning

RMS uses [Semantic Versioning](https://semver.org/) in the form `vX.Y.Z`:

- `X` is the major version. It changes for a release with incompatible changes
  to guaranteed public interfaces.
- `Y` is the minor version. It normally advances with the monthly release and
  may include new backward-compatible functionality.
- `Z` is the patch version. It advances for fixes to an existing minor release.

For example, `v0.12.0` is a minor release and `v0.12.1` is its first patch
release.

### Tags

| Tag | Meaning | Example |
| --- | --- | --- |
| `vX.Y.0` | Final monthly minor release | `v0.12.0` |
| `vX.Y.Z` where `Z > 0` | Final patch release | `v0.12.1` |
| `vX.Y.Z-rcN` | Release candidate awaiting final approval | `v0.12.0-rc2` |

Release-candidate numbers begin with `rc1` and increase whenever the candidate
source or artifacts change. Every RC tag identifies a distinct candidate; tags
are immutable and are never moved or reused. A final release drops the `-rcN`
suffix while retaining the approved `X.Y.Z` version.

## Monthly release flow

1. NVIDIA prepares a versioned release candidate and completes the planned
   validation for that release.
1. QA approves the exact source and artifacts selected for publication.
1. NVIDIA opens a public pull request containing the approved release content.
1. The pull request completes the required public checks and review.
1. NVIDIA merges the pull request and publishes the corresponding final release.

The monthly cadence is a target, not a reason to bypass validation. If the
candidate changes after QA approval, NVIDIA creates and validates a new candidate
before merging it publicly. A release may therefore move to a later date when
additional fixes or validation are required.

## Three-month rolling roadmap

RMS maintains a three-month rolling roadmap alongside the monthly release
cadence. The roadmap provides a current view of the next three planned minor
release cycles, including:

- planned feature themes and notable work for each release,
- expected code-complete dates, validation windows, and final release targets,
- known schedule or dependency risks, and
- work that moved into or out of a release since the previous update.

The roadmap is refreshed at least monthly, normally after the current release
cycle advances. It is planning guidance rather than a release guarantee. Scope
or dates may change as priorities shift, dependencies change, or QA findings
require additional work.

## Patch releases

Patch releases increment `Z` within an existing `vX.Y` release line. They are
published as needed for fixes that should not wait for the next monthly minor
release, such as a security fix, production-blocking defect, regression, or
other significant issue.

Each patch goes through validation appropriate to its changes:

1. NVIDIA prepares a `vX.Y.Z-rcN` patch candidate.
1. QA validates the candidate source and artifacts, including the affected
   regression and upgrade paths when applicable.
1. Any candidate change receives a new RC number and the affected validation is
   repeated.
1. After approval, NVIDIA merges the validated patch content publicly and
   publishes the final `vX.Y.Z` release.

Patch releases do not have a fixed cadence and may be published between monthly
minor releases when the fix and validation are ready.

## Backward compatibility

Breaking changes to guaranteed public interfaces are not allowed within an RMS
application major version.

### Deprecation and breaking-change notice

A guaranteed public interface may be deprecated before a future breaking
change, but deprecation is a warning rather than removal. The deprecated
interface remains functional for the rest of the current major version.

Removing a guaranteed public interface, or changing its behavior
incompatibly, requires a future major release. NVIDIA announces the change in
the release notes and the three-month rolling roadmap and provides a replacement
or migration path when one is available.

When practical, a deprecated interface also produces an operator-visible
warning through API status, command output, logs, or release notes. The minimum
notice for a breaking change is one full three-month roadmap window before the
release that introduces it. Emergency exceptions for security, data corruption,
or similarly severe issues are identified explicitly in the release notes.

### Guaranteed public interfaces

The RMS application major version governs compatibility for these documented,
public-facing interfaces:

- the northbound `RackManager` gRPC API, including its RPCs and protobuf
  contracts,
- documented user-facing command-line interfaces shipped with RMS,
- documented RMS configuration files, and
- documented environment variables consumed by RMS.

Within a major version, existing clients and configurations can continue using
these interfaces. New optional functionality may be added in a
backward-compatible way.

The Helm chart has an independent `MAJOR.MINOR.PATCH` version. Compatibility of
documented Helm values and templates is governed by the **chart major version**,
not the RMS application major version. A breaking value rename or removal, or a
removed template, requires a new chart major version. Chart and RMS major
versions do not need to match. See [Kubernetes deployment
versioning](deployment/kubernetes.md#versioning) for the two version axes.

### Interfaces not covered by the guarantee

The following implementation details may change between releases without the
same compatibility commitment:

- internal Rust crate APIs and component boundaries,
- undocumented development, test, debug, or administrative tools,
- internal database schemas and persistence formats,
- internal communication contracts between RMS components, and
- implementation details of southbound hardware integrations.

Consumers should use the documented public interfaces rather than depending on
these internal details.

## Release artifacts and notes

RMS releases can contain artifacts with independent version lifecycles. The
release record identifies the exact relationship between the published release
and each applicable artifact, including:

- the RMS application tag and final commit,
- the container image digest,
- the independently versioned Helm chart version,
- the bundled or separately published NVFWUPD version,
- the compatible NICo version or version range, and
- the software bill of materials (SBOM) or its published location.

Compatibility is determined by the release record and validation evidence, not
by artifacts having similar publication dates.

Release notes include, as applicable:

- the release version, date, baseline, and final commit,
- user- and operator-visible changes, including compatibility changes,
- supported hardware and integration changes,
- the exact artifact versions and digests listed in the release record,
- upgrade instructions, data-migration effects, and downgrade limitations,
- known issues, workarounds, deferred validation, and documented exceptions, and
- the QA approval state and SBOM location.

## Publication guarantees

For each RMS release, NVIDIA will:

- publish only content approved for public release,
- base the public merge on the exact release candidate accepted by QA,
- require the public repository's normal checks and review before merge, and
- retain a release record containing the immutable release-candidate tag and
  commit, final release tag and commit, published artifact digests, and QA
  approval record.

This process keeps the public repository aligned with releases that have
completed NVIDIA validation and makes each published version traceable to the
candidate accepted by QA.

## Summary

NVIDIA targets a monthly public minor release and publishes validated patch
releases as needed. QA approves the exact candidate before publication, and the
release record links that candidate to the final artifacts. The rolling roadmap
and major-version compatibility policy give users advance notice of planned and
breaking changes.
