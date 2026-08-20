# Contributing to NVIDIA Rack Management Service

Thank you for your interest in contributing to NVIDIA Rack Management Service
(RMS). We welcome focused fixes, tests, documentation improvements, and features
that align with the project's architecture and roadmap.

By participating, you agree to follow our [Code of Conduct](CODE_OF_CONDUCT.md).
For security vulnerabilities, follow [SECURITY.md](SECURITY.md) and do not open
a public issue or pull request.

## Table of Contents

- [Before You Start](#before-you-start)
- [Developer Certificate of Origin](#developer-certificate-of-origin)
- [Fork and Setup](#fork-and-setup)
- [Development Workflow](#development-workflow)
- [Engineering Guidelines](#engineering-guidelines)
- [Validation](#validation)
- [Pull Request Guidelines](#pull-request-guidelines)

## Before You Start

Search the [existing issues](https://github.com/NVIDIA/nv-rms/issues) before
opening a new one. Use the repository's issue forms for bugs, feature requests,
and documentation work. For a substantial behavior or API change, open an issue
first so maintainers can confirm scope and direction before implementation.

RMS manages privileged rack infrastructure. Changes involving authentication,
TLS, credentials, firmware, power control, device communication, persistence,
or filesystem access require focused security review and tests.

## Developer Certificate of Origin

Contributions use the [Developer Certificate of Origin (DCO), version
1.1](https://developercertificate.org/). A sign-off certifies that you wrote the
contribution or otherwise have the right to submit it under the repository's
license.

Sign off each commit with `-s`:

```bash
git commit -s -m "Describe your change"
```

This adds a trailer using the identity configured in Git:

```text
Signed-off-by: Your Name <your.email@example.com>
```

Configure that identity before committing:

```bash
git config user.name "Your Name"
git config user.email "your.email@example.com"
```

Do not add another contributor's sign-off. If a commit is missing your sign-off,
amend it locally before requesting review:

```bash
git commit --amend --signoff --no-edit
```

## Fork and Setup

### 1. Fork and clone the repository

Fork [NVIDIA/nv-rms](https://github.com/NVIDIA/nv-rms), then clone your fork:

```bash
git clone https://github.com/<your-username>/nv-rms.git
cd nv-rms
```

### 2. Add the upstream remote

```bash
git remote add upstream https://github.com/NVIDIA/nv-rms.git
git fetch upstream
```

### 3. Install prerequisites

A local build requires:

- Rust, installed through [rustup](https://rustup.rs/) (the version is pinned
  by `rust-toolchain.toml` and installed automatically on first build)
- [`just`](https://github.com/casey/just) for the repository's task recipes
- [`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny)
  (`cargo install cargo-deny@0.20.2 --locked`, matching the CI pin), used by
  `just check`
- `protoc` and the Protocol Buffers development libraries
- A standard SSL/CA bundle

Alternatively, Docker 24 or later can build and test inside the repository's
builder image. See [README.md](README.md#quick-start) for platform-specific
setup details.

### 4. Create a focused branch

Start from the latest upstream `main`:

```bash
git switch main
git pull --ff-only upstream main
git switch -c <type>/<short-description>
```

Examples include `fix/sftp-timeout`, `feature/new-rack-type`, and
`docs/update-build-guide`.

## Development Workflow

1. Keep the change limited to one clear outcome.
2. Follow existing module boundaries and patterns.
3. Add or update tests that exercise the changed behavior.
4. Run formatting, linting, and relevant tests locally.
5. Sign off every commit with the DCO trailer.
6. Open a pull request against `NVIDIA/nv-rms:main` using the repository
   template.

## Engineering Guidelines

### Keep changes focused

- Make the smallest correct change that solves the problem.
- Avoid unrelated refactors, generated-file churn, compatibility layers, or
  new dependencies unless the change requires them.
- Preserve existing behavior unless the pull request explicitly documents a
  deliberate compatibility change.
- Do not commit credentials, tokens, private keys, local environment files,
  device data, or machine-specific artifacts.

### Reuse existing code

Prefer the Rust standard library, existing workspace dependencies, and local
helpers before introducing a new abstraction or dependency. New dependencies
must have a clear need and compatible security and licensing posture.

### Preserve architecture and contracts

- Keep protocol gateways thin; business logic belongs in the domain,
  orchestrator, rack, node, transport, or persistence layer that owns it.
- Maintain asynchronous, non-blocking behavior for device and network I/O.
- Keep protobuf contracts, Helm manifests, database migrations, generated code,
  and documentation synchronized with implementation changes.
- Treat credentials and firmware artifacts as sensitive data. Never expose
  secrets in logs, errors, tests, fixtures, or review evidence.

### Verify assumptions

Back implementation claims with code, tests, schemas, generated types, runtime
output, or documentation. If hardware or integration behavior cannot be tested
locally, identify the gap and the substitute validation in the pull request.

## Validation

For Rust code changes, run the formatting, Clippy, and rustdoc checks, then
build the release binary and run the unit tests:

```bash
just check
just docs-api
just build
just test
```

Run the full release-mode workspace test suite when PostgreSQL is available:

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/rms_test
just test-full
docker compose down
```

Without PostgreSQL, run the suites that do not require it:

```bash
cargo test --release --lib
cargo test --release --test grpc_e2e
cargo test --release --test grpc_mtls
cargo test --release --test mockup_server_test
cargo test --release --test persistence memory::
```

You can run the build, formatting, and Clippy checks in the builder image:

```bash
docker build --target builder -t rms-builder .
docker run --rm rms-builder cargo clippy --workspace -- -D warnings
docker run --rm rms-builder cargo fmt --all -- --check
docker build --target release -t rms-release .
```

The CI test container is attached to the same Docker network as PostgreSQL and
receives `DATABASE_URL`. The host-side full-test commands above provide the
equivalent database-backed test coverage without requiring contributors to
reproduce that CI network setup manually.

Run narrower tests during development, but complete the applicable checks before
requesting review. See [Testing RMS](docs/getting-started/testing.md)
for integration-test and benchmark details.

## Pull Request Guidelines

- Explain the problem, the solution, and any user-visible or compatibility
  impact.
- Link related issues, using `Fixes #<number>` when appropriate.
- Describe the tests and environments used, including any validation gaps.
- Call out security-sensitive behavior, new dependencies, migrations, API
  changes, and deployment or release-note impact.
- Keep commits reviewable and DCO-compliant.
- Ensure required CI checks pass and respond to review feedback.

Maintainers may ask for changes, split an overly broad pull request, or decline
work that does not align with the project roadmap or security posture.

## Questions

For contribution questions that are not security-sensitive, open an
[issue](https://github.com/NVIDIA/nv-rms/issues) with the relevant context.
Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
