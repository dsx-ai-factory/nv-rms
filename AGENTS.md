# AGENTS.md

## Style

When writing Rust code, ensure you're following the [Style Guide](./STYLE_GUIDE.MD).

### Line Endings

Do not use `CRLF` line endings for any file. Only use normalized `LF` endings.

### Markdown

When generating Markdown (documentation or plans), always follow the [rumdl rules](./.rumdl.toml)
and run `rumdl check <markdownfile>`, fixing any errors displayed.

## Testing

Code should not be added without accompanying unit tests, unless they prove impractical or impossible to write.

## Documentation

For any code or configuration changes in the repo, ensure the [docs](./docs/) reflect and do not contradict
the new changes. If available, use the `nvidia-tech-docs` skills library to audit the changes to ensure docs
conform to NVIDIA public-facing documentation requirements.

## Pre-flight Checks

Before you're considered done with a set of changes, always run the following commands and rectify any linting/formatting
errors or failing tests before presenting the changes to the user for review.

### Build

```bash
just build
```

### Formatting/Linting

```bash
just check
just docs-api
```

### Cargo Tests

```bash
just test
cargo test --test grpc_e2e
just test-full
```

NOTE: Some e2e tests require non-loopback IP addresses to pass. Run the e2e tests outside the sandbox environment.
