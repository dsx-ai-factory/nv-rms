# AGENTS.md

## Style

When writing Rust code, ensure you're following the [Style Guide](./STYLE_GUIDE.MD).

### Line Endings

Do not use `CRLF` line endings for any file. Only use normalized `LF` endings.

### Markdown

When generating Markdown (documentation or plans), always follow the [markdownlint-cli2 rules](./.markdownlint-cli2.yaml)
and run `markdownlint-cli2 <markdownfile>`, fixing any errors displayed.

## Testing

Code should not be added without accompanying unit tests, unless they prove impractical or impossible to write.

## Pre-flight Checks

Before you're considered done with a set of changes, always run the following commands and rectify any linting/formatting
errors or failing tests before presenting the changes to the user for review.

### Build

```bash
cargo build
cargo build --release
```

### Formatting/Linting

```bash
cargo fmt -- --check
cargo clippy -- -D warnings
```

### Cargo Tests

```bash
cargo test --lib
cargo test --test grpc_e2e
cargo test --release
```
