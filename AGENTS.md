# AGENTS.md

## Project Overview

`rustquet` is a Rust project (edition 2024) using `rustc 1.93.1` stable.

## Build / Lint / Test Commands

| Command | Description |
|---|---|
| `cargo build` | Compile in debug mode |
| `cargo build --release` | Compile in release mode |
| `cargo run` | Build and run the binary |
| `cargo test` | Run all tests |
| `cargo test <name>` | Run a single test by name (substring match) |
| `cargo test -- --exact <name>` | Run a single test by exact name |
| `cargo test --lib` | Run only library tests |
| `cargo test --doc` | Run only doc tests |
| `cargo clippy` | Run the Clippy linter |
| `cargo clippy -- -D warnings` | Treat all Clippy warnings as errors |
| `cargo fmt` | Format all code with rustfmt |
| `cargo fmt -- --check` | Check formatting without modifying files |
| `cargo doc --open` | Build and open documentation |
| `cargo check` | Fast compile check without producing a binary |

Always run `cargo clippy -- -D warnings` and `cargo fmt -- --check` before
considering a task complete.

## Project Structure

```
src/
  main.rs          # Binary entry point
Cargo.toml         # Package manifest and dependencies
```

## Code Style

### General

- Rust edition 2024 conventions apply.
- No `rustfmt.toml` exists — use default `cargo fmt` settings (4-space
  indentation, max width 100).
- No `clippy.toml` exists — follow default Clippy lints; deny warnings in CI.

### Imports

- Group imports in this order, separated by a blank line:
  1. `std` and `core` crates
  2. External crates
  3. Local `crate` imports
- Use `use` imports at module top; avoid deep nesting with `mod` re-exports
  unless the module tree is large.
- Prefer explicit imports (`use std::io::Read`) over glob imports
  (`use std::io::*`).

### Naming

- `snake_case` for functions, methods, variables, modules.
- `PascalCase` for types, traits, enums, structs.
- `SCREAMING_SNAKE_CASE` for constants and statics.
- Prefix boolean getters with `is_`, `has_`, `contains_`, etc.

### Types & Generics

- Prefer concrete types until generic abstraction is needed.
- Use `type` aliases to clarify intent for complex signatures.
- Derive common traits (`Debug`, `Clone`, `PartialEq`) when sensible; avoid
  blanket derives.

### Error Handling

- Use `Result<T, E>` for recoverable errors; avoid `unwrap()` and `expect()`
  outside of tests, `main()`, or truly infallible contexts.
- Define custom error types with `thiserror` if the crate grows, or use
  `anyhow` for application-level code.
- Propagate errors with `?` operator rather than manual `match`/`if let`.

### Documentation

- Write `///` doc comments on all public items.
- Include a `# Examples` section in doc comments for non-trivial public API.
- Use `//` regular comments sparingly; prefer self-documenting code.

### Testing

- Unit tests go in the same file as the code they test, inside
  `#[cfg(test)] mod tests { ... }`.
- Integration tests go in `tests/` directory.
- Name test functions descriptively: `fn test_<behavior_under_test>()`.
- Use `assert_eq!`, `assert_ne!`, and `assert!` for assertions.
- Use `#[should_panic]` only for testing expected panics.

### Formatting

- Let `cargo fmt` handle formatting. Do not fight the formatter.
- One statement per line.
- Keep functions short (< 50 lines as a rule of thumb).
- Use `#[allow(...)]` attributes sparingly and always with a comment
  explaining why.

## Dependencies

- Add dependencies via `cargo add <crate>`.
- Prefer well-maintained crates with minimal dependency trees.
- Pin minimum versions in `Cargo.toml` when compatibility matters.

## Windows Notes

- Path separators: use `std::path::Path` / `PathBuf` instead of hardcoded
  `/` or `\`.
- Shell: development happens on Windows with PowerShell; test shell commands
  accordingly.
