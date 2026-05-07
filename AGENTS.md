# AGENTS.md

## Project purpose
Casper devnet launcher in Rust. It is heavily influenced by the NCTL workflow but targets smart contract developers and CI, with a single command to start a local devnet and a self-managed process lifecycle.

## Core workflows
- `casper-devnet assets add <path>`: install a local `.tar.gz` bundle into the assets store.
- `casper-devnet assets pull [--target] [--force]`: download bundles from the latest GitHub release, verify `.sha512`, compare `manifest.json`, update local assets, and write `assets/latest` with the release tag.
- `casper-devnet assets list`: list installed protocol versions (newest first).
- `casper-devnet start [--protocol-version <ver>]`: start devnet. Defaults to newest bundle version. Logs “resuming…” if assets already exist and `--force-setup` is not used.

## Assets layout
- Platform data dir (via `directories` crate):
  - `.../assets/` stores bundles: `v{version}/...`
  - `.../networks/<network>/...` stores runtime assets
- Bundle contents (required):
  - `vX.Y.Z/bin/casper-node`
  - `vX.Y.Z/bin/casper-sidecar`
  - `vX.Y.Z/chainspec.toml`
  - `vX.Y.Z/node-config.toml`
  - `vX.Y.Z/sidecar-config.toml`
  - `vX.Y.Z/manifest.json` (used to compare updates)

## State persistence
- Runtime state is stored at `networks/<network>/state.json`.
- JSON uses `time::serde::rfc3339` for `OffsetDateTime` timestamps.

## Process control
- casper-node runs via the embedded launcher state machine.
- casper-sidecar runs as a tokio child process.
- Signals are sent directly to PIDs (no process groups).

## Logging and SSE
- SSE logs are timestamp-prefixed (no node id).
- Child `RUST_LOG` is set from `--loglevel` (default `info`).
- Node config logging format uses `--node-log-format` (default `json`).

## Non-goals and removals
- No external `casper-node-launcher` binary.
- No `--hash`/trusted hash support.
- No `--chainspec-path` / `--config-path` overrides.
- No URLs for `assets add` (local files only).

## Key files
- `src/assets.rs`: bundle handling, asset generation, hashes
- `src/cli.rs`: CLI flows and UX
- `src/process.rs`: process lifecycle
- `src/node_launcher.rs` + `src/node_launcher/`: embedded launcher logic

## Conventions
- Use `tokio::fs` for IO and `spawn_blocking` for CPU/TOML operations.
- When considering a libc/unsafe syscall wrapper, first prefer the safe Rust equivalent from `nix`
  when one exists; use raw `libc` only when no suitable safe wrapper is available.
- Prefer concise user-facing logs.
- Default target for assets is the build target (from `build.rs`).
- For network interactions, do **not** use `casper_client` directly; use `veles_casper_rust_sdk::jsonrpc::CasperClient` instead.
- For MCP transaction construction, do **not** shell out to `casper-client` CLI binaries; use MCP constructor tools and pass `session_args` as structured JSON (not escaped text), using either full `RuntimeArgs` JSON or an array of `{name,type,value}` objects (example: `[{"name":"value","type":"I32","value":"1"}]`). Composite values (`List`, `Map`, tuples, `Result`, `ByteArray`) should use hex bytes (`0x...`). Do not use object shorthand (`{"value":1}`) or casper-client string-arg syntax (`["value:i32=1"]`). Legacy `session_args_json` is accepted only for compatibility.
- For `send_transaction_signed`, provide `transaction` as typed JSON (`transaction: {...}`); encoded JSON strings are not supported, and `transaction_json` is not accepted.
- Do not use `curl` for JSON-RPC transaction lookups; use MCP `get_transaction` (single fetch) or `wait_transaction` (poll-until-executed) tools instead.
- Keep `README.md` updated with CLI defaults/flags whenever code changes.
- Before finishing a task, run `cargo clippy --all --all-targets --all-features --tests` only if any `.rs` code is changed, and report failures.
- Update `CHANGELOG.md` only when explicitly asked. When asked, derive the changelog work from
  changes against the upstream branch, considering commits, modified files, and staged files as
  needed; summarize the relevant user-facing changes first, then distill them into
  `CHANGELOG.md` entries under `[Unreleased]`.
- Changelog maintenance: follow Keep a Changelog + SemVer; keep the standard section headings; when cutting a release, move entries from `[Unreleased]` into a new tagged section with the release date based on the latest git tag; update compare links for `[Unreleased]` and the new version.
