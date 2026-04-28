# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to
Semantic Versioning.

## [Unreleased]
### Added
- Add custom override assets and protocol staging workflows across CLI and MCP:
  `assets add <name> --casper-node --casper-sidecar --chainspec --node-config --sidecar-config`,
  `network <network> stage-protocol --custom-asset <name>`, MCP `stage_protocol`, and
  per-network live control sockets (`/tmp/<network>.socket`).
- Add explicit asset selectors for startup and staging:
  `start --asset <version>`, `start --custom-asset <name>`,
  `network <network> stage-protocol --asset <version>`, and
  `network <network> stage-protocol --custom-asset <name>`.
- Add asset and network management helpers:
  `assets path <name>`, `assets list` custom-asset visibility, `networks list`, and
  `networks rm <name>` (interactive confirmation or `--yes`).
- Add network-scoped hook samples under `networks/<network>/hooks/` for `pre-genesis`,
  `post-genesis`, `block-added`, `pre-stage-protocol`, and `post-stage-protocol`, generated from
  source-controlled templates in `examples/hooks/`.
- Add `network <network> path <protocol_version>` to print staged per-node config directories for
  that protocol version.
- Add `network <network> port --rpc|--sse|--rest|--binary|--diagnostics` to print one random
  live endpoint or diagnostics socket path from a running node.
- Add `network <network> is-ready` for scoped CI readiness checks.
- Add `derive <path> --secret-key|--public-key|--account-hash` with `-o/--output` support for
  deterministic seed-path account material lookup.

### Changed
- Protocol staging now writes per-node versioned `bin/<version>` and `config/<version>` trees,
  patches chainspec/node/sidecar configs for the target network, and restarts sidecars in live
  mode without requiring a full `start` restart cycle.
- Protocol staging now runs optional network-scoped hooks from `networks/<network>/hooks/`.
  `pre-stage-protocol` runs after staged `bin/<version>` and `config/<version>` directories are
  written and before post-stage metadata is queued, so it can inspect and edit staged chainspecs.
  If it fails, the newly staged version directories are removed and `post-stage-protocol` is not
  queued. `post-stage-protocol` still runs once at the real upgrade boundary after the launcher
  starts the target validator version. Hook stdout/stderr are streamed through `casper-devnet`
  stderr with `<hook_name> stdout|stderr: ...` prefixes and captured under
  `networks/<network>/hooks/logs/`.
- Runtime supervisor logs now make upgrade transitions explicit (launcher upgrade notes,
  SSE shutdown/disconnect visibility, and post-reconnect `Network is healthy` API re-announcement).
- Node and sidecar log aliases are atomically repointed to active versioned log files across
  protocol transitions and sidecar restarts.
- Custom `assets add <name> ...` installs are now write-once: reusing an existing custom asset
  name fails instead of replacing that directory in place.
- Hook sample scripts now include `info_get_status` examples, and the stage hook samples use
  Python templates to locate staged per-node config directories and demonstrate a safe
  chainspec TOML edit pattern.
- Reuse the shared seed-path derivation helper for both MCP signing flows and the new CLI
  `derive` command.
- Split asset selection from protocol-version overrides. `start --protocol-version` now only
  rewrites the generated chainspec protocol version; when omitted, `start` uses the selected
  asset's chainspec protocol version.
- Move protocol staging under the singular network command namespace:
  `network <network> stage-protocol`.
- Extend MCP `spawn_network` and `stage_protocol` requests with `asset` and `custom_asset`
  selectors while retaining `asset_name` as a compatibility alias for custom assets.

### Deprecated

### Removed
- Remove the top-level `casper-devnet stage-protocol` command. Use
  `casper-devnet network <network> stage-protocol` instead.
- Remove custom asset stage hook scaffolding and execution under `assets/custom/<name>/hooks/`;
  existing files in those directories are ignored.

### Fixed
- Avoid unintended offline fallback on macOS by using short `/tmp` control socket paths.
- Restore consensus keys from the network seed before live staging so `migrate-data` can complete
  at upgrade boundaries.
- Keep managed network PID state in sync across launcher-driven node restarts so `network <name>
  port ...`, `network <name> is-ready`, and MCP process inspection continue to work after staged
  upgrades.
- Make `network <name> port ...` prefer live control-socket runtime status over persisted
  `state.json` when the network is actively managed.
- Handle control-socket requests concurrently and time out stalled live port lookups so
  `network <name> port ...` no longer hangs during in-flight `stage-protocol` hooks.

### Security

## [0.8.1] - 2026-02-27
### Added
- Show diagnostics socket paths in network endpoints output.
- Add a diagnostics websocket proxy on port 32000 with `/diagnostics/node-<id>/` paths with HTTP POST fallback.
- Add `casper-devnet mcp` command with MCP server transports (`stdio`, `http`, `both`), default HTTP endpoint `127.0.0.1:32100/mcp`, and multi-network runtime management.
- Add MCP tools for network lifecycle (`spawn_network`, `wait_network_ready`, `despawn_network`, `list_networks`), managed process inspection (`managed_processes`), RPC/status/block queries, log pagination, SSE wait/history, derived account listing, transaction submission/signing, token transfers, session wasm submission, and transaction execution waiting.
- Add make-transaction-style MCP constructor tools: `make_transaction_package_call`, `make_transaction_contract_call`, and `make_transaction_session_wasm`, producing signed or unsigned transaction JSON for `send_transaction_signed`.
- Add `get_transaction` MCP tool for direct typed `info_get_transaction` lookups without external `curl`.
- Add per-network SSE collection with sequence-based history and filtering.
- Add deterministic seed/path derivation helper API in `assets` for MCP signing workflows.

### Changed
- Diagnostics port sockets now use the system temp directory with `<network>-<node>.sock` filenames.
- MCP-managed network spawns now enforce sidecar binary/config preflight checks.
- Process shutdown signal messages are emitted to stderr for MCP stdio protocol safety.
- Document MCP workflow and flags in README.
- Add Codex CLI stdio MCP configuration examples (TOML and `codex mcp add`) to README, with a Claude CLI note.
- Refactor MCP RPC/transaction paths to use `veles_casper_rust_sdk::jsonrpc::CasperClient`
  instead of direct `casper_client` usage in `mcp.rs`.
- Expand transaction argument parsing in MCP transaction tools to full CLType support
  (including nested CLTypes and aliases), with strict value parsing by declared type.
- Clarify MCP transaction tool documentation with explicit `session_args` examples and unsupported formats.
- Change MCP transaction request schema to use `session_args` as the primary field name,
  accepting structured JSON values directly (array/object), while still accepting legacy
  `session_args_json` payloads for compatibility.
- Change `send_transaction_signed` request schema to require `transaction` as the
  field name (typed JSON object).

### Deprecated

### Removed

### Fixed
- Make `assets list` and default protocol resolution tolerate bundle directory/chainspec
  version mismatches by warning and continuing with the directory version.
- Make `rpc_query_global_state` fall back to the latest block hash when
  `block_id` and `state_root_hash` are not provided.
- Improve `session_args` validation errors to clearly explain expected array/object formats and common invalid inputs.
- Enforce typed JSON-only payloads for `send_transaction_signed` transaction body:
  encoded JSON strings are rejected and `transaction_json` is not accepted.
- Make `current_block_height` return a pending response (with available block range)
  instead of failing when latest block is temporarily unavailable.

### Security

## [0.6.0] - 2026-01-19
### Added
- Add `is-ready` for CI readiness checks, validating process status/PIDs, REST health, `reactor_state` (`Validate`), and non-null `last_block_height`.

### Fixed
- Write `state.json` atomically to reduce corruption during updates.

## [0.5.2] - 2026-01-19
### Changed
- `most_recent_bundle_version` now returns `Result<Option<Version>>` for empty stores.

### Fixed
- Show bundle validation errors when resolving the default protocol version.

## [0.5.1] - 2026-01-19
### Changed
- `assets list` now exits with an error when no asset bundles are installed.

## [0.5.0] - 2026-01-19
### Added
- Start tracking releases in a Keep a Changelog format.

### Changed
- Rename derived accounts output to `derived-accounts.csv` and emit CSV rows.
- Add key type and split derivation/path fields in derived accounts output.
- Print derived accounts and network details in a tree-style layout.
- Pin GitHub release workflow to `softprops/action-gh-release@v2.4.2`.

### Removed
- Stop generating `users/user-*` directories and public key files for users.
- Stop writing `public_key.pem` and `public_key_hex` under `nodes/node-*/keys`.

### Security
- Delete consensus secret keys from disk after the first observed block.
- Recreate consensus secret keys on resume when assets already exist.

## [0.4.1] - 2026-01-19
### Changed
- Bump version to 0.4.1.

### Fixed
- Fix Dockerfile.
- Fix formatting.

## [0.4.0] - 2026-01-19
### Added
- Package tarballs for releases.
- Add progress bars for asset setup.

### Changed
- Display account hashes instead of public keys.
- Hardlink binaries instead of copying.
- Document how to run released image.
- Sync package version with tag.

## [0.3.4] - 2026-01-14
### Fixed
- Fix YAML syntax in workflows.

## [0.3.3] - 2026-01-14
### Added
- Publish ARM docker image release.

## [0.3.2] - 2026-01-14
### Changed
- Tag-only release (no code changes recorded).

## [0.3.1] - 2026-01-14
### Fixed
- Fix CI issues.

## [0.3.0] - 2026-01-14
### Added
- Docker build and release workflow.
- Dockerfile for a node image.
- Persist last block height to state.

### Changed
- Update config handling and docs.
- Track lockfile in repo.

### Fixed
- Diagnostics port socket clash.
- CI and test fixes.

## [0.2.1] - 2026-01-13
### Added
- Stdout/stderr log symlinks for nodes.
- Spinners to reduce log noise.
- Install steps and demo assets.

### Changed
- Shorten assets path output.
- Decrease default genesis activation delay.
- Documentation updates.

## [0.1.0] - 2026-01-11
### Added
- Initial release.

[Unreleased]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.8.1...HEAD
[0.8.1]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.8.0...v0.8.1
[0.6.0]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.5.2...v0.6.0
[0.5.2]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.4.1...v0.5.0
