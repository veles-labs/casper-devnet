# Casper Devnet Launcher

Casper Devnet Launcher is a Rust tool for running a local Casper network, based on and heavily
inspired by the NCTL workflow, to make local devnets quick and easy for smart contract developers.
It embeds the essential node-launcher behavior in-process, so you only need the `casper-node`
and (optionally) `casper-sidecar` binaries.

![Casper Devnet Launcher demo](casper-devnet.gif)

## Why this exists

NCTL is fantastic for core protocol development and for building assets from source trees, but it
comes with a large shell script surface, external process supervision, and multi-step UX. This tool
targets application and contract developers who want a repeatable, portable devnet for development,
CI, and tests.

## Comparison with NCTL

| Area | NCTL | Casper Devnet Launcher (this repo) |
| --- | --- | --- |
| Primary audience | Core protocol development | Smart contract/app developers, CI/tests |
| Process control | External supervisor (supervisord) | In-process process control |
| Setup workflow | Multiple commands | Single command: `casper-devnet start` |
| Implementation | Large shell script | Rust binary (portable) |
| Node launcher | External `casper-node-launcher` | Embedded launcher logic |
| Requirements | Node + launcher + sidecar + scripts | Assets bundle (node + sidecar + templates) |
| Keys/accounts | Random keys, friction to name/locate | Deterministic keys from a seed (BIP32 paths) |
| macOS devnet start | Often requires extra local compilation | Download pre-built cross-platform bundles |
| Network feedback | Extra commands to watch blocks/txs | Persistent SSE connection with live output |

## Installation

```bash
cargo install casper-devnet --locked
```

## Docker usage

Pull the image:

```bash
docker pull ghcr.io/veles-labs/casper-devnet
```

Run a devnet with the default data location (persist assets and network state with a volume):

```bash
docker run --rm -it \
  -p 11101:11101 -p 14101:14101 -p 18101:18101 -p 22101:22101 -p 28101:28101 -p 32000:32000 \
  -v "$(pwd)/casper-devnet-data:/opt/casper-devnet-data" \
  ghcr.io/veles-labs/casper-devnet
```

Use a custom data directory by overriding `XDG_DATA_HOME` and mounting it:

```bash
docker run --rm -it \
  -e XDG_DATA_HOME=/data \
  -v "$(pwd)/casper-devnet-data:/data" \
  -p 11101:11101 -p 14101:14101 -p 18101:18101 -p 22101:22101 -p 28101:28101 -p 32000:32000 \
  ghcr.io/veles-labs/casper-devnet
```

The exposed ports map to node-1 services: RPC (11101), REST (14101), SSE (18101), network gossip
(22101), binary protocol (28101), and diagnostics websocket proxy
(ws://127.0.0.1:32000/diagnostics/node-1/). The diagnostics proxy also accepts HTTP POST requests
to `/diagnostics/node-1/` for non-websocket clients and streams NDJSON responses.

## Diagnostics HTTP Proxy

The diagnostics proxy is useful in environments where you cannot or do not want to keep a
websocket connection open. It accepts plain HTTP POST requests, forwards them to the node's
diagnostics Unix socket, and returns line-delimited JSON responses. This is handy for automation
or for setting failure points and collecting detailed runtime state.

Set a failure point (stop at a specific block height):

```bash
curl -v -XPOST --data 'stop --at block:250' http://127.0.0.1:32000/diagnostics/node-1/
```

Dump network info:

```bash
curl -v -XPOST --data 'net-info' http://127.0.0.1:32000/diagnostics/node-1/
```

Dump queues:

```bash
curl -v -XPOST --data 'dump-queues' http://127.0.0.1:32000/diagnostics/node-1/
```

For interactive workflows, use websockets so you can send commands and immediately see responses
without re-establishing connections:

```bash
wscat -c ws://127.0.0.1:32000/diagnostics/node-1/
```

## Usage

Add a local assets bundle:

```bash
casper-devnet assets add /path/to/assets-bundle.tar.gz
```

Add a custom override asset (symlink-backed local paths):

```bash
casper-devnet assets add dev \
  --casper-node /path/to/casper-node \
  --casper-sidecar /path/to/casper-sidecar \
  --chainspec /path/to/chainspec.toml \
  --node-config /path/to/node-config.toml \
  --sidecar-config /path/to/sidecar-config.toml
```

Download assets from the latest release:

```bash
casper-devnet assets pull
```

Supported host architectures:

- `aarch64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `x86_64-unknown-linux-gnu`

See also [https://github.com/veles-labs/devnet-launcher-assets/releases/](https://github.com/veles-labs/devnet-launcher-assets/releases/).

Force re-download:

```bash
casper-devnet assets pull --force
```

Override the target triple:

```bash
casper-devnet assets pull --target x86_64-unknown-linux-gnu
```

## Security note

`casper-devnet assets pull` downloads pre-built binaries from
[https://github.com/veles-labs/devnet-launcher-assets/releases](https://github.com/veles-labs/devnet-launcher-assets/).
If you are not comfortable running pre-built binaries, download the assets repo and rebuild the
binaries locally using the provided scripts before installing them with `assets add`.

List available protocol versions:

```bash
casper-devnet assets list
```

Start a devnet:

```bash
casper-devnet start
```

Stage a protocol upgrade from a custom asset:

```bash
casper-devnet stage-protocol dev --protocol-version 2.2.0 --activation-point 123
```

If a managed process is running (from `start` or MCP), staging runs in live mode and restarts
sidecars. Otherwise, staging runs in offline mode and only writes versioned
`nodes/node-*/bin/<version>` and `nodes/node-*/config/<version>` assets.
Live staging control uses a per-network Unix socket at `/tmp/<network-name>.socket`
for runtime stage requests.
In live mode, consensus keys are restored from the network seed before staging so
`migrate-data` can run successfully at the upgrade boundary.
Node and sidecar log aliases (for example `node-1.stdout`) are atomically repointed to
versioned log files during protocol transitions; use `tail -F` to follow across alias swaps.

Run MCP control plane server (STDIO + HTTP):

```bash
casper-devnet mcp
```

Run MCP in HTTP-only mode:

```bash
casper-devnet mcp --transport http --http-bind 127.0.0.1:32100 --http-path /mcp
```

Check whether a devnet has produced blocks (useful for CI):

```bash
casper-devnet is-ready
```

Create assets without starting processes:

```bash
casper-devnet start --setup-only
```

Use `--setup-only` when you want to tweak chainspecs or node configs before launching.

Rebuild assets:

```bash
casper-devnet start --force-setup
```

## MCP workflow

`casper-devnet mcp` does not auto-start a network. Use MCP tools in this order:

1. `spawn_network` (defaults to `force_setup=true` for fresh setup; set `force_setup=false` to resume existing assets).
2. `wait_network_ready` (waits for running processes, healthy `/status`, `reactor_state=Validate`, and first observed block).
3. Call network tools (RPC/status/block/log/SSE/transactions).

MCP server defaults:

- `transport=both`
- `http_bind=127.0.0.1:32100`
- `http_path=/mcp`

MCP tools require `network_name`; node-scoped tools also require `node_id`.
Managed networks are stopped automatically when the MCP server exits.
Use `managed_processes` to inspect managed node/sidecar processes, with optional process-name filtering and `running_only` control.
Use `stage_protocol` to stage custom-asset upgrades for managed networks (`live_mode=true`) or
discovered stopped networks (`live_mode=false`).
`rpc_query_global_state` auto-resolves the latest block hash when both `block_id` and `state_root_hash` are omitted.
For transaction construction, use MCP tools (`make_transaction_package_call`, `make_transaction_contract_call`, `make_transaction_session_wasm`) with `send_transaction_signed` instead of invoking external `casper-client` binaries.
`session_args` supports full CLType strings (including nested types such as `Option<List<U512>>`, `Map<String,U64>`, tuples, and `ByteArray[32]`). Scalars can be passed as string/number/bool, `null` maps to `None` for `Option<T>`, and composite values should be provided as hex bytes (`0x...`). Pass this field as JSON (array/object), not an escaped JSON string. Legacy `session_args_json` is still accepted for compatibility.
`send_transaction_signed.transaction` should be a typed JSON object. Field name `transaction_json` is not accepted, and encoded JSON strings are not supported.
Use MCP transaction query tools (`get_transaction`, `wait_transaction`) instead of shelling out to `curl` for `info_get_transaction` calls.
Valid `session_args` examples:
- `[{"name":"value","type":"I32","value":"1"}]`
- `[{"name":"items","type":"List<U64>","value":"0x03000000010000000000000002000000000000000300000000000000"}]`
Unsupported formats:
- `{"value":1}` (object shorthand)
- `["value:i32=1"]` (casper-client CLI arg string format)

Codex CLI stdio MCP example (`~/.codex/config.toml`):

```toml
[mcp_servers.casper-devnet]
command = "casper-devnet"
args = ["mcp", "--transport", "stdio"]
```

Or add it via Codex CLI:

```bash
codex mcp add casper-devnet -- casper-devnet mcp --transport stdio
```

If `casper-devnet` is not on `PATH`, set `command` to an absolute binary path.
Claude CLI config example: PRs welcome.

## Common flags

- `--protocol-version <version>`: Protocol version to use from the assets store (defaults to newest bundle)
- `--network-name <name>`: Network name for configs/paths (default: `casper-dev`)
- `--net-path <path>`: Override the network runtime root (default: platform data dir `.../networks`)
- `--node-count <n>`: Number of nodes (aliases: `--nodes`, `--validators`; default: 4)
- `--users <n>`: Number of user accounts (default: node count)
- `--delay <seconds>`: Genesis activation delay (default: 3). Keep it short for local devnets; increase if you need more time to attach tooling before genesis.
- `--log-level <level>`: Child process log level (default: `info`)
- `--node-log-format <format>`: Node logging format in config (default: `json`)
- `--setup-only`: Build assets and exit
- `--force-setup`: Rebuild assets even if they exist
- `--seed <string>`: Seed for deterministic devnet keys (default: `default`)

`casper-devnet mcp` flags:

- `--transport <stdio|http|both>`: MCP transport mode (default: `both`)
- `--http-bind <addr:port>`: HTTP bind address for streamable MCP (default: `127.0.0.1:32100`)
- `--http-path <path>`: HTTP mount path for MCP endpoint (default: `/mcp`)
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

`casper-devnet stage-protocol` flags:

- `--protocol-version <version>`: Protocol version to stage (required)
- `--activation-point <era-id>`: Future era id for activation (required)
- `--network-name <name>`: Network name for runtime paths (default: `casper-dev`)
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

## Assets bundle layout

The bundle is extracted into the platform data directory and should include a versioned root with
the following shape:

```
v2.1.1/bin/casper-node
v2.1.1/bin/casper-sidecar
v2.1.1/chainspec.toml
v2.1.1/sidecar-config.toml
v2.1.1/node-config.toml
```

Custom override assets are stored separately under `assets/custom/<name>/` as symlinks to local
`casper-node`, `casper-sidecar`, `chainspec.toml`, `node-config.toml`, and `sidecar-config.toml`.

For manual rebuilds and bundle scripts, see
[https://github.com/veles-labs/devnet-launcher-assets/](https://github.com/veles-labs/devnet-launcher-assets/).

## Notes

- The launcher runs the node directly and manages processes internally; no external supervisor is required.
- The embedded launcher state is handled within the process; only the node/sidecar binaries are required.
- Assets are stored under the platform data directory (e.g., `~/.local/share/xyz.veleslabs.casper-devnet` on Linux or `~/Library/Application Support/xyz.veleslabs.casper-devnet` on macOS), with `assets/` for bundles and `networks/` for runtime assets.
