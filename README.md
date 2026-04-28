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

Custom asset names are write-once: reusing an existing name returns an error instead of replacing
the asset directory.

List installed protocol bundles and custom assets:

```bash
casper-devnet assets list
```

Print absolute path to a custom asset directory (shell-substitution friendly):

```bash
casper-devnet assets path dev
vim "$(casper-devnet assets path dev)/chainspec.toml"
```

Custom assets also include hook samples under `hooks/`. Activate one by copying or renaming the
matching `.sample` file:

```bash
cp "$(casper-devnet assets path dev)/hooks/pre-stage-protocol.sample" \
  "$(casper-devnet assets path dev)/hooks/pre-stage-protocol"
chmod +x "$(casper-devnet assets path dev)/hooks/pre-stage-protocol"
```

List managed network directories:

```bash
casper-devnet networks list
```

Remove a managed network directory from disk:

```bash
casper-devnet networks rm casper-dev
casper-devnet networks rm casper-dev --yes
```

Print the staged per-node config directories for a protocol version:

```bash
casper-devnet network casper-dev path 2.2.0
```

Print the network root:

```bash
casper-devnet network casper-dev path
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

Start from a specific installed bundle, or from a custom asset:

```bash
casper-devnet start --asset 2.1.3
casper-devnet start --custom-asset dev
```

Override the chainspec protocol version while using the selected asset files:

```bash
casper-devnet start --asset 2.1.3 --protocol-version 2.2.0
```

Stage a protocol upgrade from a versioned or custom asset:

```bash
casper-devnet network casper-dev stage-protocol --asset 2.1.3 --protocol-version 2.2.0 --activation-point 123
casper-devnet network casper-dev stage-protocol --custom-asset dev --protocol-version 2.2.0 --activation-point 123
```

Derive deterministic account material from a seed and BIP32 path:

```bash
casper-devnet derive "m/44'/506'/0'/0/0" --secret-key
casper-devnet derive "m/44'/506'/0'/0/100" --public-key
casper-devnet derive "m/44'/506'/0'/0/100" --account-hash -o /tmp/derived
casper-devnet derive "m/44'/506'/0'/0/100" --account-hash -o -
```

Print a random live endpoint for a running node:

```bash
casper-devnet network casper-dev port --rpc
casper-devnet network casper-dev port --sse
casper-devnet network casper-dev port --rest
casper-devnet network casper-dev port --binary
casper-devnet network casper-dev port --diagnostics
```

When the network is actively managed by `casper-devnet`, this command prefers the live control
socket to discover currently running nodes before choosing an endpoint. If that live query is
unavailable or unresponsive, it falls back to `state.json` instead of hanging indefinitely.

Add managed non-genesis nodes to a running network:

```bash
casper-devnet network casper-dev add-nodes --count 2
```

`network <name> add-nodes` is live-only: it requires the foreground `start` or MCP-managed process
to be running so it can prepare assets, spawn the new node and sidecar processes, track them in
`state.json`, and stop them during normal shutdown. During expansion, the manager reads a recent
trusted hash from an existing node's REST `/status`, writes it to `[node].trusted_hash`, and uses
the active config's joining sync mode (`[node].sync_handling = "ttl"`, or
`sync_to_genesis = false` for legacy configs).
Added nodes inherit only the currently active protocol version. If a future protocol version was
already staged, run `network <name> stage-protocol` again after adding nodes so the new nodes
receive that staged version too.
The foreground `start` manager logs the full endpoint summary for nodes added through the control
plane and prints node reactor state changes observed by polling each node's REST `/status` every
500ms.

If a managed process is running (from `start` or MCP), staging runs in live mode and restarts
sidecars. Otherwise, staging runs in offline mode and only writes versioned
`nodes/node-*/bin/<version>` and `nodes/node-*/config/<version>` assets.
Live staging control uses a per-network Unix socket at `/tmp/<network-name>.socket`
for runtime stage requests.
In live mode, consensus keys are restored from the network seed before staging so
`migrate-data` can run successfully at the upgrade boundary.
Node and sidecar log aliases (for example `node-1.stdout`) are atomically repointed to
versioned log files during protocol transitions; use `tail -F` to follow across alias swaps.
If `assets/custom/<name>/hooks/pre-stage-protocol` exists, it runs before any stage-protocol
filesystem mutation with argv `<network_name> <protocol_version> <activation_point>`.
If `assets/custom/<name>/hooks/post-stage-protocol` exists, it runs once later at the real
upgrade boundary, after the launcher starts the target validator version, with argv
`<network_name> <protocol_version>`.
If `networks/<network>/hooks/pre-genesis` exists, it runs after assets have been prepared
for a fresh network but before the network is started, with argv
`<network_name> <protocol_version>`.
If `networks/<network>/hooks/post-genesis` exists, it runs once after the fresh network
produces its first block, with argv `<network_name> <protocol_version>`.
If `networks/<network>/hooks/block-added` exists, it runs on each observed new block with
argv `<network_name> <protocol_version>` and the block event JSON payload on stdin.
Each hook runs in its own working directory under `networks/<network>/hooks/work/<hook-name>/`, so
hooks can leave files behind for later hooks to inspect.
Hook stdout/stderr are streamed line by line through `casper-devnet` stderr as
`<hook_name> stdout: ...` and `<hook_name> stderr: ...`. Non-zero exits are still reported, but
successful exit code `0` is quiet. The raw hook streams are also written under
`networks/<network>/hooks/logs/` for network hooks and the same log directory is reused for
custom-asset staging hooks.
The generated sample hooks live under `assets/custom/<name>/hooks/*.sample` for stage hooks and
`networks/<network>/hooks/*.sample` for network hooks. The samples show how to call
`casper-devnet network <network> port --rpc`, issue an `info_get_status` JSON-RPC request with
`curl`, consume `block-added` JSON from stdin, and use `casper-devnet network <network> path
[<protocol_version>]` to locate the network root or staged per-node config directories.

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
casper-devnet network casper-dev is-ready
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
Use `stage_protocol` to stage versioned-asset or custom-asset upgrades for managed networks
(`live_mode=true`) or discovered stopped networks (`live_mode=false`).
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

- `--asset <version>`: Versioned asset bundle to use from the assets store (accepts `2.1.3` or `v2.1.3`; defaults to newest bundle)
- `--custom-asset <name>`: Custom asset under `assets/custom/<name>` to use instead of a versioned bundle
- `--protocol-version <version>`: Override the chainspec protocol version; when omitted, `start` uses the selected asset's chainspec value
- `--network-name <name>`: Network name for configs/paths (default: `casper-dev`)
- `--net-path <path>`: Override the network runtime root (default: platform data dir `.../networks`)
- `--node-count <n>`: Number of nodes (aliases: `--nodes`, `--validators`; default: 4)
- `--users <n>`: Number of user accounts (default: node count)
- `--delay <seconds>`: Genesis activation delay (default: 3). Keep it short for local devnets; increase if you need more time to attach tooling before genesis.
- `--log-level <level>`: Child process log level (default: `info`)
- `--node-log-format <format>`: Node logging format in config (default: `json`)
- `--setup-only`: Build assets and exit
- `--force-setup`: Rebuild assets even if they exist, while preserving `networks/<network>/hooks/`
- `--seed <string>`: Seed for deterministic devnet keys (default: `default`)

`casper-devnet mcp` flags:

- `--transport <stdio|http|both>`: MCP transport mode (default: `both`)
- `--http-bind <addr:port>`: HTTP bind address for streamable MCP (default: `127.0.0.1:32100`)
- `--http-path <path>`: HTTP mount path for MCP endpoint (default: `/mcp`)
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

`casper-devnet network <network> stage-protocol` flags:

- `[asset]`: Optional positional shorthand for `--asset <version>`
- `--asset <version>`: Versioned asset bundle to stage from
- `--custom-asset <name>`: Custom asset under `assets/custom/<name>` to stage from
- `--protocol-version <version>`: Protocol version to stage (required)
- `--activation-point <era-id>`: Future era id for activation (required)
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

Exactly one of `[asset]`, `--asset`, or `--custom-asset` is required for staging.

`casper-devnet derive` flags:

- `<path>`: BIP32 derivation path to resolve
- `--secret-key`: Print or write the derived secret key PEM
- `--public-key`: Print or write the derived public key hex
- `--account-hash`: Print or write the derived account hash
- `--seed <string>`: Deterministic seed for derivation (default: `default`)
- `-o, --output <path>`: Output directory, or `-` for stdout

Exactly one of `--secret-key`, `--public-key`, or `--account-hash` must be provided.

`casper-devnet network <network> path` flags:

- `[protocol_version]`: Optional protocol version to inspect
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

When no protocol version is provided, this prints the network root directory. When a protocol
version is provided, it prints one staged config directory per known node, one path per line.

`casper-devnet network <network> add-nodes` flags:

- `--count <n>`: Number of managed non-genesis nodes to add to the live network
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

`casper-devnet network <network> port` flags:

- `--rpc`: Print one random running node RPC URL
- `--sse`: Print one random running node SSE URL
- `--rest`: Print one random running node REST URL
- `--binary`: Print one random running node binary-port address
- `--diagnostics`: Print one random running node diagnostics socket path
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

Exactly one of `--rpc`, `--sse`, `--rest`, `--binary`, or `--diagnostics` must be provided.

`casper-devnet network <network> status` flags:

- `--node-id <id>`: Node id whose REST `/status` endpoint should be queried
- `--net-path <path>`: Override network runtime root (same behavior as `start`)

`casper-devnet network <network> is-ready` flags:

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
Each custom asset also gets:

```
assets/custom/<name>/hooks/pre-stage-protocol.sample
assets/custom/<name>/hooks/post-stage-protocol.sample
```

Only exact hook filenames are executed:

```
assets/custom/<name>/hooks/pre-stage-protocol
assets/custom/<name>/hooks/post-stage-protocol
```

The `.sample` files are boilerplate only and are never executed directly.

For manual rebuilds and bundle scripts, see
[https://github.com/veles-labs/devnet-launcher-assets/](https://github.com/veles-labs/devnet-launcher-assets/).

## Notes

- The launcher runs the node directly and manages processes internally; no external supervisor is required.
- The embedded launcher state is handled within the process; only the node/sidecar binaries are required.
- Assets are stored under the platform data directory (e.g., `~/.local/share/xyz.veleslabs.casper-devnet` on Linux or `~/Library/Application Support/xyz.veleslabs.casper-devnet` on macOS), with `assets/` for bundles and `networks/` for runtime assets.
