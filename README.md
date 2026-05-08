# Casper Devnet Launcher

Casper Devnet Launcher is a Rust CLI for running a local Casper devnet from
pre-built assets. It borrows the useful parts of the NCTL workflow, but targets
smart contract developers, application developers, and CI jobs that need one
command to prepare assets, start processes, stream feedback, and shut down cleanly.

![Casper Devnet Launcher demo](casper-devnet.gif)

Links: [Crates.io](https://crates.io/crates/casper-devnet),
[docs.rs](https://docs.rs/casper-devnet),
[GitHub](https://github.com/veles-labs/casper-devnet-launcher),
[Docker image](https://github.com/veles-labs/casper-devnet-launcher/pkgs/container/casper-devnet),
[asset releases](https://github.com/veles-labs/devnet-launcher-assets/releases).

## Documentation

| Document | Use it for |
| --- | --- |
| [Docs map](docs/README.md) | Choosing the smallest doc to read or update. |
| [CLI reference](docs/cli-reference.md) | Commands, flags, defaults, and examples. |
| [MCP workflow](docs/mcp.md) | Running the MCP server and constructing transactions. |
| [Hooks and upgrades](docs/hooks-and-upgrades.md) | Network hooks, protocol staging, and live/offline upgrades. |
| [Diagnostics proxy](docs/diagnostics.md) | Websocket and HTTP access to node diagnostics sockets. |

## Quick Start

```bash
cargo install casper-devnet --locked
casper-devnet assets pull
casper-devnet start
```

`assets pull` downloads the latest matching asset bundle, verifies the `.sha512`
file, compares `manifest.json`, installs the bundle, and writes `assets/latest`
with the release tag. `start` uses the newest installed bundle by default and
starts the `casper-dev` network with four nodes.

When a network already has prepared assets, `casper-devnet start` resumes from
the existing network directory. Use `--force-setup` to rebuild generated assets.

## Why This Exists

NCTL is built for core protocol development and source-tree workflows. Casper
Devnet Launcher is built for repeatable local devnets in app development, tests,
and CI.

| Area | NCTL | Casper Devnet Launcher |
| --- | --- | --- |
| Audience | Core protocol development | Contract/app developers, CI |
| Startup | Multi-step shell workflow | `casper-devnet start` |
| Process control | External supervisor | Managed inside the Rust process |
| Node launcher | External binary | Embedded launcher logic |
| Assets | Built from local trees | Downloaded or locally added bundles |
| Accounts | Random/generated workflow | Deterministic seed-based material |
| Feedback | Extra commands | Live SSE output during the run |

## Docker

Pull the image:

```bash
docker pull ghcr.io/veles-labs/casper-devnet
```

Run a devnet and persist assets plus network state in a local volume:

```bash
docker run --rm -it \
  -p 11101:11101 -p 14101:14101 -p 18101:18101 -p 22101:22101 -p 28101:28101 -p 32000:32000 \
  -v "$(pwd)/casper-devnet-data:/opt/casper-devnet-data" \
  ghcr.io/veles-labs/casper-devnet
```

Use a custom data directory by overriding `XDG_DATA_HOME`:

```bash
docker run --rm -it \
  -e XDG_DATA_HOME=/data \
  -v "$(pwd)/casper-devnet-data:/data" \
  -p 11101:11101 -p 14101:14101 -p 18101:18101 -p 22101:22101 -p 28101:28101 -p 32000:32000 \
  ghcr.io/veles-labs/casper-devnet
```

The exposed ports map to node-1 RPC (`11101`), REST (`14101`), SSE (`18101`),
network gossip (`22101`), binary protocol (`28101`), and diagnostics websocket
proxy (`32000`). See [Diagnostics proxy](docs/diagnostics.md) for details.

## Common Workflows

Install or update downloaded assets:

```bash
casper-devnet assets pull
casper-devnet assets pull --force
casper-devnet assets pull --target x86_64-unknown-linux-gnu
```

Install local assets:

```bash
casper-devnet assets add /path/to/assets-bundle.tar.gz

casper-devnet assets add dev \
  --casper-node /path/to/casper-node \
  --casper-sidecar /path/to/casper-sidecar \
  --chainspec /path/to/chainspec.toml \
  --node-config /path/to/node-config.toml \
  --sidecar-config /path/to/sidecar-config.toml
```

List and inspect assets:

```bash
casper-devnet assets list
casper-devnet assets path dev
```

Start with a specific asset or protocol version:

```bash
casper-devnet start --asset 2.1.3
casper-devnet start --custom-asset dev
casper-devnet start --asset 2.1.3 --protocol-version 2.2.0
```

Prepare assets without starting processes, or rebuild an existing network:

```bash
casper-devnet start --setup-only
casper-devnet start --force-setup
casper-devnet start --force-setup \
  --chainspec-override 'core.minimum_era_height=1'
```

Inspect managed networks:

```bash
casper-devnet networks list
casper-devnet networks rm casper-dev --yes
casper-devnet network casper-dev is-ready
casper-devnet network casper-dev port --rpc
casper-devnet network casper-dev port --sse
```

Derive deterministic account material from a seed and BIP32 path:

```bash
casper-devnet derive "m/44'/506'/0'/0/0" --secret-key
casper-devnet derive "m/44'/506'/0'/0/100" --public-key
casper-devnet derive "m/44'/506'/0'/0/100" --account-hash -o -
```

For the full command surface, see the [CLI reference](docs/cli-reference.md).

## MCP, Upgrades, And Diagnostics

Run the MCP control plane server:

```bash
casper-devnet mcp
casper-devnet mcp --transport http --http-bind 127.0.0.1:32100 --http-path /mcp
```

MCP does not auto-start a network. Use `spawn_network`, `wait_network_ready`,
then the network tools exposed by the MCP server. Transaction construction rules
and client configuration examples are in [MCP workflow](docs/mcp.md).

Stage a protocol upgrade:

```bash
casper-devnet network casper-dev stage-protocol \
  --asset 2.1.3 \
  --protocol-version 2.2.0 \
  --activation-point 123
```

Hook lifecycle and upgrade behavior are documented in
[Hooks and upgrades](docs/hooks-and-upgrades.md). Diagnostics websocket and HTTP
proxy examples are documented in [Diagnostics proxy](docs/diagnostics.md).

## Assets And Security

Downloaded assets come from
[veles-labs/devnet-launcher-assets releases](https://github.com/veles-labs/devnet-launcher-assets/releases).
If you are not comfortable running pre-built binaries, rebuild assets locally
from the assets repository and install the resulting bundle with `assets add`.

Assets and runtime state live under the platform data directory selected by the
`directories` crate:

- `assets/` stores downloaded bundles as `v{version}/...`
- `assets/custom/<name>/` stores symlink-backed custom assets
- `networks/<network>/` stores generated runtime assets and `state.json`

Versioned bundles must contain:

```text
vX.Y.Z/bin/casper-node
vX.Y.Z/bin/casper-sidecar
vX.Y.Z/chainspec.toml
vX.Y.Z/node-config.toml
vX.Y.Z/sidecar-config.toml
vX.Y.Z/manifest.json
```

For manual rebuilds and bundle scripts, see
[veles-labs/devnet-launcher-assets](https://github.com/veles-labs/devnet-launcher-assets/).
