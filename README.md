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

Build the image:

```bash
docker build -t casper-devnet:local .
```

Run a devnet with the default data location (persist assets and network state with a volume):

```bash
docker run --rm -it \
  -p 11101:11101 -p 14101:14101 -p 18101:18101 -p 22101:22101 -p 28101:28101 \
  -v "$(pwd)/casper-devnet-data:/opt/casper-devnet-data" \
  casper-devnet:local
```

Use a custom data directory by overriding `XDG_DATA_HOME` and mounting it:

```bash
docker run --rm -it \
  -e XDG_DATA_HOME=/data \
  -v "$(pwd)/casper-devnet-data:/data" \
  -p 11101:11101 -p 14101:14101 -p 18101:18101 -p 22101:22101 -p 28101:28101 \
  casper-devnet:local
```

The exposed ports map to node-1 services: RPC (11101), REST (14101), SSE (18101), network gossip
(22101), and binary protocol (28101).

## Usage

Add a local assets bundle:

```bash
casper-devnet assets add /path/to/assets-bundle.tar.gz
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

Create assets without starting processes:

```bash
casper-devnet start --setup-only
```

Use `--setup-only` when you want to tweak chainspecs or node configs before launching.

Rebuild assets:

```bash
casper-devnet start --force-setup
```

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

For manual rebuilds and bundle scripts, see
[https://github.com/veles-labs/devnet-launcher-assets/](https://github.com/veles-labs/devnet-launcher-assets/).

## Notes

- The launcher runs the node directly and manages processes internally; no external supervisor is required.
- The embedded launcher state is handled within the process; only the node/sidecar binaries are required.
- Assets are stored under the platform data directory (e.g., `~/.local/share/xyz.veleslabs.casper-devnet` on Linux or `~/Library/Application Support/xyz.veleslabs.casper-devnet` on macOS), with `assets/` for bundles and `networks/` for runtime assets.
