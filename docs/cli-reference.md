# CLI Reference

This reference is written from the generated `--help` output. Re-check command
names, flags, and defaults with `cargo run --quiet -- ... --help` when changing
CLI behavior.

## Top-Level Commands

```text
casper-devnet <COMMAND>
```

| Command | Purpose |
| --- | --- |
| `start` | Setup assets if needed and start the devnet. |
| `mcp` | Run the MCP control plane server. |
| `assets` | Manage downloaded and custom asset bundles. |
| `derive` | Derive deterministic account material from a BIP32 path. |
| `network` | Inspect or mutate a managed network. |
| `networks` | List or remove managed network directories. |

## `casper-devnet start`

```text
casper-devnet start [OPTIONS]
```

Common examples:

```bash
casper-devnet start
casper-devnet start --asset 2.1.3
casper-devnet start --custom-asset dev
casper-devnet start --asset 2.1.3 --protocol-version 2.2.0
casper-devnet start --setup-only
casper-devnet start --force-setup
```

Options:

| Flag | Meaning |
| --- | --- |
| `--network-name <name>` | Network name used in paths and configs. Default: `casper-dev`. |
| `--net-path <path>` | Override the base path for network runtime assets. |
| `--asset <version>` | Versioned asset from the assets store, for example `2.1.3`. |
| `--custom-asset <name>` | Custom asset from `assets/custom/<name>`. |
| `--protocol-version <version>` | Protocol version to write into the generated chainspec. |
| `--chainspec-override <KEY=VALUE>` | Patch a generated chainspec value using TOML value syntax. Repeatable. |
| `--node-count <n>` | Number of nodes to create and start. Aliases: `--nodes`, `--validators`. Default: `4`. |
| `--users <n>` | Number of user accounts to generate. Defaults to node count. |
| `--delay <seconds>` | Genesis activation delay. Default: `3`. |
| `--log-level <level>` | Child process log level passed as `RUST_LOG`. Default: `info`. |
| `--node-log-format <format>` | Node logging format written to config. Default: `json`. |
| `--setup-only` | Create generated assets and exit without starting processes. |
| `--force-setup` | Rebuild generated assets even when they already exist. |
| `--seed <string>` | Deterministic seed for devnet key generation. Default: `default`. |

`--asset` and `--custom-asset` are mutually exclusive. If neither is supplied,
`start` uses the newest versioned asset bundle.

`--chainspec-override` uses `key.path=<toml-value>` syntax and only applies to a
fresh setup. Use `--force-setup` when the network directory already exists.
Overrides are applied before launcher defaults, so `--protocol-version`,
`--delay`, `--network-name`, and `--node-count` still control their generated
chainspec fields.

Use `--setup-only` when you want to tweak generated chainspecs or node configs
before launching. `--setup-only` writes configs that reference
`keys/secret_key.pem`, but it does not create regular consensus secret key PEM
files. Managed `start` and MCP runs use one-shot inherited pipe key delivery at
runtime.

## `casper-devnet assets`

```text
casper-devnet assets <COMMAND>
```

| Command | Purpose |
| --- | --- |
| `add` | Extract a local `.tar.gz` bundle, or install a named custom asset. |
| `pull` | Download bundles from the upstream asset release. |
| `list` | List installed protocol versions and custom assets. |
| `path` | Print the absolute path to a named custom asset directory. |

Download assets:

```bash
casper-devnet assets pull
casper-devnet assets pull --force
casper-devnet assets pull --target x86_64-unknown-linux-gnu
```

`assets pull` flags:

| Flag | Meaning |
| --- | --- |
| `--target <triple>` | Target triple to select from release assets. Defaults to the build target. |
| `--force` | Re-download and replace existing assets. |

Install assets:

```bash
casper-devnet assets add /path/to/assets-bundle.tar.gz

casper-devnet assets add dev \
  --casper-node /path/to/casper-node \
  --casper-sidecar /path/to/casper-sidecar \
  --chainspec /path/to/chainspec.toml \
  --node-config /path/to/node-config.toml \
  --sidecar-config /path/to/sidecar-config.toml
```

`assets add` accepts local files only; URLs are not supported.

Custom asset names are write-once. Reusing an existing name returns an error
instead of replacing the asset directory. Custom assets install symlinks to the
provided files; network hooks remain network-scoped under `networks/<network>/`.

> [!TIP]
> Symlink-backed custom assets are useful for rapid source-build feedback loops.
> Point `--casper-node` and `--casper-sidecar` at binaries under a local
> `./target` directory, rebuild those binaries from source, then restart the
> network to pick up the newly built node and sidecar through the existing
> symlinks.

Supported release targets for `assets pull`:

- `aarch64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `x86_64-unknown-linux-gnu`

Inspect assets:

```bash
casper-devnet assets list
casper-devnet assets path dev
```

## `casper-devnet networks`

```text
casper-devnet networks <COMMAND>
```

```bash
casper-devnet networks list
casper-devnet networks list --net-path /tmp/devnets
casper-devnet networks rm casper-dev
casper-devnet networks rm casper-dev --yes
```

| Command | Flags |
| --- | --- |
| `list` | `--net-path <path>` |
| `rm <network-name>` | `--net-path <path>`, `-y, --yes` |

## `casper-devnet network <network>`

```text
casper-devnet network <NETWORK_NAME> <COMMAND>
```

| Command | Purpose |
| --- | --- |
| `add-nodes` | Add managed nodes to a running network. |
| `is-ready` | Check whether a network has observed a block. |
| `path` | Print the network root or staged config paths. |
| `port` | Print a random live endpoint for a running node. |
| `status` | Print REST `/status` for a specific node. |
| `stage-protocol` | Stage a protocol upgrade. |

Examples:

```bash
casper-devnet network casper-dev is-ready
casper-devnet network casper-dev path
casper-devnet network casper-dev path 2.2.0
casper-devnet network casper-dev port --rpc
casper-devnet network casper-dev status --node-id 1
casper-devnet network casper-dev add-nodes --count 2
```

Endpoint flags for `port` are mutually exclusive:

```bash
casper-devnet network casper-dev port --rpc
casper-devnet network casper-dev port --sse
casper-devnet network casper-dev port --rest
casper-devnet network casper-dev port --binary
casper-devnet network casper-dev port --diagnostics
```

`port` prefers the live control socket when a network is actively managed by
`start` or MCP. If live discovery is unavailable, it falls back to `state.json`.

Stage a protocol upgrade:

```bash
casper-devnet network casper-dev stage-protocol \
  --asset 2.1.3 \
  --protocol-version 2.2.0 \
  --activation-point 123
```

`stage-protocol` requires exactly one asset selector: positional `[asset]`,
`--asset <version>`, or `--custom-asset <name>`. It also requires
`--protocol-version <version>` and `--activation-point <era-id>`.
`--chainspec-override <KEY=VALUE>` may be repeated.

See [Hooks and upgrades](hooks-and-upgrades.md) for live/offline staging behavior
and hook timing.

## `casper-devnet mcp`

```text
casper-devnet mcp [OPTIONS]
```

Options:

| Flag | Meaning |
| --- | --- |
| `--transport <stdio|http|both>` | MCP transport mode. Default: `both`. |
| `--http-bind <addr:port>` | HTTP bind address. Default: `127.0.0.1:32100`. |
| `--http-path <path>` | HTTP mount path. Default: `/mcp`. |
| `--net-path <path>` | Override network runtime root. |

See [MCP workflow](mcp.md) for client configuration and MCP tool usage.

## `casper-devnet derive`

```text
casper-devnet derive [OPTIONS] <--secret-key|--public-key|--account-hash> <PATH>
```

Examples:

```bash
casper-devnet derive "m/44'/506'/0'/0/0" --secret-key
casper-devnet derive "m/44'/506'/0'/0/100" --public-key
casper-devnet derive "m/44'/506'/0'/0/100" --account-hash -o -
```

Options:

| Flag | Meaning |
| --- | --- |
| `<PATH>` | BIP32 derivation path, for example `m/44'/506'/0'/0/0`. |
| `--secret-key` | Print or write the derived secret key PEM. |
| `--public-key` | Print or write the derived public key hex. |
| `--account-hash` | Print or write the derived account hash. |
| `--seed <string>` | Deterministic seed for derivation. Default: `default`. |
| `-o, --output <path>` | Output directory, or `-` to force stdout. |

Exactly one of `--secret-key`, `--public-key`, or `--account-hash` must be used.

## Asset Bundle Layout

Versioned bundles are extracted under `assets/v{version}/` and must contain:

```text
vX.Y.Z/bin/casper-node
vX.Y.Z/bin/casper-sidecar
vX.Y.Z/chainspec.toml
vX.Y.Z/node-config.toml
vX.Y.Z/sidecar-config.toml
vX.Y.Z/manifest.json
```

Runtime state is stored at `networks/<network>/state.json` under the platform
data directory unless `--net-path` overrides the network runtime root.
