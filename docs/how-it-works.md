# How It Works

This document describes what `casper-devnet` does under the hood when it pulls
assets, prepares a network, starts processes, and handles validator keys.

## Data Roots

The launcher uses the platform data directory from the `directories` crate:

```text
<data-dir>/
  assets/
    latest
    vX.Y.Z/
    custom/<name>/
  networks/
    <network>/
```

`assets/` is the global asset store. `networks/<network>/` is the generated
runtime directory for one managed network. `--net-path <path>` changes the
runtime networks root, but not the downloaded asset bundle root.

Versioned asset bundles are unpacked as `assets/vX.Y.Z/` and must provide:

```text
bin/casper-node
bin/casper-sidecar
chainspec.toml
node-config.toml
sidecar-config.toml
manifest.json
```

Custom assets live under `assets/custom/<name>/` and contain symlinks to local
files supplied with `casper-devnet assets add <name> --casper-node ...`. The
symlinks are intentional: point them at binaries under a local `./target`
directory, rebuild from source, and restart the network to pick up the newly
built node or sidecar.

## Start Flow

`casper-devnet start` follows this sequence:

1. Resolve the runtime layout as `networks/<network-name>`, defaulting to
   `casper-dev`.
2. If generated node assets already exist and neither `--force-setup` nor
   `--setup-only` is used, resume from the existing network directory.
3. If `--force-setup` is used, remove generated network assets while preserving
   `hooks/`.
4. If setup is needed, call the local setup pipeline described below.
5. Ensure hook samples exist.
6. If `state.json` does not exist yet, run `pre-genesis` if present and queue
   `post-genesis` metadata.
7. Create or refresh `state.json`.
8. Start every node through the embedded launcher and start each sidecar as a
   Tokio child process.
9. Start the diagnostics proxy, control socket, SSE listeners, reactor-state
   pollers, and exit watchers.
10. On Ctrl-C or unexpected child exit, send signals directly to child PIDs and
    persist process state.

There is no external `casper-node-launcher` process. The launcher state machine
is embedded in this binary.

## Generated Network Structure

A managed network after setup and first start contains:

```text
networks/<network>/
  chainspec/
    chainspec.toml
    accounts.toml
  derived-accounts.csv
  state.json
  hooks/
    *.sample
    logs/
    work/
    .pending/
    .status/
  nodes/
    node-1/
      bin/<protocol_version_fs>/
        casper-node
        casper-sidecar
      config/
        casper-node-launcher-state.toml
        <protocol_version_fs>/
          chainspec.toml
          accounts.toml
          config.toml
          sidecar.toml
      logs/
      storage/
```

`--setup-only` creates generated assets but does not create `state.json` or the
per-node `casper-node-launcher-state.toml` files; those are created when managed
processes are started.

Protocol version directory names replace dots with underscores, so `2.2.0`
becomes `2_2_0`.

After processes start, top-level log aliases such as `node-1.stdout`,
`node-1.stderr`, `sidecar-1.stdout`, and `sidecar-1.stderr` point at the active
log files. Versioned log aliases are updated during protocol transitions.

## Setup Pipeline

Setup starts by resolving an asset:

- no selector: newest installed versioned bundle
- `--asset <version>`: `assets/v<version>/`
- `--custom-asset <name>`: `assets/custom/<name>/`

Both `casper-node --version` and `casper-sidecar --version` are checked before
the asset is used.

The protocol version is `--protocol-version` when provided. Otherwise it is read
from the selected chainspec template.

The setup pipeline then:

1. Creates `chainspec/`, `nodes/`, and each node's `bin/`, `config/`, `logs/`,
   and `storage/` directories.
2. Installs binaries into every node's `bin/<version>/` directory. Versioned
   assets use hardlinks. Custom assets use symlinks.
3. Derives validator and user account material from the configured seed.
4. Writes `derived-accounts.csv` with public metadata, derivation paths, account
   hashes, and balances.
5. Copies the asset `chainspec.toml` to `chainspec/chainspec.toml`.
6. Applies user `--chainspec-override` values.
7. Applies launcher-owned chainspec fields: `protocol.activation_point`,
   `protocol.version`, `network.name`, and `core.validator_slots`.
8. Writes `chainspec/accounts.toml` for validators and users.
9. Copies chainspec and accounts into each node's versioned config directory.
10. Copies and rewrites `node-config.toml` as each node's `config.toml`.
11. Copies and rewrites `sidecar-config.toml` as each node's `sidecar.toml`.
12. Writes hook samples under `hooks/` when missing.

Launcher-owned chainspec fields are applied after user overrides. If an override
targets a launcher-owned field, the launcher value wins and a warning is printed.

## Config Files Picked Up

The selected asset contributes these templates:

```text
chainspec.toml
node-config.toml
sidecar-config.toml
```

After setup, each node uses the copied and rewritten files under:

```text
nodes/node-<n>/config/<protocol_version_fs>/
```

The embedded launcher starts `casper-node validator` with that versioned
`config.toml`. Sidecar starts with:

```text
casper-sidecar --path-to-config <sidecar.toml>
```

The embedded launcher also sets `CASPER_CONFIG_DIR` to the node's config root
and sets child `RUST_LOG` from `--log-level`.

## Config Mutations Before Start

`chainspec/chainspec.toml` is copied from the asset template, then rewritten:

| Field | Value |
| --- | --- |
| `protocol.activation_point` | Current UTC time plus `--delay`, formatted as RFC3339 for genesis setup. |
| `protocol.version` | `--protocol-version` or the asset chainspec version. |
| `network.name` | `--network-name`, default `casper-dev`. |
| `core.validator_slots` | `--node-count`, default `4`. |

Each node's `config.toml` is copied from `node-config.toml`, then rewritten:

| Field | Value |
| --- | --- |
| `consensus.secret_key_path` | `../../keys/secret_key.pem` on disk. Replaced at runtime by a temp config using an inherited fd path. |
| `logging.format` | `--node-log-format`, default `json`. |
| `network.bind_address` | Local network port, `0.0.0.0:22101` for node-1. |
| `network.known_addresses` | Bootstrap addresses for local nodes. |
| `storage.path` | `../../storage`. |
| `rest_server.address` | `0.0.0.0:14<n>`. |
| `event_stream_server.address` | `0.0.0.0:18<n>`. |
| `diagnostics_port.socket_path` | Temp-dir socket path for the node. |
| `binary_port_server.address` | `0.0.0.0:28<n>`. |
| `binary_port_server.allow_request_get_trie` | `true`. |
| `binary_port_server.allow_request_speculative_exec` | `true`. |

Each node's `sidecar.toml` is copied from `sidecar-config.toml`, then rewritten:

| Field | Value |
| --- | --- |
| `rpc_server.main_server.ip_address` | `0.0.0.0`. |
| `rpc_server.main_server.port` | Node RPC port, `11101` for node-1. |
| `rpc_server.node_client.ip_address` | `0.0.0.0`. |
| `rpc_server.node_client.port` | Node binary port, `28101` for node-1. |

The base port scheme is RPC `11000`, REST `14000`, SSE `18000`, network
`22000`, and binary `28000`, with an offset of `100 + node_id`.

## Deterministic Accounts And Private Keys

This tool does not implement a remote signer service or remote signing protocol.
Casper node still needs a consensus secret key path in config, so
`casper-devnet` gives it a file-like path backed by an inherited pipe at process
start.

The key model is devnet-only and deterministic:

- The root key is derived from the seed string with a Casper devnet domain.
- Validator paths use `m/44'/506'/0'/0/<node_id - 1>`.
- User paths start at `m/44'/506'/0'/0/100`.
- `derived-accounts.csv` records public keys, account hashes, paths, and
  balances, but not secret key PEMs.
- `casper-devnet derive ... --secret-key` is the explicit way to export a
  secret PEM.

During normal `start` and MCP-managed runs:

1. Any stale `nodes/node-<n>/keys/secret_key.pem` file is removed.
2. The launcher creates temporary node config files beside the real configs.
3. In those temp configs, `consensus.secret_key_path` is changed to
   `/proc/self/fd/<fd>` on Linux or `/dev/fd/<fd>` on macOS.
4. Before spawning the node, the launcher installs inherited read fds into the
   child process.
5. A Tokio task derives the node's deterministic PEM and writes it once to the
   pipe.
6. The child reads the PEM through the fd path as if it were a file.
7. Temporary configs and pipe fds are cleaned up after the child exits.

This avoids persisting regular validator secret key files during managed runs.
It is not remote signing and it is not intended as a production key-management
model.

`--setup-only` writes configs that reference `keys/secret_key.pem`, but it does
not create those key files. Managed `start` and MCP runs use the inherited-pipe
delivery described above.

## Runtime State

`state.json` stores process bookkeeping:

- creation and update timestamps as RFC3339
- last observed block height
- process records for node and sidecar children
- command, args, cwd, PID, log paths, exit code or signal, and process status

The embedded launcher separately stores node-launcher state at:

```text
nodes/node-<n>/config/casper-node-launcher-state.toml
```

That file tracks whether the node should run as a validator or run
`migrate-data` between two installed protocol versions.

## Protocol Upgrades

Staging a protocol writes a new `bin/<version>/` and `config/<version>/` for
each node. The staged chainspec uses an era activation point rather than a
genesis timestamp.

When the node exits with code `0` at an upgrade boundary, the embedded launcher:

1. Detects the next installed version.
2. Runs `casper-node migrate-data` with old and new configs.
3. Moves launcher state back to validator mode for the new version.
4. Starts the validator on the new version.
5. Runs any queued `post-stage-protocol` hook.

See [Hooks and upgrades](hooks-and-upgrades.md) for hook timing and live/offline
staging behavior.
