# Hooks And Upgrades

Casper Devnet Launcher supports network-scoped hooks and protocol staging for
versioned or custom assets.

## Stage A Protocol

```bash
casper-devnet network casper-dev stage-protocol \
  --asset 2.1.3 \
  --protocol-version 2.2.0 \
  --activation-point 123

casper-devnet network casper-dev stage-protocol \
  --custom-asset dev \
  --protocol-version 2.2.0 \
  --activation-point 123 \
  --chainspec-override 'core.minimum_era_height=1'
```

Exactly one of positional `[asset]`, `--asset <version>`, or
`--custom-asset <name>` is required.

When the network is actively managed by `casper-devnet start` or MCP, staging
runs in live mode and restarts sidecars. Otherwise, staging runs in offline mode
and only writes versioned `nodes/node-*/bin/<version>` and
`nodes/node-*/config/<version>` assets.

Live staging uses a per-network Unix control socket at
`/tmp/<network-name>.socket`.

## Hook Files

Hooks live under the managed network directory:

```text
networks/<network>/hooks/pre-genesis
networks/<network>/hooks/post-genesis
networks/<network>/hooks/block-added
networks/<network>/hooks/pre-stage-protocol
networks/<network>/hooks/post-stage-protocol
```

Generated samples live next to them:

```text
networks/<network>/hooks/pre-genesis.sample
networks/<network>/hooks/post-genesis.sample
networks/<network>/hooks/block-added.sample
networks/<network>/hooks/pre-stage-protocol.sample
networks/<network>/hooks/post-stage-protocol.sample
```

Only exact active hook filenames are executed. `.sample` files are boilerplate
and are never executed directly.

Custom assets install only symlink-backed asset files. Hooks are network-scoped
and live under `networks/<network>/hooks/`.

## Hook Lifecycle

`pre-genesis` runs after assets have been prepared for a fresh network but before
the network starts. It receives:

```text
<network_name> <protocol_version>
```

`post-genesis` runs once after the fresh network produces its first block. It
receives:

```text
<network_name> <protocol_version>
```

`block-added` runs on each observed new block. It receives:

```text
<network_name> <protocol_version>
```

The block event JSON payload is passed on stdin.

`pre-stage-protocol` runs after target version per-node `bin/<version>` and
`config/<version>` directories have been staged, and before post-stage metadata
is queued. It receives:

```text
<network_name> <protocol_version> <activation_point>
```

Use `casper-devnet network <network> path <protocol_version>` inside the hook to
locate staged per-node config directories. If this hook fails, the newly staged
version directories are removed and `post-stage-protocol` is not queued.

`post-stage-protocol` runs once at the real upgrade boundary, after the launcher
starts the target validator version. It receives:

```text
<network_name> <protocol_version>
```

## Hook Runtime

Each hook runs in a dedicated working directory:

```text
networks/<network>/hooks/work/<hook-name>/
```

Hook stdout and stderr are streamed through `casper-devnet` stderr as:

```text
<hook_name> stdout: ...
<hook_name> stderr: ...
```

Successful exit code `0` is quiet. Non-zero exits are reported. Raw hook streams
are also written under:

```text
networks/<network>/hooks/logs/
```

The sample hooks show how to call `casper-devnet network <network> port --rpc`,
issue an `info_get_status` JSON-RPC request, consume `block-added` JSON from
stdin, and use `casper-devnet network <network> path [<protocol_version>]`.

## Managed Keys And Logs

Managed node processes serve consensus secret keys through inherited pipe file
descriptors. The launcher derives the PEM from the network seed and writes it
once after each child process starts.

On-disk configs still reference `keys/secret_key.pem`, but validator and
`migrate-data` processes are started with temporary configs whose consensus key
paths point at `/proc/self/fd/<fd>` on Linux or `/dev/fd/<fd>` on macOS.

Node and sidecar log aliases, such as `node-1.stdout`, are atomically repointed
to versioned log files during protocol transitions. Use `tail -F` to follow logs
across alias swaps.

## Adding Nodes

Add managed non-genesis nodes to a running network:

```bash
casper-devnet network casper-dev add-nodes --count 2
```

`add-nodes` is live-only. It requires the foreground `start` process or an
MCP-managed process to be running so the manager can prepare assets, spawn node
and sidecar processes, track them in `state.json`, and stop them during normal
shutdown.

During expansion, the manager reads a recent trusted hash from an existing
node's REST `/status`, writes it to `[node].trusted_hash`, and uses the active
config's joining sync mode. Added nodes inherit the active protocol version and
any higher protocol versions already staged on existing nodes.
