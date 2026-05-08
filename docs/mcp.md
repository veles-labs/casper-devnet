# MCP Workflow

`casper-devnet mcp` runs a control plane server for starting, inspecting, and
mutating managed devnets from MCP clients.

```bash
casper-devnet mcp
casper-devnet mcp --transport http --http-bind 127.0.0.1:32100 --http-path /mcp
casper-devnet mcp --transport stdio
```

Defaults:

- `transport=both`
- `http_bind=127.0.0.1:32100`
- `http_path=/mcp`

`casper-devnet mcp` does not auto-start a network. Managed networks are stopped
when the MCP server exits.

MCP tools require `network_name`. Node-scoped tools also require `node_id`.

## Basic Flow

Use MCP tools in this order:

1. `spawn_network`
2. `wait_network_ready`
3. Network tools such as status, RPC, block, log, SSE, transaction, and process
   inspection tools

`spawn_network` defaults to `force_setup=true` for fresh setup. Set
`force_setup=false` to resume existing generated assets.

`wait_network_ready` waits for running processes, healthy REST `/status`,
`reactor_state=Validate`, and the first observed block.

Use `managed_processes` to inspect managed node and sidecar processes. It
supports process-name filtering and `running_only` control.

Use `stage_protocol` to stage versioned-asset or custom-asset upgrades for
managed networks. For a running managed network, staging runs in live mode. For a
discovered stopped network, staging runs in offline mode. See
[Hooks and upgrades](hooks-and-upgrades.md) for protocol staging details.

## Client Configuration

Codex CLI stdio config example in `~/.codex/config.toml`:

```toml
[mcp_servers.casper-devnet]
command = "casper-devnet"
args = ["mcp", "--transport", "stdio"]
```

Add the same server via Codex CLI:

```bash
codex mcp add casper-devnet -- casper-devnet mcp --transport stdio
```

If `casper-devnet` is not on `PATH`, set `command` to the absolute binary path.

## Transaction Construction

Do not shell out to `casper-client` CLI binaries for MCP transaction
construction. Use MCP constructor tools:

- `make_transaction_package_call`
- `make_transaction_contract_call`
- `make_transaction_session_wasm`

Then send the typed transaction object with `send_transaction_signed`.

`send_transaction_signed` expects:

```json
{
  "transaction": {
    "...": "typed transaction JSON"
  }
}
```

Encoded JSON strings are not supported. Field name `transaction_json` is not
accepted.

## Session Arguments

Pass `session_args` as structured JSON, not as escaped JSON text. Supported
forms include full `RuntimeArgs` JSON or an array of `{name,type,value}` objects:

```json
[
  {
    "name": "value",
    "type": "I32",
    "value": "1"
  }
]
```

Nested CLType strings are supported, including `Option<List<U512>>`,
`Map<String,U64>`, tuples, and `ByteArray[32]`.

Scalars may be strings, numbers, booleans, or `null` for `None` with
`Option<T>`. Composite values such as `List`, `Map`, tuples, `Result`, and
`ByteArray` should use hex bytes:

```json
[
  {
    "name": "items",
    "type": "List<U64>",
    "value": "0x03000000010000000000000002000000000000000300000000000000"
  }
]
```

Unsupported formats:

```json
{"value": 1}
```

```json
["value:i32=1"]
```

Legacy `session_args_json` is accepted only for compatibility.

## Transaction Lookups

For transaction lookups, use MCP transaction query tools:

- `get_transaction` for a single fetch
- `wait_transaction` to poll until execution

Do not use `curl` for JSON-RPC transaction lookups in MCP workflows.

`rpc_query_global_state` auto-resolves the latest block hash when both
`block_id` and `state_root_hash` are omitted.
