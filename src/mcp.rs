use self::args_parser::parse_session_args;
use crate::assets::{self, AssetsLayout, SetupOptions};
use crate::process::{self, StartPlan};
use crate::state::{ProcessKind, ProcessStatus, STATE_FILE_NAME, State};
use anyhow::{Context, Result, anyhow};
use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;
use casper_types::contracts::ContractHash;
use casper_types::{
    AddressableEntityHash, AsymmetricType, Digest, EntityVersion, Key, PricingMode, PublicKey,
    SecretKey, TimeDiff, Transaction, TransactionHash, TransactionRuntimeParams, TransactionV1Hash,
    URef,
};
use clap::{Args, ValueEnum};
use futures::StreamExt;
use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;
use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{CallToolResult, ServerCapabilities, ServerInfo};
use rmcp::service::ServiceExt;
use rmcp::transport::{
    StreamableHttpServerConfig,
    streamable_http_server::{session::local::LocalSessionManager, tower::StreamableHttpService},
};
use rmcp::{
    ErrorData, ServerHandler, tool, tool_handler, tool_router, transport::stdio as mcp_stdio,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::fs as tokio_fs;
use tokio::sync::{Mutex, Notify};
use veles_casper_rust_sdk::TransactionV1Builder;
use veles_casper_rust_sdk::jsonrpc::CasperClient;
use veles_casper_rust_sdk::sse::event::SseEvent;
use veles_casper_rust_sdk::sse::{self, config::ListenerConfig};

const DEFAULT_HTTP_BIND: &str = "127.0.0.1:32100";
const DEFAULT_HTTP_PATH: &str = "/mcp";
const DEFAULT_NETWORK_NAME: &str = "casper-dev";
const DEFAULT_NODE_COUNT: u32 = 4;
const DEFAULT_DELAY_SECS: u64 = 3;
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_NODE_LOG_FORMAT: &str = "json";
const DEFAULT_SEED: &str = "default";
const DEFAULT_TIMEOUT_SECS: u64 = 60;
const DEFAULT_LOG_PAGE_SIZE: usize = 200;
const DEFAULT_SSE_PAGE_SIZE: usize = 200;
const DEFAULT_SSE_HISTORY_CAPACITY: usize = 20_000;
const DEFAULT_PAYMENT_AMOUNT: u64 = 100_000_000_000;

mod args_parser;

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum McpTransport {
    Stdio,
    Http,
    Both,
}

#[derive(Debug, Args, Clone)]
pub struct McpArgs {
    #[arg(long, value_enum, default_value_t = McpTransport::Both)]
    transport: McpTransport,

    #[arg(long, default_value = DEFAULT_HTTP_BIND)]
    http_bind: String,

    #[arg(long, default_value = DEFAULT_HTTP_PATH)]
    http_path: String,

    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct McpServer {
    manager: Arc<NetworkManager>,
    tool_router: ToolRouter<Self>,
}

impl McpServer {
    fn new(manager: Arc<NetworkManager>) -> Self {
        Self {
            manager,
            tool_router: Self::tool_router(),
        }
    }

    fn server_info() -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Control multiple local Casper devnets. Start with spawn_network, then wait_network_ready before RPC or transaction tools. Do not use external casper-client binaries or curl; use MCP tools directly (for example get_transaction, wait_transaction, make_transaction_package_call, make_transaction_contract_call, make_transaction_session_wasm, send_transaction_signed).".to_string(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

#[tool_router]
impl McpServer {
    #[tool(
        description = "Spawn or resume a managed network. Defaults to force_setup=true for fresh devnet startup."
    )]
    async fn spawn_network(
        &self,
        Parameters(request): Parameters<SpawnNetworkRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let result = self
            .manager
            .spawn_network(request)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(result).map_err(internal_serde_error)?,
        ))
    }

    #[tool(
        description = "Wait for a managed network to be ready: processes running, /status healthy, reactor Validate, and at least one block observed."
    )]
    async fn wait_network_ready(
        &self,
        Parameters(request): Parameters<WaitNetworkReadyRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let result = self
            .manager
            .wait_network_ready(&request.network_name, request.timeout_seconds)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(result).map_err(internal_serde_error)?,
        ))
    }

    #[tool(
        description = "Despawn a managed network. By default it stops processes and keeps files; set purge=true to remove files."
    )]
    async fn despawn_network(
        &self,
        Parameters(request): Parameters<DespawnNetworkRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let result = self
            .manager
            .despawn_network(&request.network_name, request.purge.unwrap_or(false))
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(result).map_err(internal_serde_error)?,
        ))
    }

    #[tool(description = "List discovered network names and managed/running state.")]
    async fn list_networks(
        &self,
        _: Parameters<ListNetworksRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let result = self.manager.list_networks().await.map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(result).map_err(internal_serde_error)?,
        ))
    }

    #[tool(
        description = "List managed network processes, optionally filtered by process name. Defaults to running_only=true."
    )]
    async fn managed_processes(
        &self,
        Parameters(request): Parameters<ManagedProcessesRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let result = self
            .manager
            .managed_processes(request)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(result).map_err(internal_serde_error)?,
        ))
    }

    #[tool(description = "Call node REST /status for a managed network node.")]
    async fn status(
        &self,
        Parameters(request): Parameters<NodeScopedRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;
        let status = fetch_rest_status(request.node_id)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(status))
    }

    #[tool(
        description = "Run a raw JSON-RPC call against node sidecar endpoint. Useful as a generic query helper."
    )]
    async fn rpc_query(
        &self,
        Parameters(request): Parameters<RpcQueryRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;
        let value = self
            .manager
            .raw_rpc_query(request.node_id, &request.method, request.params)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(value))
    }

    #[tool(description = "Typed balance query by account/public key/uref/entity identifier.")]
    async fn rpc_query_balance(
        &self,
        Parameters(request): Parameters<RpcQueryBalanceRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let block_id = normalize_optional_identifier(request.block_id.as_deref());
        let state_root_hash = normalize_optional_identifier(request.state_root_hash.as_deref());
        let params =
            build_query_balance_params(&request.purse_identifier, &block_id, &state_root_hash)
                .map_err(to_mcp_error)?;
        let response = self
            .manager
            .raw_rpc_query(request.node_id, "query_balance", Some(params))
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            extract_rpc_result(response).map_err(to_mcp_error)?,
        ))
    }

    #[tool(
        description = "Typed global state query by key + optional path + optional block/state-root identifier. If no identifier is provided, the latest block hash is used."
    )]
    async fn rpc_query_global_state(
        &self,
        Parameters(request): Parameters<RpcQueryGlobalStateRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let (block_id, state_root_hash) = resolve_global_state_identifier(
            &request.network_name,
            request.node_id,
            request.block_id.as_deref(),
            request.state_root_hash.as_deref(),
        )
        .await
        .map_err(to_mcp_error)?;
        let params = build_query_global_state_params(
            &request.key,
            request.path.unwrap_or_default(),
            &block_id,
            &state_root_hash,
        )
        .map_err(to_mcp_error)?;
        let response = self
            .manager
            .raw_rpc_query(request.node_id, "query_global_state", Some(params))
            .await
            .map_err(to_mcp_error)?;

        Ok(ok_value(
            extract_rpc_result(response).map_err(to_mcp_error)?,
        ))
    }

    #[tool(description = "Get current block height using typed chain_get_block RPC.")]
    async fn current_block_height(
        &self,
        Parameters(request): Parameters<CurrentBlockRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let value = fetch_block_result(
            &self.manager,
            &request.network_name,
            request.node_id,
            request.block_id.as_deref(),
        )
        .await;
        let value = match value {
            Ok(value) => value,
            Err(err) => {
                if request.block_id.is_none()
                    && let Some((low, high)) =
                        parse_no_such_block_range_from_error(&err.to_string())
                {
                    return Ok(ok_value(json!({
                        "network_name": request.network_name,
                        "node_id": request.node_id,
                        "height": high,
                        "block": Value::Null,
                        "pending": true,
                        "available_block_range": {
                            "low": low,
                            "high": high,
                        },
                        "message": "latest block is not yet queryable; retry shortly",
                    })));
                }
                return Err(to_mcp_error(err));
            }
        };
        let height = extract_block_height(&value).ok_or_else(|| {
            ErrorData::internal_error("missing block height in RPC response", None)
        })?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "height": height,
            "block": value,
        })))
    }

    #[tool(description = "Get current block payload using typed chain_get_block RPC.")]
    async fn current_block(
        &self,
        Parameters(request): Parameters<CurrentBlockRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let value = fetch_block_result(
            &self.manager,
            &request.network_name,
            request.node_id,
            request.block_id.as_deref(),
        )
        .await
        .map_err(to_mcp_error)?;
        Ok(ok_value(value))
    }

    #[tool(description = "Get paginated node stdout/stderr logs from on-disk files.")]
    async fn get_node_logs(
        &self,
        Parameters(request): Parameters<GetNodeLogsRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;

        let limit = request.limit.unwrap_or(DEFAULT_LOG_PAGE_SIZE);
        if limit == 0 {
            return Err(ErrorData::invalid_params(
                "limit must be greater than 0",
                None,
            ));
        }

        let file_name = match request.stream {
            NodeLogStream::Stdout => "stdout.log",
            NodeLogStream::Stderr => "stderr.log",
        };
        let path = network
            .layout
            .node_logs_dir(request.node_id)
            .join(file_name);

        let page = read_log_page(&path, request.before_line, limit)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(page).map_err(internal_serde_error)?,
        ))
    }

    #[tool(
        description = "Wait for the next SSE event after a sequence cursor with optional event type filters."
    )]
    async fn wait_next_sse(
        &self,
        Parameters(request): Parameters<WaitNextSseRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let timeout = Duration::from_secs(request.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECS));
        let filter = SseFilter {
            include_event_types: request.include_event_types.unwrap_or_default(),
            exclude_event_types: request.exclude_event_types.unwrap_or_default(),
        };

        let after = match request.after_sequence {
            Some(value) => value,
            None => network.sse_store.latest_sequence().await,
        };

        let event = network
            .sse_store
            .wait_next(after, &filter, timeout)
            .await
            .map_err(to_mcp_error)?;

        Ok(ok_value(
            serde_json::to_value(event).map_err(internal_serde_error)?,
        ))
    }

    #[tool(
        description = "Fetch paginated SSE history with sequence cursor and optional event type filters."
    )]
    async fn sse_history(
        &self,
        Parameters(request): Parameters<SseHistoryRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;

        let limit = request.limit.unwrap_or(DEFAULT_SSE_PAGE_SIZE);
        if limit == 0 {
            return Err(ErrorData::invalid_params(
                "limit must be greater than 0",
                None,
            ));
        }

        let filter = SseFilter {
            include_event_types: request.include_event_types.unwrap_or_default(),
            exclude_event_types: request.exclude_event_types.unwrap_or_default(),
        };

        let history = network
            .sse_store
            .history(request.before_sequence, limit, &filter)
            .await;
        Ok(ok_value(
            serde_json::to_value(history).map_err(internal_serde_error)?,
        ))
    }

    #[tool(description = "List derived accounts from derived-accounts.csv for a network.")]
    async fn list_derived_accounts(
        &self,
        Parameters(request): Parameters<ListDerivedAccountsRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let accounts = self
            .manager
            .list_derived_accounts(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        Ok(ok_value(
            serde_json::to_value(accounts).map_err(internal_serde_error)?,
        ))
    }

    #[tool(
        description = "Submit signed or unsigned transaction JSON. Signer key is derived from signer_path for this managed network. transaction must be a typed Transaction JSON object."
    )]
    async fn send_transaction_signed(
        &self,
        Parameters(request): Parameters<SendTransactionSignedRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let signer = self
            .manager
            .derived_account_for_path(&network, &request.signer_path)
            .await
            .map_err(to_mcp_error)?;
        verify_path_hash_consistency(&network.layout, &request.signer_path, &signer.account_hash)
            .await
            .map_err(to_mcp_error)?;

        let mut transaction = parse_transaction_json(request.transaction)?;
        transaction.sign(&signer.secret_key);

        let rpc = mcp_rpc_client(&request.network_name, request.node_id).map_err(to_mcp_error)?;
        let response = rpc
            .put_transaction(transaction)
            .await
            .map_err(to_mcp_error)?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "transaction_hash": response.transaction_hash.to_hex_string(),
        })))
    }

    #[tool(
        description = "Create a stored package-name call transaction (make-transaction style). Returns transaction JSON for follow-up submission with send_transaction_signed. session_args accepts either: (1) array of {name,type,value} objects, e.g. [{\"name\":\"value\",\"type\":\"I32\",\"value\":\"1\"}], or (2) full RuntimeArgs JSON object. Legacy field name session_args_json is accepted as an alias, but values must be typed JSON (not encoded JSON strings). Not supported: object shorthand like {\"value\":1} or casper-client string args like [\"value:i32=1\"]. For composite CLTypes (List/Map/Tuple/Result/ByteArray), value must be hex bytes (0x...)."
    )]
    async fn make_transaction_package_call(
        &self,
        Parameters(request): Parameters<MakeTransactionPackageCallRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let runtime = match (
            request.runtime_transferred_value,
            request.runtime_seed_hex.as_deref(),
        ) {
            (None, None) => TransactionRuntimeParams::VmCasperV1,
            (transferred_value, maybe_seed_hex) => {
                let seed = parse_optional_seed_hex(maybe_seed_hex).map_err(to_mcp_error)?;
                TransactionRuntimeParams::VmCasperV2 {
                    transferred_value: transferred_value.unwrap_or(0),
                    seed,
                }
            }
        };

        let mut builder = TransactionV1Builder::new_targeting_package_via_alias(
            request.transaction_package_name.clone(),
            request.transaction_package_version,
            request.session_entry_point.clone(),
            runtime,
        );

        if let Some(args_json) = request.session_args.as_ref()
            && let Some(runtime_args) = parse_session_args(args_json).map_err(to_mcp_error)?
        {
            builder = builder.with_runtime_args(runtime_args);
        }

        if let Some(ttl_millis) = request.ttl_millis {
            builder = builder.with_ttl(TimeDiff::from_millis(ttl_millis));
        }

        let pricing = build_pricing_mode(
            request.gas_price_tolerance,
            Some(request.payment_amount.unwrap_or(DEFAULT_PAYMENT_AMOUNT)),
        );
        builder = builder.with_pricing_mode(pricing);

        let chain_name = match request.chain_name {
            Some(value) => value,
            None => fetch_chain_name(&request.network_name, request.node_id)
                .await
                .map_err(to_mcp_error)?,
        };
        builder = builder.with_chain_name(chain_name.clone());

        let mut signed = false;
        let mut signer_path = None;
        let mut derived_signer = None;
        if let Some(path) = request.signer_path {
            let signer = self
                .manager
                .derived_account_for_path(&network, &path)
                .await
                .map_err(to_mcp_error)?;
            verify_path_hash_consistency(&network.layout, &path, &signer.account_hash)
                .await
                .map_err(to_mcp_error)?;
            derived_signer = Some(signer);
            signed = true;
            signer_path = Some(path);
        } else if let Some(initiator_public_key) = request.initiator_public_key {
            let public_key = PublicKey::from_hex(initiator_public_key.trim())
                .with_context(|| "failed to parse initiator_public_key as hex public key")
                .map_err(to_mcp_error)?;
            builder = builder.with_initiator_addr(public_key);
        } else {
            return Err(ErrorData::invalid_params(
                "provide signer_path (for signed tx) or initiator_public_key (for unsigned tx)",
                None,
            ));
        }

        if let Some(signer) = derived_signer.as_ref() {
            builder = builder.with_secret_key(&signer.secret_key);
        }

        let tx = builder.build().map_err(to_mcp_error)?;
        let tx_json = serde_json::to_value(Transaction::V1(tx)).map_err(internal_serde_error)?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "chain_name": chain_name,
            "signed": signed,
            "signer_path": signer_path,
            "transaction": tx_json,
        })))
    }

    #[tool(
        description = "Create a stored contract-hash call transaction (make-transaction style). Returns transaction JSON for follow-up submission with send_transaction_signed. session_args accepts either: (1) array of {name,type,value} objects, e.g. [{\"name\":\"value\",\"type\":\"I32\",\"value\":\"1\"}], or (2) full RuntimeArgs JSON object. Legacy field name session_args_json is accepted as an alias, but values must be typed JSON (not encoded JSON strings). Not supported: object shorthand like {\"value\":1} or casper-client string args like [\"value:i32=1\"]. For composite CLTypes (List/Map/Tuple/Result/ByteArray), value must be hex bytes (0x...)."
    )]
    async fn make_transaction_contract_call(
        &self,
        Parameters(request): Parameters<MakeTransactionContractCallRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let runtime = match (
            request.runtime_transferred_value,
            request.runtime_seed_hex.as_deref(),
        ) {
            (None, None) => TransactionRuntimeParams::VmCasperV1,
            (transferred_value, maybe_seed_hex) => {
                let seed = parse_optional_seed_hex(maybe_seed_hex).map_err(to_mcp_error)?;
                TransactionRuntimeParams::VmCasperV2 {
                    transferred_value: transferred_value.unwrap_or(0),
                    seed,
                }
            }
        };

        let contract_hash = parse_contract_hash_for_invocation(&request.transaction_contract_hash)
            .map_err(to_mcp_error)?;
        let mut builder = TransactionV1Builder::new_targeting_invocable_entity(
            contract_hash,
            request.session_entry_point.clone(),
            runtime,
        );

        if let Some(args_json) = request.session_args.as_ref()
            && let Some(runtime_args) = parse_session_args(args_json).map_err(to_mcp_error)?
        {
            builder = builder.with_runtime_args(runtime_args);
        }

        if let Some(ttl_millis) = request.ttl_millis {
            builder = builder.with_ttl(TimeDiff::from_millis(ttl_millis));
        }

        let pricing = build_pricing_mode(
            request.gas_price_tolerance,
            Some(request.payment_amount.unwrap_or(DEFAULT_PAYMENT_AMOUNT)),
        );
        builder = builder.with_pricing_mode(pricing);

        let chain_name = match request.chain_name {
            Some(value) => value,
            None => fetch_chain_name(&request.network_name, request.node_id)
                .await
                .map_err(to_mcp_error)?,
        };
        builder = builder.with_chain_name(chain_name.clone());

        let mut signed = false;
        let mut signer_path = None;
        let mut derived_signer = None;
        if let Some(path) = request.signer_path {
            let signer = self
                .manager
                .derived_account_for_path(&network, &path)
                .await
                .map_err(to_mcp_error)?;
            verify_path_hash_consistency(&network.layout, &path, &signer.account_hash)
                .await
                .map_err(to_mcp_error)?;
            derived_signer = Some(signer);
            signed = true;
            signer_path = Some(path);
        } else if let Some(initiator_public_key) = request.initiator_public_key {
            let public_key = PublicKey::from_hex(initiator_public_key.trim())
                .with_context(|| "failed to parse initiator_public_key as hex public key")
                .map_err(to_mcp_error)?;
            builder = builder.with_initiator_addr(public_key);
        } else {
            return Err(ErrorData::invalid_params(
                "provide signer_path (for signed tx) or initiator_public_key (for unsigned tx)",
                None,
            ));
        }

        if let Some(signer) = derived_signer.as_ref() {
            builder = builder.with_secret_key(&signer.secret_key);
        }

        let tx = builder.build().map_err(to_mcp_error)?;
        let tx_json = serde_json::to_value(Transaction::V1(tx)).map_err(internal_serde_error)?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "chain_name": chain_name,
            "transaction_contract_hash": request.transaction_contract_hash,
            "signed": signed,
            "signer_path": signer_path,
            "transaction": tx_json,
        })))
    }

    #[tool(
        description = "Create a session wasm transaction (make-transaction style). Returns transaction JSON for follow-up submission with send_transaction_signed. session_args accepts either: (1) array of {name,type,value} objects, e.g. [{\"name\":\"value\",\"type\":\"I32\",\"value\":\"1\"}], or (2) full RuntimeArgs JSON object. Legacy field name session_args_json is accepted as an alias, but values must be typed JSON (not encoded JSON strings). Not supported: object shorthand like {\"value\":1} or casper-client string args like [\"value:i32=1\"]. For composite CLTypes (List/Map/Tuple/Result/ByteArray), value must be hex bytes (0x...)."
    )]
    async fn make_transaction_session_wasm(
        &self,
        Parameters(request): Parameters<MakeTransactionSessionWasmRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let wasm_path = PathBuf::from(&request.wasm_path);
        let wasm_bytes = tokio_fs::read(&wasm_path)
            .await
            .with_context(|| format!("failed to read wasm at {}", wasm_path.display()))
            .map_err(to_mcp_error)?;

        let runtime = match (
            request.runtime_transferred_value,
            request.runtime_seed_hex.as_deref(),
        ) {
            (None, None) => TransactionRuntimeParams::VmCasperV1,
            (transferred_value, maybe_seed_hex) => {
                let seed = parse_optional_seed_hex(maybe_seed_hex).map_err(to_mcp_error)?;
                TransactionRuntimeParams::VmCasperV2 {
                    transferred_value: transferred_value.unwrap_or(0),
                    seed,
                }
            }
        };

        let mut builder = TransactionV1Builder::new_session(
            request.is_install_upgrade.unwrap_or(true),
            wasm_bytes.into(),
            runtime,
        );

        if let Some(args_json) = request.session_args.as_ref()
            && let Some(runtime_args) = parse_session_args(args_json).map_err(to_mcp_error)?
        {
            builder = builder.with_runtime_args(runtime_args);
        }

        if let Some(ttl_millis) = request.ttl_millis {
            builder = builder.with_ttl(TimeDiff::from_millis(ttl_millis));
        }

        let pricing = build_pricing_mode(
            request.gas_price_tolerance,
            Some(request.payment_amount.unwrap_or(DEFAULT_PAYMENT_AMOUNT)),
        );
        builder = builder.with_pricing_mode(pricing);

        let chain_name = match request.chain_name {
            Some(value) => value,
            None => fetch_chain_name(&request.network_name, request.node_id)
                .await
                .map_err(to_mcp_error)?,
        };
        builder = builder.with_chain_name(chain_name.clone());

        let mut signed = false;
        let mut signer_path = None;
        let mut derived_signer = None;
        if let Some(path) = request.signer_path {
            let signer = self
                .manager
                .derived_account_for_path(&network, &path)
                .await
                .map_err(to_mcp_error)?;
            verify_path_hash_consistency(&network.layout, &path, &signer.account_hash)
                .await
                .map_err(to_mcp_error)?;
            derived_signer = Some(signer);
            signed = true;
            signer_path = Some(path);
        } else if let Some(initiator_public_key) = request.initiator_public_key {
            let public_key = PublicKey::from_hex(initiator_public_key.trim())
                .with_context(|| "failed to parse initiator_public_key as hex public key")
                .map_err(to_mcp_error)?;
            builder = builder.with_initiator_addr(public_key);
        } else {
            return Err(ErrorData::invalid_params(
                "provide signer_path (for signed tx) or initiator_public_key (for unsigned tx)",
                None,
            ));
        }

        if let Some(signer) = derived_signer.as_ref() {
            builder = builder.with_secret_key(&signer.secret_key);
        }

        let tx = builder.build().map_err(to_mcp_error)?;
        let tx_json = serde_json::to_value(Transaction::V1(tx)).map_err(internal_serde_error)?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "chain_name": chain_name,
            "wasm_path": request.wasm_path,
            "signed": signed,
            "signer_path": signer_path,
            "transaction": tx_json,
        })))
    }

    #[tool(
        description = "Build, sign and submit a session wasm transaction from a derived account path. session_args accepts either: (1) array of {name,type,value} objects, e.g. [{\"name\":\"value\",\"type\":\"I32\",\"value\":\"1\"}], or (2) full RuntimeArgs JSON object. Legacy field name session_args_json is accepted as an alias, but values must be typed JSON (not encoded JSON strings). Not supported: object shorthand like {\"value\":1} or casper-client string args like [\"value:i32=1\"]. For composite CLTypes (List/Map/Tuple/Result/ByteArray), value must be hex bytes (0x...)."
    )]
    async fn send_session_wasm(
        &self,
        Parameters(request): Parameters<SendSessionWasmRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let signer = self
            .manager
            .derived_account_for_path(&network, &request.signer_path)
            .await
            .map_err(to_mcp_error)?;
        verify_path_hash_consistency(&network.layout, &request.signer_path, &signer.account_hash)
            .await
            .map_err(to_mcp_error)?;

        let wasm_path = PathBuf::from(&request.wasm_path);
        let wasm_bytes = tokio_fs::read(&wasm_path)
            .await
            .with_context(|| format!("failed to read wasm at {}", wasm_path.display()))
            .map_err(to_mcp_error)?;

        let runtime = match (
            request.runtime_transferred_value,
            request.runtime_seed_hex.as_deref(),
        ) {
            (None, None) => TransactionRuntimeParams::VmCasperV1,
            (transferred_value, maybe_seed_hex) => {
                let seed = parse_optional_seed_hex(maybe_seed_hex).map_err(to_mcp_error)?;
                TransactionRuntimeParams::VmCasperV2 {
                    transferred_value: transferred_value.unwrap_or(0),
                    seed,
                }
            }
        };

        let mut builder = TransactionV1Builder::new_session(
            request.is_install_upgrade.unwrap_or(true),
            wasm_bytes.into(),
            runtime,
        );

        if let Some(args_json) = request.session_args.as_ref()
            && let Some(runtime_args) = parse_session_args(args_json).map_err(to_mcp_error)?
        {
            builder = builder.with_runtime_args(runtime_args);
        }

        if let Some(ttl_millis) = request.ttl_millis {
            builder = builder.with_ttl(TimeDiff::from_millis(ttl_millis));
        }

        let pricing = build_pricing_mode(
            request.gas_price_tolerance,
            Some(request.payment_amount.unwrap_or(DEFAULT_PAYMENT_AMOUNT)),
        );
        builder = builder.with_pricing_mode(pricing);

        let chain_name = match request.chain_name {
            Some(value) => value,
            None => fetch_chain_name(&request.network_name, request.node_id)
                .await
                .map_err(to_mcp_error)?,
        };

        let tx = builder
            .with_chain_name(chain_name)
            .with_secret_key(&signer.secret_key)
            .build()
            .map_err(to_mcp_error)?;

        let rpc = mcp_rpc_client(&request.network_name, request.node_id).map_err(to_mcp_error)?;
        let response = rpc
            .put_transaction(Transaction::V1(tx))
            .await
            .map_err(to_mcp_error)?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "transaction_hash": response.transaction_hash.to_hex_string(),
        })))
    }

    #[tool(
        description = "Transfer tokens between derived account paths. from_path signs, to_path resolves recipient."
    )]
    async fn transfer_tokens(
        &self,
        Parameters(request): Parameters<TransferTokensRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let from = self
            .manager
            .derived_account_for_path(&network, &request.from_path)
            .await
            .map_err(to_mcp_error)?;
        let to = self
            .manager
            .derived_account_for_path(&network, &request.to_path)
            .await
            .map_err(to_mcp_error)?;

        verify_path_hash_consistency(&network.layout, &request.from_path, &from.account_hash)
            .await
            .map_err(to_mcp_error)?;
        verify_path_hash_consistency(&network.layout, &request.to_path, &to.account_hash)
            .await
            .map_err(to_mcp_error)?;

        let amount = casper_types::U512::from_dec_str(&request.amount)
            .with_context(|| "amount must be a decimal U512 string")
            .map_err(to_mcp_error)?;
        let to_public_key = PublicKey::from_hex(&to.public_key_hex)
            .with_context(|| "failed to parse derived recipient public key")
            .map_err(to_mcp_error)?;

        let mut builder =
            TransactionV1Builder::new_transfer(amount, None, to_public_key, request.transfer_id)
                .map_err(to_mcp_error)?;

        if let Some(ttl_millis) = request.ttl_millis {
            builder = builder.with_ttl(TimeDiff::from_millis(ttl_millis));
        }

        let pricing = build_pricing_mode(
            request.gas_price_tolerance,
            Some(request.payment_amount.unwrap_or(DEFAULT_PAYMENT_AMOUNT)),
        );
        builder = builder.with_pricing_mode(pricing);

        let chain_name = match request.chain_name {
            Some(value) => value,
            None => fetch_chain_name(&request.network_name, request.node_id)
                .await
                .map_err(to_mcp_error)?,
        };

        let tx = builder
            .with_chain_name(chain_name)
            .with_secret_key(&from.secret_key)
            .build()
            .map_err(to_mcp_error)?;

        let rpc = mcp_rpc_client(&request.network_name, request.node_id).map_err(to_mcp_error)?;
        let response = rpc
            .put_transaction(Transaction::V1(tx))
            .await
            .map_err(to_mcp_error)?;

        Ok(ok_value(json!({
            "network_name": request.network_name,
            "node_id": request.node_id,
            "transaction_hash": response.transaction_hash.to_hex_string(),
            "from_path": request.from_path,
            "to_path": request.to_path,
            "amount": request.amount,
        })))
    }

    #[tool(
        description = "Wait for transaction execution result with timeout and return execution effects/result payload."
    )]
    async fn wait_transaction(
        &self,
        Parameters(request): Parameters<WaitTransactionRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let timeout = Duration::from_secs(request.timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECS));
        let poll = Duration::from_millis(request.poll_interval_millis.unwrap_or(1_000));
        let tx_hash =
            parse_transaction_hash_input(&request.transaction_hash).map_err(to_mcp_error)?;
        let rpc = mcp_rpc_client(&request.network_name, request.node_id).map_err(to_mcp_error)?;
        let deadline = Instant::now() + timeout;

        loop {
            let response = rpc.get_transaction(tx_hash).await.map_err(to_mcp_error)?;

            if let Some(exec_info) = response.execution_info.clone()
                && exec_info.execution_result.is_some()
            {
                return Ok(ok_value(
                    serde_json::to_value(response).map_err(internal_serde_error)?,
                ));
            }

            if Instant::now() >= deadline {
                return Err(ErrorData::resource_not_found(
                    format!(
                        "transaction {} execution result not available before timeout",
                        request.transaction_hash
                    ),
                    None,
                ));
            }

            tokio::time::sleep(poll).await;
        }
    }

    #[tool(
        description = "Get transaction details via info_get_transaction (non-waiting). Returns typed JSON response from node."
    )]
    async fn get_transaction(
        &self,
        Parameters(request): Parameters<GetTransactionRequest>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let network = self
            .manager
            .get_network(&request.network_name)
            .await
            .map_err(to_mcp_error)?;
        ensure_running_network(&network).await?;

        let tx_hash =
            parse_transaction_hash_input(&request.transaction_hash).map_err(to_mcp_error)?;
        let rpc = mcp_rpc_client(&request.network_name, request.node_id).map_err(to_mcp_error)?;
        let response = rpc.get_transaction(tx_hash).await.map_err(to_mcp_error)?;

        Ok(ok_value(
            serde_json::to_value(response).map_err(internal_serde_error)?,
        ))
    }
}

#[tool_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        Self::server_info()
    }
}

#[derive(Debug)]
struct NetworkManager {
    assets_root: PathBuf,
    managed: Mutex<HashMap<String, Arc<ManagedNetwork>>>,
    http: reqwest::Client,
}

#[derive(Debug)]
struct ManagedNetwork {
    layout: AssetsLayout,
    state: Arc<Mutex<State>>,
    node_count: u32,
    seed: Arc<str>,
    sse_store: Arc<SseStore>,
    shutdown: Arc<AtomicBool>,
    sse_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl ManagedNetwork {
    async fn is_running(&self) -> bool {
        let state = self.state.lock().await;
        processes_running(&state)
    }

    async fn stop(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);

        let tasks = {
            let mut guard = self.sse_tasks.lock().await;
            guard.drain(..).collect::<Vec<_>>()
        };
        for task in tasks {
            task.abort();
            let _ = task.await;
        }

        let mut state = self.state.lock().await;
        process::stop(&mut state).await
    }
}

impl NetworkManager {
    async fn new(assets_root: PathBuf) -> Result<Self> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            assets_root,
            managed: Mutex::new(HashMap::new()),
            http,
        })
    }

    async fn spawn_network(&self, request: SpawnNetworkRequest) -> Result<SpawnNetworkResponse> {
        let network_name = request
            .network_name
            .unwrap_or_else(|| DEFAULT_NETWORK_NAME.to_string());

        if network_name.trim().is_empty() {
            return Err(anyhow!("network_name must not be empty"));
        }

        let maybe_existing = {
            let mut managed = self.managed.lock().await;
            if let Some(existing) = managed.get(&network_name)
                && existing.is_running().await
                && !request.force_setup.unwrap_or(true)
            {
                return Ok(SpawnNetworkResponse {
                    network_name,
                    node_count: existing.node_count,
                    managed: true,
                    already_running: true,
                    forced_setup: false,
                });
            }
            managed.remove(&network_name)
        };

        if let Some(existing) = maybe_existing {
            let _ = existing.stop().await;
        }

        let force_setup = request.force_setup.unwrap_or(true);
        let requested_nodes = request.node_count.unwrap_or(DEFAULT_NODE_COUNT);
        let users = request.users;
        let delay = request.delay.unwrap_or(DEFAULT_DELAY_SECS);
        let log_level = request
            .log_level
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string());
        let node_log_format = request
            .node_log_format
            .unwrap_or_else(|| DEFAULT_NODE_LOG_FORMAT.to_string());
        let seed: Arc<str> = Arc::from(request.seed.unwrap_or_else(|| DEFAULT_SEED.to_string()));

        let layout = AssetsLayout::new(self.assets_root.clone(), network_name.clone());
        let assets_exist = layout.exists().await;

        let protocol_version =
            resolve_protocol_version(request.protocol_version.as_deref()).await?;

        if force_setup {
            assets::teardown(&layout).await?;
            assets::setup_local(
                &layout,
                &SetupOptions {
                    nodes: requested_nodes,
                    users,
                    delay_seconds: delay,
                    network_name: network_name.clone(),
                    protocol_version: protocol_version.clone(),
                    node_log_format,
                    seed: Arc::clone(&seed),
                },
            )
            .await?;
        } else if !assets_exist {
            assets::setup_local(
                &layout,
                &SetupOptions {
                    nodes: requested_nodes,
                    users,
                    delay_seconds: delay,
                    network_name: network_name.clone(),
                    protocol_version: protocol_version.clone(),
                    node_log_format,
                    seed: Arc::clone(&seed),
                },
            )
            .await?;
        }

        if !layout.exists().await {
            return Err(anyhow!(
                "assets missing under {}; call spawn_network with force_setup=true",
                layout.net_dir().display()
            ));
        }

        if !force_setup && assets_exist {
            let _ = assets::ensure_consensus_keys(&layout, Arc::clone(&seed)).await?;
        }

        let node_count = layout.count_nodes().await?;
        if node_count == 0 {
            return Err(anyhow!("network has no nodes to start"));
        }

        ensure_sidecar_available(&layout, node_count).await?;

        let state_path = layout.net_dir().join(STATE_FILE_NAME);
        let mut state = State::new(state_path).await?;

        process::start(
            &layout,
            &StartPlan {
                rust_log: log_level,
            },
            &mut state,
        )
        .await?;

        let managed = Arc::new(ManagedNetwork {
            layout: layout.clone(),
            state: Arc::new(Mutex::new(state)),
            node_count,
            seed,
            sse_store: Arc::new(SseStore::new(DEFAULT_SSE_HISTORY_CAPACITY)),
            shutdown: Arc::new(AtomicBool::new(false)),
            sse_tasks: Mutex::new(Vec::new()),
        });

        self.spawn_sse_collectors(&managed).await;

        self.managed
            .lock()
            .await
            .insert(network_name.clone(), Arc::clone(&managed));

        Ok(SpawnNetworkResponse {
            network_name,
            node_count,
            managed: true,
            already_running: false,
            forced_setup: force_setup,
        })
    }

    async fn spawn_sse_collectors(&self, network: &Arc<ManagedNetwork>) {
        let mut tasks = Vec::new();
        for node_id in 1..=network.node_count {
            let endpoint = assets::sse_endpoint(node_id);
            let network = Arc::clone(network);
            let task = tokio::spawn(async move {
                run_sse_listener(network, node_id, endpoint).await;
            });
            tasks.push(task);
        }
        let mut guard = network.sse_tasks.lock().await;
        guard.extend(tasks);
    }

    async fn wait_network_ready(
        &self,
        network_name: &str,
        timeout_seconds: Option<u64>,
    ) -> Result<WaitReadyResponse> {
        let network = self.get_network(network_name).await?;
        let timeout = Duration::from_secs(timeout_seconds.unwrap_or(DEFAULT_TIMEOUT_SECS));
        let deadline = Instant::now() + timeout;

        loop {
            let running = ensure_running_network(&network).await.is_ok();
            if running {
                let status = check_rest_ready(&network).await;
                if let Ok(rest_by_node) = status {
                    let state = network.state.lock().await;
                    let block_observed = state.last_block_height.is_some()
                        || rest_by_node.values().any(rest_has_block);
                    if block_observed {
                        return Ok(WaitReadyResponse {
                            network_name: network_name.to_string(),
                            ready: true,
                            node_count: network.node_count,
                            rest: rest_by_node,
                            last_block_height: state.last_block_height,
                        });
                    }
                }
            }

            if Instant::now() >= deadline {
                return Ok(WaitReadyResponse {
                    network_name: network_name.to_string(),
                    ready: false,
                    node_count: network.node_count,
                    rest: HashMap::new(),
                    last_block_height: None,
                });
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn despawn_network(&self, network_name: &str, purge: bool) -> Result<DespawnResponse> {
        let managed = {
            let mut guard = self.managed.lock().await;
            guard.remove(network_name)
        };

        let Some(network) = managed else {
            return Err(anyhow!("network '{}' is not managed", network_name));
        };

        network.stop().await?;
        if purge {
            assets::teardown(&network.layout).await?;
        }

        Ok(DespawnResponse {
            network_name: network_name.to_string(),
            purged: purge,
        })
    }

    async fn list_networks(&self) -> Result<ListNetworksResponse> {
        let discovered = discover_network_names(&self.assets_root).await?;
        let managed_snapshot = {
            let guard = self.managed.lock().await;
            guard
                .iter()
                .map(|(name, network)| (name.clone(), Arc::clone(network)))
                .collect::<Vec<_>>()
        };

        let mut rows = Vec::new();

        for name in &discovered {
            if let Some((_, network)) = managed_snapshot.iter().find(|(n, _)| n == name) {
                rows.push(NetworkRow {
                    network_name: name.clone(),
                    discovered: true,
                    managed: true,
                    running: network.is_running().await,
                    node_count: Some(network.node_count),
                });
            } else {
                let layout = AssetsLayout::new(self.assets_root.clone(), name.clone());
                rows.push(NetworkRow {
                    network_name: name.clone(),
                    discovered: true,
                    managed: false,
                    running: false,
                    node_count: layout.count_nodes().await.ok(),
                });
            }
        }

        for (name, network) in managed_snapshot {
            if !discovered.iter().any(|candidate| candidate == &name) {
                rows.push(NetworkRow {
                    network_name: name,
                    discovered: false,
                    managed: true,
                    running: network.is_running().await,
                    node_count: Some(network.node_count),
                });
            }
        }

        rows.sort_by(|a, b| a.network_name.cmp(&b.network_name));

        Ok(ListNetworksResponse { networks: rows })
    }

    async fn managed_processes(
        &self,
        request: ManagedProcessesRequest,
    ) -> Result<ManagedProcessesResponse> {
        let network = self.get_network(&request.network_name).await?;
        let running_only = request.running_only.unwrap_or(true);
        let process_name = request
            .process_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToString::to_string);

        let process_name_lc = process_name.as_ref().map(|name| name.to_ascii_lowercase());
        let state = network.state.lock().await;
        let mut processes = Vec::new();
        for process in &state.processes {
            if let Some(name_lc) = process_name_lc.as_deref()
                && !process.id.to_ascii_lowercase().contains(name_lc)
            {
                continue;
            }

            let running = matches!(process.last_status, ProcessStatus::Running)
                && process.pid.is_some_and(is_pid_running);
            if running_only && !running {
                continue;
            }

            processes.push(ManagedProcessRow {
                id: process.id.clone(),
                node_id: process.node_id,
                kind: process_kind_name(&process.kind).to_string(),
                pid: process.pid,
                running,
                last_status: process_status_name(&process.last_status).to_string(),
                command: process.command.clone(),
                args: process.args.clone(),
                cwd: process.cwd.clone(),
                stdout_path: process.stdout_path.clone(),
                stderr_path: process.stderr_path.clone(),
            });
        }

        Ok(ManagedProcessesResponse {
            network_name: request.network_name,
            running_only,
            process_name,
            processes,
        })
    }

    async fn get_network(&self, network_name: &str) -> Result<Arc<ManagedNetwork>> {
        let guard = self.managed.lock().await;
        guard.get(network_name).cloned().ok_or_else(|| {
            anyhow!(
                "network '{}' is not managed; call spawn_network first",
                network_name
            )
        })
    }

    async fn stop_all_networks(&self) -> Result<()> {
        let managed = {
            let mut guard = self.managed.lock().await;
            guard
                .drain()
                .map(|(_, network)| network)
                .collect::<Vec<_>>()
        };

        let mut errors = Vec::new();
        for network in managed {
            if let Err(err) = network.stop().await {
                errors.push(err.to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(errors.join("\n")))
        }
    }

    async fn raw_rpc_query(
        &self,
        node_id: u32,
        method: &str,
        params: Option<Value>,
    ) -> Result<Value> {
        let endpoint = assets::rpc_endpoint(node_id);
        let payload = match params {
            Some(params) => json!({
                "id": 1,
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }),
            None => json!({
                "id": 1,
                "jsonrpc": "2.0",
                "method": method,
            }),
        };

        let response = self
            .http
            .post(endpoint)
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }

    async fn list_derived_accounts(&self, network_name: &str) -> Result<Vec<DerivedAccountRow>> {
        let layout = AssetsLayout::new(self.assets_root.clone(), network_name.to_string());
        let csv = assets::derived_accounts_summary(&layout)
            .await
            .ok_or_else(|| {
                anyhow!(
                    "missing derived-accounts.csv for network '{}'",
                    network_name
                )
            })?;

        let seed = {
            let managed = self.managed.lock().await;
            managed
                .get(network_name)
                .map(|network| Arc::clone(&network.seed))
        };

        parse_derived_accounts_csv(&csv, seed).await
    }

    async fn derived_account_for_path(
        &self,
        network: &Arc<ManagedNetwork>,
        path: &str,
    ) -> Result<DerivedSigner> {
        let material =
            assets::derive_account_from_seed_path(Arc::clone(&network.seed), path).await?;
        let secret_key = SecretKey::from_pem(&material.secret_key_pem)?;
        Ok(DerivedSigner {
            public_key_hex: material.public_key_hex,
            account_hash: material.account_hash,
            secret_key,
        })
    }
}

#[derive(Debug)]
struct SseStore {
    sequence: AtomicU64,
    events: Mutex<VecDeque<SseRecord>>,
    notify: Notify,
    capacity: usize,
}

impl SseStore {
    fn new(capacity: usize) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            events: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            capacity,
        }
    }

    async fn latest_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    async fn push(&self, node_id: u32, event: SseEvent) {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let event_type = sse_event_type(&event).to_string();
        let payload = sse_event_payload(&event);

        let record = SseRecord {
            sequence,
            timestamp_rfc3339: timestamp_prefix(),
            node_id,
            event_type,
            payload,
        };

        let mut guard = self.events.lock().await;
        guard.push_back(record);
        while guard.len() > self.capacity {
            let _ = guard.pop_front();
        }
        drop(guard);

        self.notify.notify_waiters();
    }

    async fn wait_next(
        &self,
        after_sequence: u64,
        filter: &SseFilter,
        timeout: Duration,
    ) -> Result<SseRecord> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(record) = self.find_first_after(after_sequence, filter).await {
                return Ok(record);
            }

            if Instant::now() >= deadline {
                return Err(anyhow!("timed out waiting for SSE event"));
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::timeout(remaining, self.notify.notified()).await?;
        }
    }

    async fn find_first_after(&self, after_sequence: u64, filter: &SseFilter) -> Option<SseRecord> {
        let guard = self.events.lock().await;
        guard
            .iter()
            .find(|event| event.sequence > after_sequence && filter.matches(event))
            .cloned()
    }

    async fn history(
        &self,
        before_sequence: Option<u64>,
        limit: usize,
        filter: &SseFilter,
    ) -> SseHistoryPage {
        let before = before_sequence.unwrap_or(u64::MAX);
        let guard = self.events.lock().await;

        let mut matched = guard
            .iter()
            .filter(|event| event.sequence < before)
            .filter(|event| filter.matches(event))
            .cloned()
            .collect::<Vec<_>>();

        let total = matched.len();

        if matched.len() > limit {
            matched = matched.split_off(matched.len() - limit);
        }

        let next_before_sequence = matched.first().map(|event| event.sequence);
        SseHistoryPage {
            total_matching: total,
            returned: matched.len(),
            next_before_sequence,
            events: matched,
        }
    }
}

#[derive(Debug, Default)]
struct SseFilter {
    include_event_types: Vec<String>,
    exclude_event_types: Vec<String>,
}

impl SseFilter {
    fn matches(&self, event: &SseRecord) -> bool {
        let include = if self.include_event_types.is_empty() {
            true
        } else {
            self.include_event_types
                .iter()
                .any(|name| name == &event.event_type)
        };
        if !include {
            return false;
        }
        !self
            .exclude_event_types
            .iter()
            .any(|name| name == &event.event_type)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, rmcp::schemars::JsonSchema)]
struct SseRecord {
    sequence: u64,
    timestamp_rfc3339: String,
    node_id: u32,
    event_type: String,
    payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, rmcp::schemars::JsonSchema)]
struct SseHistoryPage {
    total_matching: usize,
    returned: usize,
    next_before_sequence: Option<u64>,
    events: Vec<SseRecord>,
}

#[derive(Debug)]
struct DerivedSigner {
    public_key_hex: String,
    account_hash: String,
    secret_key: SecretKey,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct SpawnNetworkRequest {
    network_name: Option<String>,
    protocol_version: Option<String>,
    node_count: Option<u32>,
    users: Option<u32>,
    delay: Option<u64>,
    log_level: Option<String>,
    node_log_format: Option<String>,
    seed: Option<String>,
    force_setup: Option<bool>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct SpawnNetworkResponse {
    network_name: String,
    node_count: u32,
    managed: bool,
    already_running: bool,
    forced_setup: bool,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct WaitNetworkReadyRequest {
    network_name: String,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct WaitReadyResponse {
    network_name: String,
    ready: bool,
    node_count: u32,
    rest: HashMap<u32, Value>,
    last_block_height: Option<u64>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct DespawnNetworkRequest {
    network_name: String,
    purge: Option<bool>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct DespawnResponse {
    network_name: String,
    purged: bool,
}

#[derive(Debug, Deserialize, Default, rmcp::schemars::JsonSchema)]
struct ListNetworksRequest {}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct ListNetworksResponse {
    networks: Vec<NetworkRow>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct NetworkRow {
    network_name: String,
    discovered: bool,
    managed: bool,
    running: bool,
    node_count: Option<u32>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct ManagedProcessesRequest {
    network_name: String,
    process_name: Option<String>,
    running_only: Option<bool>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct ManagedProcessesResponse {
    network_name: String,
    running_only: bool,
    process_name: Option<String>,
    processes: Vec<ManagedProcessRow>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct ManagedProcessRow {
    id: String,
    node_id: u32,
    kind: String,
    pid: Option<u32>,
    running: bool,
    last_status: String,
    command: String,
    args: Vec<String>,
    cwd: String,
    stdout_path: String,
    stderr_path: String,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct NodeScopedRequest {
    network_name: String,
    node_id: u32,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct RpcQueryRequest {
    network_name: String,
    node_id: u32,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct RpcQueryBalanceRequest {
    network_name: String,
    node_id: u32,
    purse_identifier: String,
    block_id: Option<String>,
    state_root_hash: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct RpcQueryGlobalStateRequest {
    network_name: String,
    node_id: u32,
    key: String,
    path: Option<Vec<String>>,
    block_id: Option<String>,
    state_root_hash: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct CurrentBlockRequest {
    network_name: String,
    node_id: u32,
    block_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, rmcp::schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
enum NodeLogStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct GetNodeLogsRequest {
    network_name: String,
    node_id: u32,
    stream: NodeLogStream,
    limit: Option<usize>,
    before_line: Option<usize>,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct LogLine {
    line_number: usize,
    content: String,
}

#[derive(Debug, Serialize, rmcp::schemars::JsonSchema)]
struct LogPage {
    path: String,
    total_lines: usize,
    returned: usize,
    next_before_line: Option<usize>,
    lines: Vec<LogLine>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct WaitNextSseRequest {
    network_name: String,
    include_event_types: Option<Vec<String>>,
    exclude_event_types: Option<Vec<String>>,
    after_sequence: Option<u64>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct SseHistoryRequest {
    network_name: String,
    include_event_types: Option<Vec<String>>,
    exclude_event_types: Option<Vec<String>>,
    before_sequence: Option<u64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct ListDerivedAccountsRequest {
    network_name: String,
}

#[derive(Debug, Serialize, Deserialize, rmcp::schemars::JsonSchema)]
struct DerivedAccountRow {
    kind: String,
    name: String,
    key_type: String,
    derivation: String,
    path: String,
    account_hash: String,
    balance: String,
    public_key: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct SendTransactionSignedRequest {
    network_name: String,
    node_id: u32,
    signer_path: String,
    transaction: Value,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct MakeTransactionPackageCallRequest {
    network_name: String,
    node_id: u32,
    transaction_package_name: String,
    transaction_package_version: Option<EntityVersion>,
    session_entry_point: String,
    #[serde(alias = "session_args_json")]
    session_args: Option<Value>,
    signer_path: Option<String>,
    initiator_public_key: Option<String>,
    chain_name: Option<String>,
    ttl_millis: Option<u64>,
    gas_price_tolerance: Option<u8>,
    payment_amount: Option<u64>,
    runtime_transferred_value: Option<u64>,
    runtime_seed_hex: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct MakeTransactionContractCallRequest {
    network_name: String,
    node_id: u32,
    transaction_contract_hash: String,
    session_entry_point: String,
    #[serde(alias = "session_args_json")]
    session_args: Option<Value>,
    signer_path: Option<String>,
    initiator_public_key: Option<String>,
    chain_name: Option<String>,
    ttl_millis: Option<u64>,
    gas_price_tolerance: Option<u8>,
    payment_amount: Option<u64>,
    runtime_transferred_value: Option<u64>,
    runtime_seed_hex: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct MakeTransactionSessionWasmRequest {
    network_name: String,
    node_id: u32,
    wasm_path: String,
    #[serde(alias = "session_args_json")]
    session_args: Option<Value>,
    is_install_upgrade: Option<bool>,
    signer_path: Option<String>,
    initiator_public_key: Option<String>,
    chain_name: Option<String>,
    ttl_millis: Option<u64>,
    gas_price_tolerance: Option<u8>,
    payment_amount: Option<u64>,
    runtime_transferred_value: Option<u64>,
    runtime_seed_hex: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct SendSessionWasmRequest {
    network_name: String,
    node_id: u32,
    signer_path: String,
    wasm_path: String,
    #[serde(alias = "session_args_json")]
    session_args: Option<Value>,
    chain_name: Option<String>,
    is_install_upgrade: Option<bool>,
    ttl_millis: Option<u64>,
    gas_price_tolerance: Option<u8>,
    payment_amount: Option<u64>,
    runtime_transferred_value: Option<u64>,
    runtime_seed_hex: Option<String>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct TransferTokensRequest {
    network_name: String,
    node_id: u32,
    from_path: String,
    to_path: String,
    amount: String,
    transfer_id: Option<u64>,
    chain_name: Option<String>,
    ttl_millis: Option<u64>,
    gas_price_tolerance: Option<u8>,
    payment_amount: Option<u64>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct WaitTransactionRequest {
    network_name: String,
    node_id: u32,
    transaction_hash: String,
    timeout_seconds: Option<u64>,
    poll_interval_millis: Option<u64>,
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
struct GetTransactionRequest {
    network_name: String,
    node_id: u32,
    transaction_hash: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct RestStatusProbe {
    reactor_state: Option<String>,
}

fn to_mcp_error(err: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error(err.to_string(), None)
}

fn internal_serde_error(err: serde_json::Error) -> ErrorData {
    ErrorData::internal_error(format!("serde error: {}", err), None)
}

fn ok_value(value: Value) -> rmcp::model::CallToolResult {
    rmcp::model::CallToolResult::structured(value)
}

fn parse_transaction_json(value: Value) -> std::result::Result<Transaction, ErrorData> {
    if let Value::String(encoded) = &value {
        let _ = encoded;
        return Err(ErrorData::invalid_params(
            "invalid transaction payload: encoded JSON strings are not supported. Provide transaction as a typed JSON object",
            None,
        ));
    }

    if let Ok(transaction) = serde_json::from_value::<Transaction>(value.clone()) {
        return Ok(transaction);
    }

    if let Some(inner) = value.get("transaction") {
        return parse_transaction_json(inner.clone());
    }

    Err(ErrorData::invalid_params(
        "failed to parse transaction JSON; expected Transaction object or { transaction: Transaction }, with typed JSON values only.",
        None,
    ))
}

fn parse_transaction_hash_input(input: &str) -> Result<TransactionHash> {
    let value = input.trim();
    if value.is_empty() {
        return Err(anyhow!("transaction_hash must not be empty"));
    }

    let unwrapped = value
        .strip_prefix("transaction-v1-hash(")
        .and_then(|inner| inner.strip_suffix(')'))
        .or_else(|| {
            value
                .strip_prefix("deploy-hash(")
                .and_then(|inner| inner.strip_suffix(')'))
        })
        .unwrap_or(value);

    if unwrapped.contains("..") {
        return Err(anyhow!(
            "transaction_hash appears abbreviated; provide full hex digest"
        ));
    }

    let normalized = unwrapped.strip_prefix("0x").unwrap_or(unwrapped);
    let digest = Digest::from_hex(normalized)
        .map_err(|err| anyhow!("failed to parse transaction hash digest: {}", err))?;
    Ok(TransactionHash::from(TransactionV1Hash::from(digest)))
}

fn mcp_rpc_client(network_name: &str, node_id: u32) -> Result<CasperClient> {
    CasperClient::new(
        network_name.to_string(),
        vec![assets::rpc_endpoint(node_id)],
    )
    .map_err(|err| {
        anyhow!(
            "failed to initialize rpc client for node {}: {}",
            node_id,
            err
        )
    })
}

fn extract_rpc_result(response: Value) -> Result<Value> {
    if let Some(error) = response.get("error") {
        return Err(anyhow!("rpc request failed: {}", error));
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("rpc response missing result field"))
}

async fn fetch_block_result(
    manager: &NetworkManager,
    network_name: &str,
    node_id: u32,
    block_id: Option<&str>,
) -> Result<Value> {
    let block_id = normalize_optional_identifier(block_id);
    if block_id.is_empty() {
        let rpc = mcp_rpc_client(network_name, node_id)?;
        let result = rpc.get_block().await?;
        return serde_json::to_value(result).map_err(Into::into);
    }

    let block_identifier = parse_block_identifier_value(&block_id)?;
    let response = manager
        .raw_rpc_query(
            node_id,
            "chain_get_block",
            Some(json!({
                "block_identifier": block_identifier
            })),
        )
        .await?;
    extract_rpc_result(response)
}

fn build_query_global_state_params(
    key: &str,
    path: Vec<String>,
    block_id: &str,
    state_root_hash: &str,
) -> Result<Value> {
    let key = parse_query_key(key)?;
    let state_identifier = parse_state_identifier(block_id, state_root_hash)?;
    let mut params = serde_json::Map::new();
    params.insert("key".to_string(), Value::String(key));
    params.insert(
        "path".to_string(),
        Value::Array(path.into_iter().map(Value::String).collect()),
    );
    if let Some(state_identifier) = state_identifier {
        params.insert("state_identifier".to_string(), state_identifier);
    }
    Ok(Value::Object(params))
}

fn build_query_balance_params(
    purse_identifier: &str,
    block_id: &str,
    state_root_hash: &str,
) -> Result<Value> {
    let purse_identifier = parse_purse_identifier(purse_identifier)?;
    let state_identifier = parse_state_identifier(block_id, state_root_hash)?;
    let mut params = serde_json::Map::new();
    params.insert("purse_identifier".to_string(), purse_identifier);
    if let Some(state_identifier) = state_identifier {
        params.insert("state_identifier".to_string(), state_identifier);
    }
    Ok(Value::Object(params))
}

fn parse_state_identifier(block_id: &str, state_root_hash: &str) -> Result<Option<Value>> {
    let block_id = block_id.trim();
    if !block_id.is_empty() {
        if block_id.len() == Digest::LENGTH * 2 {
            Digest::from_hex(block_id)
                .map_err(|err| anyhow!("invalid block hash digest in block_id: {}", err))?;
            return Ok(Some(json!({
                "BlockHash": block_id,
            })));
        }

        let height = block_id
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid block height in block_id: {}", err))?;
        return Ok(Some(json!({
            "BlockHeight": height,
        })));
    }

    let state_root_hash = state_root_hash.trim();
    if state_root_hash.is_empty() {
        return Ok(None);
    }
    Digest::from_hex(state_root_hash)
        .map_err(|err| anyhow!("invalid state_root_hash digest: {}", err))?;
    Ok(Some(json!({
        "StateRootHash": state_root_hash,
    })))
}

fn parse_block_identifier_value(block_id: &str) -> Result<Value> {
    let block_id = block_id.trim();
    if block_id.is_empty() {
        return Err(anyhow!("block identifier must not be empty"));
    }
    if block_id.len() == Digest::LENGTH * 2 {
        Digest::from_hex(block_id)
            .map_err(|err| anyhow!("invalid block hash digest in block_id: {}", err))?;
        Ok(json!({
            "Hash": block_id,
        }))
    } else {
        let height = block_id
            .parse::<u64>()
            .map_err(|err| anyhow!("invalid block height in block_id: {}", err))?;
        Ok(json!({
            "Height": height,
        }))
    }
}

fn parse_query_key(key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        return Err(anyhow!("key must not be empty"));
    }
    if let Ok(contract_hash) = ContractHash::from_formatted_str(key) {
        return Ok(Key::Hash(contract_hash.value()).to_formatted_string());
    }
    if let Ok(parsed) = Key::from_formatted_str(key) {
        return Ok(parsed.to_formatted_string());
    }
    if let Ok(public_key) = PublicKey::from_hex(key) {
        return Ok(Key::Account(public_key.to_account_hash()).to_formatted_string());
    }
    Err(anyhow!(
        "failed to parse key for query; expected formatted Key or public key hex"
    ))
}

fn parse_contract_hash_for_invocation(input: &str) -> Result<AddressableEntityHash> {
    let input = input.trim();
    if input.is_empty() {
        return Err(anyhow!("transaction_contract_hash must not be empty"));
    }

    if let Ok(contract_hash) = ContractHash::from_formatted_str(input) {
        return Ok(contract_hash.into());
    }

    let digest_hex = input
        .strip_prefix("hash-")
        .or_else(|| input.strip_prefix("contract-hash-"))
        .unwrap_or(input);
    let bytes = hex_to_bytes(digest_hex)?;
    if bytes.len() != Digest::LENGTH {
        return Err(anyhow!(
            "transaction_contract_hash must be {} bytes",
            Digest::LENGTH
        ));
    }
    let mut hash = [0u8; Digest::LENGTH];
    hash.copy_from_slice(&bytes);
    Ok(ContractHash::new(hash).into())
}

fn parse_purse_identifier(input: &str) -> Result<Value> {
    let input = input.trim();
    if input.is_empty() {
        return Err(anyhow!("purse_identifier must not be empty"));
    }

    if input.starts_with("account-hash-") {
        casper_types::account::AccountHash::from_formatted_str(input)
            .map_err(|err| anyhow!("invalid account hash purse identifier: {}", err))?;
        return Ok(json!({
            "main_purse_under_account_hash": input,
        }));
    }

    if input.starts_with("entity-") {
        return Ok(json!({
            "main_purse_under_entity_addr": input,
        }));
    }

    if input.starts_with("uref-") {
        URef::from_formatted_str(input)
            .map_err(|err| anyhow!("invalid uref purse identifier: {}", err))?;
        return Ok(json!({
            "purse_uref": input,
        }));
    }

    let public_key = PublicKey::from_hex(input)
        .map_err(|err| anyhow!("invalid public key purse identifier: {}", err))?;
    Ok(json!({
        "main_purse_under_public_key": public_key.to_hex_string(),
    }))
}

fn build_pricing_mode(gas_price_tolerance: Option<u8>, payment_amount: Option<u64>) -> PricingMode {
    if let Some(payment_amount) = payment_amount {
        PricingMode::PaymentLimited {
            payment_amount,
            gas_price_tolerance: gas_price_tolerance.unwrap_or(5),
            standard_payment: true,
        }
    } else {
        PricingMode::Fixed {
            gas_price_tolerance: gas_price_tolerance.unwrap_or(5),
            additional_computation_factor: 0,
        }
    }
}

fn parse_optional_seed_hex(seed: Option<&str>) -> Result<Option<[u8; 32]>> {
    let Some(seed) = seed else {
        return Ok(None);
    };
    let seed = seed.trim();
    if seed.is_empty() {
        return Ok(None);
    }

    let cleaned = seed.strip_prefix("0x").unwrap_or(seed);
    let bytes = hex_to_bytes(cleaned)?;
    if bytes.len() != 32 {
        return Err(anyhow!("runtime_seed_hex must be exactly 32 bytes"));
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(Some(out))
}

fn normalize_optional_identifier(value: Option<&str>) -> String {
    value.map(str::trim).unwrap_or_default().to_string()
}

async fn resolve_global_state_identifier(
    network_name: &str,
    node_id: u32,
    block_id: Option<&str>,
    state_root_hash: Option<&str>,
) -> Result<(String, String)> {
    let block_id = normalize_optional_identifier(block_id);
    let state_root_hash = normalize_optional_identifier(state_root_hash);

    if !block_id.is_empty() || !state_root_hash.is_empty() {
        return Ok((block_id, state_root_hash));
    }

    let rpc = mcp_rpc_client(network_name, node_id)?;
    let response = rpc.get_block().await?;
    let latest_block = response
        .block_with_signatures
        .ok_or_else(|| anyhow!("latest block was not returned by chain_get_block"))?;

    Ok((latest_block.block.hash().to_hex_string(), String::new()))
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(anyhow!("hex string must have even length"));
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let mut chars = input.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        let byte = ((hex_nibble(hi)? as u8) << 4) | (hex_nibble(lo)? as u8);
        out.push(byte);
    }
    Ok(out)
}

fn hex_nibble(ch: char) -> Result<u32> {
    ch.to_digit(16)
        .ok_or_else(|| anyhow!("invalid hex character '{}'", ch))
}

async fn ensure_running_network(
    network: &Arc<ManagedNetwork>,
) -> std::result::Result<(), ErrorData> {
    if network.is_running().await {
        Ok(())
    } else {
        Err(ErrorData::resource_not_found(
            "network is not running; call spawn_network then wait_network_ready",
            None,
        ))
    }
}

async fn check_rest_ready(network: &Arc<ManagedNetwork>) -> Result<HashMap<u32, Value>> {
    let node_ids = {
        let state = network.state.lock().await;
        state
            .processes
            .iter()
            .filter_map(|process| {
                if matches!(process.kind, ProcessKind::Node) {
                    Some(process.node_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };

    if node_ids.is_empty() {
        return Err(anyhow!("network has no node processes"));
    }

    let mut by_node = HashMap::new();
    for node_id in node_ids {
        let value = fetch_rest_status(node_id).await?;
        let probe: RestStatusProbe = serde_json::from_value(value.clone())
            .with_context(|| format!("invalid /status payload for node {}", node_id))?;
        if probe.reactor_state.as_deref() != Some("Validate") {
            return Err(anyhow!("node {} reactor is not Validate", node_id));
        }
        by_node.insert(node_id, value);
    }

    Ok(by_node)
}

fn rest_has_block(value: &Value) -> bool {
    value
        .get("last_added_block_info")
        .and_then(|entry| entry.get("height"))
        .and_then(Value::as_u64)
        .is_some()
}

async fn fetch_rest_status(node_id: u32) -> Result<Value> {
    let url = format!("{}/status", assets::rest_endpoint(node_id));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(4))
        .build()?;
    let response = client.get(url).send().await?.error_for_status()?;
    Ok(response.json::<Value>().await?)
}

async fn run_sse_listener(network: Arc<ManagedNetwork>, node_id: u32, endpoint: String) {
    let mut backoff = ExponentialBackoff::default();

    loop {
        if network.shutdown.load(Ordering::SeqCst) {
            return;
        }

        let config = match ListenerConfig::builder()
            .with_endpoint(endpoint.clone())
            .build()
        {
            Ok(config) => config,
            Err(_) => {
                if !sleep_backoff(&mut backoff).await {
                    return;
                }
                continue;
            }
        };

        let stream = match sse::listener(config).await {
            Ok(stream) => {
                backoff.reset();
                stream
            }
            Err(_) => {
                if !sleep_backoff(&mut backoff).await {
                    return;
                }
                continue;
            }
        };

        futures::pin_mut!(stream);
        let mut failed = false;

        while let Some(event) = stream.next().await {
            if network.shutdown.load(Ordering::SeqCst) {
                return;
            }

            match event {
                Ok(event) => {
                    if let SseEvent::BlockAdded { block, .. } = &event {
                        let _ = record_last_block_height(&network.state, block.height()).await;
                    }
                    network.sse_store.push(node_id, event).await;
                }
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }

        if failed && !sleep_backoff(&mut backoff).await {
            return;
        }
    }
}

async fn record_last_block_height(state: &Arc<Mutex<State>>, height: u64) -> Result<()> {
    let mut state = state.lock().await;
    if state.last_block_height == Some(height) {
        return Ok(());
    }
    state.last_block_height = Some(height);
    state.touch().await
}

async fn sleep_backoff(backoff: &mut ExponentialBackoff) -> bool {
    if let Some(delay) = backoff.next_backoff() {
        tokio::time::sleep(delay).await;
        true
    } else {
        false
    }
}

fn sse_event_type(event: &SseEvent) -> &'static str {
    match event {
        SseEvent::ApiVersion(_) => "ApiVersion",
        SseEvent::DeployAccepted(_) => "DeployAccepted",
        SseEvent::BlockAdded { .. } => "BlockAdded",
        SseEvent::DeployProcessed(_) => "DeployProcessed",
        SseEvent::DeployExpired(_) => "DeployExpired",
        SseEvent::TransactionAccepted(_) => "TransactionAccepted",
        SseEvent::TransactionProcessed { .. } => "TransactionProcessed",
        SseEvent::TransactionExpired { .. } => "TransactionExpired",
        SseEvent::Fault { .. } => "Fault",
        SseEvent::FinalitySignature(_) => "FinalitySignature",
        SseEvent::Step { .. } => "Step",
        SseEvent::Shutdown => "Shutdown",
    }
}

fn process_kind_name(kind: &ProcessKind) -> &'static str {
    match kind {
        ProcessKind::Node => "node",
        ProcessKind::Sidecar => "sidecar",
    }
}

fn process_status_name(status: &ProcessStatus) -> &'static str {
    match status {
        ProcessStatus::Running => "running",
        ProcessStatus::Stopped => "stopped",
        ProcessStatus::Exited => "exited",
        ProcessStatus::Unknown => "unknown",
        ProcessStatus::Skipped => "skipped",
    }
}

fn sse_event_payload(event: &SseEvent) -> Value {
    match event {
        SseEvent::ApiVersion(version) => json!({ "api_version": version.to_string() }),
        SseEvent::DeployAccepted(payload) => payload.clone(),
        SseEvent::BlockAdded { block_hash, block } => json!({
            "block_hash": block_hash.to_string(),
            "height": block.height(),
            "era_id": block.era_id().value(),
        }),
        SseEvent::DeployProcessed(payload) => payload.clone(),
        SseEvent::DeployExpired(payload) => payload.clone(),
        SseEvent::TransactionAccepted(transaction) => {
            json!({ "transaction_hash": transaction.hash().to_hex_string() })
        }
        SseEvent::TransactionProcessed {
            transaction_hash,
            execution_result,
            messages,
            ..
        } => json!({
            "transaction_hash": transaction_hash.to_hex_string(),
            "execution_result": execution_result,
            "messages": messages,
        }),
        SseEvent::TransactionExpired { transaction_hash } => json!({
            "transaction_hash": transaction_hash.to_hex_string(),
        }),
        SseEvent::Fault {
            era_id,
            public_key,
            timestamp,
        } => json!({
            "era_id": era_id.value(),
            "public_key": public_key.to_hex(),
            "timestamp": timestamp,
        }),
        SseEvent::FinalitySignature(signature) => json!({
            "block_hash": signature.block_hash().to_string(),
            "era_id": signature.era_id().value(),
            "signature": signature.signature().to_hex(),
        }),
        SseEvent::Step {
            era_id,
            execution_effects,
        } => json!({
            "era_id": era_id.value(),
            "execution_effects": execution_effects.get(),
        }),
        SseEvent::Shutdown => json!({}),
    }
}

fn timestamp_prefix() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string())
}

fn processes_running(state: &State) -> bool {
    if state.processes.is_empty() {
        return false;
    }

    state.processes.iter().all(|process| {
        matches!(process.last_status, ProcessStatus::Running)
            && process.pid.is_some_and(is_pid_running)
    })
}

fn is_pid_running(pid: u32) -> bool {
    let pid = Pid::from_raw(pid as i32);
    match kill(pid, None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => true,
    }
}

async fn discover_network_names(assets_root: &Path) -> Result<Vec<String>> {
    if !is_dir(assets_root).await {
        return Ok(Vec::new());
    }

    let mut names = Vec::new();
    let mut entries = tokio_fs::read_dir(assets_root).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.is_empty() {
            names.push(name);
        }
    }

    names.sort();
    Ok(names)
}

async fn ensure_sidecar_available(layout: &AssetsLayout, node_count: u32) -> Result<()> {
    for node_id in 1..=node_count {
        let version_dir = layout.latest_protocol_version_dir(node_id).await?;
        let sidecar_bin = layout
            .node_bin_dir(node_id)
            .join(&version_dir)
            .join("casper-sidecar");
        let sidecar_cfg = layout
            .node_config_root(node_id)
            .join(&version_dir)
            .join("sidecar.toml");

        if !is_file(&sidecar_bin).await {
            return Err(anyhow!(
                "missing sidecar binary for node {}: {}",
                node_id,
                sidecar_bin.display()
            ));
        }

        if !is_file(&sidecar_cfg).await {
            return Err(anyhow!(
                "missing sidecar.toml for node {}: {}",
                node_id,
                sidecar_cfg.display()
            ));
        }
    }

    Ok(())
}

async fn resolve_protocol_version(candidate: Option<&str>) -> Result<String> {
    if let Some(raw) = candidate {
        let version = assets::parse_protocol_version(raw)?;
        if !assets::has_bundle_version(&version).await? {
            return Err(anyhow!("assets for version {} not found", version));
        }
        return Ok(version.to_string());
    }

    let version = assets::most_recent_bundle_version()
        .await?
        .ok_or_else(|| anyhow!("no assets bundles found"))?;
    Ok(version.to_string())
}

async fn parse_derived_accounts_csv(
    csv: &str,
    seed: Option<Arc<str>>,
) -> Result<Vec<DerivedAccountRow>> {
    let mut lines = csv.lines();
    let header = lines
        .next()
        .ok_or_else(|| anyhow!("derived accounts csv is empty"))?;
    if header.trim() != "kind,name,key_type,derivation,path,account_hash,balance" {
        return Err(anyhow!("unexpected derived-accounts.csv header"));
    }

    let mut rows = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts = line.splitn(7, ',').collect::<Vec<_>>();
        if parts.len() != 7 {
            return Err(anyhow!("invalid derived account row: {}", line));
        }

        let path = parts[4].to_string();
        let public_key = if let Some(seed) = &seed {
            match assets::derive_account_from_seed_path(Arc::clone(seed), &path).await {
                Ok(material) => Some(material.public_key_hex),
                Err(_) => None,
            }
        } else {
            None
        };

        rows.push(DerivedAccountRow {
            kind: parts[0].to_string(),
            name: parts[1].to_string(),
            key_type: parts[2].to_string(),
            derivation: parts[3].to_string(),
            path,
            account_hash: parts[5].to_string(),
            balance: parts[6].to_string(),
            public_key,
        });
    }

    Ok(rows)
}

async fn verify_path_hash_consistency(
    layout: &AssetsLayout,
    path: &str,
    expected_account_hash: &str,
) -> Result<()> {
    let Some(csv) = assets::derived_accounts_summary(layout).await else {
        return Ok(());
    };

    for line in csv.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts = line.splitn(7, ',').collect::<Vec<_>>();
        if parts.len() != 7 {
            continue;
        }
        if parts[4] == path {
            if parts[5] != expected_account_hash {
                return Err(anyhow!(
                    "derived account hash mismatch for path {}: csv={} derived={}",
                    path,
                    parts[5],
                    expected_account_hash
                ));
            }
            return Ok(());
        }
    }

    Ok(())
}

async fn fetch_chain_name(network_name: &str, node_id: u32) -> Result<String> {
    let rpc = mcp_rpc_client(network_name, node_id)?;
    rpc.get_network_name().await.map_err(Into::into)
}

fn extract_block_height(value: &Value) -> Option<u64> {
    value
        .pointer("/block_with_signatures/block/header/height")
        .and_then(Value::as_u64)
        .or_else(|| {
            value
                .pointer("/block/block/header/height")
                .and_then(Value::as_u64)
        })
        .or_else(|| {
            value
                .pointer("/block_with_signatures/Version2/block/Version2/header/height")
                .and_then(Value::as_u64)
        })
        .or_else(|| {
            value
                .pointer("/block_with_signatures/Version1/block/Version1/header/height")
                .and_then(Value::as_u64)
        })
        .or_else(|| find_first_height(value))
}

fn find_first_height(value: &Value) -> Option<u64> {
    match value {
        Value::Object(map) => {
            if let Some(height) = map.get("height").and_then(Value::as_u64) {
                return Some(height);
            }
            for nested in map.values() {
                if let Some(height) = find_first_height(nested) {
                    return Some(height);
                }
            }
            None
        }
        Value::Array(items) => {
            for item in items {
                if let Some(height) = find_first_height(item) {
                    return Some(height);
                }
            }
            None
        }
        _ => None,
    }
}

fn parse_no_such_block_range_from_error(error_text: &str) -> Option<(u64, u64)> {
    let start = error_text.find('{')?;
    let payload = &error_text[start..];
    let value: Value = serde_json::from_str(payload).ok()?;

    let code = value.get("code").and_then(Value::as_i64)?;
    let message = value.get("message").and_then(Value::as_str)?;
    if code != -32001 || !message.eq_ignore_ascii_case("No such block") {
        return None;
    }

    let low = value
        .pointer("/data/available_block_range/low")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let high = value
        .pointer("/data/available_block_range/high")
        .and_then(Value::as_u64)
        .unwrap_or(low);
    Some((low, high))
}

async fn read_log_page(path: &Path, before_line: Option<usize>, limit: usize) -> Result<LogPage> {
    let contents = tokio_fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read log file {}", path.display()))?;

    let all_lines = contents
        .lines()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let total_lines = all_lines.len();

    let before = before_line.unwrap_or(total_lines + 1);
    if before == 0 {
        return Err(anyhow!("before_line must be >= 1"));
    }

    let end_exclusive = before.saturating_sub(1).min(total_lines);
    let start = end_exclusive.saturating_sub(limit);

    let mut lines = Vec::new();
    for (idx, content) in all_lines[start..end_exclusive].iter().enumerate() {
        lines.push(LogLine {
            line_number: start + idx + 1,
            content: content.clone(),
        });
    }

    let next_before_line = if start == 0 { None } else { Some(start + 1) };

    Ok(LogPage {
        path: path.display().to_string(),
        total_lines,
        returned: lines.len(),
        next_before_line,
        lines,
    })
}

async fn is_dir(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

async fn is_file(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

pub async fn run(args: McpArgs) -> Result<()> {
    let assets_root = match args.net_path {
        Some(path) => path,
        None => assets::default_assets_root()?,
    };

    let manager = Arc::new(NetworkManager::new(assets_root).await?);

    let result = match args.transport {
        McpTransport::Stdio => run_stdio(manager.clone()).await,
        McpTransport::Http => run_http(manager.clone(), &args.http_bind, &args.http_path).await,
        McpTransport::Both => run_both(manager.clone(), &args.http_bind, &args.http_path).await,
    };

    let stop_result = manager.stop_all_networks().await;

    match (result, stop_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(())) => Err(err),
        (Ok(()), Err(stop_err)) => Err(stop_err),
        (Err(run_err), Err(stop_err)) => Err(anyhow!(
            "mcp server failed: {run_err}; additionally failed to stop networks: {stop_err}"
        )),
    }
}

async fn run_stdio(manager: Arc<NetworkManager>) -> Result<()> {
    let service = McpServer::new(manager).serve(mcp_stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn run_http(manager: Arc<NetworkManager>, bind: &str, path: &str) -> Result<()> {
    let path = normalize_http_path(path);
    let socket = std::net::SocketAddr::from_str(bind)
        .with_context(|| format!("invalid http bind address '{}'", bind))?;

    let service: StreamableHttpService<McpServer, LocalSessionManager> = StreamableHttpService::new(
        {
            let manager = manager.clone();
            move || Ok(McpServer::new(manager.clone()))
        },
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );

    let router = axum::Router::new().nest_service(&path, service);
    let listener = tokio::net::TcpListener::bind(socket).await?;

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}

async fn run_both(manager: Arc<NetworkManager>, bind: &str, path: &str) -> Result<()> {
    let path = normalize_http_path(path);
    let socket = std::net::SocketAddr::from_str(bind)
        .with_context(|| format!("invalid http bind address '{}'", bind))?;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    let mut http_task = {
        let manager = manager.clone();
        tokio::spawn(async move {
            let service: StreamableHttpService<McpServer, LocalSessionManager> =
                StreamableHttpService::new(
                    {
                        let manager = manager.clone();
                        move || Ok(McpServer::new(manager.clone()))
                    },
                    Arc::new(LocalSessionManager::default()),
                    StreamableHttpServerConfig::default(),
                );

            let router = axum::Router::new().nest_service(&path, service);
            let listener = tokio::net::TcpListener::bind(socket).await?;

            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    tokio::select! {
                        _ = async {
                            loop {
                                if *shutdown_rx.borrow() {
                                    break;
                                }
                                if shutdown_rx.changed().await.is_err() {
                                    break;
                                }
                            }
                        } => {}
                        _ = tokio::signal::ctrl_c() => {}
                    }
                })
                .await
                .map_err(anyhow::Error::from)
        })
    };

    let mut stdio_task = tokio::spawn(async move { run_stdio(manager).await });

    let result = tokio::select! {
        res = &mut stdio_task => match res {
            Ok(inner) => inner,
            Err(join_err) => Err(anyhow!("stdio task failed: {}", join_err)),
        },
        res = &mut http_task => match res {
            Ok(inner) => inner,
            Err(join_err) => Err(anyhow!("http task failed: {}", join_err)),
        },
    };

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), &mut http_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), &mut stdio_task).await;

    result
}

fn normalize_http_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_derived_accounts_rows() {
        let csv = "kind,name,key_type,derivation,path,account_hash,balance\nvalidator,node-1,secp256k1,bip32,m/44'/506'/0'/0/0,account-hash-a,100\nuser,user-1,secp256k1,bip32,m/44'/506'/0'/0/100,account-hash-b,200";
        let rows = parse_derived_accounts_csv(csv, None).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].path, "m/44'/506'/0'/0/0");
        assert_eq!(rows[1].account_hash, "account-hash-b");
    }

    #[test]
    fn sse_filter_include_exclude() {
        let filter = SseFilter {
            include_event_types: vec!["BlockAdded".to_string()],
            exclude_event_types: vec!["TransactionAccepted".to_string()],
        };

        let block = SseRecord {
            sequence: 1,
            timestamp_rfc3339: "t".to_string(),
            node_id: 1,
            event_type: "BlockAdded".to_string(),
            payload: json!({}),
        };
        let tx = SseRecord {
            sequence: 2,
            timestamp_rfc3339: "t".to_string(),
            node_id: 1,
            event_type: "TransactionAccepted".to_string(),
            payload: json!({}),
        };

        assert!(filter.matches(&block));
        assert!(!filter.matches(&tx));
    }

    #[tokio::test]
    async fn sse_history_paginates_by_sequence() {
        let store = SseStore::new(100);
        for sequence in 1..=5 {
            let mut guard = store.events.lock().await;
            guard.push_back(SseRecord {
                sequence,
                timestamp_rfc3339: "t".to_string(),
                node_id: 1,
                event_type: "BlockAdded".to_string(),
                payload: json!({ "sequence": sequence }),
            });
            drop(guard);
        }
        let filter = SseFilter::default();
        let page = store.history(Some(6), 2, &filter).await;
        assert_eq!(page.returned, 2);
        assert_eq!(page.events[0].sequence, 4);
        assert_eq!(page.events[1].sequence, 5);
        assert_eq!(page.next_before_sequence, Some(4));
    }

    #[tokio::test]
    async fn read_log_page_uses_before_cursor() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        tokio_fs::write(temp.path(), "a\nb\nc\nd\n").await.unwrap();

        let page = read_log_page(temp.path(), Some(5), 2).await.unwrap();
        assert_eq!(page.lines.len(), 2);
        assert_eq!(page.lines[0].line_number, 3);
        assert_eq!(page.lines[1].line_number, 4);
        assert_eq!(page.next_before_line, Some(3));
    }

    #[tokio::test]
    async fn sidecar_preflight_fails_when_missing() {
        let root = tempfile::tempdir().unwrap();
        let layout = AssetsLayout::new(root.path().to_path_buf(), "test-net".to_string());
        let node_bin = layout.node_bin_dir(1).join("2_0_0");
        let node_cfg = layout.node_config_root(1).join("2_0_0");
        tokio_fs::create_dir_all(&node_bin).await.unwrap();
        tokio_fs::create_dir_all(&node_cfg).await.unwrap();
        tokio_fs::write(node_bin.join("casper-node"), "bin")
            .await
            .unwrap();
        tokio_fs::write(node_cfg.join("sidecar.toml"), "cfg")
            .await
            .unwrap();

        let result = ensure_sidecar_available(&layout, 1).await;
        assert!(result.is_err());
    }

    #[test]
    fn normalize_optional_identifier_trims_and_defaults() {
        assert_eq!(normalize_optional_identifier(None), "");
        assert_eq!(normalize_optional_identifier(Some("   ")), "");
        assert_eq!(normalize_optional_identifier(Some(" 123 ")), "123");
    }

    #[test]
    fn parse_state_identifier_variants() {
        assert_eq!(
            parse_state_identifier("42", "").unwrap(),
            Some(json!({ "BlockHeight": 42u64 }))
        );
        assert_eq!(
            parse_state_identifier(
                "2f6fbeebbe1bdf6f8ff05880edfa4e4f79849d2b4f0ecf65482177e4fabc1234",
                ""
            )
            .unwrap(),
            Some(json!({
                "BlockHash": "2f6fbeebbe1bdf6f8ff05880edfa4e4f79849d2b4f0ecf65482177e4fabc1234"
            }))
        );
        assert_eq!(parse_state_identifier("", "").unwrap(), None,);
    }

    #[test]
    fn parse_contract_hash_for_invocation_accepts_common_formats() {
        let hash = "2f6fbeebbe1bdf6f8ff05880edfa4e4f79849d2b4f0ecf65482177e4fabc1234";
        let contract = parse_contract_hash_for_invocation(&format!("contract-{}", hash)).unwrap();
        let key_hash = parse_contract_hash_for_invocation(&format!("hash-{}", hash)).unwrap();
        let raw = parse_contract_hash_for_invocation(hash).unwrap();
        assert_eq!(contract.to_hex_string(), hash);
        assert_eq!(key_hash.to_hex_string(), hash);
        assert_eq!(raw.to_hex_string(), hash);
    }

    #[test]
    fn parse_query_key_accepts_contract_hash_format() {
        let hash = "2f6fbeebbe1bdf6f8ff05880edfa4e4f79849d2b4f0ecf65482177e4fabc1234";
        let key = parse_query_key(&format!("contract-{}", hash)).unwrap();
        assert_eq!(key, format!("hash-{}", hash));
    }

    #[test]
    fn parse_transaction_json_rejects_escaped_string_payload() {
        let tx_json = json!({
            "Version1": {
                "hash": "7eeb092361e31b4cc9885e3621f1470f29631338ecc703643c22da1d38fd81a9",
                "payload": {
                    "initiator_addr": {
                        "PublicKey": "0202f9bae6a6c5a8345c2aa8339b54ff3fcf82d2f6a9cce1732e765c2cc403b3be9f"
                    },
                    "timestamp": "2026-02-27T18:03:18.541Z",
                    "ttl": "30m",
                    "chain_name": "casper-devnet",
                    "pricing_mode": {
                        "PaymentLimited": {
                            "payment_amount": 100000000000u64,
                            "gas_price_tolerance": 5,
                            "standard_payment": true
                        }
                    },
                    "fields": {
                        "args": {"Named": []},
                        "entry_point": {"Custom": "counter_inc"},
                        "scheduling": "Standard",
                        "target": {
                            "Stored": {
                                "id": {
                                    "ByPackageName": {
                                        "name": "counter_package_name",
                                        "version": null
                                    }
                                },
                                "runtime": "VmCasperV1"
                            }
                        }
                    }
                },
                "approvals": [{
                    "signer": "0202f9bae6a6c5a8345c2aa8339b54ff3fcf82d2f6a9cce1732e765c2cc403b3be9f",
                    "signature": "02c64336e5ed2832bdb84adb3f334d585548ee096066aa9d0797c11ab3f074ec9d7bd396994bc9b9c239342be801bc385a9c5083779bace4dfe0b400d4a13c07db"
                }]
            }
        });
        let direct = parse_transaction_json(tx_json.clone()).unwrap();
        let wrapped = parse_transaction_json(json!({ "transaction": tx_json })).unwrap();
        let encoded = serde_json::to_string(&direct).unwrap();
        let from_string_err = parse_transaction_json(Value::String(encoded.clone()));
        let wrapped_string_err = parse_transaction_json(json!({
            "transaction": encoded
        }));

        assert_eq!(
            direct.hash().to_hex_string(),
            wrapped.hash().to_hex_string()
        );
        assert!(from_string_err.is_err());
        assert!(wrapped_string_err.is_err());
    }

    #[test]
    fn parse_no_such_block_range_from_error_extracts_bounds() {
        let error = "casper client error: response for rpc-id 1 chain_get_block is json-rpc error: {\"code\":-32001,\"message\":\"No such block\",\"data\":{\"message\":\"no block found for the provided identifier\",\"available_block_range\":{\"low\":0,\"high\":0}}}";
        let range = parse_no_such_block_range_from_error(error).unwrap();
        assert_eq!(range, (0, 0));
    }

    #[test]
    fn send_transaction_signed_request_requires_transaction_field() {
        let payload = json!({
            "network_name": "casper-devnet",
            "node_id": 1,
            "signer_path": "m/44'/506'/0'/0/100",
            "transaction": { "Version1": { "hash": "abc" } }
        });
        let request: SendTransactionSignedRequest = serde_json::from_value(payload).unwrap();
        assert!(request.transaction.get("Version1").is_some());

        let legacy_payload = json!({
            "network_name": "casper-devnet",
            "node_id": 1,
            "signer_path": "m/44'/506'/0'/0/100",
            "transaction_json": { "Version1": { "hash": "def" } }
        });
        let legacy_err = serde_json::from_value::<SendTransactionSignedRequest>(legacy_payload)
            .unwrap_err()
            .to_string();
        assert!(legacy_err.contains("missing field `transaction`"));
    }
}
