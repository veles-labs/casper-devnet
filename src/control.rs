use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ControlRequest {
    RuntimeStatus,
    StageProtocol {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        asset: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        custom_asset: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        asset_name: Option<String>,
        protocol_version: String,
        activation_point: u64,
        restart_sidecars: bool,
        rust_log: Option<String>,
    },
    AddNodes {
        count: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControlResponse {
    Ok { result: ControlResult },
    Error { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ControlResult {
    RuntimeStatus {
        running_node_ids: Vec<u32>,
        last_block_height: Option<u64>,
    },
    StageProtocol {
        live_mode: bool,
        staged_nodes: u32,
        restarted_sidecars: Vec<u32>,
    },
    AddNodes {
        added_node_ids: Vec<u32>,
        total_nodes: u32,
        started_processes: u32,
    },
}

pub async fn send_request(socket_path: &Path, request: &ControlRequest) -> Result<ControlResponse> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let request_bytes = serde_json::to_vec(request)?;
    stream.write_all(&request_bytes).await?;
    stream.shutdown().await?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    if response_bytes.is_empty() {
        return Err(anyhow!(
            "empty control response from {}",
            socket_path.display()
        ));
    }

    let response = serde_json::from_slice::<ControlResponse>(&response_bytes)?;
    Ok(response)
}
