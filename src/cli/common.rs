use crate::assets;
use anyhow::Result;
use directories::BaseDirs;
use serde::Deserialize;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs as tokio_fs;

pub(crate) const DEFAULT_SEED: &str = "default";

pub(crate) fn shorten_home_path(path: &str) -> String {
    let path = Path::new(path);
    let Some(base_dirs) = BaseDirs::new() else {
        return path.display().to_string();
    };
    let home = base_dirs.home_dir();
    match path.strip_prefix(home) {
        Ok(stripped) => {
            if stripped.as_os_str().is_empty() {
                return "~".to_string();
            }
            let mut shorthand = PathBuf::from("~");
            shorthand.push(stripped);
            shorthand.display().to_string()
        }
        Err(_) => path.display().to_string(),
    }
}

pub(crate) async fn is_dir(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

pub(crate) async fn is_file(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

pub(crate) async fn is_control_socket(path: &Path) -> bool {
    tokio_fs::symlink_metadata(path)
        .await
        .map(|metadata| metadata.file_type().is_socket())
        .unwrap_or(false)
}

pub(crate) async fn read_current_era_from_status(node_id: u32) -> Result<Option<u64>> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let url = format!("{}/status", assets::rest_endpoint(node_id));
    let response = client.get(&url).send().await?;
    if response.status() != reqwest::StatusCode::OK {
        return Ok(None);
    }
    let status = response.json::<NodeStatus>().await?;
    Ok(extract_era_id(status.last_added_block_info.as_ref()))
}

pub(crate) fn extract_era_id(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    if let Some(era_id) = value.get("era_id").and_then(|era_id| era_id.as_u64()) {
        return Some(era_id);
    }
    value
        .get("era_id")
        .and_then(|era_id| era_id.as_str())
        .and_then(|era_id| era_id.parse::<u64>().ok())
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
struct NodeStatus {
    last_added_block_info: Option<serde_json::Value>,
}

pub(crate) fn is_ready_reactor_state(state: &str) -> bool {
    state == "Validate"
}

#[cfg(test)]
mod tests {
    use super::{extract_era_id, shorten_home_path};
    use directories::BaseDirs;
    use serde_json::json;

    #[test]
    fn shorten_home_path_replaces_home_prefix() {
        let Some(base_dirs) = BaseDirs::new() else {
            return;
        };
        let home = base_dirs.home_dir();
        let shortened = shorten_home_path(&home.to_string_lossy());
        assert_eq!(shortened, "~");

        let nested = home.join("devnet/logs/stdout.log");
        let shortened_nested = shorten_home_path(&nested.to_string_lossy());
        assert!(shortened_nested.starts_with("~"));
        assert!(shortened_nested.contains("devnet"));
    }

    #[test]
    fn shorten_home_path_keeps_relative_paths() {
        let input = "relative/path";
        assert_eq!(shorten_home_path(input), input);
    }

    #[test]
    fn extract_era_id_from_status_payload() {
        assert_eq!(extract_era_id(Some(&json!({"era_id": 123}))), Some(123));
        assert_eq!(extract_era_id(Some(&json!({"era_id": "456"}))), Some(456));
        assert_eq!(extract_era_id(Some(&json!({"era_id": "abc"}))), None);
        assert_eq!(extract_era_id(None), None);
    }
}
