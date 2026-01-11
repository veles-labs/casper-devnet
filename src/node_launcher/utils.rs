use std::{
    collections::BTreeSet,
    fmt::Display,
    path::Path,
    str::FromStr,
    sync::atomic::{AtomicU32, Ordering},
};

use anyhow::{bail, Error, Result};
use semver::Version;
use tokio::fs as tokio_fs;
use tokio::process::Command;
use tracing::{debug, warn};

/// Represents the exit code of the node process.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(i32)]
pub(crate) enum NodeExitCode {
    /// Indicates a successful execution.
    Success = 0,
    /// Indicates the node version should be downgraded.
    ShouldDowngrade = 102,
    /// Indicates the node launcher should attempt to run the shutdown script.
    ShouldExitLauncher = 103,
}

/// Iterates the given path, returning the subdir representing the immediate next SemVer version
/// after `current_version`.
///
/// Subdir names should be semvers with dots replaced with underscores.
pub(crate) async fn next_installed_version<P: AsRef<Path>>(
    dir: P,
    current_version: &Version,
) -> Result<Version> {
    let max_version = Version::new(u64::MAX, u64::MAX, u64::MAX);

    let mut next_version = max_version.clone();
    for installed_version in versions_from_path(dir).await? {
        if installed_version > *current_version && installed_version < next_version {
            next_version = installed_version;
        }
    }

    if next_version == max_version {
        next_version = current_version.clone();
    }

    Ok(next_version)
}

/// Iterates the given path, returning the subdir representing the immediate previous SemVer version
/// before `current_version`.
///
/// Subdir names should be semvers with dots replaced with underscores.
pub(crate) async fn previous_installed_version<P: AsRef<Path>>(
    dir: P,
    current_version: &Version,
) -> Result<Version> {
    let min_version = Version::new(0, 0, 0);

    let mut previous_version = min_version.clone();
    for installed_version in versions_from_path(dir).await? {
        if installed_version < *current_version && installed_version > previous_version {
            previous_version = installed_version;
        }
    }

    if previous_version == min_version {
        previous_version = current_version.clone();
    }

    Ok(previous_version)
}

pub(crate) async fn versions_from_path<P: AsRef<Path>>(dir: P) -> Result<BTreeSet<Version>> {
    let mut versions = BTreeSet::new();

    let mut entries = map_and_log_error(
        tokio_fs::read_dir(dir.as_ref()).await,
        format!("failed to read dir {}", dir.as_ref().display()),
    )?;
    while let Some(entry) = map_and_log_error(
        entries.next_entry().await,
        format!("bad dir entry in {}", dir.as_ref().display()),
    )? {
        let path = entry.path();
        let file_type = map_and_log_error(
            entry.file_type().await,
            format!("failed to read file type in {}", dir.as_ref().display()),
        )?;
        if !file_type.is_dir() {
            continue;
        }
        let subdir_name = match path.file_name() {
            Some(name) => name.to_string_lossy().replace('_', "."),
            None => {
                debug!("{} has no final path component", path.display());
                continue;
            }
        };
        let version = match Version::from_str(&subdir_name) {
            Ok(version) => version,
            Err(error) => {
                debug!(%error, path=%path.display(), "failed to get a version");
                continue;
            }
        };

        versions.insert(version);
    }

    if versions.is_empty() {
        let msg = format!(
            "failed to get a valid version from subdirs in {}",
            dir.as_ref().display()
        );
        warn!("{}", msg);
        bail!(msg);
    }

    Ok(versions)
}

/// Runs the given command as a child process.
pub(crate) async fn run_node(mut command: Command, child_pid: &AtomicU32) -> Result<NodeExitCode> {
    let mut child = map_and_log_error(command.spawn(), format!("failed to execute {command:?}"))?;
    if let Some(pid) = child.id() {
        child_pid.store(pid, Ordering::SeqCst);
    }

    let exit_status = map_and_log_error(
        child.wait().await,
        format!("failed to wait for completion of {command:?}"),
    )?;
    child_pid.store(0, Ordering::SeqCst);

    match exit_status.code() {
        Some(code) if code == NodeExitCode::Success as i32 => {
            debug!("successfully finished running {command:?}");
            Ok(NodeExitCode::Success)
        }
        Some(code) if code == NodeExitCode::ShouldDowngrade as i32 => {
            debug!("finished running {command:?} - should downgrade now");
            Ok(NodeExitCode::ShouldDowngrade)
        }
        Some(code) if code == NodeExitCode::ShouldExitLauncher as i32 => {
            debug!(
                "finished running {:?} - trying to run shutdown script now",
                command
            );
            Ok(NodeExitCode::ShouldExitLauncher)
        }
        _ => {
            warn!(%exit_status, "failed running {command:?}");
            bail!("{command:?} exited with error");
        }
    }
}

/// Maps an error to a different type of error, while also logging the error at warn level.
pub(crate) fn map_and_log_error<T, E: std::error::Error + Send + Sync + 'static>(
    result: std::result::Result<T, E>,
    error_msg: String,
) -> Result<T> {
    match result {
        Ok(t) => Ok(t),
        Err(error) => {
            warn!(%error, "{error_msg}");
            Err(Error::new(error).context(error_msg))
        }
    }
}

/// Joins the items into a single string.
/// The input `[1, 2, 3]` will result in a string "1, 2, 3".
pub(crate) fn iter_to_string<I>(iterable: I) -> String
where
    I: IntoIterator,
    I::Item: Display,
{
    let result = iterable
        .into_iter()
        .fold(String::new(), |result, item| format!("{result}{item}, "));
    if result.is_empty() {
        result
    } else {
        String::from(&result[0..result.len() - 2])
    }
}
