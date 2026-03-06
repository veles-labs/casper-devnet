use crate::assets;
use crate::cli::common::DEFAULT_SEED;
use anyhow::{Result, anyhow};
use clap::{ArgGroup, Args};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs as tokio_fs;

const DERIVE_SECRET_KEY_FILE: &str = "secret_key.pem";
const DERIVE_PUBLIC_KEY_FILE: &str = "public_key_hex";
const DERIVE_ACCOUNT_HASH_FILE: &str = "account_hash";

#[derive(Args, Clone)]
#[command(group(
    ArgGroup::new("material")
        .required(true)
        .multiple(false)
        .args(["secret_key", "public_key", "account_hash"])
))]
pub(crate) struct DeriveArgs {
    /// BIP32 derivation path (for example m/44'/506'/0'/0/0).
    #[arg(value_name = "PATH")]
    path: String,

    /// Print or write the derived secret key PEM.
    #[arg(long, group = "material")]
    secret_key: bool,

    /// Print or write the derived public key hex.
    #[arg(long, group = "material")]
    public_key: bool,

    /// Print or write the derived account hash.
    #[arg(long, group = "material")]
    account_hash: bool,

    /// Deterministic seed for derivation.
    #[arg(long, default_value = DEFAULT_SEED)]
    seed: Arc<str>,

    /// Output directory, or `-` to force stdout.
    #[arg(short = 'o', long, value_name = "PATH")]
    output: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeriveSelection {
    SecretKey,
    PublicKey,
    AccountHash,
}

impl DeriveArgs {
    fn selection(&self) -> DeriveSelection {
        if self.secret_key {
            DeriveSelection::SecretKey
        } else if self.public_key {
            DeriveSelection::PublicKey
        } else {
            DeriveSelection::AccountHash
        }
    }
}

pub(crate) async fn run(args: DeriveArgs) -> Result<()> {
    let selection = args.selection();
    let material =
        assets::derive_account_from_seed_path(Arc::clone(&args.seed), &args.path).await?;
    let output = derive_output_content(&material, selection);

    if let Some(path) = args.output.as_deref() {
        if path == Path::new("-") {
            print!("{}", output);
            if !output.ends_with('\n') {
                println!();
            }
            return Ok(());
        }

        let output_path = write_derive_output(path, selection, &output).await?;
        println!("{}", output_path.display());
        return Ok(());
    }

    print!("{}", output);
    if !output.ends_with('\n') {
        println!();
    }
    Ok(())
}

fn derive_output_content(
    material: &assets::DerivedPathMaterial,
    selection: DeriveSelection,
) -> String {
    match selection {
        DeriveSelection::SecretKey => material.secret_key_pem.clone(),
        DeriveSelection::PublicKey => format!("{}\n", material.public_key_hex),
        DeriveSelection::AccountHash => format!("{}\n", material.account_hash),
    }
}

fn derive_output_file_name(selection: DeriveSelection) -> &'static str {
    match selection {
        DeriveSelection::SecretKey => DERIVE_SECRET_KEY_FILE,
        DeriveSelection::PublicKey => DERIVE_PUBLIC_KEY_FILE,
        DeriveSelection::AccountHash => DERIVE_ACCOUNT_HASH_FILE,
    }
}

async fn write_derive_output(
    output_dir: &Path,
    selection: DeriveSelection,
    content: &str,
) -> Result<PathBuf> {
    if let Ok(metadata) = tokio_fs::metadata(output_dir).await
        && !metadata.is_dir()
    {
        return Err(anyhow!(
            "derive output path {} is not a directory",
            output_dir.display()
        ));
    }

    tokio_fs::create_dir_all(output_dir).await?;
    let output_path = output_dir.join(derive_output_file_name(selection));
    tokio_fs::write(&output_path, content).await?;
    Ok(output_path)
}

#[cfg(test)]
mod tests {
    use super::{DeriveSelection, derive_output_file_name, write_derive_output};
    use tempfile::TempDir;
    use tokio::fs as tokio_fs;

    #[test]
    fn derive_output_file_names_match_selection() {
        assert_eq!(
            derive_output_file_name(DeriveSelection::SecretKey),
            "secret_key.pem"
        );
        assert_eq!(
            derive_output_file_name(DeriveSelection::PublicKey),
            "public_key_hex"
        );
        assert_eq!(
            derive_output_file_name(DeriveSelection::AccountHash),
            "account_hash"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_derive_output_creates_named_file_in_directory() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("derived");

        let output_path = write_derive_output(&output_dir, DeriveSelection::PublicKey, "abc123\n")
            .await
            .unwrap();

        assert_eq!(output_path, output_dir.join("public_key_hex"));
        assert_eq!(
            tokio_fs::read_to_string(&output_path).await.unwrap(),
            "abc123\n"
        );
    }
}
