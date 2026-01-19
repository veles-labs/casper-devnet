use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use flate2::{Compression, write::GzEncoder};
use serde::Deserialize;
use tar::Builder;
use xshell::{Shell, cmd};

const CLI_NAME: &str = "casper-devnet";

#[derive(Parser)]
#[command(name = "xtask", about = "Project automation tasks")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build a release tarball for the host platform.
    Package,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Package => package(),
    }
}

fn package() -> Result<()> {
    let sh = Shell::new()?;
    let metadata = load_metadata(&sh)?;
    let target = rustc_host_triple(&sh)?;
    let version = package_version(&metadata, CLI_NAME)?;
    let target_dir = metadata.target_directory;
    let workspace_root = metadata.workspace_root;

    build_cli(&sh)?;

    let exe_suffix = exe_suffix();
    let binary_name = format!("{CLI_NAME}{exe_suffix}");
    let binary_path = target_dir.join("release").join(&binary_name);
    if !binary_path.is_file() {
        bail!("expected CLI binary at {}", binary_path.display());
    }

    let package_name = format!("{CLI_NAME}-{version}-{target}");
    let dist_dir = workspace_root.join("dist");
    fs::create_dir_all(&dist_dir).context("create dist directory")?;

    let archive_path = dist_dir.join(format!("{package_name}.tar.gz"));
    write_archive(&archive_path, &binary_path, &package_name, &binary_name)?;

    println!("Wrote {}", archive_path.display());
    Ok(())
}

fn build_cli(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo build -p {CLI_NAME} --release")
        .run()
        .context("running cargo build")?;
    Ok(())
}

fn exe_suffix() -> String {
    let extension = env::consts::EXE_EXTENSION;
    if extension.is_empty() {
        String::new()
    } else {
        format!(".{extension}")
    }
}

fn rustc_host_triple(sh: &Shell) -> Result<String> {
    let stdout = cmd!(sh, "rustc -vV").read().context("running rustc -vV")?;
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("host: ").map(str::to_string))
        .context("host triple not found in rustc -vV output")
}

fn write_archive(
    archive_path: &Path,
    binary_path: &Path,
    package_name: &str,
    binary_name: &str,
) -> Result<()> {
    let file = File::create(archive_path).context("create archive file")?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    let mut binary = File::open(binary_path).context("open CLI binary")?;
    let metadata = binary.metadata().context("read CLI metadata")?;

    let mut header = tar::Header::new_gnu();
    header.set_size(metadata.len());
    header.set_mode(0o755);
    header.set_cksum();

    let entry_path = Path::new(package_name).join(binary_name);
    builder
        .append_data(&mut header, entry_path, &mut binary)
        .context("append CLI binary")?;

    let encoder = builder.into_inner().context("finish tar builder")?;
    encoder.finish().context("finish gzip encoder")?;
    Ok(())
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_root: PathBuf,
    target_directory: PathBuf,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    version: String,
}

fn load_metadata(sh: &Shell) -> Result<Metadata> {
    let output = cmd!(sh, "cargo metadata --format-version 1 --no-deps")
        .read()
        .context("running cargo metadata")?;
    let metadata: Metadata =
        serde_json::from_str(&output).context("parse cargo metadata output")?;
    Ok(metadata)
}

fn package_version(metadata: &Metadata, name: &str) -> Result<String> {
    metadata
        .packages
        .iter()
        .find(|package| package.name == name)
        .map(|package| package.version.clone())
        .with_context(|| format!("package {name} not found in metadata"))
}
