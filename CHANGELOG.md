# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to
Semantic Versioning.

## [Unreleased]
### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.5.1] - 2026-01-19
### Changed
- `assets list` now exits with an error when no asset bundles are installed.

## [0.5.0] - 2026-01-19
### Added
- Start tracking releases in a Keep a Changelog format.

### Changed
- Rename derived accounts output to `derived-accounts.csv` and emit CSV rows.
- Add key type and split derivation/path fields in derived accounts output.
- Print derived accounts and network details in a tree-style layout.
- Pin GitHub release workflow to `softprops/action-gh-release@v2.4.2`.

### Removed
- Stop generating `users/user-*` directories and public key files for users.
- Stop writing `public_key.pem` and `public_key_hex` under `nodes/node-*/keys`.

### Security
- Delete consensus secret keys from disk after the first observed block.
- Recreate consensus secret keys on resume when assets already exist.

## [0.4.1] - 2026-01-19
### Changed
- Bump version to 0.4.1.

### Fixed
- Fix Dockerfile.
- Fix formatting.

## [0.4.0] - 2026-01-19
### Added
- Package tarballs for releases.
- Add progress bars for asset setup.

### Changed
- Display account hashes instead of public keys.
- Hardlink binaries instead of copying.
- Document how to run released image.
- Sync package version with tag.

## [0.3.4] - 2026-01-14
### Fixed
- Fix YAML syntax in workflows.

## [0.3.3] - 2026-01-14
### Added
- Publish ARM docker image release.

## [0.3.2] - 2026-01-14
### Changed
- Tag-only release (no code changes recorded).

## [0.3.1] - 2026-01-14
### Fixed
- Fix CI issues.

## [0.3.0] - 2026-01-14
### Added
- Docker build and release workflow.
- Dockerfile for a node image.
- Persist last block height to state.

### Changed
- Update config handling and docs.
- Track lockfile in repo.

### Fixed
- Diagnostics port socket clash.
- CI and test fixes.

## [0.2.1] - 2026-01-13
### Added
- Stdout/stderr log symlinks for nodes.
- Spinners to reduce log noise.
- Install steps and demo assets.

### Changed
- Shorten assets path output.
- Decrease default genesis activation delay.
- Documentation updates.

## [0.1.0] - 2026-01-11
### Added
- Initial release.

[Unreleased]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/veles-labs/casper-devnet-launcher/compare/v0.4.1...v0.5.0
