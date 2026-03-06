use super::Cli;
use clap::Parser;

#[test]
fn network_port_requires_exactly_one_selector() {
    assert!(Cli::try_parse_from(["nctl", "network", "casper-dev", "port"]).is_err());
    assert!(
        Cli::try_parse_from(["nctl", "network", "casper-dev", "port", "--rpc", "--rest"]).is_err()
    );
    assert!(Cli::try_parse_from(["nctl", "network", "casper-dev", "port", "--rpc"]).is_ok());
}

#[test]
fn derive_requires_exactly_one_selector() {
    assert!(Cli::try_parse_from(["nctl", "derive", "m/44'/506'/0'/0/0"]).is_err());
    assert!(
        Cli::try_parse_from([
            "nctl",
            "derive",
            "m/44'/506'/0'/0/0",
            "--secret-key",
            "--public-key",
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["nctl", "derive", "m/44'/506'/0'/0/0", "--account-hash"]).is_ok());
}

#[test]
fn network_command_parser_validates_nested_commands() {
    assert!(
        Cli::try_parse_from(["nctl", "network", "casper-dev", "port", "--rpc", "--rest"]).is_err()
    );
    assert!(Cli::try_parse_from(["nctl", "network", "casper-dev", "port", "--rpc"]).is_ok());
    assert!(Cli::try_parse_from(["nctl", "network", "casper-dev", "path"]).is_ok());
    assert!(Cli::try_parse_from(["nctl", "network", "casper-dev", "path", "2.2.0"]).is_ok());
    assert!(Cli::try_parse_from(["nctl", "network", "casper-dev", "is-ready"]).is_ok());
    assert!(Cli::try_parse_from(["nctl", "networks", "casper-dev", "port", "--rpc"]).is_err());
}
