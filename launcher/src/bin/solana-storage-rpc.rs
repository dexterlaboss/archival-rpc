
use {
    clap::{
        value_t_or_exit,
    },
    log::*,
    solana_rpc::{
        storage_rpc::{JsonRpcConfig, RpcHBaseConfig},
    },
    solana_rpc_core::RpcNodeBuilder,
    solana_launcher::{
        cli,
        redirect_stderr_to_file,
    },
    std::{
        fs,
        path::{
            PathBuf
        },
        process::exit,
        time::{
            SystemTime,
            UNIX_EPOCH
        },
    },
};

#[derive(PartialEq, Eq)]
enum Output {
    None,
    Log,
}

fn main() {
    let default_args = cli::DefaultStorageRpcArgs::new();
    let version = solana_version::version!();
    let matches = cli::storage_rpc_service(version, &default_args).get_matches();

    let output = if matches.is_present("quiet") {
        Output::None
    } else {
        Output::Log
    };

    let log_path = value_t_or_exit!(matches, "log_path", PathBuf);

    if !log_path.exists() {
        fs::create_dir(&log_path).unwrap_or_else(|err| {
            println!(
                "Error: Unable to create directory {}: {}",
                log_path.display(),
                err
            );
            exit(1);
        });
    }

    let rpc_service_log_symlink = log_path.join("service.log");

    let logfile = if output != Output::Log {
        let rpc_service_log_with_timestamp = format!(
            "service-{}.log",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let _ = fs::remove_file(&rpc_service_log_symlink);
        symlink::symlink_file(&rpc_service_log_with_timestamp, &rpc_service_log_symlink).unwrap();

        Some(
            log_path
                .join(rpc_service_log_with_timestamp)
                .into_os_string()
                .into_string()
                .unwrap(),
        )
    } else {
        None
    };
    let _logger_thread = redirect_stderr_to_file(logfile);

    info!("solana-storage-rpc {}", solana_version::version!());
    info!("Starting storage rpc service with: {:#?}", std::env::args_os());

    let rpc_port = value_t_or_exit!(matches, "rpc_port", u16);

    let bind_address = matches.value_of("bind_address").map(|bind_address| {
        solana_net_utils::parse_host(bind_address).unwrap_or_else(|err| {
            eprintln!("Failed to parse --bind-address: {err}");
            exit(1);
        })
    });

    let mut builder = RpcNodeBuilder::default();

    let rpc_hbase_config = Some(RpcHBaseConfig {
        enable_hbase_ledger_upload: false,
        hbase_address: value_t_or_exit!(matches, "rpc_hbase_address", String),
        timeout: None,
    });

    builder.rpc_port(rpc_port);

    builder.rpc_config(JsonRpcConfig {
        enable_rpc_transaction_history: true,
        rpc_hbase_config,
        ..JsonRpcConfig::default_for_storage_rpc()
    });

    if let Some(bind_address) = bind_address {
        builder.bind_ip_addr(bind_address);
    }

    match builder.start() {
        Ok(rpc_node) => {
            rpc_node.join();
        }
        Err(err) => {
            println!("Error: failed to start storage rpc service: {err}");
            exit(1);
        }
    }
}