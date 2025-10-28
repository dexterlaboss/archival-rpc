#![allow(clippy::arithmetic_side_effects)]
#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
use jemallocator::Jemalloc;
use {
    solana_rpc::{
        rpc_core::RpcNodeBuilder,
        cli,
        logging::redirect_stderr_to_file,
        request_processor::{JsonRpcConfig, RpcHBaseConfig},
    },
    clap::{
        value_t_or_exit,
    },
    log::*,
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

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

    // let full_api = matches.is_present("full_rpc_api");

    let mut builder = RpcNodeBuilder::default();

    let rpc_hbase_config = Some(RpcHBaseConfig {
        enable_hbase_ledger_upload: false,
        hbase_address: value_t_or_exit!(matches, "rpc_hbase_address", String),
        namespace: if matches.is_present("hbase_namespace") {
            Some(value_t_or_exit!(
                matches,
                "hbase_namespace",
                String
            ))
        } else {
            None
        },
        hdfs_url: if matches.is_present("use_webhdfs") {
            // Not required when using WebHDFS; leave default from storage-hbase
            solana_storage_hbase::DEFAULT_HDFS_URL.to_string()
        } else {
            value_t_or_exit!(matches, "hdfs_url", String)
        },
        hdfs_path: value_t_or_exit!(matches, "hdfs_path", String),
        // hdfs_url: if matches.is_present("hdfs_url") {
        //     Some(value_t_or_exit!(
        //         matches,
        //         "hdfs_url",
        //         String
        //     ))
        // } else {
        //     None
        // },
        // hdfs_path: if matches.is_present("hdfs_path") {
        //     Some(value_t_or_exit!(
        //         matches,
        //         "hdfs_path",
        //         String
        //     ))
        // } else {
        //     None
        // },
        fallback_hbase_address: if matches.is_present("fallback_hbase_address") {
            Some(value_t_or_exit!(
                matches,
                "fallback_hbase_address",
                String
            ))
        } else {
            None
        },
        timeout: None,
        // block_cache: if matches.is_present("block_cache") {
        //     Some(value_t_or_exit!(
        //         matches,
        //         "block_cache",
        //         NonZeroUsize
        //     ))
        // } else {
        //     None
        // },
        use_md5_row_key_salt: matches.is_present("use_md5_row_key_salt"),
        hash_tx_full_row_keys: matches.is_present("hash_tx_full_row_keys"),
        enable_full_tx_cache: matches.is_present("enable_full_tx_cache"),
        disable_tx_fallback: matches.is_present("disable_tx_fallback"),
        cache_address: if matches.is_present("cache_address") {
            Some(value_t_or_exit!(
                matches,
                "cache_address",
                String
            ))
        } else {
            None
        },
        use_block_car_files: !matches.is_present("disable_block_car_files"),
        use_hbase_blocks_meta: matches.is_present("use_hbase_blocks_meta"),
        use_webhdfs: matches.is_present("use_webhdfs"),
        webhdfs_url: if matches.is_present("use_webhdfs") {
            Some(value_t_or_exit!(matches, "webhdfs_url", String))
        } else { None },
    });

    builder.rpc_port(rpc_port);

    builder.rpc_config(JsonRpcConfig {
        enable_rpc_transaction_history: true,
        rpc_hbase_config,
        // full_api,
        obsolete_v1_7_api: matches.is_present("obsolete_v1_7_rpc_api"),
        rpc_threads: value_t_or_exit!(matches, "rpc_threads", usize),
        rpc_niceness_adj: value_t_or_exit!(matches, "rpc_niceness_adj", i8),
        max_request_body_size: Some(value_t_or_exit!(
                matches,
                "rpc_max_request_body_size",
                usize
            )),
        max_get_blocks_range: if matches.is_present("max_get_blocks_range") {
            Some(value_t_or_exit!(
                matches,
                "max_get_blocks_range",
                u64
            ))
        } else {
            None
        },
        genesis_config_path: matches.value_of("genesis_config_path").map(|s| s.to_string()),
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