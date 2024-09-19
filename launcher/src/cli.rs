use std::num::NonZeroUsize;
use {
    clap::{
        App,
        Arg,
        ArgMatches,
    },
    log::warn,
    solana_clap_utils::{
        input_validators::{
            is_parsable,
            is_niceness_adjustment_valid,
        },
    },
    solana_net_utils::{
        MINIMUM_VALIDATOR_PORT_RANGE_WIDTH,
    },
    solana_sdk::{
        quic::QUIC_PORT_OFFSET,
        rpc_port,
    },
};
use solana_rpc::storage_rpc::MAX_REQUEST_BODY_SIZE;

/// Deprecated argument description should be moved into the [`deprecated_arguments()`] function,
/// expressed as an instance of this type.
struct DeprecatedArg {
    /// Deprecated argument description, moved here as is.
    ///
    /// `hidden` property will be modified by [`deprecated_arguments()`] to only show this argument
    /// if [`hidden_unless_forced()`] says they should be displayed.
    arg: Arg<'static, 'static>,

    /// If simply replaced by a different argument, this is the name of the replacement.
    ///
    /// Content should be an argument name, as presented to users.
    replaced_by: Option<&'static str>,

    /// An explanation to be shown to the user if they still use this argument.
    ///
    /// Content should be a complete sentence or several, ending with a period.
    usage_warning: Option<&'static str>,
}

fn deprecated_arguments() -> Vec<DeprecatedArg> {
    let mut res = vec![];

    // This macro reduces indentation and removes some noise from the argument declaration list.
    macro_rules! add_arg {
        (
            $arg:expr
            $( , replaced_by: $replaced_by:expr )?
            $( , usage_warning: $usage_warning:expr )?
            $(,)?
        ) => {
            let replaced_by = add_arg!(@into-option $( $replaced_by )?);
            let usage_warning = add_arg!(@into-option $( $usage_warning )?);
            res.push(DeprecatedArg {
                arg: $arg,
                replaced_by,
                usage_warning,
            });
        };

        (@into-option) => { None };
        (@into-option $v:expr) => { Some($v) };
    }

    add_arg!(Arg::with_name("minimal_rpc_api")
        .long("minimal-rpc-api")
        .takes_value(false)
        .help("Only expose the RPC methods required to serve snapshots to other nodes"));

    res
}

pub fn warn_for_deprecated_arguments(matches: &ArgMatches) {
    for DeprecatedArg {
        arg,
        replaced_by,
        usage_warning,
    } in deprecated_arguments().into_iter()
    {
        if matches.is_present(arg.b.name) {
            let mut msg = format!("--{} is deprecated", arg.b.name.replace('_', "-"));
            if let Some(replaced_by) = replaced_by {
                msg.push_str(&format!(", please use --{replaced_by}"));
            }
            msg.push('.');
            if let Some(usage_warning) = usage_warning {
                msg.push_str(&format!("  {usage_warning}"));
                if !msg.ends_with('.') {
                    msg.push('.');
                }
            }
            warn!("{}", msg);
        }
    }
}

pub fn port_validator(port: String) -> Result<(), String> {
    port.parse::<u16>()
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

pub fn port_range_validator(port_range: String) -> Result<(), String> {
    if let Some((start, end)) = solana_net_utils::parse_port_range(&port_range) {
        if end - start < MINIMUM_VALIDATOR_PORT_RANGE_WIDTH {
            Err(format!(
                "Port range is too small.  Try --dynamic-port-range {}-{}",
                start,
                start + MINIMUM_VALIDATOR_PORT_RANGE_WIDTH
            ))
        } else if end.checked_add(QUIC_PORT_OFFSET).is_none() {
            Err("Invalid dynamic_port_range.".to_string())
        } else {
            Ok(())
        }
    } else {
        Err("Invalid port range".to_string())
    }
}

pub fn storage_rpc_service<'a>(version: &'a str, default_args: &'a DefaultStorageRpcArgs) -> App<'a, 'a> {
    return App::new("solana-storage-rpc")
        .about("Solana Storage RPC Service")
        .version(version)
        .arg(
            Arg::with_name("log_path")
                .short("l")
                .long("log-path")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                // .launcher(|value| {
                //     value
                //         .parse::<PathBuf>()
                //         .map_err(|err| format!("error parsing '{value}': {err}"))
                //         .and_then(|path| {
                //             if path.exists() && path.is_dir() {
                //                 Ok(())
                //             } else {
                //                 Err(format!("path does not exist or is not a directory: {value}"))
                //             }
                //         })
                // })
                .default_value("log")
                .help("Use DIR as log location"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .takes_value(false)
                .conflicts_with("log")
                .help("Quiet mode: suppress normal output"),
        )
        .arg(
            Arg::with_name("log")
                .long("log")
                .takes_value(false)
                .conflicts_with("quiet")
                .help("Log mode: stream the launcher log"),
        )
        .arg(
            Arg::with_name("rpc_port")
                .long("rpc-port")
                .value_name("PORT")
                .takes_value(true)
                .default_value(&default_args.rpc_port)
                .validator(port_validator)
                .help("Port for the RPC service"),
        )
        .arg(
            Arg::with_name("enable_rpc_transaction_history")
                .long("enable-rpc-transaction-history")
                .takes_value(false)
                .help("Enable historical transaction info over JSON RPC, \
                       including the 'getConfirmedBlock' API."),
        )
        .arg(
            Arg::with_name("enable_rpc_hbase_ledger_storage")
                .long("enable-rpc-hbase-ledger-storage")
                .takes_value(false)
                .hidden(true)
                .help("Fetch historical transaction info from a HBase instance"),
        )
        .arg(
            Arg::with_name("rpc_hbase_address")
                .long("rpc-hbase-address")
                .value_name("ADDRESS")
                .takes_value(true)
                .hidden(true)
                .default_value("127.0.0.1:9090")
                .help("Address of HBase instance to use"),
        )
        .arg(
            Arg::with_name("rpc_hbase_timeout")
                .long("rpc-hbase-timeout")
                .value_name("SECONDS")
                .validator(is_parsable::<u64>)
                .takes_value(true)
                .default_value(&default_args.rpc_hbase_timeout)
                .help("Number of seconds before timing out RPC requests backed by HBase"),
        )
        .arg(
            Arg::with_name("enable_rpc_bigtable_ledger_storage")
                .long("enable-rpc-bigtable-ledger-storage")
                .takes_value(false)
                .hidden(true)
                .help("Fetch historical transaction info from a BigTable instance"),
        )
        .arg(
            Arg::with_name("rpc_bigtable_instance_name")
                .long("rpc-bigtable-instance-name")
                .takes_value(true)
                .value_name("INSTANCE_NAME")
                .default_value(&default_args.rpc_bigtable_instance_name)
                .help("Name of the Bigtable instance to use")
        )
        .arg(
            Arg::with_name("rpc_bigtable_app_profile_id")
                .long("rpc-bigtable-app-profile-id")
                .takes_value(true)
                .value_name("APP_PROFILE_ID")
                .default_value(&default_args.rpc_bigtable_app_profile_id)
                .help("Bigtable application profile id to use for requests")
        )
        .arg(
            Arg::with_name("rpc_bigtable_timeout")
                .long("rpc-bigtable-timeout")
                .value_name("SECONDS")
                .validator(is_parsable::<u64>)
                .takes_value(true)
                .default_value(&default_args.rpc_bigtable_timeout)
                .help("Number of seconds before timing out RPC requests backed by BigTable"),
        )
        .arg(
            Arg::with_name("bind_address")
                .long("bind-address")
                .value_name("HOST")
                .takes_value(true)
                .validator(solana_net_utils::is_host)
                .default_value("0.0.0.0")
                .help("IP address to bind the rpc service [default: 0.0.0.0]"),
        )
        .arg(
            Arg::with_name("rpc_threads")
                .long("rpc-threads")
                .value_name("NUMBER")
                .validator(is_parsable::<usize>)
                .takes_value(true)
                .default_value(&default_args.rpc_threads)
                .help("Number of threads to use for servicing RPC requests"),
        )
        .arg(
            Arg::with_name("rpc_niceness_adj")
                .long("rpc-niceness-adjustment")
                .value_name("ADJUSTMENT")
                .takes_value(true)
                .validator(is_niceness_adjustment_valid)
                .default_value(&default_args.rpc_niceness_adjustment)
                .help("Add this value to niceness of RPC threads. Negative value \
                      increases priority, positive value decreases priority.")
        )
        .arg(
            Arg::with_name("rpc_max_request_body_size")
                .long("rpc-max-request-body-size")
                .value_name("BYTES")
                .takes_value(true)
                .validator(is_parsable::<usize>)
                .default_value(&default_args.rpc_max_request_body_size)
                .help("The maximum request body size accepted by rpc service"),
        )
        .arg(
            Arg::with_name("block_cache")
                .long("block-cache")
                .value_name("SIZE")
                .takes_value(true)
                .validator(is_parsable::<NonZeroUsize>)
                .help("HBase storage block cache size"),
        )
        .arg(
            Arg::with_name("log_messages_bytes_limit")
                .long("log-messages-bytes-limit")
                .value_name("BYTES")
                .validator(is_parsable::<usize>)
                .takes_value(true)
                .help("Maximum number of bytes written to the program log before truncation")
        )
        .arg(
            Arg::with_name("use_md5_row_key_salt")
                .long("use-md5-row-key-salt")
                .takes_value(false)
                .help("Enable md5 row key salt for block HBase keys."),
        )
        .arg(
            Arg::with_name("enable_full_tx_cache")
                .long("enable-full-tx-cache")
                .takes_value(false)
                .help("Enable block transaction cache."),
        )
        .arg(
            Arg::with_name("cache_address")
                .long("cache-address")
                .value_name("ADDRESS")
                .takes_value(true)
                .help("Block transaction cache server address"),
        )
    ;
}

pub struct DefaultStorageRpcArgs {
    pub rpc_port: String,
    pub rpc_hbase_timeout: String,
    pub rpc_threads: String,
    pub rpc_niceness_adjustment: String,
    pub rpc_max_request_body_size: String,
    pub rpc_bigtable_timeout: String,
    pub rpc_bigtable_instance_name: String,
    pub rpc_bigtable_app_profile_id: String,
    pub block_cache: Option<NonZeroUsize>,
}

impl DefaultStorageRpcArgs {
    pub fn new() -> Self {
        DefaultStorageRpcArgs {
            rpc_port: rpc_port::DEFAULT_RPC_PORT.to_string(),
            rpc_hbase_timeout: "5".to_string(),
            rpc_threads: num_cpus::get().to_string(),
            rpc_niceness_adjustment: "0".to_string(),
            rpc_max_request_body_size: MAX_REQUEST_BODY_SIZE.to_string(),
            rpc_bigtable_timeout: "30".to_string(),
            rpc_bigtable_instance_name: solana_storage_bigtable::DEFAULT_INSTANCE_NAME.to_string(),
            rpc_bigtable_app_profile_id: solana_storage_bigtable::DEFAULT_APP_PROFILE_ID
                .to_string(),
            block_cache: None,
        }
    }
}

impl Default for DefaultStorageRpcArgs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn make_sure_deprecated_arguments_are_sorted_alphabetically() {
        let deprecated = deprecated_arguments();

        for i in 0..deprecated.len().saturating_sub(1) {
            let curr_name = deprecated[i].arg.b.name;
            let next_name = deprecated[i + 1].arg.b.name;

            assert!(
                curr_name != next_name,
                "Arguments in `deprecated_arguments()` should be distinct.\n\
                 Arguments {} and {} use the same name: {}",
                i,
                i + 1,
                curr_name,
            );

            assert!(
                curr_name < next_name,
                "To generate better diffs and for readability purposes, `deprecated_arguments()` \
                 should list arguments in alphabetical order.\n\
                 Arguments {} and {} are not.\n\
                 Argument {} name: {}\n\
                 Argument {} name: {}",
                i,
                i + 1,
                i,
                curr_name,
                i + 1,
                next_name,
            );
        }
    }
}
