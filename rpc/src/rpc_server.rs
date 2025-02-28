
use {
    crate::{
        rpc_network_node::*,
        request_processor::JsonRpcConfig,
        rpc_service::JsonRpcService,
    },
    solana_metrics::Metrics,
    solana_validator_exit::{
        Exit,
    },
    std::{
        net::SocketAddr,
        path::{
            Path,
        },
        sync::{
            atomic::{
                AtomicBool,
                Ordering
            },
            Arc, RwLock,
        },
    },
};

pub struct RpcServiceConfig {
    pub rpc_config: JsonRpcConfig,
    pub rpc_addr: Option<SocketAddr>,
    pub enforce_ulimit_nofile: bool,
    pub rpc_service_exit: Arc<RwLock<Exit>>,
}

impl Default for RpcServiceConfig {
    fn default() -> Self {
        Self {
            rpc_config: JsonRpcConfig::default(),
            rpc_addr: None,
            enforce_ulimit_nofile: true,

            rpc_service_exit: Arc::new(RwLock::new(Exit::default())),
        }
    }
}

impl RpcServiceConfig {
    pub fn default_for_storage_rpc() -> Self {
        Self {
            enforce_ulimit_nofile: false,
            rpc_config: JsonRpcConfig::default_for_storage_rpc(),
            ..Self::default()
        }
    }
}


pub struct RpcService {
    rpc_service_exit: Arc<RwLock<Exit>>,
    json_rpc_service: Option<JsonRpcService>,
}

impl RpcService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node: RpcNetworkNode,
        log_path: &Path,
        config: &RpcServiceConfig,
        // should_check_duplicate_instance: bool,
    ) -> Result<Self, String> {
        if rayon::ThreadPoolBuilder::new()
            .thread_name(|ix| format!("solRayonGlob{ix:02}"))
            .build_global()
            .is_err()
        {
            warn!("Rayon global thread pool already initialized");
        }

        if !log_path.is_dir() {
            return Err(format!(
                "log directory does not exist or is not accessible: {log_path:?}"
            ));
        }

        let exit = Arc::new(AtomicBool::new(false));
        {
            let exit = exit.clone();
            config
                .rpc_service_exit
                .write()
                .unwrap()
                .register_exit(Box::new(move || exit.store(true, Ordering::Relaxed)));
        }

        Self::print_node_info(&node);

        let metrics = Arc::new(Metrics::new());

        let json_rpc_service= if let Some(rpc_addr) = config.rpc_addr {
            let json_rpc_service = JsonRpcService::new(
                rpc_addr,
                config.rpc_config.clone(),
                log_path,
                config.rpc_service_exit.clone(),
                metrics,
            )?;

            Some(json_rpc_service)
        } else {
            None
        };

        // datapoint_info!(
        //     "launcher-new",
        //     ("version", solana_version::version!(), String)
        // );

        Ok(Self {
            json_rpc_service,
            rpc_service_exit: config.rpc_service_exit.clone(),
        })
    }

    pub fn exit(&mut self) {
        self.rpc_service_exit.write().unwrap().exit();
    }

    pub fn close(mut self) {
        self.exit();
        self.join();
    }

    fn print_node_info(node: &RpcNetworkNode) {
        info!("{:?}", node.info);
    }

    pub fn join(self) {
        if let Some(json_rpc_service) = self.json_rpc_service {
            json_rpc_service.join().expect("rpc_service");
        }
    }
}