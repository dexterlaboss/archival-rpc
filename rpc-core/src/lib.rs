#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(min_specialization))]
#![allow(clippy::integer_arithmetic)]
#![recursion_limit = "2048"]

use {
    crate::{
        rpc_service::*,
        rpc_network_node::*,
    },
    log::*,
    solana_rpc::{
        storage_rpc::JsonRpcConfig,
    },
    solana_sdk::{
        exit::Exit,
        rpc_port,
    },
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        sync::{Arc, RwLock},
        process::exit,
        env,
    },
};

pub mod stats_reporter_service;

pub mod rpc_service;

#[macro_use]
extern crate log;

pub mod rpc_network_node;

#[macro_use]
pub mod rpc_network_info;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate matches;


#[derive(Debug)]
pub struct RpcNodeConfig {
    bind_ip_addr: IpAddr,
}

impl Default for RpcNodeConfig {
    fn default() -> Self {
        let bind_ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

        Self {
            bind_ip_addr,
        }
    }
}

pub struct RpcNodeBuilder {
    log_path: Option<PathBuf>,
    rpc_config: JsonRpcConfig,
    rpc_port: Option<u16>,
    node_config: RpcNodeConfig,
    pub rpc_service_exit: Arc<RwLock<Exit>>,
}

impl Default for RpcNodeBuilder {
    fn default() -> Self {
        Self {
            log_path: Option::<PathBuf>::default(),
            rpc_config: JsonRpcConfig::default_for_storage_rpc(),
            rpc_port: Option::<u16>::default(),
            node_config: RpcNodeConfig::default(),
            rpc_service_exit: Arc::<RwLock<Exit>>::default(),
        }
    }
}

impl RpcNodeBuilder {
    pub fn log_path<P: Into<PathBuf>>(&mut self, log_path: P) -> &mut Self {
        self.log_path = Some(log_path.into());
        self
    }

    /// Check if a given RpcNode ledger has already been initialized
    pub fn ledger_exists(log_path: &Path) -> bool {
        log_path.exists()
    }

    pub fn rpc_config(&mut self, rpc_config: JsonRpcConfig) -> &mut Self {
        self.rpc_config = rpc_config;
        self
    }

    pub fn rpc_port(&mut self, rpc_port: u16) -> &mut Self {
        self.rpc_port = Some(rpc_port);
        self
    }

    pub fn bind_ip_addr(&mut self, bind_ip_addr: IpAddr) -> &mut Self {
        self.node_config.bind_ip_addr = bind_ip_addr;
        self
    }

    pub fn start(
        &self,
    ) -> Result<RpcNode, Box<dyn std::error::Error>> {
        RpcNode::start(self).map(|rpc_node| {
            rpc_node
        })
    }
}


pub struct RpcNode {
    rpc_url: String,
    rpc_service: Option<RpcService>,
}

impl RpcNode {
    /// Initialize the log directory
    fn init_log_dir(
        config: &RpcNodeBuilder,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let log_path = match &config.log_path {
            None => match env::current_dir() {
                Ok(current_dir) => current_dir,
                Err(e) => {
                    println!("Error getting current working directory: {:?}", e);
                    exit(1);
                }
            },
            Some(log_path) => {
                log_path.to_path_buf()
            }
        };

        Ok(log_path)
    }

    fn start(
        config: &RpcNodeBuilder,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let log_path = RpcNode::init_log_dir(config)?;

        info!("Starting rpc server at {:?}", config.node_config.bind_ip_addr);

        let mut node = RpcNetworkNode::new_single_bind(
            rpc_port::DEFAULT_RPC_PORT,
            config.node_config.bind_ip_addr,
        );
        if let Some(rpc) = config.rpc_port {
            node.info.rpc = SocketAddr::new(config.node_config.bind_ip_addr, rpc);
        }

        let rpc_url = format!("http://{}", node.info.rpc);

        let rpc_service_config = RpcServiceConfig {
            rpc_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), node.info.rpc.port())),
            rpc_config: config.rpc_config.clone(),
            rpc_service_exit: config.rpc_service_exit.clone(),
            ..RpcServiceConfig::default_for_storage_rpc()
        };

        let rpc_service = Some(RpcService::new(
            node,
            &log_path,
            &rpc_service_config,
        )?);

        let rpc_node = RpcNode {
            rpc_url,
            rpc_service,
        };
        Ok(rpc_node)
    }

    /// Return the launcher's JSON RPC URL
    pub fn rpc_url(&self) -> String {
        self.rpc_url.clone()
    }

    pub fn join(mut self) {
        if let Some(rpc_service) = self.rpc_service.take() {
            rpc_service.join();
        }
    }
}