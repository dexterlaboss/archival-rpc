
use {
    crate::{
        rpc_network_info::RpcNetworkInfo,
    },
    std::{
        net::{
            SocketAddr,
            IpAddr,
        },
    },
};

#[derive(Debug)]
pub struct RpcNetworkNode {
    pub info: RpcNetworkInfo,
}

impl RpcNetworkNode {
    pub fn new_single_bind(
        rpc_port: u16,
        bind_ip_addr: IpAddr,
    ) -> Self {
        let info = RpcNetworkInfo {
            rpc: SocketAddr::new(bind_ip_addr, rpc_port),
        };

        RpcNetworkNode {
            info,
        }
    }
}