
use {
    crate::{
        rpc_network_info::RpcNetworkInfo,
    },
    solana_net_utils::{
        find_available_port_in_range, PortRange,
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
        port_range: PortRange,
        bind_ip_addr: IpAddr,
    ) -> Self {
        let rpc_port = find_available_port_in_range(bind_ip_addr, port_range).unwrap();

        let info = RpcNetworkInfo {
            rpc: SocketAddr::new(bind_ip_addr, rpc_port),
        };

        RpcNetworkNode {
            info,
        }
    }
}