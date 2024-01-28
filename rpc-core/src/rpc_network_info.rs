use {
    std::net::{SocketAddr, IpAddr},
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Duplicate IP address: {0}")]
    DuplicateIpAddr(IpAddr),
    #[error("Duplicate socket: {0}")]
    DuplicateSocket(/*key:*/ u8),
    #[error("Invalid IP address index: {index}, num addrs: {num_addrs}")]
    InvalidIpAddrIndex { index: u8, num_addrs: usize },
    #[error("Invalid port: {0}")]
    InvalidPort(/*port:*/ u16),
    #[error("Invalid {0:?} (udp) and {1:?} (quic) sockets")]
    InvalidQuicSocket(Option<SocketAddr>, Option<SocketAddr>),
    #[error("IP addresses saturated")]
    IpAddrsSaturated,
    #[error("Multicast IP address: {0}")]
    MulticastIpAddr(IpAddr),
    #[error("Port offsets overflow")]
    PortOffsetsOverflow,
    #[error("Socket not found: {0}")]
    SocketNotFound(/*key:*/ u8),
    #[error("Unspecified IP address: {0}")]
    UnspecifiedIpAddr(IpAddr),
    #[error("Unused IP address: {0}")]
    UnusedIpAddr(IpAddr),
}

/// Structure representing a node on the network
#[derive(
Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, AbiExample, Deserialize, Serialize,
)]
pub struct RpcNetworkInfo {
    /// address to which to send JSON-RPC requests
    pub rpc: SocketAddr,
}

macro_rules! get_socket {
    ($name:ident) => {
        pub fn $name(&self) -> Result<SocketAddr, Error> {
            let socket = &self.$name;
            sanitize_socket(socket)?;
            Ok(socket).copied()
        }
    };
}

macro_rules! set_socket {
    ($name:ident, $key:ident) => {
        pub fn $name<T>(&mut self, socket: T) -> Result<(), Error>
        where
            SocketAddr: From<T>,
        {
            let socket = SocketAddr::from(socket);
            sanitize_socket(&socket)?;
            self.$key = socket;
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! socketaddr {
    ($ip:expr, $port:expr) => {
        std::net::SocketAddr::from((std::net::Ipv4Addr::from($ip), $port))
    };
    ($str:expr) => {{
        $str.parse::<std::net::SocketAddr>().unwrap()
    }};
}

#[macro_export]
macro_rules! socketaddr_any {
    () => {
        socketaddr!(std::net::Ipv4Addr::UNSPECIFIED, 0)
    };
}

impl Default for RpcNetworkInfo {
    fn default() -> Self {
        RpcNetworkInfo {
            rpc: socketaddr_any!(),
        }
    }
}

impl RpcNetworkInfo {
    get_socket!(rpc);
    set_socket!(set_rpc, rpc);
}

pub(crate) fn sanitize_socket(socket: &SocketAddr) -> Result<(), Error> {
    if socket.port() == 0u16 {
        return Err(Error::InvalidPort(socket.port()));
    }
    let addr = socket.ip();
    if addr.is_unspecified() {
        return Err(Error::UnspecifiedIpAddr(addr));
    }
    if addr.is_multicast() {
        return Err(Error::MulticastIpAddr(addr));
    }
    Ok(())
}
