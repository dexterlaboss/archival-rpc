//! Implementation defined RPC server errors
use {
    jsonrpc_core::{Error, ErrorCode},
    serde::{Deserialize, Serialize},
    solana_clock::Slot,
    solana_transaction_status_client_types::{
        EncodeError,
    },
    thiserror::Error,
};

pub const JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE: i64 = -32004;
pub const JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY: i64 = -32005;
pub const JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE: i64 = -32011;
pub const JSON_RPC_SCAN_ERROR: i64 = -32012;
pub const JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION: i64 = -32015;

pub const JSON_RPC_HBASE_ERROR: i64 = -32017;
pub const JSON_RPC_SERVER_ERROR_SLOT_NOT_EPOCH_BOUNDARY: i64 = -32018;
pub const JSON_RPC_SERVER_ERROR_EPOCH_REWARDS_PERIOD_ACTIVE: i64 = -32019;
pub const JSON_RPC_SERVER_ERROR_METHOD_NOT_SUPPORTED: i64 = -32020;

#[derive(Error, Debug)]
pub enum RpcCustomError {
    #[error("BlockNotAvailable")]
    BlockNotAvailable { slot: Slot },
    #[error("NodeUnhealthy")]
    NodeUnhealthy { num_slots_behind: Option<Slot> },
    #[error("LongTermStorageSlotSkipped")]
    LongTermStorageSlotSkipped { slot: Slot },
    #[error("TransactionHistoryNotAvailable")]
    TransactionHistoryNotAvailable,
    #[error("ScanError")]
    ScanError { message: String },
    #[error("UnsupportedTransactionVersion")]
    UnsupportedTransactionVersion(u8),
    #[error("HBaseError")]
    HBaseError { message: String },
    #[error("SlotNotEpochBoundary")]
    SlotNotEpochBoundary { slot: Slot },
    #[error("EpochRewardsPeriodActive")]
    EpochRewardsPeriodActive {
        slot: Slot,
        current_block_height: u64,
        rewards_complete_block_height: u64,
    },
    #[error("MethodNotSupported")]
    MethodNotSupported(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeUnhealthyErrorData {
    pub num_slots_behind: Option<Slot>,
}

impl From<EncodeError> for RpcCustomError {
    fn from(err: EncodeError) -> Self {
        match err {
            EncodeError::UnsupportedTransactionVersion(version) => {
                Self::UnsupportedTransactionVersion(version)
            }
        }
    }
}

impl From<RpcCustomError> for Error {
    fn from(e: RpcCustomError) -> Self {
        match e {
            RpcCustomError::BlockNotAvailable { slot } => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE),
                message: format!("Block not available for slot {slot}"),
                data: None,
            },
            RpcCustomError::NodeUnhealthy { num_slots_behind } => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY),
                message: if let Some(num_slots_behind) = num_slots_behind {
                    format!("Node is behind by {num_slots_behind} slots")
                } else {
                    "Node is unhealthy".to_string()
                },
                data: Some(serde_json::json!(NodeUnhealthyErrorData {
                    num_slots_behind
                })),
            },
            RpcCustomError::LongTermStorageSlotSkipped { slot } => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED),
                message: format!("Slot {slot} was skipped, or missing in long-term storage"),
                data: None,
            },
            RpcCustomError::TransactionHistoryNotAvailable => Self {
                code: ErrorCode::ServerError(
                    JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE,
                ),
                message: "Transaction history is not available from this node".to_string(),
                data: None,
            },
            RpcCustomError::ScanError { message } => Self {
                code: ErrorCode::ServerError(JSON_RPC_SCAN_ERROR),
                message,
                data: None,
            },
            RpcCustomError::UnsupportedTransactionVersion(version) => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION),
                message: format!(
                    "Transaction version ({version}) is not supported by the requesting client. \
                    Please try the request again with the following configuration parameter: \
                    \"maxSupportedTransactionVersion\": {version}"
                ),
                data: None,
            },
            RpcCustomError::HBaseError { message } => Self {
                code: ErrorCode::ServerError(JSON_RPC_HBASE_ERROR),
                message,
                data: None,
            },
            RpcCustomError::SlotNotEpochBoundary { slot } => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_SLOT_NOT_EPOCH_BOUNDARY),
                message: format!("Slot {slot} is not an epoch boundary"),
                data: None,
            },
            RpcCustomError::EpochRewardsPeriodActive {
                slot,
                current_block_height,
                rewards_complete_block_height,
            } => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_EPOCH_REWARDS_PERIOD_ACTIVE),
                message: format!(
                    "Epoch rewards period is active. Slot: {slot}, current block height: {current_block_height}, rewards will be complete at block height: {rewards_complete_block_height}"
                ),
                data: None,
            },
            RpcCustomError::MethodNotSupported(message) => Self {
                code: ErrorCode::ServerError(JSON_RPC_SERVER_ERROR_METHOD_NOT_SUPPORTED),
                message,
                data: None,
            },
        }
    }
}
