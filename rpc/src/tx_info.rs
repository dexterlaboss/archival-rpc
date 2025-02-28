// pub use crate::{
//     send_transaction_service_stats::SendTransactionServiceStats,
//     transaction_client::{CurrentLeaderInfo, LEADER_INFO_REFRESH_RATE_MS},
// };
use {
    // crate::{
    //     send_transaction_service_stats::SendTransactionServiceStatsReport,
    //     tpu_info::TpuInfo,
    //     transaction_client::{ConnectionCacheClient, TransactionClient},
    // },
    // crossbeam_channel::{Receiver, RecvTimeoutError},
    // itertools::Itertools,
    // log::*,
    // solana_client::connection_cache::ConnectionCache,
    // solana_runtime::{bank::Bank, bank_forks::BankForks},
    // solana_sdk::{
    //     hash::Hash, nonce_account, pubkey::Pubkey, saturating_add_assign, signature::Signature,
    // },
    solana_signature::{
        Signature,
    },
    solana_pubkey::{
        Pubkey,
    },
    solana_hash::{
        Hash,
    },
    std::{
        time::{Instant},
    },
};

pub struct TransactionInfo {
    pub signature: Signature,
    pub wire_transaction: Vec<u8>,
    pub last_valid_block_height: u64,
    pub durable_nonce_info: Option<(Pubkey, Hash)>,
    pub max_retries: Option<usize>,
    retries: usize,
    /// Last time the transaction was sent
    last_sent_time: Option<Instant>,
}

impl TransactionInfo {
    pub fn new(
        signature: Signature,
        wire_transaction: Vec<u8>,
        last_valid_block_height: u64,
        durable_nonce_info: Option<(Pubkey, Hash)>,
        max_retries: Option<usize>,
        last_sent_time: Option<Instant>,
    ) -> Self {
        Self {
            signature,
            wire_transaction,
            last_valid_block_height,
            durable_nonce_info,
            max_retries,
            retries: 0,
            last_sent_time,
        }
    }
}