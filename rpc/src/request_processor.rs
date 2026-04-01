//! The `storage_rpc` module implements the Solana storage RPC interface.

use {
    crate::{
        custom_error::RpcCustomError,
    },
    futures::future::OptionFuture,
    serde_json,
    jsonrpc_core::{
        Error, Metadata, Result
    },
    solana_storage_adapter::LedgerStorageAdapter,
    solana_rpc_client_api::{
        config::*,
        request::{
            MAX_GET_CONFIRMED_BLOCKS_RANGE,
            // MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT,
        },
        response::{Response as RpcResponse, *},
    },
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_clock::{
        Slot,
        UnixTimestamp,
    },
    solana_validator_exit::{
        Exit,
    },
    solana_commitment_config::{
        CommitmentConfig,
    },
    solana_pubkey::{
        Pubkey,
    },
    solana_signature::{
        Signature,
    },
    solana_transaction_status::{
        BlockEncodingOptions,
        ConfirmedBlock,
        ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta,
        TransactionWithStatusMeta,
    },
    solana_transaction_status_client_types::{
        EncodedConfirmedTransactionWithStatusMeta,
        TransactionConfirmationStatus,
        TransactionStatus,
        UiConfirmedBlock,
        UiTransactionEncoding,
        Reward,
    },
    solana_rpc_client_api::{
        config::RpcEpochConfig,
        response::RpcInflationReward,
    },
    solana_epoch_schedule::EpochSchedule,
    solana_genesis_config::GenesisConfig,
    solana_hash::Hash,
    solana_reward_info::RewardType,
    solana_epoch_rewards_hasher::EpochRewardsHasher,
    std::{
        collections::{
            HashSet,
            HashMap,
        },
        str::FromStr,
        sync::{
            Arc,
            RwLock,
            atomic::{
                AtomicBool,
                Ordering
            },
        },
        time::{Duration, Instant},
    },
    tokio::runtime::Runtime,
};

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10); // 50kB
pub const MAX_GENESIS_ARCHIVE_UNPACKED_SIZE: u64 = 10 * 1024 * 1024; // 10MB

pub const MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT: usize = 10_000;
pub const MAX_GET_TRANSACTIONS_FOR_ADDRESS_LIMIT: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum TransactionDetailsMode {
    Full,
    #[default]
    Signatures,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum TransactionStatusFilter {
    #[default]
    Any,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Desc,
    Asc,
}

/// Comparison-operator range for blockTime (Unix timestamps).
/// Matches Helius filter format: filters.blockTime.gte / .gt / .lte / .lt
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RpcBlockTimeRange {
    pub gte: Option<i64>,
    pub gt: Option<i64>,
    pub lte: Option<i64>,
    pub lt: Option<i64>,
}

/// Comparison-operator range for slot numbers.
/// Matches Helius filter format: filters.slot.gte / .gt / .lte / .lt
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RpcSlotRange {
    pub gte: Option<Slot>,
    pub gt: Option<Slot>,
    pub lte: Option<Slot>,
    pub lt: Option<Slot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum TokenAccountsFilter {
    /// No token account filtering (include everything) — default
    #[default]
    None,
    /// Only transactions where at least one token balance changed
    BalanceChanged,
    /// All transactions that involve any token account
    All,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransactionFilters {
    pub status: Option<TransactionStatusFilter>,
    pub block_time: Option<RpcBlockTimeRange>,
    pub slot: Option<RpcSlotRange>,
    pub token_accounts: Option<TokenAccountsFilter>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionsForAddressResponse {
    pub data: Vec<serde_json::Value>,
    pub pagination_token: Option<String>,
}


/// Returns true if the transaction has any token account involvement.
fn tx_has_token_accounts(tx: &ConfirmedTransactionWithStatusMeta) -> bool {
    if let TransactionWithStatusMeta::Complete(ref vtx) = tx.tx_with_meta {
        vtx.meta.pre_token_balances.as_ref().map_or(false, |v| !v.is_empty())
            || vtx.meta.post_token_balances.as_ref().map_or(false, |v| !v.is_empty())
    } else {
        false
    }
}

/// Returns true if at least one token balance changed between pre and post.
fn tx_has_token_balance_changed(tx: &ConfirmedTransactionWithStatusMeta) -> bool {
    if let TransactionWithStatusMeta::Complete(ref vtx) = tx.tx_with_meta {
        let pre = vtx.meta.pre_token_balances.as_deref().unwrap_or(&[]);
        let post = vtx.meta.post_token_balances.as_deref().unwrap_or(&[]);
        if pre.len() != post.len() {
            return true;
        }
        pre.iter().zip(post.iter()).any(|(p, q)| {
            p.account_index != q.account_index || p.ui_token_amount.amount != q.ui_token_amount.amount
        })
    } else {
        false
    }
}

type Rewards = Vec<Reward>;

pub(crate) fn verify_and_parse_signatures_for_address_params(
    address: String,
    before: Option<String>,
    until: Option<String>,
    limit: Option<usize>,
) -> Result<(Pubkey, Option<Signature>, Option<Signature>, usize)> {
    let address = verify_pubkey(&address)?;
    let before = before
        .map(|ref before| verify_signature(before))
        .transpose()?;
    let until = until.map(|ref until| verify_signature(until)).transpose()?;
    let limit = limit.unwrap_or(MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT);

    if limit == 0 || limit > MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT {
        return Err(Error::invalid_params(format!(
            "Invalid limit; max {MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT}"
        )));
    }
    Ok((address, before, until, limit))
}

pub(crate) fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<()> {
    if !commitment.is_at_least_confirmed() {
        return Err(Error::invalid_params(
            "Method does not support commitment below `confirmed`",
        ));
    }
    Ok(())
}

pub(crate) fn verify_signature(input: &str) -> Result<Signature> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {e:?}")))
}

pub fn verify_pubkey(input: &str) -> Result<Pubkey> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {e:?}")))
}

fn new_response<T>(context: Option<RpcResponseContext>, value: T) -> RpcResponse<T> {
    RpcResponse {
        // TODO: Maybe add actual slot to the contect?
        context: context.unwrap_or(RpcResponseContext::new(
            /*bank.slot()*/ Slot::default(),
        )),
        value,
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockCheck {
    pub exists: bool,
}

#[derive(Debug, Default, Clone)]
pub struct JsonRpcConfig {
    pub enable_rpc_transaction_history: bool,
    pub rpc_hbase_config: Option<RpcHBaseConfig>,
    pub rpc_node_config: UpstreamRpcConfig,
    pub rpc_threads: usize,
    pub rpc_blocking_threads: usize,
    pub rpc_niceness_adj: i8,
    pub full_api: bool,
    pub obsolete_v1_7_api: bool,
    pub max_request_body_size: Option<usize>,
    pub max_get_blocks_range: Option<u64>,
    pub genesis_config_path: Option<String>,
}

impl JsonRpcConfig {
    pub fn default_for_storage_rpc() -> Self {
        Self {
            full_api: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct RpcHBaseConfig {
    pub enable_hbase_ledger_upload: bool,
    pub hbase_address: String,
    pub namespace: Option<String>,
    pub thrift_connection_pool_size: u32,
    pub hdfs_url: String,
    pub hdfs_path: String,
    pub fallback_hbase_address: Option<String>,
    pub hbase_address_tx: Option<String>,
    pub timeout: Option<Duration>,
    // pub block_cache: Option<NonZeroUsize>,
    pub use_md5_row_key_salt: bool,
    pub hash_tx_full_row_keys: bool,
    pub enable_full_tx_cache: bool,
    pub disable_tx_fallback: bool,
    pub cache_address: Option<String>,
    pub use_block_car_files: bool,
    pub use_hbase_blocks_meta: bool,
    pub use_webhdfs: bool,
    pub webhdfs_url: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct UpstreamRpcConfig {
    pub rpc_node_address: Option<String>,
}

impl Default for RpcHBaseConfig {
    fn default() -> Self {
        let hbase_address = solana_storage_hbase::DEFAULT_ADDRESS.to_string();
        let hdfs_url = solana_storage_hbase::DEFAULT_HDFS_URL.to_string();
        let hdfs_path = solana_storage_hbase::DEFAULT_HDFS_PATH.to_string();
        Self {
            enable_hbase_ledger_upload: false,
            hbase_address,
            namespace: None,
            thrift_connection_pool_size: 100,
            hdfs_url,
            hdfs_path,
            fallback_hbase_address: None,
            hbase_address_tx: None,
            timeout: None,
            // block_cache: None,
            use_md5_row_key_salt: false,
            hash_tx_full_row_keys: false,
            enable_full_tx_cache: false,
            disable_tx_fallback: false,
            cache_address: None,
            use_block_car_files: true,
            use_hbase_blocks_meta: false,
            use_webhdfs: false,
            webhdfs_url: None,
        }
    }
}

// #[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    runtime: Arc<Runtime>,
    config: JsonRpcConfig,
    #[allow(dead_code)]
    rpc_service_exit: Arc<RwLock<Exit>>,
    hbase_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
    fallback_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
    tx_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
    genesis_config: Option<GenesisConfig>,
    rpc_node_client: Option<Arc<RpcClient>>,
}

impl Metadata for JsonRpcRequestProcessor {}

impl Clone for JsonRpcRequestProcessor {
    fn clone(&self) -> Self {
        JsonRpcRequestProcessor {
            runtime: self.runtime.clone(),
            config: self.config.clone(),
            rpc_service_exit: Arc::clone(&self.rpc_service_exit),
            hbase_ledger_storage: self.hbase_ledger_storage.as_ref().map(|storage| storage.clone_box()),
            fallback_ledger_storage: self.fallback_ledger_storage.as_ref().map(|storage| storage.clone_box()),
            tx_ledger_storage: self.tx_ledger_storage.as_ref().map(|storage| storage.clone_box()),
            genesis_config: self.genesis_config.clone(),
            rpc_node_client: self.rpc_node_client.clone(),
        }
    }
}

impl JsonRpcRequestProcessor {
    fn genesis_creation_time(&self) -> UnixTimestamp {
        // TODO: Get genesis creation time from launcher?
        0
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: Arc<Runtime>,
        config: JsonRpcConfig,
        rpc_service_exit: Arc<RwLock<Exit>>,
        hbase_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
        fallback_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
        tx_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
    ) -> Self {
    // ) -> (Self, Receiver<TransactionInfo>) {
    //     let (_sender, receiver) = unbounded();
        // Load genesis config if path is provided
        let genesis_config = if let Some(ref path) = config.genesis_config_path {
            match crate::genesis_unpack::open_genesis_config(
                &std::path::Path::new(path),
                MAX_GENESIS_ARCHIVE_UNPACKED_SIZE,
            ) {
                Ok(config) => {
                    info!("Successfully loaded genesis config from: {}", path);
                    Some(config)
                }
                Err(err) => {
                    warn!("Failed to load genesis config from {}: {}. getInflationReward method will be unavailable.", path, err);
                    None
                }
            }
        } else {
            info!("No genesis config path provided. getInflationReward method will be unavailable.");
            None
        };

        let rpc_node_client = config.rpc_node_config.rpc_node_address.as_ref().map(|addr| {
            Arc::new(RpcClient::new(addr.clone()))
        });

        Self {
            runtime,
            config,
            rpc_service_exit,
            hbase_ledger_storage,
            fallback_ledger_storage,
            tx_ledger_storage,
            genesis_config,
            rpc_node_client,
        }
        // (
        //     Self {
        //         config,
        //         rpc_service_exit,
        //         hbase_ledger_storage,
        //         fallback_ledger_storage,
        //     },
        //     receiver,
        // )
    }

    fn check_hbase_result<T>(
        &self,
        result: &std::result::Result<T, solana_storage_adapter::Error>,
    ) -> Result<()> {
        debug!("Checking hbase block");
        if let Err(e) = result {
            debug!("Block error: {}", e);
        }
        if let Err(solana_storage_adapter::Error::BlockNotFound(slot)) = result {
            return Err(RpcCustomError::LongTermStorageSlotSkipped { slot: *slot }.into());
        }
        debug!("Block check successful");
        Ok(())
    }

    pub async fn get_block(
        &self,
        slot: Slot,
        config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
    ) -> Result<Option<UiConfirmedBlock>> {
        if self.config.enable_rpc_transaction_history {
            let config = config
                .map(|config| config.convert_to_current())
                .unwrap_or_default();
            let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json).into();
            let encoding_options = BlockEncodingOptions {
                transaction_details: config.transaction_details.unwrap_or_default(),
                show_rewards: config.rewards.unwrap_or(true),
                max_supported_transaction_version: config.max_supported_transaction_version,
            };
            let commitment = config.commitment.unwrap_or_default();
            check_is_at_least_confirmed(commitment)?;

            let encode_block = |confirmed_block: ConfirmedBlock| async move {
                debug!("Encoding block");
                let mut encoded_block = self.runtime.spawn_blocking(move || {
                confirmed_block
                    .encode_with_options(encoding, encoding_options)
                    .map_err(|e| {
                        debug!("Encoding error: {:?}", e);
                        RpcCustomError::from(e)
                    })
                })
                .await
                .expect("Failed to spawn blocking task")?;
                if slot == 0 {
                    encoded_block.block_time = Some(self.genesis_creation_time());
                    encoded_block.block_height = Some(0);
                }
                debug!("Encoded block");

                Ok::<UiConfirmedBlock, Error>(encoded_block)
            };
            if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                let hbase_result = hbase_ledger_storage.get_confirmed_block(slot, false).await;
                debug!("Got confirmed block");

                self.check_hbase_result(&hbase_result)?;
                let encoded_block_future: OptionFuture<_> =
                    hbase_result.ok().map(encode_block).into();
                return encoded_block_future.await.transpose();
            }
        } else {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }
        Err(RpcCustomError::BlockNotAvailable { slot }.into())
    }

    pub async fn check_block(
        &self,
        slot: Slot,
    ) -> Result<RpcBlockCheck> {
        if !self.config.enable_rpc_transaction_history {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }

        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            let start = Instant::now();
            let hbase_result = hbase_ledger_storage.get_confirmed_block(slot, false).await;
            let duration: Duration = start.elapsed();
            debug!("HBase request took {:?}", duration);

            return match hbase_result {
                Ok(_) => Ok(RpcBlockCheck { exists: true }),
                Err(solana_storage_adapter::Error::BlockNotFound(_)) => Ok(RpcBlockCheck { exists: false }),
                Err(err) => Err(RpcCustomError::HBaseError { message: err.to_string() }.into()),
            }
        }

        Err(RpcCustomError::BlockNotAvailable { slot }.into())
    }

    pub async fn get_blocks(
        &self,
        start_slot: Slot,
        // FIXME: Maybe make this non-optional?
        end_slot: Option<Slot>,
        config: Option<RpcContextConfig>,
    ) -> Result<Vec<Slot>> {
        let config = config.unwrap_or_default();
        let commitment = config.commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        let max_get_blocks_range = self.config.max_get_blocks_range.unwrap_or(MAX_GET_CONFIRMED_BLOCKS_RANGE);

        if end_slot.unwrap() < start_slot {
            return Ok(vec![]);
        }
        if end_slot.unwrap() - start_slot > max_get_blocks_range {
            return Err(Error::invalid_params(format!(
                "Slot range too large; max {max_get_blocks_range}"
            )));
        }

        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            // Check if MD5 salt is enabled but blocks_meta is not - return error
            if let Some(hbase_config) = &self.config.rpc_hbase_config {
                if hbase_config.use_md5_row_key_salt && !hbase_config.use_hbase_blocks_meta {
                    return Err(Error::invalid_params(
                        "get_blocks is not supported with MD5 row key salt unless --use-hbase-blocks-meta is enabled"
                            .to_string(),
                    ));
                }
            }

            return hbase_ledger_storage
                .get_confirmed_blocks(start_slot, (end_slot.unwrap() - start_slot) as usize + 1) // increment limit by 1 to ensure returned range is inclusive of both start_slot and end_slot
                .await
                .map(|mut hbase_blocks| {
                    hbase_blocks.retain(|&slot| slot <= end_slot.unwrap());
                    hbase_blocks
                })
                .map_err(|_| {
                    Error::invalid_params(
                        "HBase query failed (maybe timeout due to too large range?)"
                            .to_string(),
                    )
                });
        }

        Ok(vec![])
    }

    pub async fn get_blocks_with_limit(
        &self,
        start_slot: Slot,
        limit: usize,
        commitment: Option<CommitmentConfig>,
    ) -> Result<Vec<Slot>> {
        // info!(
        //     "getBlocksWithLimit request received [start_slot: {:?}, limit: {:?}]",
        //     start_slot, limit
        // );

        let commitment = commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        let max_get_blocks_range = self.config.max_get_blocks_range.unwrap_or(MAX_GET_CONFIRMED_BLOCKS_RANGE);

        if limit > max_get_blocks_range as usize {
            return Err(Error::invalid_params(format!(
                "Limit too large; max {max_get_blocks_range}"
            )));
        }

        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            // Check if MD5 salt is enabled but blocks_meta is not - return error
            if let Some(hbase_config) = &self.config.rpc_hbase_config {
                if hbase_config.use_md5_row_key_salt && !hbase_config.use_hbase_blocks_meta {
                    return Err(Error::invalid_params(
                        "get_blocks_with_limit is not supported with MD5 row key salt unless --use-hbase-blocks-meta is enabled"
                            .to_string(),
                    ));
                }
            }

            return Ok(hbase_ledger_storage
                .get_confirmed_blocks(start_slot, limit)
                .await
                .unwrap_or_default());
        }

        Ok(vec![])
    }


    pub async fn get_block_time(&self, slot: Slot) -> Result<Option<UnixTimestamp>> {
        if slot == 0 {
            return Ok(Some(self.genesis_creation_time()));
        }

        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            // Check if we should use blocks_meta table for efficient metadata lookup
            if let Some(hbase_config) = &self.config.rpc_hbase_config {
                if hbase_config.use_hbase_blocks_meta {
                    // Cast to concrete type to access blocks_meta methods
                    if let Some(hbase_storage) = hbase_ledger_storage.as_any().downcast_ref::<solana_storage_hbase::LedgerStorage>() {
                        match hbase_storage.get_block_time(slot).await {
                            Ok(Some(block_time)) => return Ok(Some(block_time)),
                            Ok(None) => return Ok(None),
                            Err(solana_storage_adapter::Error::BlockNotFound(_)) => return Ok(None),
                            Err(err) => return Err(RpcCustomError::HBaseError { message: err.to_string() }.into()),
                        }
                    }
                }
            }

            // Fall back to legacy method (reading full block)
            let hbase_result = hbase_ledger_storage.get_confirmed_block(slot, false).await;
            self.check_hbase_result(&hbase_result)?;
            return Ok(hbase_result
                .ok()
                .and_then(|confirmed_block| confirmed_block.block_time));
        }

        Ok(None)
    }

    pub async fn get_transaction(
        &self,
        signature: Signature,
        config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
    ) -> Result<Option<EncodedConfirmedTransactionWithStatusMeta>> {
        let config = config
            .map(|config| config.convert_to_current())
            .unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
        let max_supported_transaction_version = config.max_supported_transaction_version;
        let commitment = config.commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        if self.config.enable_rpc_transaction_history {
            let encode_transaction =
                |confirmed_tx_with_meta: ConfirmedTransactionWithStatusMeta| -> Result<EncodedConfirmedTransactionWithStatusMeta> {
                    Ok(confirmed_tx_with_meta.encode(encoding, max_supported_transaction_version).map_err(RpcCustomError::from)?)
                };

            // First, try to get the transaction from hbase_ledger_storage
            if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                if let Some(tx) = hbase_ledger_storage
                    .get_confirmed_transaction(&signature)
                    .await
                    .unwrap_or(None)
                    .map(encode_transaction)
                    .transpose()? {
                    return Ok(Some(tx));
                }
            }

            // If not found, fall back to fallback_ledger_storage
            if let Some(fallback_ledger_storage) = &self.fallback_ledger_storage {
                if let Some(tx) = fallback_ledger_storage
                    .get_confirmed_transaction(&signature)
                    .await
                    .unwrap_or(None)
                    .map(encode_transaction)
                    .transpose()? {
                    return Ok(Some(tx));
                } else {
                    info!("Transaction not found in the fallback ledger storage");
                }
            }
        } else {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }

        Ok(None)
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: Vec<Signature>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
        let search_transaction_history = config
            .map(|x| x.search_transaction_history)
            .unwrap_or(false);

        if search_transaction_history && !self.config.enable_rpc_transaction_history {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }

        let mut statuses: Vec<Option<TransactionStatus>> = vec![None; signatures.len()];
        let mut context: Option<RpcResponseContext> = None;

        // signatures that need HBase lookup
        let mut missing_sigs: Vec<Signature> = vec![];
        let mut missing_idxs: Vec<usize> = vec![];

        if let Some(client) = &self.rpc_node_client {
            let response = if search_transaction_history {
                client
                    .get_signature_statuses_with_history(&signatures)
                    .await
            } else {
                client.get_signature_statuses(&signatures).await
            }
            .map_err(|e| match *e.kind {
                solana_client::client_error::ClientErrorKind::RpcError(
                    solana_client::rpc_request::RpcError::RpcResponseError {
                        code,
                        message,
                        data: _,
                    },
                ) => Error {
                    code: jsonrpc_core::ErrorCode::from(code),
                    message,
                    data: None,
                },
                e => {
                    warn!("getSignatureStatuses to RPC node failed: {}", e);
                    Error::internal_error()
                }
            })?;

            context = Some(response.context);

            for (idx, (signature, tx_status)) in
                signatures.into_iter().zip(response.value).enumerate()
            {
                if tx_status.is_some() {
                    statuses[idx] = tx_status;
                } else {
                    missing_sigs.push(signature);
                    missing_idxs.push(idx);
                }
            }
        } else {
            // no RPC node, all signatures need HBase lookup
            missing_sigs = signatures;
            missing_idxs = (0..missing_sigs.len()).collect();
        }

        if missing_sigs.is_empty() {
            return Ok(new_response(context, statuses));
        }

        if search_transaction_history {
            if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                let response = hbase_ledger_storage
                    .get_signatures_status(&missing_sigs)
                    .await
                    .map_err(|e| {
                        warn!("getSignatureStatuses to Hbase failed: {}", e);
                        RpcCustomError::LongTermStorageUnreachable
                    })?;

                for (idx, tx_status) in missing_idxs.into_iter().zip(response) {
                    statuses[idx] = tx_status.map(Some).unwrap_or(None)
                }
            }
        }

        Ok(new_response(context, statuses))
    }

    pub async fn get_signatures_for_address(
        &self,
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        reversed: Option<bool>,
        config: RpcContextConfig,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let commitment = config.commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        debug!(
           "getSignaturesForAddress request received [address: {:?}, before: {:?}, until: {:?}], limit: {:?}",
           address, before, until, limit
        );

        if self.config.enable_rpc_transaction_history {
            let map_results = |results: Vec<ConfirmedTransactionStatusWithSignature>| {
                results
                    .into_iter()
                    .map(|x| {
                        let mut item: RpcConfirmedTransactionStatusWithSignature = x.into();
                        item.confirmation_status =
                            Some(TransactionConfirmationStatus::Finalized);
                        item
                    })
                    .collect()
            };

            let mut results: Vec<ConfirmedTransactionStatusWithSignature> = vec![];

            if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                let hbase_before = before;

                let hbase_results = hbase_ledger_storage
                    .get_confirmed_signatures_for_address(
                        &address,
                        hbase_before.as_ref(),
                        until.as_ref(),
                        limit,
                        reversed,
                        None,
                        None,
                    )
                    .await;
                match hbase_results {
                    Ok(hbase_results) => {
                        let results_set: HashSet<_> =
                            results.iter().map(|result| result.signature).collect();
                        for (hbase_result, _) in hbase_results {
                            // In the upload race condition, latest address-signatures in
                            // long-term storage may include original `before` signature...
                            if before != Some(hbase_result.signature)
                                && !results_set.contains(&hbase_result.signature)
                            {
                                results.push(hbase_result);
                            }
                        }
                    }
                    Err(err) => {
                        warn!("{:?}", err);
                    }
                }
            }

            Ok(map_results(results))
        } else {
            Err(RpcCustomError::TransactionHistoryNotAvailable.into())
        }
    }

    pub async fn get_transactions_for_address(
        &self,
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        sort_order: Option<SortOrder>,
        details_mode: TransactionDetailsMode,
        status_filter: TransactionStatusFilter,
        encoding: Option<UiTransactionEncoding>,
        max_supported_transaction_version: Option<u8>,
        config: RpcContextConfig,
        filters: Option<RpcTransactionFilters>,
    ) -> Result<GetTransactionsForAddressResponse> {
        let commitment = config.commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;

        // Derive `reversed` from sort_order (Asc = oldest first = forward scan)
        let reversed = sort_order.as_ref().map(|o| matches!(o, SortOrder::Asc));

        // Extract slot bounds from explicit slot filters only.
        // blockTime filtering is handled exactly via post-scan filter; no approximation needed.
        let (before_slot, until_slot) = if let Some(ref f) = filters {
            let before_slot = f.slot.as_ref().and_then(|s| {
                s.lte.or_else(|| s.lt.map(|v| v.saturating_sub(1)))
            });
            let until_slot = f.slot.as_ref().and_then(|s| {
                s.gte.or_else(|| s.gt.map(|v| v + 1))
            });
            (before_slot, until_slot)
        } else {
            (None, None)
        };

        // Merge status filter: filters.status overrides top-level status_filter
        let status_filter = filters
            .as_ref()
            .and_then(|f| f.status.clone())
            .unwrap_or(status_filter);

        info!(
            "getTransactionsForAddress request received [address: {:?}, before: {:?}, until: {:?}, limit: {:?}, details: {:?}, sort: {:?}, before_slot: {:?}, until_slot: {:?}]",
            address, before, until, limit, details_mode, sort_order, before_slot, until_slot
        );

        if !self.config.enable_rpc_transaction_history {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }

        let encoding = encoding.unwrap_or(UiTransactionEncoding::Json);
        // Default to supporting versioned (v0) transactions
        let max_supported_transaction_version = max_supported_transaction_version.or(Some(0));

        // Step 1: range scan on tx-by-addr (one HBase call)
        let t0 = Instant::now();
        let mut sig_results = if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            hbase_ledger_storage
                .get_confirmed_signatures_for_address(
                    &address,
                    before.as_ref(),
                    until.as_ref(),
                    limit,
                    reversed,
                    before_slot,
                    until_slot,
                )
                .await
                .unwrap_or_default()
        } else {
            return Ok(GetTransactionsForAddressResponse { data: vec![], pagination_token: None });
        };
        let sig_elapsed = t0.elapsed();

        // Apply status filter
        match status_filter {
            TransactionStatusFilter::Succeeded => sig_results.retain(|(s, _)| s.err.is_none()),
            TransactionStatusFilter::Failed    => sig_results.retain(|(s, _)| s.err.is_some()),
            TransactionStatusFilter::Any       => {}
        }

        // Exact block_time post-filter using the block_time stored in each tx-by-addr row.
        // May return fewer than limit (same behavior as status/tokenAccounts filters).
        if let Some(Some(ref bt)) = filters.as_ref().map(|f| f.block_time.as_ref()) {
            sig_results.retain(|(s, _)| {
                let Some(block_time) = s.block_time else { return true };
                bt.gte.map_or(true, |t| block_time >= t)
                    && bt.gt.map_or(true, |t| block_time > t)
                    && bt.lte.map_or(true, |t| block_time <= t)
                    && bt.lt.map_or(true, |t| block_time < t)
            });
        }

        if sig_results.is_empty() {
            info!("getTransactionsForAddress: sig scan took {:?}, 0 results after filter", sig_elapsed);
            return Ok(GetTransactionsForAddressResponse { data: vec![], pagination_token: None });
        }

        // Token account filtering requires full tx data even in signatures mode
        let token_filter = filters.as_ref().and_then(|f| f.token_accounts.as_ref());
        let needs_full_for_token_filter = matches!(
            token_filter,
            Some(TokenAccountsFilter::All) | Some(TokenAccountsFilter::BalanceChanged)
        );

        // Signatures-only mode: skip tx fetch unless token filter needs full data
        if details_mode == TransactionDetailsMode::Signatures && !needs_full_for_token_filter {
            let pagination_token = sig_results.last().map(|(s, _)| s.signature.to_string());
            info!("getTransactionsForAddress: sig scan {:?} ({} sigs, signatures mode), total {:?}",
                sig_elapsed, sig_results.len(), t0.elapsed());
            let data = sig_results
                .into_iter()
                .map(|(s, _)| {
                    let mut item: RpcConfirmedTransactionStatusWithSignature = s.into();
                    item.confirmation_status = Some(TransactionConfirmationStatus::Finalized);
                    serde_json::to_value(item).unwrap_or(serde_json::Value::Null)
                })
                .collect();
            return Ok(GetTransactionsForAddressResponse { data, pagination_token });
        }

        let signatures: Vec<Signature> = sig_results.iter().map(|(s, _)| s.signature).collect();

        // Step 2: batch-fetch full transactions (single HBase getRows call)
        let t1 = Instant::now();
        let tx_results: Vec<Option<ConfirmedTransactionWithStatusMeta>> =
            if let Some(tx_storage) = &self.tx_ledger_storage {
                tx_storage
                    .get_confirmed_transactions_batch(&signatures)
                    .await
                    .unwrap_or_else(|e| {
                        warn!("Batch tx fetch failed: {:?}", e);
                        vec![None; signatures.len()]
                    })
            } else {
                let primary = self.hbase_ledger_storage.as_ref();
                let fallback = self.fallback_ledger_storage.as_ref();
                let fetch_futures: Vec<_> = signatures
                    .iter()
                    .map(|sig| {
                        let sig = *sig;
                        async move {
                            if let Some(storage) = primary {
                                if let Ok(Some(tx)) = storage.get_confirmed_transaction(&sig).await {
                                    return Some(tx);
                                }
                            }
                            if let Some(storage) = fallback {
                                if let Ok(Some(tx)) = storage.get_confirmed_transaction(&sig).await {
                                    return Some(tx);
                                }
                            }
                            None
                        }
                    })
                    .collect();
                futures::future::join_all(fetch_futures).await
            };
        let tx_elapsed = t1.elapsed();

        // Apply token accounts filter (post-fetch, may return fewer than limit)
        let (sig_results, tx_results): (Vec<_>, Vec<_>) = sig_results
            .into_iter()
            .zip(tx_results.into_iter())
            .filter(|(_, tx_opt)| match token_filter {
                Some(TokenAccountsFilter::All) => {
                    tx_opt.as_ref().map_or(false, tx_has_token_accounts)
                }
                Some(TokenAccountsFilter::BalanceChanged) => {
                    tx_opt.as_ref().map_or(false, tx_has_token_balance_changed)
                }
                _ => true,
            })
            .unzip();

        let pagination_token = sig_results.last().map(|(s, _)| s.signature.to_string());

        info!(
            "getTransactionsForAddress: sig scan {:?} ({} sigs), tx fetch {:?} ({} txs, {} after token filter), total {:?}",
            sig_elapsed, signatures.len(), tx_elapsed,
            tx_results.iter().filter(|t| t.is_some()).count(),
            tx_results.len(),
            t0.elapsed(),
        );

        // If we only fetched full data for the token filter, return signatures
        if details_mode == TransactionDetailsMode::Signatures {
            let data = sig_results
                .into_iter()
                .map(|(s, _)| {
                    let mut item: RpcConfirmedTransactionStatusWithSignature = s.into();
                    item.confirmation_status = Some(TransactionConfirmationStatus::Finalized);
                    serde_json::to_value(item).unwrap_or(serde_json::Value::Null)
                })
                .collect();
            return Ok(GetTransactionsForAddressResponse { data, pagination_token });
        }

        let mut data = Vec::with_capacity(tx_results.len());
        for tx_opt in tx_results {
            if let Some(tx) = tx_opt {
                match tx.encode(encoding, max_supported_transaction_version).map_err(RpcCustomError::from) {
                    Ok(encoded_tx) => data.push(serde_json::to_value(encoded_tx).unwrap_or(serde_json::Value::Null)),
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(GetTransactionsForAddressResponse { data, pagination_token })
    }

    pub async fn get_first_available_block(&self) -> Slot {
        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            let hbase_slot = hbase_ledger_storage
                .get_first_available_block()
                .await
                .unwrap_or(None)
                .unwrap_or(Slot::default());

            return hbase_slot;
        }
        Slot::default()
    }

    pub async fn get_slot(&self, _config: RpcContextConfig) -> Slot {
        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            if let Ok(hbase_slot) = hbase_ledger_storage.get_latest_stored_slot().await {
                return hbase_slot;
            }
        }
        Slot::default()
    }

    pub async fn get_block_height(&self, _config: RpcContextConfig) -> Result<u64> {
        if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
            // Check if we should use blocks_meta table for efficient metadata lookup
            if let Some(hbase_config) = &self.config.rpc_hbase_config {
                if hbase_config.use_hbase_blocks_meta {
                    // Cast to concrete type to access blocks_meta methods
                    if let Some(hbase_storage) = hbase_ledger_storage.as_any().downcast_ref::<solana_storage_hbase::LedgerStorage>() {
                        // Get latest slot using blocks_meta table
                        if let Ok(latest_slot) = hbase_storage.get_latest_stored_slot().await {
                            if latest_slot == 0 {
                                return Ok(0);
                            }
                            // Try to get actual block height from metadata
                            match hbase_storage.get_block_height(latest_slot).await {
                                Ok(Some(block_height)) => return Ok(block_height),
                                Ok(None) => return Ok(latest_slot), // fallback to slot as block height
                                Err(_) => return Ok(latest_slot), // fallback to slot as block height
                            }
                        }
                    }
                }
            }

            // Fall back to using latest slot as block height
            if let Ok(latest_slot) = hbase_ledger_storage.get_latest_stored_slot().await {
                return Ok(latest_slot);
            }
        }
        Ok(0)
    }

    pub fn get_epoch_schedule(&self) -> Result<EpochSchedule> {
        match &self.genesis_config {
            Some(genesis_config) => Ok(genesis_config.epoch_schedule.clone()),
            None => Err(RpcCustomError::MethodNotSupported("Method not supported".to_string()).into()),
        }
    }

    pub async fn get_inflation_reward(
        &self,
        addresses: Vec<Pubkey>,
        config: Option<RpcEpochConfig>,
    ) -> Result<Vec<Option<RpcInflationReward>>> {
        // Check if genesis config is available
        if self.genesis_config.is_none() {
            return Err(RpcCustomError::MethodNotSupported("Method not supported".to_string()).into());
        }

        let config = config.unwrap_or_default();
        debug!(
            "get_inflation_reward: inputs => addresses: {:?}, commitment: {:?}, min_context_slot: {:?}, epoch: {:?}",
            addresses,
            config.commitment,
            config.min_context_slot,
            config.epoch,
        );
        let epoch_schedule = self.get_epoch_schedule()?;
        let first_available_block = self.get_first_available_block().await;
        let context_config = RpcContextConfig {
            commitment: config.commitment,
            min_context_slot: config.min_context_slot,
        };
        let current_slot = self.get_slot(context_config).await;
        let epoch = match config.epoch {
            Some(epoch) => epoch,
            None => epoch_schedule
                .get_epoch(current_slot)
                .saturating_sub(1),
        };
        debug!(
            "get_inflation_reward: derived => current_slot: {:?}, epoch: {:?}, first_available_block: {:?}",
            current_slot, epoch, first_available_block
        );

        // Rewards for this epoch are found in the first confirmed block of the next epoch
        let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch.saturating_add(1));
        debug!(
            "get_inflation_reward: first_slot_in_next_epoch: {:?}",
            first_slot_in_epoch
        );
        if first_slot_in_epoch < first_available_block {
            debug!(
                "get_inflation_reward: aborting due to LongTermStorageSlotSkipped at slot {:?}",
                first_slot_in_epoch
            );
            return Err(RpcCustomError::LongTermStorageSlotSkipped {
                slot: first_slot_in_epoch,
            }
            .into());
        }

        let first_confirmed_block_in_epoch = *self
            .get_blocks_with_limit(first_slot_in_epoch, 1, context_config.commitment)
            .await?
            .first()
            .ok_or(RpcCustomError::BlockNotAvailable {
                slot: first_slot_in_epoch,
            })?;
        debug!(
            "get_inflation_reward: first_confirmed_block_in_epoch: {:?}",
            first_confirmed_block_in_epoch
        );

        // Get first block in the epoch
        let epoch_boundary_block = match self
            .get_block(
                first_confirmed_block_in_epoch,
                Some(RpcBlockConfig::rewards_with_commitment(Some(context_config.commitment.unwrap_or_default())).into()),
            )
            .await?
        {
            Some(block) => block,
            None => {
                debug!(
                    "get_inflation_reward: epoch boundary block not available at slot {:?}",
                    first_confirmed_block_in_epoch
                );
                return Err(RpcCustomError::BlockNotAvailable {
                    slot: first_confirmed_block_in_epoch,
                }
                .into());
            }
        };
        debug!(
            "get_inflation_reward: epoch_boundary_block => parent_slot: {:?}, rewards_len: {:?}, num_reward_partitions: {:?}",
            epoch_boundary_block.parent_slot,
            epoch_boundary_block.rewards.as_ref().map(|r| r.len()),
            epoch_boundary_block.num_reward_partitions
        );

        // If there is a gap in blockstore or long-term historical storage that
        // includes the epoch boundary, the `get_blocks_with_limit()` call above
        // will return the slot of the block at the end of that gap, not a
        // legitimate epoch-boundary block. Therefore, verify that the parent of
        // `epoch_boundary_block` occurred before the `first_slot_in_epoch`. If
        // it didn't, return an error; it will be impossible to locate
        // rewards properly.
        if epoch_boundary_block.parent_slot >= first_slot_in_epoch {
            debug!(
                "get_inflation_reward: SlotNotEpochBoundary (parent_slot {:?} >= first_slot_in_epoch {:?})",
                epoch_boundary_block.parent_slot,
                first_slot_in_epoch
            );
            return Err(RpcCustomError::SlotNotEpochBoundary {
                slot: first_confirmed_block_in_epoch,
            }
            .into());
        }

        let epoch_has_partitioned_rewards = epoch_boundary_block.num_reward_partitions.is_some();
        debug!(
            "get_inflation_reward: epoch_has_partitioned_rewards: {:?}",
            epoch_has_partitioned_rewards
        );

        // Collect rewards from first block in the epoch if partitioned epoch
        // rewards not enabled, or address is a vote account
        let mut reward_map: HashMap<String, (Reward, Slot)> = {
            let addresses: Vec<String> =
                addresses.iter().map(|pubkey| pubkey.to_string()).collect();
            Self::filter_map_rewards(
                &epoch_boundary_block.rewards,
                first_confirmed_block_in_epoch,
                &addresses,
                &|reward_type| -> bool {
                    reward_type == RewardType::Voting
                        || (!epoch_has_partitioned_rewards && reward_type == RewardType::Staking)
                },
            )
        };
        debug!(
            "get_inflation_reward: initial reward_map size: {:?}",
            reward_map.len()
        );

        // Fallback: some clusters may produce rewards a few blocks after the epoch boundary
        // even when num_reward_partitions is not populated. If requested addresses are still
        // missing, scan forward a bounded number of blocks to locate their rewards.
        let mut missing_addresses: Vec<String> = addresses
            .iter()
            .map(|pk| pk.to_string())
            .filter(|addr| !reward_map.contains_key(addr))
            .collect();
        if !missing_addresses.is_empty() {
            let scan_limit = 256usize; // bounded scan for safety/perf
            debug!(
                "get_inflation_reward: fallback scan => missing: {:?}, scan_limit: {:?}",
                missing_addresses.len(),
                scan_limit
            );
            let block_list = self
                .get_blocks_with_limit(
                    first_confirmed_block_in_epoch.saturating_add(1),
                    scan_limit,
                    context_config.commitment,
                )
                .await?;
            for slot in block_list.into_iter() {
                if missing_addresses.is_empty() {
                    break;
                }
                let maybe_block = self
                    .get_block(
                        slot,
                        Some(
                            RpcBlockConfig::rewards_with_commitment(
                                Some(context_config.commitment.unwrap_or_default()),
                            )
                            .into(),
                        ),
                    )
                    .await?;
                if let Some(block) = maybe_block {
                    let delta = Self::filter_map_rewards(
                        &block.rewards,
                        slot,
                        &missing_addresses,
                        &|reward_type| -> bool {
                            // Include both reward types to be robust
                            reward_type == RewardType::Voting || reward_type == RewardType::Staking
                        },
                    );
                    if !delta.is_empty() {
                        debug!(
                            "get_inflation_reward: fallback scan => slot {:?} matched {:?} addresses",
                            slot,
                            delta.len()
                        );
                        for key in delta.keys() {
                            if let Some(idx) = missing_addresses.iter().position(|a| a == key) {
                                missing_addresses.swap_remove(idx);
                            }
                        }
                        reward_map.extend(delta);
                    }
                }
            }
            debug!(
                "get_inflation_reward: fallback scan complete => still missing: {:?}",
                missing_addresses.len()
            );
        }

        // Append stake account rewards from partitions if partitions epoch
        // rewards is enabled
        if epoch_has_partitioned_rewards {
            let num_partitions = epoch_boundary_block.num_reward_partitions.expect(
                "epoch-boundary block should have num_reward_partitions for epochs with \
                 partitioned rewards enabled",
            );

            let num_partitions = usize::try_from(num_partitions)
                .expect("num_partitions should never exceed usize::MAX");
            let hasher = EpochRewardsHasher::new(
                num_partitions,
                &Hash::from_str(&epoch_boundary_block.previous_blockhash)
                    .expect("UiConfirmedBlock::previous_blockhash should be properly formed"),
            );
            let mut partition_index_addresses: HashMap<usize, Vec<String>> = HashMap::new();
            for address in addresses.iter() {
                let address_string = address.to_string();
                // Skip this address if (Voting) rewards were already found in
                // the first block of the epoch
                if !reward_map.contains_key(&address_string) {
                    let partition_index = hasher.clone().hash_address_to_partition(address);
                    partition_index_addresses
                        .entry(partition_index)
                        .and_modify(|list| list.push(address_string.clone()))
                        .or_insert(vec![address_string]);
                }
            }
            debug!(
                "get_inflation_reward: partitioning => num_partitions: {:?}, queried_partition_indexes: {:?}",
                num_partitions,
                partition_index_addresses.keys().cloned().collect::<Vec<_>>()
            );

            let block_list = self
                .get_blocks_with_limit(
                    first_confirmed_block_in_epoch + 1,
                    num_partitions,
                    context_config.commitment,
                )
                .await?;

            for (partition_index, addresses) in partition_index_addresses.iter() {
                let slot = match block_list.get(*partition_index) {
                    Some(slot) => *slot,
                    None => {
                        // If block_list.len() too short to contain
                        // partition_index, the epoch rewards period must be
                        // currently active.
                        let latest_slot = self.get_slot(context_config).await;
                        let current_block_height = self.get_block_height(context_config).await.unwrap_or(latest_slot);
                        let rewards_complete_block_height = epoch_boundary_block
                            .block_height
                            .map(|block_height| {
                                block_height
                                    .saturating_add(num_partitions as u64)
                                    .saturating_add(1)
                            })
                            .expect(
                                "every block after partitioned_epoch_reward_enabled should have a \
                                 populated block_height",
                            );
                        debug!(
                            "get_inflation_reward: EpochRewardsPeriodActive => latest_slot: {:?}, current_block_height: {:?}, rewards_complete_block_height: {:?}",
                            latest_slot,
                            current_block_height,
                            rewards_complete_block_height
                        );
                        return Err(RpcCustomError::EpochRewardsPeriodActive {
                            slot: latest_slot,
                            current_block_height,
                            rewards_complete_block_height,
                        }.into());
                    }
                };

                let block = match self
                    .get_block(
                        slot,
                        Some(RpcBlockConfig::rewards_with_commitment(context_config.commitment).into()),
                    )
                    .await?
                {
                    Some(block) => block,
                    None => {
                        debug!(
                            "get_inflation_reward: BlockNotAvailable at partition slot {:?}",
                            slot
                        );
                        return Err(RpcCustomError::BlockNotAvailable { slot }.into());
                    }
                };

                let index_reward_map = Self::filter_map_rewards(
                    &block.rewards,
                    slot,
                    addresses,
                    &|reward_type| -> bool { reward_type == RewardType::Staking },
                );
                debug!(
                    "get_inflation_reward: partition {:?} => found {:?} rewards",
                    partition_index,
                    index_reward_map.len()
                );
                reward_map.extend(index_reward_map);
            }
        }

        let rewards: Vec<Option<RpcInflationReward>> = addresses
            .iter()
            .map(|address| {
                if let Some((reward, slot)) = reward_map.get(&address.to_string()) {
                    return Some(RpcInflationReward {
                        epoch,
                        effective_slot: *slot,
                        amount: reward.lamports.unsigned_abs(),
                        post_balance: reward.post_balance,
                        commission: reward.commission,
                    });
                }
                None
            })
            .collect();
        debug!(
            "get_inflation_reward: result => requested: {:?}, returned: {:?}",
            addresses.len(),
            rewards.iter().filter(|r| r.is_some()).count()
        );
        Ok(rewards)
    }

    fn filter_map_rewards<'a, F>(
        rewards: &'a Option<Rewards>,
        slot: Slot,
        addresses: &'a [String],
        reward_type_filter: &'a F,
    ) -> HashMap<String, (Reward, Slot)>
    where
        F: Fn(RewardType) -> bool,
    {
        Self::filter_rewards(rewards, reward_type_filter)
            .filter(|reward| addresses.contains(&reward.pubkey))
            .map(|reward| (reward.pubkey.clone(), (reward.clone(), slot)))
            .collect()
    }

    fn filter_rewards<'a, F>(
        rewards: &'a Option<Rewards>,
        reward_type_filter: &'a F,
    ) -> impl Iterator<Item = &'a Reward>
    where
        F: Fn(RewardType) -> bool,
    {
        rewards
            .iter()
            .flatten()
            .filter(move |reward| reward.reward_type.is_some_and(reward_type_filter))
    }
}






pub fn create_validator_exit(exit: &Arc<AtomicBool>) -> Arc<RwLock<Exit>> {
    let mut validator_exit = Exit::default();
    let exit_ = exit.clone();
    validator_exit.register_exit(Box::new(move || exit_.store(true, Ordering::Relaxed)));
    Arc::new(RwLock::new(validator_exit))
}


