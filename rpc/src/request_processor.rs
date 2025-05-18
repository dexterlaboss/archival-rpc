//! The `storage_rpc` module implements the Solana storage RPC interface.

use {
    crate::{
        custom_error::RpcCustomError,
    },
    jsonrpc_core::{
        Error, Metadata, Result
    },
    solana_rpc_client_api::{
        config::*,
        request::{
            MAX_GET_CONFIRMED_BLOCKS_RANGE,
            MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT,
        },
        response::{Response as RpcResponse, *},
    },
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
    },
    solana_transaction_status_client_types::{
        EncodedConfirmedTransactionWithStatusMeta,
        TransactionConfirmationStatus,
        TransactionStatus,
        UiConfirmedBlock,
        UiTransactionEncoding,
    },
    std::{
        collections::{
            HashSet
        },
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
};

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10); // 50kB

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

fn new_response<T>(value: T) -> RpcResponse<T> {
    RpcResponse {
        // TODO: Maybe add actual slot to the contect?
        context: RpcResponseContext::new(/*bank.slot()*/ Slot::default()),
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
    pub rpc_threads: usize,
    pub rpc_niceness_adj: i8,
    pub full_api: bool,
    pub obsolete_v1_7_api: bool,
    pub max_request_body_size: Option<usize>,
    pub max_get_blocks_range: Option<u64>,
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
    pub hdfs_url: String,
    pub hdfs_path: String,
    pub fallback_hbase_address: Option<String>,
    pub timeout: Option<Duration>,
    // pub block_cache: Option<NonZeroUsize>,
    pub use_md5_row_key_salt: bool,
    pub hash_tx_full_row_keys: bool,
    pub enable_full_tx_cache: bool,
    pub disable_tx_fallback: bool,
    pub cache_address: Option<String>,
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
            hdfs_url,
            hdfs_path,
            fallback_hbase_address: None,
            timeout: None,
            // block_cache: None,
            use_md5_row_key_salt: false,
            hash_tx_full_row_keys: false,
            enable_full_tx_cache: false,
            disable_tx_fallback: false,
            cache_address: None,
        }
    }
}

// #[derive(Clone)]
pub struct JsonRpcRequestProcessor {
    config: JsonRpcConfig,
    #[allow(dead_code)]
    rpc_service_exit: Arc<RwLock<Exit>>,
    hbase_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
    fallback_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
}

impl Metadata for JsonRpcRequestProcessor {}

impl Clone for JsonRpcRequestProcessor {
    fn clone(&self) -> Self {
        JsonRpcRequestProcessor {
            config: self.config.clone(),
            rpc_service_exit: Arc::clone(&self.rpc_service_exit),
            hbase_ledger_storage: self.hbase_ledger_storage.as_ref().map(|storage| storage.clone_box()),
            fallback_ledger_storage: self.fallback_ledger_storage.as_ref().map(|storage| storage.clone_box()),
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
        config: JsonRpcConfig,
        rpc_service_exit: Arc<RwLock<Exit>>,
        hbase_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
        fallback_ledger_storage: Option<Box<dyn solana_storage_adapter::LedgerStorageAdapter>>,
    ) -> Self {
    // ) -> (Self, Receiver<TransactionInfo>) {
    //     let (_sender, receiver) = unbounded();
        Self {
            config,
            rpc_service_exit,
            hbase_ledger_storage,
            fallback_ledger_storage,
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

            let encode_block = |confirmed_block: ConfirmedBlock| -> Result<UiConfirmedBlock> {
                debug!("Encoding block");
                let mut encoded_block = confirmed_block
                    .encode_with_options(encoding, encoding_options)
                    .map_err(|e| {
                        debug!("Encoding error: {:?}", e);
                        RpcCustomError::from(e)
                    })?;
                if slot == 0 {
                    encoded_block.block_time = Some(self.genesis_creation_time());
                    encoded_block.block_height = Some(0);
                }
                debug!("Encoded block");

                Ok(encoded_block)
            };
            if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                let hbase_result = hbase_ledger_storage.get_confirmed_block(slot, false).await;
                debug!("Got confirmed block");

                self.check_hbase_result(&hbase_result)?;

                return hbase_result.ok().map(encode_block).transpose();
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
            let hbase_result = hbase_ledger_storage.get_confirmed_block(slot, false).await;
            self.check_hbase_result(&hbase_result)?;
            return Ok(hbase_result
                .ok()
                .and_then(|confirmed_block| confirmed_block.block_time));
        }

        Ok(None)
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: Vec<Signature>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
        let mut statuses: Vec<Option<TransactionStatus>> = vec![];

        let search_transaction_history = config
            .map(|x| x.search_transaction_history)
            .unwrap_or(false);

        if search_transaction_history && !self.config.enable_rpc_transaction_history {
            return Err(RpcCustomError::TransactionHistoryNotAvailable.into());
        }

        for signature in signatures {
            let status = if self.config.enable_rpc_transaction_history && search_transaction_history {
                if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                    hbase_ledger_storage
                        .get_signature_status(&signature)
                        .await
                        .map(Some)
                        .unwrap_or(None)
                } else {
                    None
                }
            } else {
                None
            };
            statuses.push(status);
        }
        Ok(new_response(/*&bank,*/ statuses))
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

        info!(
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
}




pub fn create_validator_exit(exit: &Arc<AtomicBool>) -> Arc<RwLock<Exit>> {
    let mut validator_exit = Exit::default();
    let exit_ = exit.clone();
    validator_exit.register_exit(Box::new(move || exit_.store(true, Ordering::Relaxed)));
    Arc::new(RwLock::new(validator_exit))
}


