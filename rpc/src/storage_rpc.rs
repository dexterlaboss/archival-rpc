//! The `storage_rpc` module implements the Solana storage RPC interface.

use {
    crate::{
        rpc::{
            verify_and_parse_signatures_for_address_params,
            check_is_at_least_confirmed,
            verify_signature,
        },
        custom_error::RpcCustomError,
    },
    crossbeam_channel::{
        unbounded,
        Receiver,
    },
    jsonrpc_core::{
        futures::future,
        BoxFuture, Error, Metadata, Result
    },
    jsonrpc_derive::rpc,
    solana_rpc_client_api::{
        config::*,
        deprecated_config::*,
        request::{
            MAX_GET_CONFIRMED_BLOCKS_RANGE,
            MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
        },
        response::{Response as RpcResponse, *},
    },
    solana_sdk::{
        clock::{
            Slot,
            UnixTimestamp,
        },
        commitment_config::{
            CommitmentConfig,
        },
        exit::Exit,
        pubkey::{
            Pubkey,
        },
        signature::{
            Signature,
        },
    },
    solana_send_transaction_service::{
        send_transaction_service::{
            TransactionInfo
        },
    },
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta, EncodedConfirmedTransactionWithStatusMeta,
        TransactionConfirmationStatus, TransactionStatus,
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
        },
        time::{Duration, Instant},
        num::NonZeroUsize,
    },
};

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10); // 50kB

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
    // pub enable_extended_tx_metadata_storage: bool,
    pub rpc_hbase_config: Option<RpcHBaseConfig>,
    pub rpc_threads: usize,
    pub rpc_niceness_adj: i8,
    pub full_api: bool,
    pub obsolete_v1_7_api: bool,
    pub max_request_body_size: Option<usize>,
    // pub block_cache: Option<NonZeroUsize>,
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
    pub timeout: Option<Duration>,
    pub block_cache: Option<NonZeroUsize>,
    pub use_md5_row_key_salt: bool,
    pub enable_full_tx_cache: bool,
    pub cache_address: Option<String>,
}

impl Default for RpcHBaseConfig {
    fn default() -> Self {
        let hbase_address = solana_storage_hbase::DEFAULT_ADDRESS.to_string();
        Self {
            enable_hbase_ledger_upload: false,
            hbase_address,
            timeout: None,
            block_cache: None,
            use_md5_row_key_salt: false,
            enable_full_tx_cache: false,
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
}

impl Metadata for JsonRpcRequestProcessor {}

impl Clone for JsonRpcRequestProcessor {
    fn clone(&self) -> Self {
        JsonRpcRequestProcessor {
            config: self.config.clone(),
            rpc_service_exit: Arc::clone(&self.rpc_service_exit),
            hbase_ledger_storage: self.hbase_ledger_storage.as_ref().map(|storage| storage.clone_box()),
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
    ) -> (Self, Receiver<TransactionInfo>) {
        let (_sender, receiver) = unbounded();
        (
            Self {
                config,
                rpc_service_exit,
                hbase_ledger_storage,
            },
            receiver,
        )
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
        commitment: Option<CommitmentConfig>,
    ) -> Result<Vec<Slot>> {
        // info!(
        //     "getBlocks request received [start_slot: {:?}, end_slot: {:?}]",
        //     start_slot, end_slot
        // );

        let commitment = commitment.unwrap_or_default();
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

            if let Some(hbase_ledger_storage) = &self.hbase_ledger_storage {
                return hbase_ledger_storage
                    .get_confirmed_transaction(&signature)
                    .await
                    .unwrap_or(None)
                    .map(encode_transaction)
                    .transpose();
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


// Minimal RPC interface
pub mod storage_rpc_minimal {
    use super::*;
    #[rpc]
    pub trait Minimal {
        type Metadata;

        #[rpc(meta, name = "getHealth")]
        fn get_health(&self, meta: Self::Metadata) -> Result<String>;

        #[rpc(meta, name = "getVersion")]
        fn get_version(&self, meta: Self::Metadata) -> Result<RpcVersionInfo>;

        #[rpc(meta, name = "getSlot")]
        fn get_slot(&self, meta: Self::Metadata, config: Option<RpcContextConfig>) -> BoxFuture<Result<Slot>>;
    }

    pub struct MinimalImpl;
    impl Minimal for MinimalImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_health(&self, _meta: Self::Metadata) -> Result<String> {
            Ok("ok".to_string())
        }

        fn get_version(&self, _: Self::Metadata) -> Result<RpcVersionInfo> {
            info!("getVersion request received");
            let version = solana_version::Version::default();
            Ok(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            })
        }

        fn get_slot(&self, meta: Self::Metadata, config: Option<RpcContextConfig>) -> BoxFuture<Result<Slot>> {
            info!("getSlot request received");
            Box::pin(async move { Ok(meta.get_slot(config.unwrap_or_default()).await) })
        }
    }
}

// Full RPC interface that an API node is expected to provide
// (rpc_minimal should also be provided by an API node)
pub mod storage_rpc_full {
    use {
        super::*,
        // solana_sdk::message::{SanitizedVersionedMessage, VersionedMessage},
    };
    #[rpc]
    pub trait Full {
        type Metadata;

        #[rpc(meta, name = "getSignatureStatuses")]
        fn get_signature_statuses(
            &self,
            meta: Self::Metadata,
            signature_strs: Vec<String>,
            config: Option<RpcSignatureStatusConfig>,
        ) -> BoxFuture<Result<RpcResponse<Vec<Option<TransactionStatus>>>>>;

        #[rpc(meta, name = "getBlock")]
        fn get_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>>;

        #[rpc(meta, name = "checkBlock")]
        fn check_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
        ) -> BoxFuture<Result<RpcBlockCheck>>;

        #[rpc(meta, name = "getBlockTime")]
        fn get_block_time(
            &self,
            meta: Self::Metadata,
            slot: Slot,
        ) -> BoxFuture<Result<Option<UnixTimestamp>>>;

        #[rpc(meta, name = "getBlocks")]
        fn get_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        #[rpc(meta, name = "getBlocksWithLimit")]
        fn get_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        #[rpc(meta, name = "getTransaction")]
        fn get_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransactionWithStatusMeta>>>;

        #[rpc(meta, name = "getSignaturesForAddress")]
        fn get_signatures_for_address(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcSignaturesForAddressConfig>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>>;

        #[rpc(meta, name = "getFirstAvailableBlock")]
        fn get_first_available_block(&self, meta: Self::Metadata) -> BoxFuture<Result<Slot>>;
    }

    pub struct FullImpl;
    impl Full for FullImpl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_signature_statuses(
            &self,
            meta: Self::Metadata,
            signature_strs: Vec<String>,
            config: Option<RpcSignatureStatusConfig>,
        ) -> BoxFuture<Result<RpcResponse<Vec<Option<TransactionStatus>>>>> {
            info!(
                "getSignatureStatuses request received: {:?}",
                signature_strs.len()
            );
            if signature_strs.len() > MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS {
                return Box::pin(future::err(Error::invalid_params(format!(
                    "Too many inputs provided; max {MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS}"
                ))));
            }
            let mut signatures: Vec<Signature> = vec![];
            for signature_str in signature_strs {
                match verify_signature(&signature_str) {
                    Ok(signature) => {
                        signatures.push(signature);
                    }
                    Err(err) => return Box::pin(future::err(err)),
                }
            }
            Box::pin(async move { meta.get_signature_statuses(signatures, config).await })
        }

        fn get_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>> {
            info!("getBlock request received: {:?}", slot);
            Box::pin(async move { meta.get_block(slot, config).await })
        }

        fn check_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
        ) -> BoxFuture<Result<RpcBlockCheck>> {
            info!("checkBlock request received: {:?}", slot);
            Box::pin(async move { meta.check_block(slot).await })
        }

        fn get_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            let (end_slot, maybe_commitment) =
                config.map(|config| config.unzip()).unwrap_or_default();
            info!(
                "getBlocks request received [start_slot: {:?}, end_slot: {:?}]",
                start_slot, end_slot
            );
            Box::pin(async move {
                meta.get_blocks(start_slot, end_slot, commitment.or(maybe_commitment))
                    .await
            })
        }

        fn get_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            info!(
                "getBlocksWithLimit request received [start_slot: {:?}, limit: {:?}]",
                start_slot, limit
            );

            Box::pin(async move {
                meta.get_blocks_with_limit(start_slot, limit, commitment)
                    .await
            })
        }

        fn get_block_time(
            &self,
            meta: Self::Metadata,
            slot: Slot,
        ) -> BoxFuture<Result<Option<UnixTimestamp>>> {
            info!("getBlockTime request received [slot: {:?}]", slot);
            Box::pin(async move { meta.get_block_time(slot).await })
        }

        fn get_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransactionWithStatusMeta>>> {
            info!("getTransaction request received: {:?}", signature_str);
            let signature = verify_signature(&signature_str);
            if let Err(err) = signature {
                return Box::pin(future::err(err));
            }
            Box::pin(async move { meta.get_transaction(signature.unwrap(), config).await })
        }

        fn get_signatures_for_address(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcSignaturesForAddressConfig>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>> {
            let RpcSignaturesForAddressConfig {
                before,
                until,
                limit,
                commitment,
                min_context_slot,
            } = config.unwrap_or_default();
            let verification =
                verify_and_parse_signatures_for_address_params(address, before, until, limit);

            match verification {
                Err(err) => Box::pin(future::err(err)),
                Ok((address, before, until, limit)) => Box::pin(async move {
                    meta.get_signatures_for_address(
                        address,
                        before,
                        until,
                        limit,
                        RpcContextConfig {
                            commitment,
                            min_context_slot,
                        },
                    )
                        .await
                }),
            }
        }

        fn get_first_available_block(&self, meta: Self::Metadata) -> BoxFuture<Result<Slot>> {
            info!("getFirstAvailableBlock request received");
            Box::pin(async move { Ok(meta.get_first_available_block().await) })
        }
    }
}

// RPC methods deprecated in v1.7
pub mod storage_rpc_deprecated_v1_7 {
    #![allow(deprecated)]
    use super::*;
    #[rpc]
    pub trait DeprecatedV1_7 {
        type Metadata;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedBlock")]
        fn get_confirmed_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedBlocks")]
        fn get_confirmed_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcConfirmedBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedBlocksWithLimit")]
        fn get_confirmed_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedTransaction")]
        fn get_confirmed_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransactionWithStatusMeta>>>;

        // DEPRECATED
        #[rpc(meta, name = "getConfirmedSignaturesForAddress2")]
        fn get_confirmed_signatures_for_address2(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcGetConfirmedSignaturesForAddress2Config>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>>;
    }

    pub struct DeprecatedV1_7Impl;
    impl DeprecatedV1_7 for DeprecatedV1_7Impl {
        type Metadata = JsonRpcRequestProcessor;

        fn get_confirmed_block(
            &self,
            meta: Self::Metadata,
            slot: Slot,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedBlockConfig>>,
        ) -> BoxFuture<Result<Option<UiConfirmedBlock>>> {
            info!("getConfirmedBlock rpc request received: {:?}", slot);
            Box::pin(async move {
                meta.get_block(slot, config.map(|config| config.convert()))
                    .await
            })
        }

        fn get_confirmed_blocks(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            config: Option<RpcConfirmedBlocksConfigWrapper>,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            let (end_slot, maybe_commitment) =
                config.map(|config| config.unzip()).unwrap_or_default();
            info!(
                "getConfirmedBlocks rpc request received: {}-{:?}",
                start_slot, end_slot
            );
            Box::pin(async move {
                meta.get_blocks(start_slot, end_slot, commitment.or(maybe_commitment))
                    .await
            })
        }

        fn get_confirmed_blocks_with_limit(
            &self,
            meta: Self::Metadata,
            start_slot: Slot,
            limit: usize,
            commitment: Option<CommitmentConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            info!(
                "getConfirmedBlocksWithLimit rpc request received: {}-{}",
                start_slot, limit,
            );
            Box::pin(async move {
                meta.get_blocks_with_limit(start_slot, limit, commitment)
                    .await
            })
        }

        fn get_confirmed_transaction(
            &self,
            meta: Self::Metadata,
            signature_str: String,
            config: Option<RpcEncodingConfigWrapper<RpcConfirmedTransactionConfig>>,
        ) -> BoxFuture<Result<Option<EncodedConfirmedTransactionWithStatusMeta>>> {
            info!(
                "getConfirmedTransaction rpc request received: {:?}",
                signature_str
            );
            let signature = verify_signature(&signature_str);
            if let Err(err) = signature {
                return Box::pin(future::err(err));
            }
            Box::pin(async move {
                meta.get_transaction(signature.unwrap(), config.map(|config| config.convert()))
                    .await
            })
        }

        fn get_confirmed_signatures_for_address2(
            &self,
            meta: Self::Metadata,
            address: String,
            config: Option<RpcGetConfirmedSignaturesForAddress2Config>,
        ) -> BoxFuture<Result<Vec<RpcConfirmedTransactionStatusWithSignature>>> {
            let config = config.unwrap_or_default();
            let commitment = config.commitment;
            let verification = verify_and_parse_signatures_for_address_params(
                address,
                config.before,
                config.until,
                config.limit,
            );

            match verification {
                Err(err) => Box::pin(future::err(err)),
                Ok((address, before, until, limit)) => Box::pin(async move {
                    meta.get_signatures_for_address(
                        address,
                        before,
                        until,
                        limit,
                        RpcContextConfig {
                            commitment,
                            min_context_slot: None,
                        },
                    )
                        .await
                }),
            }
        }
    }
}

