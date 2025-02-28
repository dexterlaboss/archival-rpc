use {
    crate::{
        request_processor::{
            JsonRpcRequestProcessor,
            RpcBlockCheck,
            verify_signature,
            verify_and_parse_signatures_for_address_params,
        },
        deprecated::*,
    },
    jsonrpc_core::{
        futures::future,
        BoxFuture, Error, Metadata, Result
    },
    jsonrpc_derive::rpc,
    solana_rpc_client_api::{
        config::{
            RpcContextConfig,
            RpcEncodingConfigWrapper,
            RpcBlocksConfigWrapper,
            RpcSignaturesForAddressConfig,
            RpcTransactionConfig,
            RpcBlockConfig,
            RpcSignatureStatusConfig,

        },
        request::{
            MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS,
        },
        response::{Response as RpcResponse, *},
    },
    solana_clock::{
        Slot,
        UnixTimestamp,
    },
    solana_commitment_config::{
        CommitmentConfig,
    },
    solana_signature::{
        Signature,
    },
    solana_transaction_status_client_types::{
        EncodedConfirmedTransactionWithStatusMeta,
        TransactionStatus,
        UiConfirmedBlock,
    },
};

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
            wrapper: Option<RpcBlocksConfigWrapper>,
            config: Option<RpcContextConfig>,
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
            wrapper: Option<RpcBlocksConfigWrapper>,
            config: Option<RpcContextConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            let (end_slot, maybe_config) =
                wrapper.map(|wrapper| wrapper.unzip()).unwrap_or_default();
            debug!(
                "get_blocks rpc request received: {}-{:?}",
                start_slot, end_slot
            );
            Box::pin(async move {
                meta.get_blocks(start_slot, end_slot, config.or(maybe_config))
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
            wrapper: Option<RpcBlocksConfigWrapper>,
            config: Option<RpcContextConfig>,
            // config: Option<RpcConfirmedBlocksConfigWrapper>,
            // commitment: Option<CommitmentConfig>,
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
            wrapper: Option<RpcBlocksConfigWrapper>,
            config: Option<RpcContextConfig>,
            // wrapper: Option<RpcBlocksConfigWrapper>,
            // config: Option<RpcContextConfig>,
        ) -> BoxFuture<Result<Vec<Slot>>> {
            let (end_slot, maybe_config) =
                wrapper.map(|wrapper| wrapper.unzip()).unwrap_or_default();
            info!(
                "getConfirmedBlocks rpc request received: {}-{:?}",
                start_slot, end_slot
            );
            Box::pin(async move {
                meta.get_blocks(start_slot, end_slot, config.or(maybe_config))
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

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use {
        super::{
            // storage_rpc_deprecated_v1_7::*,
            storage_rpc_full::*, storage_rpc_minimal::*, *,
        },
        solana_sdk::{
            system_transaction,
            signature::{Keypair, Signer},
            hash::{hash, Hash},
        },
        serde::de::DeserializeOwned,
        jsonrpc_core::{futures, ErrorCode, MetaIoHandler, Output, Response, Value},
    };
    use crate::request_processor::{create_validator_exit, verify_pubkey, JsonRpcConfig};

    fn create_test_request(method: &str, params: Option<serde_json::Value>) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": method,
            "params": params,
        })
    }

    fn parse_success_result<T: DeserializeOwned>(response: Response) -> T {
        if let Response::Single(output) = response {
            match output {
                Output::Success(success) => serde_json::from_value(success.result).unwrap(),
                Output::Failure(failure) => {
                    panic!("Expected success but received: {failure:?}");
                }
            }
        } else {
            panic!("Expected single response");
        }
    }

    fn parse_failure_response(response: Response) -> (i64, String) {
        if let Response::Single(output) = response {
            match output {
                Output::Success(success) => {
                    panic!("Expected failure but received: {success:?}");
                }
                Output::Failure(failure) => (failure.error.code.code(), failure.error.message),
            }
        } else {
            panic!("Expected single response");
        }
    }

    struct RpcHandler {
        io: MetaIoHandler<JsonRpcRequestProcessor>,
        meta: JsonRpcRequestProcessor,
    }

    impl RpcHandler {
        fn start() -> Self {
            Self::start_with_config(JsonRpcConfig {
                enable_rpc_transaction_history: true,
                ..JsonRpcConfig::default()
            })
        }

        fn start_with_config(config: JsonRpcConfig) -> Self {
            let exit = Arc::new(AtomicBool::new(false));
            let validator_exit = create_validator_exit(&exit);

            let meta = JsonRpcRequestProcessor::new(
                config,
                validator_exit,
                None,
                None,
            )
                .0;

            let mut io = MetaIoHandler::default();
            io.extend_with(storage_rpc_minimal::MinimalImpl.to_delegate());
            io.extend_with(storage_rpc_full::FullImpl.to_delegate());
            // io.extend_with(storage_rpc_deprecated_v1_7::DeprecatedV1_7Impl.to_delegate());
            Self {
                io,
                meta,
            }
        }

        fn handle_request_sync(&self, req: serde_json::Value) -> Response {
            let response = &self
                .io
                .handle_request_sync(&req.to_string(), self.meta.clone())
                .expect("no response");
            serde_json::from_str(response).expect("failed to deserialize response")
        }
    }

    #[test]
    fn test_rpc_verify_pubkey() {
        let pubkey = solana_sdk::pubkey::new_rand();
        assert_eq!(verify_pubkey(&pubkey.to_string()).unwrap(), pubkey);
        let bad_pubkey = "a1b2c3d4";
        assert_eq!(
            verify_pubkey(bad_pubkey),
            Err(Error::invalid_params("Invalid param: WrongSize"))
        );
    }

    #[test]
    fn test_rpc_verify_signature() {
        let tx = system_transaction::transfer(
            &Keypair::new(),
            &solana_sdk::pubkey::new_rand(),
            20,
            hash(&[0]),
        );
        assert_eq!(
            verify_signature(&tx.signatures[0].to_string()).unwrap(),
            tx.signatures[0]
        );
        let bad_signature = "a1b2c3d4";
        assert_eq!(
            verify_signature(bad_signature),
            Err(Error::invalid_params("Invalid param: WrongSize"))
        );
    }

    #[test]
    fn test_rpc_get_version() {
        let rpc = RpcHandler::start();
        let request = create_test_request("getVersion", None);
        let result: Value = parse_success_result(rpc.handle_request_sync(request));
        let expected = {
            let version = solana_version::Version::default();
            json!({
                "solana-core": version.to_string(),
                "feature-set": version.feature_set,
            })
        };
        assert_eq!(result, expected);
    }
}