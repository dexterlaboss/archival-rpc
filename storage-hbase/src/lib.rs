#![allow(clippy::integer_arithmetic)]

use {
    crate::{
        hbase::{
            RowData,
            deserialize_protobuf_or_bincode_cell_data,
        },
        compression::{decompress},
    },
    async_trait::async_trait,
    log::*,
    solana_metrics::{datapoint_info, inc_new_counter_debug},
    solana_sdk::{
        clock::{
            Slot,
        },
        pubkey::Pubkey,
        signature::Signature,
        sysvar::is_sysvar_id,
    },
    solana_storage_proto::convert::{generated, tx_by_addr},
    solana_transaction_status::{
        extract_memos::extract_and_fmt_memos, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta,
        TransactionByAddrInfo,
        TransactionStatus,
        VersionedConfirmedBlock, VersionedTransactionWithStatusMeta,
    },
    solana_storage_adapter::{
        Error, Result, LedgerStorageAdapter,
        StoredConfirmedBlock,
        StoredConfirmedTransactionWithStatusMeta,
        TransactionInfo,
        LegacyTransactionByAddrInfo,
        slot_to_blocks_key,
        slot_to_tx_by_addr_key,
        key_to_slot,
    },
    moka::sync::Cache,
    std::{
        collections::{
            HashMap,
        },
        convert::{TryInto},
        time::{Duration, Instant},
        boxed::Box,
        num::NonZeroUsize,
    },
    memcache::Client as MemcacheClient,
    bincode,
    tokio::task,
};

#[macro_use]
extern crate solana_metrics;

#[macro_use]
extern crate serde_derive;

mod hbase;
mod compression;


impl std::convert::From<hbase::Error> for Error {
    fn from(err: hbase::Error) -> Self {
        Self::StorageBackendError(Box::new(err))
    }
}

#[derive(Debug)]
pub struct CacheErrorWrapper(pub memcache::MemcacheError);

impl From<CacheErrorWrapper> for Error {
    fn from(err: CacheErrorWrapper) -> Self {
        Error::CacheError(err.0.to_string())  // Convert the wrapped error into a string and store it in CacheError
    }
}

pub const DEFAULT_ADDRESS: &str = "127.0.0.1:9090";
pub const DEFAULT_CACHE_ADDRESS: &str = "127.0.0.1:11211";

#[derive(Debug)]
pub struct LedgerStorageConfig {
    pub read_only: bool,
    pub timeout: Option<std::time::Duration>,
    pub address: String,
    pub block_cache: Option<NonZeroUsize>,
    pub use_md5_row_key_salt: bool,
    pub enable_full_tx_cache: bool,
    pub cache_address: Option<String>,
}

impl Default for LedgerStorageConfig {
    fn default() -> Self {
        Self {
            read_only: true,
            timeout: None,
            address: DEFAULT_ADDRESS.to_string(),
            block_cache: None,
            use_md5_row_key_salt: false,
            enable_full_tx_cache: false,
            cache_address: Some(DEFAULT_ADDRESS.to_string())
        }
    }
}

#[derive(Clone)]
pub struct LedgerStorage {
    connection: hbase::HBaseConnection,
    cache: Option<Cache<Slot, RowData>>,
    use_md5_row_key_salt: bool,
    cache_client: Option<MemcacheClient>,
}

impl LedgerStorage {
    pub async fn new(
        read_only: bool,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self> {
        Self::new_with_config(LedgerStorageConfig {
            read_only,
            timeout,
            ..LedgerStorageConfig::default()
        })
            .await
    }

    pub async fn new_with_config(config: LedgerStorageConfig) -> Result<Self> {
        let LedgerStorageConfig {
            read_only,
            timeout,
            address,
            block_cache,
            use_md5_row_key_salt,
            enable_full_tx_cache,
            mut cache_address
        } = config;
        let connection = hbase::HBaseConnection::new(
            address.as_str(),
            read_only,
            timeout,
        )
            .await?;

        let cache_client = if enable_full_tx_cache {
            if let Some(cache_addr) = cache_address {
                let cache_addr = format!("memcache://{}?timeout=1&protocol=ascii", cache_addr);

                let cache_addr_clone = cache_addr.clone();

                match task::spawn_blocking(move || MemcacheClient::connect(cache_addr_clone.as_str())).await {
                    Ok(Ok(client)) => Some(client),
                    Ok(Err(e)) => {
                        error!("Failed to connect to cache server at {}: {}", cache_addr, e);
                        None
                    },
                    Err(e) => {
                        error!("Tokio task join error while connecting to cache server: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let cache = if let Some(capacity) = block_cache {
            // let lru_cache = LruCache::new(capacity);
            // Some(Arc::new(Mutex::new(lru_cache)))
            let lru_cache = Cache::new(capacity.get() as u64);
            Some(lru_cache)
        } else {
            None
        };

        Ok(Self {
            connection,
            cache,
            use_md5_row_key_salt,
            cache_client,
        })
    }
}

#[async_trait]
impl LedgerStorageAdapter for LedgerStorage {
    /// Return the available slot that contains a block
    async fn get_first_available_block(&self) -> Result<Option<Slot>> {
        debug!("LedgerStorage::get_first_available_block request received");

        if self.use_md5_row_key_salt {
            return Ok(Some(0));
        }

        inc_new_counter_debug!("storage-hbase-query", 1);
        let mut hbase = self.connection.client();
        let blocks = hbase.get_row_keys("blocks", None, None, 1, false).await?;
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(key_to_slot(&blocks[0]))
    }

    /// Fetch the next slots after the provided slot that contains a block
    ///
    /// start_slot: slot to start the search from (inclusive)
    /// limit: stop after this many slots have been found
    async fn get_confirmed_blocks(&self, start_slot: Slot, limit: usize) -> Result<Vec<Slot>> {
        debug!(
            "LedgerStorage::get_confirmed_blocks request received: {:?} {:?}",
            start_slot, limit
        );

        if self.use_md5_row_key_salt {
            return Ok(vec![]);
        }

        inc_new_counter_debug!("storage-hbase-query", 1);
        let mut hbase = self.connection.client();
        let blocks = hbase
            .get_row_keys(
                "blocks",
                Some(slot_to_blocks_key(start_slot, false)),
                Some(slot_to_blocks_key(start_slot + limit as u64, false)), // None,
                limit as i64,
                false
            )
            .await?;
        Ok(blocks.into_iter().filter_map(|s| key_to_slot(&s)).collect())
    }

    /// Fetch the confirmed block from the desired slot
    async fn get_confirmed_block(&self, slot: Slot, use_cache: bool) -> Result<ConfirmedBlock> {
        debug!(
            "LedgerStorage::get_confirmed_block request received: {:?}",
            slot
        );
        inc_new_counter_debug!("storage-hbase-query", 1);

        let start = Instant::now();
        let mut hbase = self.connection.client();
        let duration: Duration = start.elapsed();
        debug!("HBase connection took {:?}", duration);

        if use_cache {
            if let Some(cache) = &self.cache {
                if let Some(serialized_block) = cache.get(&slot) {
                    // print_cache_info(&locked_cache);
                    debug!("Using result from cache for {}", slot);
                    let block_cell_data =
                        deserialize_protobuf_or_bincode_cell_data::<StoredConfirmedBlock, generated::ConfirmedBlock>(
                            &serialized_block,
                            "blocks",
                            slot_to_blocks_key(slot, self.use_md5_row_key_salt)
                        )
                            .map_err(|err| match err {
                                hbase::Error::RowNotFound => Error::BlockNotFound(slot),
                                _ => err.into(),
                            })?;

                    let block: ConfirmedBlock = match block_cell_data {
                        hbase::CellData::Bincode(block) => block.into(),
                        hbase::CellData::Protobuf(block) => block.try_into().map_err(|_err| {
                            error!("Protobuf object is corrupted");
                            hbase::Error::ObjectCorrupt(format!("blocks/{}", slot_to_blocks_key(slot, self.use_md5_row_key_salt)))
                        })?,
                    };

                    return Ok(block.clone());
                } else {
                    // print_cache_info(&locked_cache);
                }
            }
        }

        let block_cell_data_serialized = hbase
            .get_protobuf_or_bincode_cell_serialized::<StoredConfirmedBlock, generated::ConfirmedBlock>(
                "blocks",
                slot_to_blocks_key(slot, self.use_md5_row_key_salt),
            )
            .await
            .map_err(|err| {
                match err {
                    hbase::Error::RowNotFound => Error::BlockNotFound(slot),
                    _ => err.into(),
                }
            })?;

        let block_cell_data =
            deserialize_protobuf_or_bincode_cell_data::<StoredConfirmedBlock, generated::ConfirmedBlock>(
                &block_cell_data_serialized,
                "blocks",
                slot_to_blocks_key(slot, self.use_md5_row_key_salt),
            )?;

        let block: ConfirmedBlock = match block_cell_data {
            hbase::CellData::Bincode(block) => block.into(),
            hbase::CellData::Protobuf(block) => block.try_into().map_err(|_err| {
                error!("Protobuf object is corrupted");
                hbase::Error::ObjectCorrupt(format!("blocks/{}", slot_to_blocks_key(slot, self.use_md5_row_key_salt)))
            })?,
        };

        if use_cache {
            if let Some(cache) = &self.cache {
                debug!("Storing block {} in cache", slot);
                cache.insert(slot, block_cell_data_serialized.clone());
            }
        }

        Ok(block)
    }

    // async fn get_full_tx(&self, signature: &Signature) -> Result<ConfirmedTransaction> {
    //     let mut hbase = self.connection.client();
    //     let transaction = hbase
    //         .get_protobuf_or_bincode_cell::<StoredConfirmedBlockTransaction, generated::ConfirmedTransaction>(
    //             "tx_full",
    //             signature.to_string(),
    //         )
    //         .await
    //         .map_err(|err| match err {
    //             hbase::Error::RowNotFound => Error::SignatureNotFound,
    //             _ => err.into(),
    //         })?;
    //     Ok(transaction.into())
    // }

    async fn get_signature_status(&self, signature: &Signature) -> Result<TransactionStatus> {
        debug!(
            "LedgerStorage::get_signature_status request received: {:?}",
            signature
        );
        inc_new_counter_debug!("storage-hbase-query", 1);
        let mut hbase = self.connection.client();
        let transaction_info = hbase
            .get_bincode_cell::<TransactionInfo>("tx", signature.to_string())
            .await
            .map_err(|err| match err {
                hbase::Error::RowNotFound => Error::SignatureNotFound,
                _ => err.into(),
            })?;
        Ok(transaction_info.into())
    }

    async fn get_full_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<ConfirmedTransactionWithStatusMeta>> {
        debug!(
            "LedgerStorage::get_full_transaction request received: {:?}",
            signature
        );
        inc_new_counter_debug!("storage-hbase-query", 1);

        let mut hbase = self.connection.client();

        let tx_cell_data = hbase
            .get_protobuf_or_bincode_cell::<StoredConfirmedTransactionWithStatusMeta, generated::ConfirmedTransactionWithStatusMeta>(
                "tx_full",
                signature.to_string(),
            )
            .await
            .map_err(|err| match err {
                hbase::Error::RowNotFound => Error::SignatureNotFound,
                _ => err.into(),
            })?;

        Ok(match tx_cell_data {
            hbase::CellData::Bincode(tx) => Some(tx.into()),
            hbase::CellData::Protobuf(tx) => Some(tx.try_into().map_err(|_err| {
                error!("Protobuf object is corrupted");
                hbase::Error::ObjectCorrupt(format!("tx_full/{}", signature.to_string()))
            })?),
        })
    }

    /// Fetch a confirmed transaction
    async fn get_confirmed_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<ConfirmedTransactionWithStatusMeta>> {
        debug!(
        "LedgerStorage::get_confirmed_transaction request received: {:?}",
        signature
    );
        if let Some(cache_client) = &self.cache_client {
            match get_cached_transaction::<generated::ConfirmedTransactionWithStatusMeta>(cache_client, signature).await {
                Ok(Some(tx)) => {
                    if let Ok(confirmed_tx) = tx.try_into() {
                        return Ok(Some(confirmed_tx));
                    } else {
                        warn!(
                            "Cached protobuf object is corrupted for transaction {}",
                            signature.to_string()
                        );
                    }
                }
                Ok(None) => {
                    debug!("Transaction {} not found in cache", signature);
                }
                Err(e) => {
                    warn!("Failed to read transaction from cache for {}: {:?}",signature, e);
                }
            }
        }

        inc_new_counter_debug!("storage-hbase-query", 1);

        if let Ok(Some(full_tx)) = self.get_full_transaction(signature).await {
            return Ok(Some(full_tx));
        }

        let mut hbase = self.connection.client();

        // Figure out which block the transaction is located in
        let TransactionInfo { slot, index, .. } = hbase
            .get_bincode_cell("tx", signature.to_string())
            .await
            .map_err(|err| match err {
                hbase::Error::RowNotFound => Error::SignatureNotFound,
                _ => Error::StorageBackendError(Box::new(err)),
            })?;

        // Load the block and return the transaction
        let block = self.get_confirmed_block(slot, true).await?;
        match block.transactions.into_iter().nth(index as usize) {
            None => {
                warn!("Transaction info for {} is corrupt", signature);
                Ok(None)
            }
            Some(tx_with_meta) => {
                if tx_with_meta.transaction_signature() != signature {
                    warn!(
                    "Transaction info or confirmed block for {} is corrupt",
                    signature
                );
                    Ok(None)
                } else {
                    Ok(Some(ConfirmedTransactionWithStatusMeta {
                        slot,
                        tx_with_meta,
                        block_time: block.block_time,
                    }))
                }
            }
        }
    }

    async fn get_confirmed_signatures_for_address(
        &self,
        address: &Pubkey,
        before_signature: Option<&Signature>,
        until_signature: Option<&Signature>,
        limit: usize,
    ) -> Result<
        Vec<(
            ConfirmedTransactionStatusWithSignature,
            u32,
        )>,
    > {
        // info!(
        //     "LedgerStorage::get_confirmed_signatures_for_address: {:?}",
        //     address
        // );
        // info!("Using signature range [before: {:?}, until: {:?}]", before_signature.clone(), until_signature.clone());

        inc_new_counter_debug!("storage-hbase-query", 1);
        let mut hbase = self.connection.client();
        let address_prefix = format!("{address}/");

        // Figure out where to start listing from based on `before_signature`
        let (first_slot, before_transaction_index, before_fallback) = match before_signature {
            None => (Slot::MAX, 0, false),
            Some(before_signature) => {
                // Try fetching from `tx` first
                match hbase.get_bincode_cell("tx", before_signature.to_string()).await {
                    Ok(TransactionInfo { slot, index, .. }) => (slot, index, false),
                    // Fallback to `tx_full` if `tx` is not found
                    Err(hbase::Error::RowNotFound) => {
                        match self.get_full_transaction(before_signature).await? {
                            Some(full_transaction) => (full_transaction.slot, 0, true),
                            None => return Ok(vec![]),
                        }
                    },
                    Err(err) => return Err(err.into()),
                }
            }
        };

        debug!("Got starting slot: {:?}, index: {:?}, using tx_full fallback: {:?}",
            first_slot.clone(),
            before_transaction_index.clone(),
            before_fallback
        );

        // Figure out where to end listing from based on `until_signature`
        let (last_slot, until_transaction_index, until_fallback) = match until_signature {
            None => (0, u32::MAX, false),
            Some(until_signature) => {
                // Try fetching from `tx` first
                match hbase.get_bincode_cell("tx", until_signature.to_string()).await {
                    Ok(TransactionInfo { slot, index, .. }) => (slot, index, false),
                    // Fallback to `tx_full` if `tx` is not found
                    Err(hbase::Error::RowNotFound) => {
                        match self.get_full_transaction(until_signature).await? {
                            Some(full_transaction) => (full_transaction.slot, 0, true),
                            None => return Ok(vec![]),
                        }
                    },
                    Err(err) => return Err(err.into()),
                }
            }
        };

        debug!("Got ending slot: {:?}, index: {:?}, using tx_full fallback: {:?}",
            last_slot.clone(),
            until_transaction_index.clone(),
            until_fallback
        );

        let mut infos = vec![];

        debug!("Getting the starting slot length from tx-by-addr");

        let starting_slot_tx_len = hbase
            .get_protobuf_or_bincode_cell::<Vec<LegacyTransactionByAddrInfo>, tx_by_addr::TransactionByAddr>(
                "tx-by-addr",
                format!("{}{}", address_prefix, slot_to_tx_by_addr_key(first_slot)),
            )
            .await
            .map(|cell_data| {
                match cell_data {
                    hbase::CellData::Bincode(tx_by_addr) => tx_by_addr.len(),
                    hbase::CellData::Protobuf(tx_by_addr) => tx_by_addr.tx_by_addrs.len(),
                }
            })
            .unwrap_or(0);

        debug!("Got starting slot tx len: {:?}", starting_slot_tx_len);

        // Return the next tx-by-addr data of amount `limit` plus extra to account for the largest
        // number that might be flitered out
        let tx_by_addr_data = hbase
            .get_row_data(
                "tx-by-addr",
                Some(format!(
                    "{}{}",
                    address_prefix,
                    slot_to_tx_by_addr_key(first_slot),
                )),
                Some(format!(
                    "{}{}",
                    address_prefix,
                    slot_to_tx_by_addr_key(last_slot),
                )),
                limit as i64 + starting_slot_tx_len as i64,
            )
            .await?;

        debug!("Loaded {:?} tx-by-addr entries", tx_by_addr_data.len());

        'outer: for (row_key, data) in tx_by_addr_data {
            let slot = !key_to_slot(&row_key[address_prefix.len()..]).ok_or_else(|| {
                hbase::Error::ObjectCorrupt(format!(
                    "Failed to convert key to slot: tx-by-addr/{row_key}"
                ))
            })?;

            debug!("Deserializing tx-by-addr result data");

            let deserialized_cell_data = hbase::deserialize_protobuf_or_bincode_cell_data::<
                Vec<LegacyTransactionByAddrInfo>,
                tx_by_addr::TransactionByAddr,
            >(&data, "tx-by-addr", row_key.clone())?;

            let mut cell_data: Vec<TransactionByAddrInfo> = match deserialized_cell_data {
                hbase::CellData::Bincode(tx_by_addr) => {
                    tx_by_addr.into_iter().map(|legacy| legacy.into()).collect()
                }
                hbase::CellData::Protobuf(tx_by_addr) => {
                    tx_by_addr.try_into().map_err(|error| {
                        hbase::Error::ObjectCorrupt(format!(
                            "Failed to deserialize: {}: tx-by-addr/{}",
                            error,
                            row_key.clone()
                        ))
                    })?
                }
            };

            cell_data.reverse();

            debug!("Filtering the result data");

            for tx_by_addr_info in cell_data.into_iter() {
                // Filter out records before `before_transaction_index`
                if !before_fallback && slot == first_slot && tx_by_addr_info.index >= before_transaction_index {
                    continue;
                }

                // Filter out records after `until_transaction_index` unless fallback was used
                if !until_fallback && slot == last_slot && tx_by_addr_info.index <= until_transaction_index {
                    continue;
                }

                infos.push((
                    ConfirmedTransactionStatusWithSignature {
                        signature: tx_by_addr_info.signature,
                        slot,
                        err: tx_by_addr_info.err,
                        memo: tx_by_addr_info.memo,
                        block_time: tx_by_addr_info.block_time,
                    },
                    tx_by_addr_info.index,
                ));
                // Respect limit
                debug!("Checking the limit: {:?}/{:?}", infos.len(), limit);
                if infos.len() >= limit {
                    debug!("Limit was reached, exiting loop");
                    break 'outer;
                }
            }
        }

        debug!("Returning {:?} result entries", infos.len());

        Ok(infos)
    }

    async fn get_latest_stored_slot(&self) -> Result<Slot> {
        // inc_new_counter_debug!("storage-hbase-query", 1);
        let mut hbase = self.connection.client();
        match hbase.get_last_row_key("blocks").await {
            Ok(last_row_key) => {
                match key_to_slot(&last_row_key) {
                    Some(slot) => Ok(slot),
                    None => Err(Error::StorageBackendError(Box::new(hbase::Error::ObjectCorrupt(format!(
                        "Failed to parse row key '{}' as slot number",
                        last_row_key
                    ))))),
                }
            },
            Err(hbase::Error::RowNotFound) => {
                // If the table is empty, return a default value (e.g., first_slot - 1)
                Ok(Slot::default())
            },
            Err(e) => Err(Error::StorageBackendError(Box::new(e))),
        }
    }

    async fn upload_confirmed_block(
        &self,
        slot: Slot,
        confirmed_block: VersionedConfirmedBlock,
    ) -> Result<()> {
        let mut by_addr: HashMap<&Pubkey, Vec<TransactionByAddrInfo>> = HashMap::new();

        // println!("Uploading block: {:?}", confirmed_block);

        let mut tx_cells = vec![];
        for (index, transaction_with_meta) in confirmed_block.transactions.iter().enumerate() {
            let VersionedTransactionWithStatusMeta { meta, transaction } = transaction_with_meta;
            let err = meta.status.clone().err();
            let index = index as u32;
            let signature = transaction.signatures[0];
            let memo = extract_and_fmt_memos(transaction_with_meta);

            for address in transaction_with_meta.account_keys().iter() {
                if !is_sysvar_id(address) {
                    by_addr
                        .entry(address)
                        .or_default()
                        .push(TransactionByAddrInfo {
                            signature,
                            err: err.clone(),
                            index,
                            memo: memo.clone(),
                            block_time: confirmed_block.block_time,
                        });
                }
            }

            tx_cells.push((
                signature.to_string(),
                TransactionInfo {
                    slot,
                    index,
                    err,
                    // memo,
                },
            ));
        }

        let tx_by_addr_cells: Vec<_> = by_addr
            .into_iter()
            .map(|(address, transaction_info_by_addr)| {
                (
                    format!("{}/{}", address, slot_to_tx_by_addr_key(slot)),
                    tx_by_addr::TransactionByAddr {
                        tx_by_addrs: transaction_info_by_addr
                            .into_iter()
                            .map(|by_addr| by_addr.into())
                            .collect(),
                    },
                )
            })
            .collect();

        let mut tasks = vec![];

        if !tx_cells.is_empty() {
            let conn = self.connection.clone();
            tasks.push(tokio::spawn(async move {
                conn.put_bincode_cells_with_retry::<TransactionInfo>("tx", &tx_cells)
                    .await
            }));
        }

        if !tx_by_addr_cells.is_empty() {
            let conn = self.connection.clone();
            tasks.push(tokio::spawn(async move {
                conn.put_protobuf_cells_with_retry::<tx_by_addr::TransactionByAddr>(
                    "tx-by-addr",
                    &tx_by_addr_cells,
                )
                    .await
            }));
        }

        let mut bytes_written = 0;
        let mut maybe_first_err: Option<Error> = None;

        let results = futures::future::join_all(tasks).await;
        for result in results {
            match result {
                Err(err) => {
                    if maybe_first_err.is_none() {
                        maybe_first_err = Some(Error::TokioJoinError(err));
                    }
                }
                Ok(Err(err)) => {
                    if maybe_first_err.is_none() {
                        maybe_first_err = Some(Error::StorageBackendError(Box::new(err)));
                    }
                }
                Ok(Ok(bytes)) => {
                    bytes_written += bytes;
                }
            }
        }

        if let Some(err) = maybe_first_err {
            return Err(err);
        }

        let num_transactions = confirmed_block.transactions.len();

        // Store the block itself last, after all other metadata about the block has been
        // successfully stored.  This avoids partial uploaded blocks from becoming visible to
        // `get_confirmed_block()` and `get_confirmed_blocks()`
        let blocks_cells = [(slot_to_blocks_key(slot, self.use_md5_row_key_salt), confirmed_block.into())];
        bytes_written += self
            .connection
            .put_protobuf_cells_with_retry::<generated::ConfirmedBlock>("blocks", &blocks_cells)
            .await?;
        datapoint_info!(
            "storage-hbase-upload-block",
            ("slot", slot, i64),
            ("transactions", num_transactions, i64),
            ("bytes", bytes_written, i64),
        );
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn LedgerStorageAdapter> {
        Box::new(self.clone())
    }
}

async fn get_cached_transaction<P>(
    cache_client: &MemcacheClient,
    signature: &Signature,
) -> Result<Option<P>>
    where
        P: prost::Message + Default
{
    let key = signature.to_string();
    let key_clone = key.clone();
    let cache_client_clone = cache_client.clone();

    let result = tokio::task::spawn_blocking(move || {
        cache_client_clone.get::<Vec<u8>>(&key_clone).map_err(CacheErrorWrapper)
    })
        .await
        .map_err(Error::TokioJoinError)??;

    if let Some(cached_bytes) = result {
        // Decompress the cached data
        let data = decompress(&cached_bytes).map_err(|e| {
            warn!("Failed to decompress transaction from cache for {}", key);
            Error::CacheError(format!("Decompression error: {}", e))
        })?;

        // Deserialize the data using protobuf instead of bincode
        let tx = P::decode(&data[..]).map_err(|e| {
            warn!("Failed to deserialize transaction from cache for {}", key);
            Error::CacheError(format!("Protobuf deserialization error: {}", e))
        })?;

        debug!("Transaction {} found in cache", key);
        return Ok(Some(tx));
    }

    Ok(None)
}

// pub async fn get_cached_transaction<P>(
//     cache_client: &Client,
//     signature: &str,
// ) -> Result<P>
//     where
//         P: prost::Message + Default,
// {
//     let data = decompress(value)?;
//     P::decode(&data[..]).map_err(|err| {
//         warn!("Failed to deserialize {}/{}: {}", table, key, err);
//         Error::ObjectCorrupt(format!("{table}/{key}"))
//     })
// }

// fn print_cache_info(cache: &MutexGuard<LruCache<Slot, ConfirmedBlock>>) {
//     if cache.len() > 0 {
//         println!("Cache Size: {}", cache.len());
//         println!("Cache Keys: {:?}", cache.iter().map(|(k, _v)| *k).collect::<Vec<_>>());
//     } else {
//         println!("Cache is empty");
//     }
// }

