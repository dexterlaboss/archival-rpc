#![allow(clippy::integer_arithmetic)]

use {
    crate::bigtable::RowKey,
    async_trait::async_trait,
    log::*,
    // solana_metrics::datapoint_info,
    solana_sdk::{
        clock::{
            Slot,
        },
        pubkey::Pubkey,
        signature::Signature,
        sysvar::is_sysvar_id,
        timing::AtomicInterval,
    },
    solana_storage_proto::convert::{generated, tx_by_addr},
    solana_transaction_status::{
        extract_and_fmt_memos, ConfirmedBlock, ConfirmedTransactionStatusWithSignature,
        ConfirmedTransactionWithStatusMeta,
        TransactionByAddrInfo,
        TransactionStatus,
        TransactionWithStatusMeta, VersionedConfirmedBlock, VersionedTransactionWithStatusMeta,
    },
    solana_storage_adapter::{
        Error, Result, LedgerStorageAdapter,
        StoredConfirmedBlock,
        TransactionInfo,
        UploadedTransaction,
        LegacyTransactionByAddrInfo,
        slot_to_blocks_key,
        slot_to_tx_by_addr_key,
        key_to_slot,
    },
    std::{
        collections::{HashMap, HashSet},
        convert::TryInto,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
};

// #[macro_use]
// extern crate solana_metrics;

// #[macro_use]
// extern crate serde_derive;

mod access_token;
mod bigtable;
mod root_ca_certificate;


impl std::convert::From<bigtable::Error> for Error {
    fn from(err: bigtable::Error) -> Self {
        Self::StorageBackendError(Box::new(err))
    }
}

pub const DEFAULT_INSTANCE_NAME: &str = "solana-ledger";
pub const DEFAULT_APP_PROFILE_ID: &str = "default";

#[derive(Debug)]
pub enum CredentialType {
    Filepath(Option<String>),
    Stringified(String),
}

#[derive(Debug)]
pub struct LedgerStorageConfig {
    pub read_only: bool,
    pub timeout: Option<std::time::Duration>,
    pub credential_type: CredentialType,
    pub instance_name: String,
    pub app_profile_id: String,
}

impl Default for LedgerStorageConfig {
    fn default() -> Self {
        Self {
            read_only: true,
            timeout: None,
            credential_type: CredentialType::Filepath(None),
            instance_name: DEFAULT_INSTANCE_NAME.to_string(),
            app_profile_id: DEFAULT_APP_PROFILE_ID.to_string(),
        }
    }
}

const METRICS_REPORT_INTERVAL_MS: u64 = 10_000;

#[derive(Default)]
struct LedgerStorageStats {
    num_queries: AtomicUsize,
    last_report: AtomicInterval,
}

impl LedgerStorageStats {
    fn increment_num_queries(&self) {
        self.num_queries.fetch_add(1, Ordering::Relaxed);
        self.maybe_report();
    }

    fn maybe_report(&self) {
        if self.last_report.should_update(METRICS_REPORT_INTERVAL_MS) {
            // datapoint_debug!(
            //     "storage-bigtable-query",
            //     (
            //         "num_queries",
            //         self.num_queries.swap(0, Ordering::Relaxed) as i64,
            //         i64
            //     )
            // );
        }
    }
}

#[derive(Clone)]
pub struct LedgerStorage {
    connection: bigtable::BigTableConnection,
    stats: Arc<LedgerStorageStats>,
}

impl LedgerStorage {
    pub async fn new(
        read_only: bool,
        timeout: Option<std::time::Duration>,
        credential_path: Option<String>,
    ) -> Result<Self> {
        Self::new_with_config(LedgerStorageConfig {
            read_only,
            timeout,
            credential_type: CredentialType::Filepath(credential_path),
            ..LedgerStorageConfig::default()
        })
            .await
    }

    pub fn new_for_emulator(
        instance_name: &str,
        app_profile_id: &str,
        endpoint: &str,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let stats = Arc::new(LedgerStorageStats::default());
        Ok(Self {
            connection: bigtable::BigTableConnection::new_for_emulator(
                instance_name,
                app_profile_id,
                endpoint,
                timeout,
            )?,
            stats,
        })
    }

    pub async fn new_with_config(config: LedgerStorageConfig) -> Result<Self> {
        let stats = Arc::new(LedgerStorageStats::default());
        let LedgerStorageConfig {
            read_only,
            timeout,
            instance_name,
            app_profile_id,
            credential_type,
        } = config;
        let connection = bigtable::BigTableConnection::new(
            instance_name.as_str(),
            app_profile_id.as_str(),
            read_only,
            timeout,
            credential_type,
        )
            .await?;
        Ok(Self { stats, connection })
    }

    pub async fn new_with_stringified_credential(credential: String) -> Result<Self> {
        Self::new_with_config(LedgerStorageConfig {
            credential_type: CredentialType::Stringified(credential),
            ..LedgerStorageConfig::default()
        })
            .await
    }

    // Fetches and gets a vector of confirmed blocks via a multirow fetch
    pub async fn get_confirmed_blocks_with_data<'a>(
        &self,
        slots: &'a [Slot],
    ) -> Result<impl Iterator<Item = (Slot, ConfirmedBlock)> + 'a> {
        trace!(
            "LedgerStorage::get_confirmed_blocks_with_data request received: {:?}",
            slots
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();
        let row_keys = slots.iter().copied().map(|slot| slot_to_blocks_key(slot, false));
        let data = bigtable
            .get_protobuf_or_bincode_cells("blocks", row_keys)
            .await?
            .filter_map(
                |(row_key, block_cell_data): (
                    RowKey,
                    bigtable::CellData<StoredConfirmedBlock, generated::ConfirmedBlock>,
                )| {
                    let block = match block_cell_data {
                        bigtable::CellData::Bincode(block) => block.into(),
                        bigtable::CellData::Protobuf(block) => block.try_into().ok()?,
                    };
                    Some((key_to_slot(&row_key).unwrap(), block))
                },
            );
        Ok(data)
    }

    /// Does the confirmed block exist in the Bigtable
    pub async fn confirmed_block_exists(&self, slot: Slot) -> Result<bool> {
        trace!(
            "LedgerStorage::confirmed_block_exists request received: {:?}",
            slot
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();

        let block_exists = bigtable
            .row_key_exists("blocks", slot_to_blocks_key(slot, false))
            .await?;

        Ok(block_exists)
    }

    // Fetches and gets a vector of confirmed transactions via a multirow fetch
    pub async fn get_confirmed_transactions(
        &self,
        signatures: &[Signature],
    ) -> Result<Vec<ConfirmedTransactionWithStatusMeta>> {
        trace!(
            "LedgerStorage::get_confirmed_transactions request received: {:?}",
            signatures
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();

        // Fetch transactions info
        let keys = signatures.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let cells = bigtable
            .get_bincode_cells::<TransactionInfo>("tx", &keys)
            .await?;

        // Collect by slot
        let mut order: Vec<(Slot, u32, String)> = Vec::new();
        let mut slots: HashSet<Slot> = HashSet::new();
        for cell in cells {
            if let (signature, Ok(TransactionInfo { slot, index, .. })) = cell {
                order.push((slot, index, signature));
                slots.insert(slot);
            }
        }

        // Fetch blocks
        let blocks = self
            .get_confirmed_blocks_with_data(&slots.into_iter().collect::<Vec<_>>())
            .await?
            .collect::<HashMap<_, _>>();

        // Extract transactions
        Ok(order
            .into_iter()
            .filter_map(|(slot, index, signature)| {
                blocks.get(&slot).and_then(|block| {
                    block
                        .transactions
                        .get(index as usize)
                        .and_then(|tx_with_meta| {
                            if tx_with_meta.transaction_signature().to_string() != *signature {
                                warn!(
                                    "Transaction info or confirmed block for {} is corrupt",
                                    signature
                                );
                                None
                            } else {
                                Some(ConfirmedTransactionWithStatusMeta {
                                    slot,
                                    tx_with_meta: tx_with_meta.clone(),
                                    block_time: block.block_time,
                                })
                            }
                        })
                })
            })
            .collect::<Vec<_>>())
    }

    // Delete a confirmed block and associated meta data.
    pub async fn delete_confirmed_block(&self, slot: Slot, dry_run: bool) -> Result<()> {
        let mut addresses: HashSet<&Pubkey> = HashSet::new();
        let mut expected_tx_infos: HashMap<String, UploadedTransaction> = HashMap::new();
        let confirmed_block = self.get_confirmed_block(slot, false).await?;
        for (index, transaction_with_meta) in confirmed_block.transactions.iter().enumerate() {
            match transaction_with_meta {
                TransactionWithStatusMeta::MissingMetadata(transaction) => {
                    let signature = transaction.signatures[0];
                    let index = index as u32;
                    let err = None;

                    for address in transaction.message.account_keys.iter() {
                        if !is_sysvar_id(address) {
                            addresses.insert(address);
                        }
                    }

                    expected_tx_infos.insert(
                        signature.to_string(),
                        UploadedTransaction { slot, index, err },
                    );
                }
                TransactionWithStatusMeta::Complete(tx_with_meta) => {
                    let VersionedTransactionWithStatusMeta { transaction, meta } = tx_with_meta;
                    let signature = transaction.signatures[0];
                    let index = index as u32;
                    let err = meta.status.clone().err();

                    for address in tx_with_meta.account_keys().iter() {
                        if !is_sysvar_id(address) {
                            addresses.insert(address);
                        }
                    }

                    expected_tx_infos.insert(
                        signature.to_string(),
                        UploadedTransaction { slot, index, err },
                    );
                }
            }
        }

        let address_slot_rows: Vec<_> = addresses
            .into_iter()
            .map(|address| format!("{}/{}", address, slot_to_tx_by_addr_key(slot)))
            .collect();

        let tx_deletion_rows = if !expected_tx_infos.is_empty() {
            let signatures = expected_tx_infos.keys().cloned().collect::<Vec<_>>();
            let fetched_tx_infos: HashMap<String, std::result::Result<UploadedTransaction, _>> =
                self.connection
                    .get_bincode_cells_with_retry::<TransactionInfo>("tx", &signatures)
                    .await?
                    .into_iter()
                    .map(|(signature, tx_info_res)| (signature, tx_info_res.map(Into::into)))
                    .collect::<HashMap<_, _>>();

            let mut deletion_rows = Vec::with_capacity(expected_tx_infos.len());
            for (signature, expected_tx_info) in expected_tx_infos {
                match fetched_tx_infos.get(&signature) {
                    Some(Ok(fetched_tx_info)) if fetched_tx_info == &expected_tx_info => {
                        deletion_rows.push(signature);
                    }
                    Some(Ok(fetched_tx_info)) => {
                        warn!(
                            "skipped tx row {} because the bigtable entry ({:?}) did not match to {:?}",
                            signature,
                            fetched_tx_info,
                            &expected_tx_info,
                        );
                    }
                    Some(Err(err)) => {
                        warn!(
                            "skipped tx row {} because the bigtable entry was corrupted: {:?}",
                            signature, err
                        );
                    }
                    None => {
                        warn!("skipped tx row {} because it was not found", signature);
                    }
                }
            }
            deletion_rows
        } else {
            vec![]
        };

        if !dry_run {
            if !address_slot_rows.is_empty() {
                self.connection
                    .delete_rows_with_retry("tx-by-addr", &address_slot_rows)
                    .await?;
            }

            if !tx_deletion_rows.is_empty() {
                self.connection
                    .delete_rows_with_retry("tx", &tx_deletion_rows)
                    .await?;
            }

            self.connection
                .delete_rows_with_retry("blocks", &[slot_to_blocks_key(slot, false)])
                .await?;
        }

        info!(
            "{}deleted ledger data for slot {}: {} transaction rows, {} address slot rows",
            if dry_run { "[dry run] " } else { "" },
            slot,
            tx_deletion_rows.len(),
            address_slot_rows.len()
        );

        Ok(())
    }
}

#[async_trait]
impl LedgerStorageAdapter for LedgerStorage {
    /// Return the available slot that contains a block
    async fn get_first_available_block(&self) -> Result<Option<Slot>> {
        trace!("LedgerStorage::get_first_available_block request received");
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();
        let blocks = bigtable.get_row_keys("blocks", None, None, 1).await?;
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
        trace!(
            "LedgerStorage::get_confirmed_blocks request received: {:?} {:?}",
            start_slot,
            limit
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();
        let blocks = bigtable
            .get_row_keys(
                "blocks",
                Some(slot_to_blocks_key(start_slot, false)),
                None,
                limit as i64,
            )
            .await?;
        Ok(blocks.into_iter().filter_map(|s| key_to_slot(&s)).collect())
    }

    /// Fetch the confirmed block from the desired slot
    async fn get_confirmed_block(&self, slot: Slot, _use_cache: bool) -> Result<ConfirmedBlock> {
        trace!(
            "LedgerStorage::get_confirmed_block request received: {:?}",
            slot
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();
        let block_cell_data = bigtable
            .get_protobuf_or_bincode_cell::<StoredConfirmedBlock, generated::ConfirmedBlock>(
                "blocks",
                slot_to_blocks_key(slot, false),
            )
            .await
            .map_err(|err| match err {
                bigtable::Error::RowNotFound => Error::BlockNotFound(slot),
                _ => err.into(),
            })?;
        Ok(match block_cell_data {
            bigtable::CellData::Bincode(block) => block.into(),
            bigtable::CellData::Protobuf(block) => block.try_into().map_err(|_err| {
                bigtable::Error::ObjectCorrupt(format!("blocks/{}", slot_to_blocks_key(slot, false)))
            })?,
        })
    }

    async fn get_signature_status(&self, signature: &Signature) -> Result<TransactionStatus> {
        trace!(
            "LedgerStorage::get_signature_status request received: {:?}",
            signature
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();
        let transaction_info = bigtable
            .get_bincode_cell::<TransactionInfo>("tx", signature.to_string())
            .await
            .map_err(|err| match err {
                bigtable::Error::RowNotFound => Error::SignatureNotFound,
                _ => err.into(),
            })?;
        Ok(transaction_info.into())
    }

    async fn get_full_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<ConfirmedTransactionWithStatusMeta>> {
        self.get_confirmed_transaction(signature).await
    }

    /// Fetch a confirmed transaction
    async fn get_confirmed_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<ConfirmedTransactionWithStatusMeta>> {
        trace!(
            "LedgerStorage::get_confirmed_transaction request received: {:?}",
            signature
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();

        // Figure out which block the transaction is located in
        let TransactionInfo { slot, index, .. } = bigtable
            .get_bincode_cell("tx", signature.to_string())
            .await
            .map_err(|err| match err {
                bigtable::Error::RowNotFound => Error::SignatureNotFound,
                _ => err.into(),
            })?;

        // Load the block and return the transaction
        let block = self.get_confirmed_block(slot, true).await?;
        match block.transactions.into_iter().nth(index as usize) {
            None => {
                // report this somewhere actionable?
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

    /// Get confirmed signatures for the provided address, in descending ledger order
    ///
    /// address: address to search for
    /// before_signature: start with the first signature older than this one
    /// until_signature: end with the last signature more recent than this one
    /// limit: stop after this many signatures; if limit==0, all records in the table will be read
    async fn get_confirmed_signatures_for_address(
        &self,
        address: &Pubkey,
        before_signature: Option<&Signature>,
        until_signature: Option<&Signature>,
        limit: usize,
    ) -> Result<
        Vec<(
            ConfirmedTransactionStatusWithSignature,
            u32, /*slot index*/
        )>,
    > {
        trace!(
            "LedgerStorage::get_confirmed_signatures_for_address request received: {:?}",
            address
        );
        self.stats.increment_num_queries();
        let mut bigtable = self.connection.client();
        let address_prefix = format!("{address}/");

        // Figure out where to start listing from based on `before_signature`
        let (first_slot, before_transaction_index) = match before_signature {
            None => (Slot::MAX, 0),
            Some(before_signature) => {
                let TransactionInfo { slot, index, .. } = bigtable
                    .get_bincode_cell("tx", before_signature.to_string())
                    .await?;

                (slot, index)
            }
        };

        // Figure out where to end listing from based on `until_signature`
        let (last_slot, until_transaction_index) = match until_signature {
            None => (0, u32::MAX),
            Some(until_signature) => {
                let TransactionInfo { slot, index, .. } = bigtable
                    .get_bincode_cell("tx", until_signature.to_string())
                    .await?;

                (slot, index)
            }
        };

        let mut infos = vec![];

        let starting_slot_tx_len = bigtable
            .get_protobuf_or_bincode_cell::<Vec<LegacyTransactionByAddrInfo>, tx_by_addr::TransactionByAddr>(
                "tx-by-addr",
                format!("{}{}", address_prefix, slot_to_tx_by_addr_key(first_slot)),
            )
            .await
            .map(|cell_data| {
                match cell_data {
                    bigtable::CellData::Bincode(tx_by_addr) => tx_by_addr.len(),
                    bigtable::CellData::Protobuf(tx_by_addr) => tx_by_addr.tx_by_addrs.len(),
                }
            })
            .unwrap_or(0);

        // Return the next tx-by-addr data of amount `limit` plus extra to account for the largest
        // number that might be flitered out
        let tx_by_addr_data = bigtable
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

        'outer: for (row_key, data) in tx_by_addr_data {
            let slot = !key_to_slot(&row_key[address_prefix.len()..]).ok_or_else(|| {
                bigtable::Error::ObjectCorrupt(format!(
                    "Failed to convert key to slot: tx-by-addr/{row_key}"
                ))
            })?;

            let deserialized_cell_data = bigtable::deserialize_protobuf_or_bincode_cell_data::<
                Vec<LegacyTransactionByAddrInfo>,
                tx_by_addr::TransactionByAddr,
            >(&data, "tx-by-addr", row_key.clone())?;

            let mut cell_data: Vec<TransactionByAddrInfo> = match deserialized_cell_data {
                bigtable::CellData::Bincode(tx_by_addr) => {
                    tx_by_addr.into_iter().map(|legacy| legacy.into()).collect()
                }
                bigtable::CellData::Protobuf(tx_by_addr) => {
                    tx_by_addr.try_into().map_err(|error| {
                        bigtable::Error::ObjectCorrupt(format!(
                            "Failed to deserialize: {}: tx-by-addr/{}",
                            error,
                            row_key.clone()
                        ))
                    })?
                }
            };

            cell_data.reverse();
            for tx_by_addr_info in cell_data.into_iter() {
                // Filter out records before `before_transaction_index`
                if slot == first_slot && tx_by_addr_info.index >= before_transaction_index {
                    continue;
                }
                // Filter out records after `until_transaction_index`
                if slot == last_slot && tx_by_addr_info.index <= until_transaction_index {
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
                if infos.len() >= limit {
                    break 'outer;
                }
            }
        }
        Ok(infos)
    }

    async fn get_latest_stored_slot(&self) -> Result<Slot> {
        Err(Error::StorageBackendError(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Method not supported",
        ))))
    }

    /// Upload a new confirmed block and associated meta data.
    async fn upload_confirmed_block(
        &self,
        slot: Slot,
        confirmed_block: VersionedConfirmedBlock,
    ) -> Result<()> {
        trace!(
            "LedgerStorage::upload_confirmed_block request received: {:?}",
            slot
        );
        let mut by_addr: HashMap<&Pubkey, Vec<TransactionByAddrInfo>> = HashMap::new();

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

        let mut _bytes_written = 0;
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
                    _bytes_written += bytes;
                }
            }
        }

        if let Some(err) = maybe_first_err {
            return Err(err);
        }

        let _num_transactions = confirmed_block.transactions.len();

        // Store the block itself last, after all other metadata about the block has been
        // successfully stored.  This avoids partial uploaded blocks from becoming visible to
        // `get_confirmed_block()` and `get_confirmed_blocks()`
        let blocks_cells = [(slot_to_blocks_key(slot, false), confirmed_block.into())];
        _bytes_written += self
            .connection
            .put_protobuf_cells_with_retry::<generated::ConfirmedBlock>("blocks", &blocks_cells)
            .await?;
        // datapoint_info!(
        //     "storage-bigtable-upload-block",
        //     ("slot", slot, i64),
        //     ("transactions", num_transactions, i64),
        //     ("bytes", bytes_written, i64),
        // );
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn LedgerStorageAdapter> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_storage_adapter::slot_to_key;

    #[test]
    fn test_slot_to_key() {
        assert_eq!(slot_to_key(0), "0000000000000000");
        assert_eq!(slot_to_key(!0), "ffffffffffffffff");
    }
}
