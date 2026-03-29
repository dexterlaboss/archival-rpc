use {
    solana_storage_adapter::{
        compression::{compress_best, decompress},
    },
    backoff::{future::retry, ExponentialBackoff},
    log::*,
    thiserror::Error,
    hbase_thrift::hbase::{BatchMutation, HbaseSyncClient, THbaseSyncClient, TScan},
    hbase_thrift::{
        MutationBuilder
    },
    thrift::{
        protocol::{
            TBinaryInputProtocol, TBinaryOutputProtocol,
        },
        transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel},
    },
    std::collections::{BTreeMap, HashMap, VecDeque},
    std::convert::TryInto,
};

pub type RowKey = String;
pub type RowData = Vec<(CellName, CellValue)>;
pub type RowDataSlice<'a> = &'a [(CellName, CellValue)];
pub type CellName = String;
pub type CellValue = Vec<u8>;
pub enum CellData<B, P> {
    Bincode(B),
    Protobuf(P),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(std::io::Error),

    #[error("Row not found")]
    RowNotFound,

    #[error("Row write failed")]
    RowWriteFailed,

    #[error("Row delete failed")]
    RowDeleteFailed,

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object is corrupt: {0}")]
    ObjectCorrupt(String),

    #[error("Timeout")]
    Timeout,

    #[error("Thrift")]
    Thrift(thrift::Error),
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl std::convert::From<thrift::Error> for Error {
    fn from(err: thrift::Error) -> Self {
        Self::Thrift(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

enum HBaseClient<'a> {
    Owned(HbaseSyncClient<InputProtocol, OutputProtocol>),
    Borrowed(&'a mut HbaseSyncClient<InputProtocol, OutputProtocol>),
}

impl<'a> HBaseClient<'a> {
    fn get(&mut self) -> &mut HbaseSyncClient<InputProtocol, OutputProtocol> {
        match self {
            HBaseClient::Owned(c) => c,
            HBaseClient::Borrowed(c) => c,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HBaseConnection {
    address: String,
    // timeout: Option<Duration>,
    namespace: Option<String>,
}

impl HBaseConnection {
    pub async fn new(
        address: &str,
        namespace: Option<&str>,
    ) -> Result<Self> {
        debug!("Creating HBase connection instance");

        Ok(Self {
            address: address.to_string(),
            namespace: namespace.map(|ns| ns.to_string()),
        })
    }

    // pub fn client(&self) -> HBase {
    //     let mut channel = TTcpChannel::new();
    //     channel.open(self.address.clone()).unwrap();
    //
    //     let (input_chan, output_chan) = channel.split().unwrap();
    //     let input_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(input_chan), true);
    //     let output_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(output_chan), true);
    //
    //     let client = HbaseSyncClient::new(input_prot, output_prot);
    //
    //     HBase {
    //         client,
    //         namespace: self.namespace.clone(),
    //     }
    // }

    pub fn client(&self) -> HBase {
        let mut channel = TTcpChannel::new();

        channel.open(self.address.clone()).unwrap();

        let (input_chan, output_chan) = channel.split().unwrap();

        let input_prot = TBinaryInputProtocol::new(
            TBufferedReadTransport::new(input_chan),
            true
        );
        let output_prot = TBinaryOutputProtocol::new(
            TBufferedWriteTransport::new(output_chan),
            true
        );

        let client = HbaseSyncClient::new(
            input_prot,
            output_prot
        );

        HBase::new_owned(client, self.namespace.clone())
    }

    pub async fn put_bincode_cells_with_retry<T>(
        &self,
        table: &str,
        cells: &[(RowKey, T)],
    ) -> Result<usize>
        where
            T: serde::ser::Serialize,
    {
        retry(ExponentialBackoff::default(), || async {
            let mut client = self.client();
            Ok(client.put_bincode_cells(table, cells).await?)
        })
            .await
    }

    pub async fn put_protobuf_cells_with_retry<T>(
        &self,
        table: &str,
        cells: &[(RowKey, T)],
    ) -> Result<usize>
        where
            T: prost::Message,
    {
        retry(ExponentialBackoff::default(), || async {
            let mut client = self.client();
            Ok(client.put_protobuf_cells(table, cells).await?)
        })
            .await
    }
}

pub type InputProtocol = TBinaryInputProtocol<TBufferedReadTransport<thrift::transport::ReadHalf<TTcpChannel>>>;
pub type OutputProtocol = TBinaryOutputProtocol<TBufferedWriteTransport<thrift::transport::WriteHalf<TTcpChannel>>>;

pub struct HBase<'a> {
    client: HBaseClient<'a>,
    namespace: Option<String>,
}

impl<'a> HBase<'a> {
    fn qualified_table_name(&self, table_name: &str) -> String {
        if let Some(namespace) = &self.namespace {
            format!("{}:{}", namespace, table_name)
        } else {
            table_name.to_string()
        }
    }

    pub fn new_owned(
        client: HbaseSyncClient<InputProtocol, OutputProtocol>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            client: HBaseClient::Owned(client),
            namespace,
        }
    }

    pub fn new_borrowed(
        client: &'a mut HbaseSyncClient<InputProtocol, OutputProtocol>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            client: HBaseClient::Borrowed(client),
            namespace,
        }
    }

    /// Get `table` row keys in lexical order.
    ///
    /// If `start_at` is provided, the row key listing will start with key.
    /// Otherwise the listing will start from the start of the table.
    ///
    /// If `end_at` is provided, the row key listing will end at the key. Otherwise it will
    /// continue until the `rows_limit` is reached or the end of the table, whichever comes first.
    /// If `rows_limit` is zero, this method will return an empty array.
    pub fn get_row_keys(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
        reversed: bool,
    ) -> Result<Vec<RowKey>> {
        if rows_limit == 0 {
            return Ok(vec![]);
        }

        debug!("Trying to get row keys in range {:?} - {:?} with limit {:?}", start_at, end_at, rows_limit);

        let qualified_name = self.qualified_table_name(table_name);

        let mut scan = TScan::default();
        scan.start_row = start_at.map(|start_key| {
            start_key.into_bytes()
        });
        scan.stop_row = end_at.map(|end_key| {
            end_key.into_bytes()
        });
        scan.columns = None;
        scan.batch_size = Some(rows_limit as i32);
        scan.timestamp = None;
        scan.caching = rows_limit.try_into().ok();
        scan.reversed = Some(reversed);
        scan.filter_string = Some(b"KeyOnlyFilter()".to_vec());

        let scan_id = self.client.get().scanner_open_with_scan(
            qualified_name.as_bytes().to_vec(),
            scan,
            BTreeMap::new(),
        )?;

        let mut results: Vec<(RowKey, RowData)> = Vec::new();
        let mut count = 0;
        loop {
            let row_results = self.client.get().scanner_get_list(scan_id, rows_limit as i32)?;
            if row_results.is_empty() {
                break;
            }
            for row_result in row_results {
                let row_key_bytes = row_result.row.unwrap();
                let row_key = String::from_utf8(row_key_bytes.clone()).unwrap();
                let mut column_values: RowData = Vec::new();
                for (key, column) in row_result.columns.unwrap_or_default() {
                    let column_value_bytes = column.value.unwrap_or_default();
                    column_values.push((String::from_utf8(key).unwrap(), column_value_bytes.into()));
                }
                results.push((row_key, column_values));
                count += 1;
                if count >= rows_limit {
                    break;
                }
            }
            if count >= rows_limit {
                break;
            }
        }

        self.client.get().scanner_close(scan_id)?;

        Ok(results.into_iter().map(|r| r.0).collect())
    }

    /// Get latest data from `table`.
    ///
    /// All column families are accepted, and only the latest version of each column cell will be
    /// returned.
    ///
    /// If `start_at` is provided, the row key listing will start with key, or the next key in the
    /// table if the explicit key does not exist. Otherwise the listing will start from the start
    /// of the table.
    ///
    /// If `end_at` is provided, the row key listing will end at the key. Otherwise it will
    /// continue until the `rows_limit` is reached or the end of the table, whichever comes first.
    /// If `rows_limit` is zero, this method will return an empty array.
    pub fn get_row_data(
        &mut self,
        table_name: &str,
        start_at: Option<RowKey>,
        end_at: Option<RowKey>,
        rows_limit: i64,
        reversed: bool,
    ) -> Result<Vec<(RowKey, RowData)>> {
        if rows_limit == 0 {
            return Ok(vec![]);
        }

        debug!("Trying to get rows in range {:?} - {:?} with limit {:?}", start_at, end_at, rows_limit);

        let qualified_name = self.qualified_table_name(table_name);

        let mut scan = TScan::default();

        scan.start_row = start_at.map(|start_key| {
            start_key.into_bytes()
        });
        scan.stop_row = end_at.map(|end_key| {
            end_key.into_bytes()
        });
        scan.columns = Some(vec!["x".as_bytes().to_vec()]);
        scan.batch_size = Some(rows_limit as i32);
        scan.timestamp = None;
        scan.caching = rows_limit.try_into().ok();
        scan.reversed = Some(reversed);
        scan.filter_string = Some(b"ColumnPaginationFilter(1,0)".to_vec());

        let scan_id = self.client.get().scanner_open_with_scan(
            qualified_name.as_bytes().to_vec(),
            scan,
            BTreeMap::new(),
        )?;
        // ).unwrap_or_else(|err| {
        //     println!("scanner_open_with_scan error: {:?}", err);
        //     std::process::exit(1);
        // });

        let mut results: Vec<(RowKey, RowData)> = Vec::new();
        let mut count = 0;

        loop {
            let row_results = self.client.get().scanner_get_list(
                scan_id,
                rows_limit as i32
            )?;
            // ).unwrap_or_else(|err| {
            //     println!("scanner_get_list error: {:?}", err);
            //     std::process::exit(1);
            // });

            if row_results.is_empty() {
                break;
            }

            for row_result in row_results {
                let row_key_bytes = row_result.row.unwrap();
                let row_key = String::from_utf8(row_key_bytes.clone()).unwrap();
                let mut column_values: RowData = Vec::new();
                for (key, column) in row_result.columns.unwrap_or_default() {
                    let column_value_bytes = column.value.unwrap_or_default();
                    column_values.push((String::from_utf8(key).unwrap(), column_value_bytes.into()));
                }
                results.push((row_key, column_values));
                count += 1;
                if count >= rows_limit {
                    break;
                }
            }
            if count >= rows_limit {
                break;
            }
        }

        self.client.get().scanner_close(scan_id)?;

        Ok(results)
    }

    pub fn get_single_row_data(
        &mut self,
        table_name: &str,
        row_key: RowKey,
    ) -> Result<RowData> {
        debug!("Trying to get row data with key {:?} from table {:?}", row_key, table_name);

        let qualified_name = self.qualified_table_name(table_name);

        let row_result = self.client.get().get_row_with_columns(
            qualified_name.as_bytes().to_vec(),
            row_key.as_bytes().to_vec(),
            vec!["x".as_bytes().to_vec()],
            BTreeMap::new(),
        )?;

        let first_row_result = &row_result.into_iter()
            .next()
            .ok_or(Error::RowNotFound)?;

        let mut result_value: RowData = vec![];
        if let Some(cols) = &first_row_result.columns {
            for (col_name, cell) in cols {
                if let Some(value) = &cell.value {
                    result_value.push((String::from_utf8(col_name.to_vec()).unwrap().to_string(), value.to_vec()));
                }
            }
        }

        Ok(result_value)
    }

    pub fn get_rows_data(
        &mut self,
        table_name: &str,
        row_keys: &[RowKey],
    ) -> Result<Vec<Option<RowData>>> {
        let qualified_name = self.qualified_table_name(table_name);

        // doesn't deduplicate row keys
        let rows_result = self.client.get().get_rows(
            qualified_name.as_bytes().to_vec(),
            row_keys
                .iter()
                .map(|row_key| row_key.as_bytes().to_vec())
                .collect(),
            BTreeMap::new(),
        )?;

        let mut rows_result_map: HashMap<RowKey, VecDeque<RowData>> =
            HashMap::with_capacity(rows_result.len());

        for row_result in rows_result {
            let row_key_bytes = row_result.row.unwrap();
            let row_key = String::from_utf8(row_key_bytes).unwrap();

            let mut column_values: RowData = Vec::new();
            for (key, column) in row_result.columns.unwrap_or_default() {
                let column_value_bytes = column.value.unwrap_or_default();
                column_values.push((String::from_utf8(key).unwrap(), column_value_bytes));
            }

            rows_result_map
                .entry(row_key)
                .or_insert_with(VecDeque::new)
                .push_back(column_values);
        }

        let results = row_keys
            .iter()
            .map(|row_key| rows_result_map.get_mut(row_key).and_then(|v| v.pop_front()))
            .collect();

        Ok(results)
    }

    pub fn get_bincode_cell<T>(&mut self, table: &str, key: RowKey) -> Result<T>
        where
            T: serde::de::DeserializeOwned,
    {
        let row_data = self.get_single_row_data(table, key.clone())?;
        deserialize_bincode_cell_data(&row_data, table, key.to_string())
    }

    pub fn get_bincode_cells<T>(
        &mut self,
        table: &str,
        keys: Vec<RowKey>,
    ) -> Result<Vec<Result<T>>>
    where
        T: serde::de::DeserializeOwned,
    {
        let rows_data = self.get_rows_data(table, &keys)?;

        Ok(rows_data
            .into_iter()
            .zip(keys.into_iter())
            .map(|(row_data, key)| match row_data {
                Some(data) => deserialize_bincode_cell_data(&data, table, key),
                None => Err(Error::RowNotFound),
            })
            .collect())
    }

    pub fn get_protobuf_or_bincode_cell<B, P>(
        &mut self,
        table: &str,
        key: RowKey,
    ) -> Result<CellData<B, P>>
        where
            B: serde::de::DeserializeOwned,
            P: prost::Message + Default,
    {
        let row_data = self.get_single_row_data(table, key.clone())?;
        deserialize_protobuf_or_bincode_cell_data(&row_data, table, key)
    }

    pub fn get_protobuf_or_bincode_cell_serialized<B, P>(
        &mut self,
        table: &str,
        key: RowKey,
    ) -> Result<RowData>
        where
            B: serde::de::DeserializeOwned,
            P: prost::Message + Default,
    {
        self.get_single_row_data(table, key.clone())
    }

    pub async fn put_bincode_cells<T>(
        &mut self,
        table: &str,
        cells: &[(RowKey, T)],
    ) -> Result<usize>
        where
            T: serde::ser::Serialize,
    {
        let mut bytes_written = 0;
        let mut new_row_data = vec![];
        for (row_key, data) in cells {
            let data = compress_best(&bincode::serialize(&data).unwrap())?;
            bytes_written += data.len();
            new_row_data.push((row_key, vec![("bin".to_string(), data)]));
        }

        self.put_row_data(table, "x", &new_row_data).await?;
        Ok(bytes_written)
    }

    pub async fn put_protobuf_cells<T>(
        &mut self,
        table: &str,
        cells: &[(RowKey, T)],
    ) -> Result<usize>
        where
            T: prost::Message,
    {
        let mut bytes_written = 0;
        let mut new_row_data = vec![];
        for (row_key, data) in cells {
            let mut buf = Vec::with_capacity(data.encoded_len());
            data.encode(&mut buf).unwrap();
            let data = compress_best(&buf)?;
            bytes_written += data.len();
            new_row_data.push((row_key, vec![("proto".to_string(), data)]));
        }

        self.put_row_data(table, "x", &new_row_data).await?;
        Ok(bytes_written)
    }

    async fn put_row_data(
        &mut self,
        table_name: &str,
        family_name: &str,
        row_data: &[(&RowKey, RowData)],
    ) -> Result<()> {
        let mut mutation_batches = Vec::new();
        for (row_key, cell_data) in row_data {
            let mut mutations = Vec::new();
            for (cell_name, cell_value) in cell_data {
                let mut mutation_builder = MutationBuilder::default();
                mutation_builder.column(family_name, cell_name);
                mutation_builder.value(cell_value.clone());
                mutations.push(mutation_builder.build());
            }
            mutation_batches.push(BatchMutation::new(Some(row_key.as_bytes().to_vec()), mutations));
        }

        self.client.get().mutate_rows(table_name.as_bytes().to_vec(), mutation_batches, Default::default())?;

        Ok(())
    }

    pub fn get_last_row_key(&mut self, table_name: &str) -> Result<String> {
        let row_keys = self.get_row_keys(table_name, None, None, 1, true)?;
        if let Some(last_row_key) = row_keys.first() {
            Ok(last_row_key.clone())
        } else {
            Err(Error::RowNotFound)
        }
    }
}

pub(crate) fn deserialize_protobuf_or_bincode_cell_data<B, P>(
    row_data: RowDataSlice,
    table: &str,
    key: RowKey,
) -> Result<CellData<B, P>>
    where
        B: serde::de::DeserializeOwned,
        P: prost::Message + Default,
{
    match deserialize_protobuf_cell_data(row_data, table, key.to_string()) {
        Ok(result) => {
            return Ok(CellData::Protobuf(result))
        },
        Err(err) => {
            match err {
                Error::ObjectNotFound(_) => {}
                _ => return Err(err),
            }
        },
    }
    deserialize_bincode_cell_data(row_data, table, key).map(CellData::Bincode)
}

pub(crate) fn deserialize_protobuf_cell_data<T>(
    row_data: RowDataSlice,
    table: &str,
    key: RowKey,
) -> Result<T>
    where
        T: prost::Message + Default,
{
    let value = &row_data
        .iter()
        .find(|(name, _)| name == "x:proto")
        .ok_or_else(|| Error::ObjectNotFound(format!("{table}/{key}")))?
        .1;

    let data = decompress(value)?;
    T::decode(&data[..]).map_err(|err| {
        warn!("Failed to deserialize {}/{}: {}", table, key, err);
        Error::ObjectCorrupt(format!("{table}/{key}"))
    })
}

pub(crate) fn deserialize_bincode_cell_data<T>(
    row_data: RowDataSlice,
    table: &str,
    key: RowKey,
) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
{
    let value = &row_data
        .iter()
        .find(|(name, _)| name == "x:bin")
        .ok_or_else(|| Error::ObjectNotFound(format!("{table}/{key}")))?
        .1;

    let data = decompress(value)?;
    bincode::deserialize(&data).map_err(|err| {
        warn!("Failed to deserialize {}/{}: {}", table, key, err);
        Error::ObjectCorrupt(format!("{table}/{key}"))
    })
}
