
use {
    crate::{
        rpc::{
            storage_rpc_deprecated_v1_7::*,
            storage_rpc_full::*,
            storage_rpc_minimal::*,
            *,
        },
        request_processor::{
            *,
        },
        middleware::{
            MetricsMiddleware,
            RpcRequestMiddleware,
        },
    },
    solana_metrics::Metrics,
    crossbeam_channel::unbounded,
    jsonrpc_core::{
        MetaIoHandler,
    },
    jsonrpc_http_server::{
        hyper, AccessControlAllowOrigin, CloseHandle, DomainsValidation,
        ServerBuilder,
    },
    solana_storage_adapter::LedgerStorageAdapter,
    solana_perf::thread::renice_this_thread,
    solana_validator_exit::{
        Exit,
    },

    std::{
        net::SocketAddr,
        path::{
            Path,
        },
        sync::{
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
    },

};

pub struct JsonRpcService {
    thread_hdl: JoinHandle<()>,

    #[cfg(test)]
    pub request_processor: JsonRpcRequestProcessor,

    close_handle: Option<CloseHandle>,
}

impl JsonRpcService {
    pub fn new(
        rpc_addr: SocketAddr,
        config: JsonRpcConfig,
        log_path: &Path,
        rpc_service_exit: Arc<RwLock<Exit>>,
        metrics: Arc<Metrics>,
    ) -> Result<Self, String> {
        info!("rpc bound to {:?}", rpc_addr);
        info!("rpc configuration: {:?}", config);
        let rpc_threads = 1.max(config.rpc_threads);
        let rpc_niceness_adj = config.rpc_niceness_adj;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(rpc_threads)
                .on_thread_start(
                    move || {
                        renice_this_thread(rpc_niceness_adj).unwrap();
                    }
                )
                .thread_name("solRpcEl")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        let hbase_ledger_storage =
            if let Some(RpcHBaseConfig {
                            enable_hbase_ledger_upload: false,
                            ref hbase_address,
                            ref namespace,
                            ref hdfs_url,
                            ref hdfs_path,
                            timeout,
                            // block_cache,
                            use_md5_row_key_salt,
                            hash_tx_full_row_keys,
                            enable_full_tx_cache,
                            disable_tx_fallback,
                            ref cache_address,
                            use_block_car_files,
                            use_hbase_blocks_meta,
                            use_webhdfs,
                            ref webhdfs_url,
                            ..
                        }) = config.rpc_hbase_config
            {
                let hbase_config = solana_storage_hbase::LedgerStorageConfig {
                    read_only: true,
                    timeout,
                    address: hbase_address.clone(),
                    namespace: namespace.clone(),
                    hdfs_url: hdfs_url.clone(),
                    hdfs_path: hdfs_path.clone(),
                    // block_cache,
                    use_md5_row_key_salt,
                    hash_tx_full_row_keys,
                    enable_full_tx_cache,
                    disable_tx_fallback,
                    cache_address: cache_address.clone(),
                    use_block_car_files: use_block_car_files,
                    use_hbase_blocks_meta,
                    use_webhdfs: use_webhdfs,
                    webhdfs_url: webhdfs_url.clone(),
                };
                runtime
                    .block_on(solana_storage_hbase::LedgerStorage::new_with_config(hbase_config, metrics.clone()))
                    .map(|hbase_ledger_storage| {
                        info!("HBase ledger storage initialized");
                        Some(Box::new(hbase_ledger_storage) as Box<dyn LedgerStorageAdapter>)
                    })
                    .unwrap_or_else(|err| {
                        error!("Failed to initialize HBase ledger storage: {:?}", err);
                        None
                    })
            } else {
                None
            };

        let fallback_ledger_storage =
            if let Some(RpcHBaseConfig {
                            enable_hbase_ledger_upload: false,
                            ref fallback_hbase_address,
                            ref namespace,
                            ref hdfs_url,
                            ref hdfs_path,
                            timeout,
                            // block_cache,
                            use_md5_row_key_salt,
                            hash_tx_full_row_keys,
                            enable_full_tx_cache,
                            ref cache_address,
                            use_block_car_files,
                            use_hbase_blocks_meta,
                            use_webhdfs,
                            ref webhdfs_url,
                            ..
                        }) = config.rpc_hbase_config
            {
                if let Some(fallback_address) = fallback_hbase_address {
                    let fallback_config = solana_storage_hbase::LedgerStorageConfig {
                        read_only: true,
                        timeout,
                        address: fallback_address.clone(),
                        namespace: namespace.clone(),
                        hdfs_url: hdfs_url.clone(),
                        hdfs_path: hdfs_path.clone(),
                        // block_cache,
                        use_md5_row_key_salt,
                        hash_tx_full_row_keys,
                        enable_full_tx_cache,
                        disable_tx_fallback: false,
                        cache_address: cache_address.clone(),
                        use_block_car_files: use_block_car_files,
                        use_hbase_blocks_meta,
                        use_webhdfs: use_webhdfs,
                        webhdfs_url: webhdfs_url.clone(),
                    };
                    runtime
                        .block_on(solana_storage_hbase::LedgerStorage::new_with_config(fallback_config, metrics.clone()))
                        .map(|fallback_ledger_storage| {
                            info!("Fallback ledger storage initialized");
                            Some(Box::new(fallback_ledger_storage) as Box<dyn LedgerStorageAdapter>)
                        })
                        .unwrap_or_else(|err| {
                            error!("Failed to initialize Fallback ledger storage: {:?}", err);
                            None
                        })
                } else {
                    None
                }
            } else {
                None
            };

        let full_api = config.full_api;
        let max_request_body_size = config
            .max_request_body_size
            .unwrap_or(MAX_REQUEST_BODY_SIZE);
        // let (request_processor, _receiver) = JsonRpcRequestProcessor::new(
        let request_processor = JsonRpcRequestProcessor::new(
            config,
            rpc_service_exit.clone(),
            hbase_ledger_storage,
            fallback_ledger_storage,
        );

        #[cfg(test)]
            let test_request_processor = request_processor.clone();

        let log_path = log_path.to_path_buf();

        let (close_handle_sender, close_handle_receiver) = unbounded();
        let thread_hdl = Builder::new()
            .name("solJsonRpcSvc".to_string())
            .spawn({
                move || {
                    renice_this_thread(rpc_niceness_adj).unwrap();

                    let metrics_middleware = MetricsMiddleware::new(metrics.clone());
                    metrics.idle_threads_counter.set(rpc_threads as i64);

                    // Create the MetaIoHandler and apply the middleware
                    let mut io = MetaIoHandler::with_middleware(metrics_middleware);

                    io.extend_with(storage_rpc_minimal::MinimalImpl.to_delegate());
                    if full_api {
                        io.extend_with(storage_rpc_full::FullImpl.to_delegate());
                        io.extend_with(storage_rpc_deprecated_v1_7::DeprecatedV1_7Impl.to_delegate());
                    }

                    let request_middleware = RpcRequestMiddleware::new(
                        log_path,
                    );
                    let server = ServerBuilder::with_meta_extractor(
                        io,
                        move |_req: &hyper::Request<hyper::Body>| request_processor.clone(),
                    )
                        .event_loop_executor(runtime.handle().clone())
                        .threads(1)
                        .cors(DomainsValidation::AllowOnly(vec![
                            AccessControlAllowOrigin::Any,
                        ]))
                        .cors_max_age(86400)
                        .request_middleware(request_middleware)
                        .max_request_body_size(max_request_body_size)
                        .start_http(&rpc_addr);

                    if let Err(e) = server {
                        warn!(
                        "JSON RPC service unavailable error: {:?}. \n\
                           Also, check that port {} is not already in use by another application",
                        e,
                        rpc_addr.port()
                    );
                        close_handle_sender.send(Err(e.to_string())).unwrap();
                        return;
                    }

                    let server = server.unwrap();
                    close_handle_sender.send(Ok(server.close_handle())).unwrap();
                    server.wait();
                }
            })
            .unwrap();

        let close_handle = close_handle_receiver.recv().unwrap()?;
        let close_handle_ = close_handle.clone();
        rpc_service_exit
            .write()
            .unwrap()
            .register_exit(Box::new(move || close_handle_.close()));
        Ok(Self {
            thread_hdl,
            #[cfg(test)]
            request_processor: test_request_processor,
            close_handle: Some(close_handle),
        })
    }

    pub fn exit(&mut self) {
        if let Some(c) = self.close_handle.take() {
            c.close()
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

