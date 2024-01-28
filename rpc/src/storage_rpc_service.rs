//! The `storage_rpc_service` module implements the Solana storage JSON RPC service.

use {
    crate::{
        storage_rpc::{
            storage_rpc_deprecated_v1_7::*,
            storage_rpc_full::*,
            storage_rpc_minimal::*,
            *,
        },
    },
    crossbeam_channel::unbounded,
    jsonrpc_core::{
        MetaIoHandler
    },
    jsonrpc_http_server::{
        hyper, AccessControlAllowOrigin, CloseHandle, DomainsValidation, RequestMiddleware,
        RequestMiddlewareAction, ServerBuilder,
    },
    solana_storage_adapter::LedgerStorageAdapter,
    solana_perf::thread::renice_this_thread,
    solana_sdk::{
        exit::Exit,
    },
    std::{
        net::SocketAddr,
        path::{Path, PathBuf},
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

struct RpcRequestMiddleware {
    // log_path: PathBuf,
}

impl RpcRequestMiddleware {
    pub fn new(
        _log_path: PathBuf,
    ) -> Self {
        Self {
            // log_path,
        }
    }

    #[allow(dead_code)]
    fn internal_server_error() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
            .body(hyper::Body::empty())
            .unwrap()
    }

    fn health_check(&self) -> &'static str {
        let response = "ok";
        info!("health check: {}", response);
        response
    }
}

impl RequestMiddleware for RpcRequestMiddleware {
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        trace!("request uri: {}", request.uri());

        if let Some(result) = process_rest(request.uri().path()) {
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .body(hyper::Body::from(result))
                .unwrap()
                .into()
        } else if request.uri().path() == "/health" {
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .body(hyper::Body::from(self.health_check()))
                .unwrap()
                .into()
        } else {
            request.into()
        }
    }
}

fn process_rest(path: &str) -> Option<String> {
    match path {
        //
        // Add custom url endpoints here
        //
        _ => None,
    }
}

impl JsonRpcService {
    pub fn new(
        rpc_addr: SocketAddr,
        config: JsonRpcConfig,
        log_path: &Path,
        rpc_service_exit: Arc<RwLock<Exit>>,
    ) -> Result<Self, String> {
        info!("rpc bound to {:?}", rpc_addr);
        info!("rpc configuration: {:?}", config);
        let rpc_threads = 1.max(config.rpc_threads);
        let rpc_niceness_adj = config.rpc_niceness_adj;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(rpc_threads)
                .on_thread_start(move || renice_this_thread(rpc_niceness_adj).unwrap())
                .thread_name("solRpcEl")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        let hbase_ledger_storage =
            if let Some(RpcHBaseConfig {
                            enable_hbase_ledger_upload: false,
                            ref hbase_address,
                            timeout,
                        }) = config.rpc_hbase_config
            {
                let hbase_config = solana_storage_hbase::LedgerStorageConfig {
                    read_only: true,
                    timeout,
                    address: hbase_address.clone(),
                };
                runtime
                    .block_on(solana_storage_hbase::LedgerStorage::new_with_config(hbase_config))
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

        let full_api = config.full_api;
        let max_request_body_size = config
            .max_request_body_size
            .unwrap_or(MAX_REQUEST_BODY_SIZE);
        let (request_processor, _receiver) = JsonRpcRequestProcessor::new(
            config,
            rpc_service_exit.clone(),
            hbase_ledger_storage,
        );

        #[cfg(test)]
            let test_request_processor = request_processor.clone();

        let log_path = log_path.to_path_buf();

        let (close_handle_sender, close_handle_receiver) = unbounded();
        let thread_hdl = Builder::new()
            .name("solJsonRpcSvc".to_string())
            .spawn(move || {
                renice_this_thread(rpc_niceness_adj).unwrap();

                let mut io = MetaIoHandler::default();

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

