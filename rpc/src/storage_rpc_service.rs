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
    solana_metrics::Metrics,
    crossbeam_channel::unbounded,
    jsonrpc_core::{
        MetaIoHandler,
        Middleware,
        Call,
        Output,
        Response,
        Metadata,
        Error,
        Failure,
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
    prometheus::{
        TextEncoder,
        Encoder,
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
use std::future::Future;
use futures::future::{Either, FutureExt, BoxFuture};
use std::panic::AssertUnwindSafe;
use hyper::{body::to_bytes, Request, Body, StatusCode};
use futures::stream::{self, StreamExt};
use serde_json::Value;

#[derive(Clone)]
struct MetricsMiddleware {
    metrics: Arc<Metrics>,
}

impl MetricsMiddleware {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<M: Metadata> Middleware<M> for MetricsMiddleware {
    type Future = BoxFuture<'static, Option<Response>>;
    type CallFuture = BoxFuture<'static, Option<Output>>;

    fn on_call<F, X>(&self, call: Call, meta: M, next: F) -> Either<Self::CallFuture, X>
        where
            F: FnOnce(Call, M) -> X + Send + Sync,
            X: Future<Output = Option<Output>> + Send + 'static,
    {
        info!("Request on_call executed");
        if let Call::MethodCall(ref request) = call {
            let method = request.method.clone();
            let metrics = self.metrics.clone();

            // Ignore getTransaction in middleware; it'll be tracked in another module
            if method != "getTransaction" {
                metrics.increment_total_requests(&method);
            }

            // Record request duration for all methods
            let timer = metrics.record_duration(&method);

            // Mark thread as started
            metrics.thread_started();

            let request_id = request.id.clone();
            let request_jsonrpc = request.jsonrpc.clone();

            let future = AssertUnwindSafe(next(call, meta))
                .catch_unwind()
                .then(move |result| {
                    timer.observe_duration();

                    // Mark thread as stopped
                    metrics.thread_stopped();

                    match result {
                        Err(_) => {
                            let error = Error::new(jsonrpc_core::ErrorCode::InternalError);
                            debug!("Request panicked with error: {:?}", error);

                            let failure_output = Output::Failure(Failure {
                                jsonrpc: request_jsonrpc,
                                error,
                                id: request_id,
                            });

                            futures::future::ready(Some(failure_output))
                        }

                        Ok(Some(output)) => {
                            futures::future::ready(Some(output))
                        }
                        Ok(None) => {
                            let error = Error::new(jsonrpc_core::ErrorCode::InternalError);
                            debug!("Request failed with error: {:?}", error);

                            let failure_output = Output::Failure(Failure {
                                jsonrpc: request_jsonrpc,
                                error,
                                id: request_id,
                            });

                            futures::future::ready(Some(failure_output))
                        }
                    }
                });

            Either::Left(Box::pin(future))
        } else {
            Either::Right(next(call, meta))
        }
    }
}

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
    fn on_request(&self, request: Request<Body>) -> RequestMiddlewareAction {
        trace!("request uri: {}", request.uri());

        if let Some(result) = process_rest(request.uri().path()) {
            hyper::Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(result))
                .unwrap()
                .into()
        } else if request.uri().path() == "/health" {
            hyper::Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(self.health_check()))
                .unwrap()
                .into()
        } else {
            request.into()
        }

        //
        // This gets the number of requests in a batch.
        //

        // let (parts, body) = request.into_parts();
        //
        // let body_bytes_future = async {
        //     match to_bytes(body).await {
        //         Ok(bytes) => {
        //             let count = count_jsonrpc_requests(&bytes);
        //             debug!("Received JSON-RPC batch size: {}", count);
        //             bytes
        //         }
        //         Err(e) => {
        //             error!("Error reading body: {}", e);
        //             hyper::body::Bytes::new()
        //         }
        //     }
        // };
        //
        // let new_body = Body::wrap_stream(
        //     stream::once(body_bytes_future)
        //         .map(|bytes| Ok::<_, hyper::Error>(bytes))
        // );
        //
        // Request::from_parts(parts, new_body).into()
    }
}

fn count_jsonrpc_requests(body: &hyper::body::Bytes) -> usize {
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Array(batch)) => batch.len(),  // JSON-RPC batch request
        Ok(_) => 1,  // Single JSON-RPC request
        Err(_) => {
            error!("Failed to parse JSON-RPC request");
            0  // Invalid JSON
        }
    }
}

fn process_rest(path: &str) -> Option<String> {
    match path {
        "/metrics" => {
            let encoder = TextEncoder::new();

            let metric_families = prometheus::gather();

            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            let metrics_output = String::from_utf8(buffer).unwrap();

            Some(metrics_output)
        }
        _ => None,
    }
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
        let (request_processor, _receiver) = JsonRpcRequestProcessor::new(
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

