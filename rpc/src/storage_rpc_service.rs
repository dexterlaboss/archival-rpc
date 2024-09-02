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
        MetaIoHandler,
        Middleware,
        Params,
        Value,
        Call,
        Output,
        Response,
        Metadata,
        // Result,
        // Error,
        RpcMethodSimple,
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
    prometheus::{
        Opts,
        HistogramVec,
        IntCounterVec,
        register_histogram_vec,
        register_int_counter_vec
    },
};
use std::future::Future;
use futures::future::{self, Either, FutureExt, BoxFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

lazy_static::lazy_static! {
    static ref JSONRPC_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        Opts::new("requests_total", "Total number of RPC requests."),
        &["method"]
    ).unwrap();

    static ref JSONRPC_REQUEST_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "request_duration_seconds",
        "The RPC request latencies in seconds.",
        &["method"],
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0] // Buckets in seconds
    ).unwrap();
}

struct MetricsMiddleware;

// impl<M: Metadata> Middleware<M> for MetricsMiddleware {
//     type CallFuture = BoxFuture<'static, Option<Output>>;
//
//     type Future = Either<BoxFuture<'static, Option<Response>>, Self::CallFuture>;
//
//     fn on_call<F, X>(
//         &self,
//         call: Call,
//         meta: M,
//         next: F,
//     ) -> Self::Future
//         where
//             F: FnOnce(Call, M) -> X + Send + Sync,
//             X: Future<Output = Option<Output>> + Send + 'static,
//     {
//         let method_name = match &call {
//             Call::MethodCall(method_call) => method_call.method.clone(),
//             _ => "unknown".to_string(),
//         };
//
//         // Increment the request counter
//         JSONRPC_REQUESTS_TOTAL.with_label_values(&[&method_name]).inc();
//
//         // Start timing the request
//         let timer = JSONRPC_REQUEST_DURATION_SECONDS.with_label_values(&[&method_name]).start_timer();
//
//         let future = next(call.clone(), meta);
//
//         let wrapped_future = async move {
//             let result = future.await;
//             timer.observe_duration(); // Stop timing when the request is done

//             result.map(|output| call.into_response(output))
//         }
//             .boxed();
//
//         Either::Left(wrapped_future)
//     }
// }

// impl Call {
//     // Manually convert Output to Response using the original Call's context
//     fn into_response(self, output: Output) -> Response {
//         match self {
//             Call::MethodCall(method_call) => {
//                 Response::Single(jsonrpc_core::response::Output::Success(jsonrpc_core::Success {
//                     jsonrpc: method_call.jsonrpc,
//                     result: output.result,
//                     id: method_call.id,
//                 }))
//             }
//             _ => Response::Single(jsonrpc_core::response::Output::Failure(jsonrpc_core::Failure {
//                 jsonrpc: None,
//                 error: jsonrpc_core::Error::method_not_found(),
//                 id: jsonrpc_core::Id::Null,
//             })),
//         }
//     }
// }

// fn with_metrics<F>(method_name: &str, f: F) -> impl Fn(Params) -> Result<Value>
//     where
//         F: Fn(Params) -> Result<Value> + Send + Sync + 'static,
// {
//     move |params: Params| {
//         // Increment the request counter with the method label
//         JSONRPC_REQUESTS_TOTAL.with_label_values(&[method_name]).inc();
//
//         // Start measuring the duration with the method label
//         let timer = JSONRPC_REQUEST_DURATION_SECONDS.with_label_values(&[method_name]).start_timer();
//
//         // Execute the actual method logic
//         let result = f(params);
//
//         // Stop the timer and record the duration
//         timer.observe_duration();
//
//         result
//     }
// }

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
                            block_cache,
                            use_md5_row_key_salt,
                        }) = config.rpc_hbase_config
            {
                let hbase_config = solana_storage_hbase::LedgerStorageConfig {
                    read_only: true,
                    timeout,
                    address: hbase_address.clone(),
                    block_cache,
                    use_md5_row_key_salt,
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
                // let mut io = MetaIoHandler::with_middleware(MetricsMiddleware);

                io.extend_with(storage_rpc_minimal::MinimalImpl.to_delegate());
                if full_api {
                    io.extend_with(storage_rpc_full::FullImpl.to_delegate());
                    io.extend_with(storage_rpc_deprecated_v1_7::DeprecatedV1_7Impl.to_delegate());
                }

                // io.extend_with(storage_rpc_minimal::MinimalImpl.to_delegate().map(|method| {
                //     with_metrics("MinimalImpl", method)
                // }));
                //
                // if full_api {
                //     io.extend_with(storage_rpc_full::FullImpl.to_delegate().map(|method| {
                //         with_metrics("FullImpl", method)
                //     }));
                //     io.extend_with(storage_rpc_deprecated_v1_7::DeprecatedV1_7Impl.to_delegate().map(|method| {
                //         with_metrics("DeprecatedV1_7Impl", method)
                //     }));
                // }

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

