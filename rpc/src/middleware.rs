use {
    // crate::{
    //     // rpc::{
    //     //     // storage_rpc_deprecated_v1_7::*,
    //     //     // storage_rpc_full::*,
    //     //     // storage_rpc_minimal::*,
    //     //     // *,
    //     // },
    //     // request_processor::{
    //     //     *,
    //     // },
    // },
    solana_metrics::Metrics,
    // crossbeam_channel::unbounded,
    jsonrpc_core::{
        // MetaIoHandler,
        Middleware,
        Call,
        Output,
        Response,
        Metadata,
        Error,
        Failure,
    },
    jsonrpc_http_server::{
        hyper,
        // AccessControlAllowOrigin,
        // CloseHandle,
        // DomainsValidation,
        RequestMiddleware,
        RequestMiddlewareAction,
        // ServerBuilder,
    },
    // solana_storage_adapter::LedgerStorageAdapter,
    // solana_perf::thread::renice_this_thread,
    // solana_validator_exit::{
    //     Exit,
    // },
    prometheus::{
        TextEncoder,
        Encoder,
    },
    std::{
        // net::SocketAddr,
        path::{
            // Path,
            PathBuf
        },
        sync::{
            Arc,
            // RwLock,
        },
        // thread::{self, Builder, JoinHandle},
    },

};
use std::future::Future;
use futures::future::{Either, FutureExt, BoxFuture};
use std::panic::AssertUnwindSafe;
use hyper::{
    // body::to_bytes,
    Request, Body, StatusCode
};
// use futures::stream::{self};
use serde_json::Value;

#[derive(Clone)]
pub struct MetricsMiddleware {
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

pub struct RpcRequestMiddleware {
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

#[allow(dead_code)]
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