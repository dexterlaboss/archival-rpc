use {
    prometheus::{
        HistogramTimer,
        HistogramVec,
        IntCounterVec,
        IntGauge,
        register_histogram_vec,
        register_int_counter_vec,
        register_int_gauge,
    },
    std::{
        sync::{
            Arc
        },
    },
};

#[derive(Clone)]
pub struct Metrics {
    request_counter: Arc<IntCounterVec>,
    tx_request_counter: Arc<IntCounterVec>,
    request_duration_histogram: Arc<HistogramVec>,
    pub idle_threads_counter: Arc<IntGauge>,
    active_threads_counter: Arc<IntGauge>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            request_counter: Arc::new(register_int_counter_vec!(
                "requests_total",
                "Total number of RPC requests",
                &["method"]
            ).unwrap()),

            tx_request_counter: Arc::new(register_int_counter_vec!(
                "tx_requests_total",
                "Total number of getTransaction RPC requests",
                &["method", "source", "epoch", "type"]
            ).unwrap()),

            request_duration_histogram: Arc::new(register_histogram_vec!(
                "request_duration_seconds",
                "Duration of RPC requests in seconds",
                &["method"]
            ).unwrap()),

            idle_threads_counter: Arc::new(register_int_gauge!(
                "rpc_idle_threads_total",
                "Total number of idle threads in the RPC service"
            ).unwrap()),

            active_threads_counter: Arc::new(register_int_gauge!(
                "rpc_active_threads_total",
                "Total number of active threads in the RPC service"
            ).unwrap()),
        }
    }

    pub fn increment_total_requests(&self, method: &str) {
        self.request_counter.with_label_values(&[method]).inc();
    }

    pub fn record_transaction(&self, source: &str, epoch: u64, tx_type: &str) {
        self.tx_request_counter
            .with_label_values(&["getTransaction", source, &epoch.to_string(), tx_type])
            .inc();
    }

    pub fn record_duration(&self, method: &str) -> HistogramTimer {
        self.request_duration_histogram.with_label_values(&[method]).start_timer()
    }

    pub fn thread_started(&self) {
        self.active_threads_counter.inc();
        self.idle_threads_counter.dec();
    }

    pub fn thread_stopped(&self) {
        self.active_threads_counter.dec();
        self.idle_threads_counter.inc();
    }
}
