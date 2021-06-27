use crate::k8s::Index;
use chrono::prelude::*;
use prometheus::{
    self, Encoder, Histogram, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, TextEncoder,
};
use std::sync::RwLock;

lazy_static! {
    static ref KUBERNETES_REQUEST_TIME: Histogram = register_histogram!(
        "e8mitter_kubernetes_request_histogram",
        "Kubernetes, get Pods and get EGP request-time"
    )
    .unwrap();
    static ref KUBERNETES_POLICY_MATCH: IntGaugeVec = register_int_gauge_vec!(
        "e8mitter_kubernetes_policy_match",
        "Kubernetes, pods matching the egress policy",
        &["namespace", "name"]
    )
    .unwrap();
    static ref RESPONSE_TIME: Histogram = register_histogram!(
        "e8mitter_response_histogram",
        "Plots my own relative response time"
    )
    .unwrap();
    static ref RESPONSE_CODE: IntCounterVec = register_int_counter_vec!(
        "e8mitter_response_code_total",
        "Counting all http response codes returned",
        &["code"]
    )
    .unwrap();
    static ref SYNC_TIME: Histogram = register_histogram!(
        "e8mitter_sync_histogram",
        "Plots my own total relative kubernetes sync time"
    )
    .unwrap();
    static ref SUCCESSFUL_SYNC_TIME: IntGauge = register_int_gauge!(
        "e8mitter_sucessful_sync_time",
        "Absolute timestamp at which last succesful sync took place"
    )
    .unwrap();
    static ref SUCCESSFUL_SYNC_COUNT: IntCounter = register_int_counter!(
        "e8mitter_sucessful_sync_total",
        "Counting all successful kubernetes syncs"
    )
    .unwrap();
    static ref FAILED_SYNC_COUNT: IntCounter = register_int_counter!(
        "e8mitter_failed_sync_total",
        "Counting all failed kubernetes syncs"
    )
    .unwrap();
    static ref LOCK: RwLock<()> = RwLock::new(());
}

pub fn serve() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    // Gather the metrics.
    let metric_families = {
        let _guard = LOCK.read();
        prometheus::gather()
    }; // mutex falls out of scope here and unlocks

    // Encode them to send.
    encoder.encode(&metric_families, &mut buffer).unwrap();

    String::from_utf8(buffer.clone()).unwrap()
}

pub fn observe_kubernetes_requests(time: f64) {
    KUBERNETES_REQUEST_TIME.observe(time);
}

pub fn observe_response_time(time: f64) {
    RESPONSE_TIME.observe(time);
}

pub fn observe_sync_time(time: f64) {
    SYNC_TIME.observe(time);
}

pub fn record_policy_matches(index: &Index) {
    let _guard = LOCK.write();
    KUBERNETES_POLICY_MATCH.reset();
    for (pol, pods) in &index.reverse {
        KUBERNETES_POLICY_MATCH
            .with_label_values(&[&pol.metadata.namespace, &pol.metadata.name])
            .set(pods.len() as i64);
    }
}

pub fn record_response_code(code: u16) {
    RESPONSE_CODE.with_label_values(&[&code.to_string()]).inc();
}

pub fn record_successful_sync() {
    SUCCESSFUL_SYNC_TIME.set(epoch_now());
    SUCCESSFUL_SYNC_COUNT.inc();
}

pub fn record_failed_sync() {
    FAILED_SYNC_COUNT.inc();
}

pub fn has_at_least_one_successful_sync() -> bool {
    SUCCESSFUL_SYNC_COUNT.get() > 0
}

fn epoch_now() -> i64 {
    let now: DateTime<Utc> = Utc::now();
    now.format("%s").to_string().parse().unwrap_or(0) // instead of panicing the metrics thread here, go for returning epoch and cross fingers that this will raise an alarm somewhere
}
