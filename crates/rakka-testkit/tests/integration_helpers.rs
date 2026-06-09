//! Integration tests for reusable Phase 5 testkit helpers.

use std::time::Duration;

use axum::http::StatusCode;
use rakka_core::{InMemoryMetricsRecorder, MetricKind, MetricsRecorder};
use rakka_grpc::{server_streaming_response, unary_service, GrpcUnaryConfig};
use rakka_http::{json_service_route, HttpRouteConfig};
use rakka_k8s::{KubernetesDrainController, KubernetesDrainStepResult, KubernetesNodeHealth};
use rakka_stream::{bounded_channel, StreamLifecycle};
use rakka_testkit::{
    assert_counter_total, assert_drain_complete, assert_http_status, assert_metric_attribute,
    assert_probe_failed_with_reason, assert_stream_lifecycle, expect_grpc_stream_items,
    expect_grpc_unary_ok, expect_metric_observation, expect_stream_source_items, grpc_request,
    http_post_json,
};
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn testkit_helpers_cover_phase_5_surfaces() {
    let router = json_service_route(
        "/double",
        HttpRouteConfig::default(),
        |request: NumberRequest| async move {
            Ok(NumberReply {
                value: request.value * 2,
            })
        },
    );
    let http = http_post_json(router, "/double", &NumberRequest { value: 4 }).await;
    assert_http_status(&http, StatusCode::OK);
    assert_eq!(http.json::<NumberReply>().value, 8);

    let grpc = expect_grpc_unary_ok(unary_service(
        grpc_request(NumberRequest { value: 5 }),
        GrpcUnaryConfig::default(),
        |request: NumberRequest| async move {
            Ok(NumberReply {
                value: request.value + 1,
            })
        },
    ))
    .await;
    assert_eq!(grpc.value, 6);

    let (grpc_sink, grpc_source) = bounded_channel(2).expect("gRPC stream channel");
    grpc_sink
        .send(NumberReply { value: 1 })
        .await
        .expect("first gRPC stream item");
    grpc_sink
        .send(NumberReply { value: 2 })
        .await
        .expect("second gRPC stream item");
    grpc_sink.drain().expect("gRPC stream drain");
    let grpc_stream = server_streaming_response(grpc_source);
    assert_eq!(
        expect_grpc_stream_items(grpc_stream.into_inner()).await,
        vec![NumberReply { value: 1 }, NumberReply { value: 2 }]
    );

    let (sink, source) = bounded_channel(2).expect("Rakka stream channel");
    sink.send("one".to_owned()).await.expect("first item");
    sink.send("two".to_owned()).await.expect("second item");
    sink.drain().expect("stream drain");
    expect_stream_source_items(&source, vec!["one".to_owned(), "two".to_owned()]).await;
    assert_stream_lifecycle(&source, StreamLifecycle::Completed);

    let health = KubernetesNodeHealth::new(rakka_cluster::NodeId::new("rakka-0", "uid-a"));
    assert_probe_failed_with_reason(&health.readiness_probe(), "cluster-not-up:joining");
    let mut drain = KubernetesDrainController::new(health);
    drain.add_step("custom", || async {
        KubernetesDrainStepResult::completed("done")
    });
    let report = drain.drain(Duration::from_secs(1)).await;
    assert_drain_complete(&report);

    let recorder = InMemoryMetricsRecorder::new();
    recorder.increment_counter("rakka.test.events", 2, &[("surface", "testkit")]);
    assert_counter_total(&recorder.snapshot(), "rakka.test.events", 2.0);
    let observation = expect_metric_observation(
        &recorder.snapshot(),
        "rakka.test.events",
        MetricKind::Counter,
    );
    assert_metric_attribute(&observation, "surface", "testkit");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NumberRequest {
    value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NumberReply {
    value: i64,
}
