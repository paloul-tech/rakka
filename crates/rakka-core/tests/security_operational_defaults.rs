//! Security profile and operational timeout default tests.

use std::time::Duration;

use rakka_core::{
    DeploymentProfile, OperationalTimeoutDefaults, SecurityDefaults, DEFAULT_ACTOR_ASK_TIMEOUT,
    DEFAULT_KUBERNETES_PRESTOP_TIMEOUT, DEFAULT_KUBERNETES_TERMINATION_GRACE_PERIOD_SECONDS,
    DEFAULT_PROCESS_SHUTDOWN_TIMEOUT, DEFAULT_PROCESS_STARTUP_TIMEOUT,
    DEFAULT_REMOTE_CONNECT_TIMEOUT, DEFAULT_REMOTE_IDLE_TIMEOUT,
    DEFAULT_REMOTE_OUTBOUND_QUEUE_CAPACITY, DEFAULT_STREAM_DRAIN_TIMEOUT,
};

#[test]
fn security_profiles_keep_internal_remoting_trusted_and_fail_closed() {
    let development = SecurityDefaults::development();
    assert_eq!(development.profile(), DeploymentProfile::Development);
    assert_eq!(development.remoting_bind_host(), "127.0.0.1");
    assert!(development.remoting_requires_registered_peers());
    assert!(!development.remoting_is_public_api());
    assert!(development.process_requires_executable_allowlist());
    assert!(!development.process_inherits_environment_by_default());
    assert_eq!(development.public_http_bind_host(), "127.0.0.1");
    assert_eq!(development.public_grpc_bind_host(), "127.0.0.1");

    let local_cluster = SecurityDefaults::local_cluster();
    assert_eq!(local_cluster.profile(), DeploymentProfile::LocalCluster);
    assert_eq!(local_cluster.remoting_bind_host(), "0.0.0.0");
    assert!(local_cluster.remoting_requires_registered_peers());
    assert!(!local_cluster.remoting_is_public_api());

    let production = SecurityDefaults::production_like();
    assert_eq!(production.profile(), DeploymentProfile::ProductionLike);
    assert_eq!(production.remoting_bind_host(), "0.0.0.0");
    assert!(production.remoting_requires_registered_peers());
    assert!(production.process_requires_executable_allowlist());
}

#[test]
fn operational_timeout_defaults_are_explicit_and_conservative() {
    let timeouts = OperationalTimeoutDefaults::new();

    assert_eq!(timeouts.actor_ask(), DEFAULT_ACTOR_ASK_TIMEOUT);
    assert_eq!(timeouts.remote_connect(), DEFAULT_REMOTE_CONNECT_TIMEOUT);
    assert_eq!(timeouts.remote_idle(), DEFAULT_REMOTE_IDLE_TIMEOUT);
    assert_eq!(timeouts.stream_drain(), DEFAULT_STREAM_DRAIN_TIMEOUT);
    assert_eq!(timeouts.process_startup(), DEFAULT_PROCESS_STARTUP_TIMEOUT);
    assert_eq!(
        timeouts.process_shutdown(),
        DEFAULT_PROCESS_SHUTDOWN_TIMEOUT
    );
    assert_eq!(
        timeouts.kubernetes_prestop(),
        DEFAULT_KUBERNETES_PRESTOP_TIMEOUT
    );
    assert_eq!(DEFAULT_REMOTE_OUTBOUND_QUEUE_CAPACITY, 1024);
    assert_eq!(DEFAULT_KUBERNETES_TERMINATION_GRACE_PERIOD_SECONDS, 45);
    assert!(timeouts.remote_connect() <= Duration::from_secs(5));
    assert!(timeouts.kubernetes_prestop() < Duration::from_secs(45));
}
