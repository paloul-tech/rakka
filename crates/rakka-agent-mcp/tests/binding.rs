//! A binding names a server, its transport, a logical credential, and an
//! allow-list of tools with operator-declared safety; every refusal has a
//! stable code and nothing in a binding is a secret.

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::{
    McpDescriptorRefresh, McpRegistrationError, McpServerBinding, McpServerId, McpToolPolicy,
    MCP_DEFAULT_PROTOCOL_VERSIONS,
};

fn server(id: &str) -> McpServerId {
    McpServerId::new(id).expect("server id")
}

fn policy() -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(
        AgentEffectSafetyClass::Idempotent,
    ))
}

#[test]
fn a_server_id_is_a_validated_identity_segment_without_dots() {
    assert_eq!(server("crm").as_str(), "crm");
    assert_eq!(
        McpServerId::new("").expect_err("empty").code(),
        "mcp-binding-invalid"
    );
    assert_eq!(
        McpServerId::new("a/b").expect_err("separator").code(),
        "mcp-binding-invalid"
    );
    assert_eq!(
        McpServerId::new("a.b").expect_err("dot").code(),
        "mcp-binding-invalid"
    );
    let decoded: Result<McpServerId, _> = serde_json::from_str("\"bad/id\"");
    assert!(decoded.is_err(), "decoding validates too");
}

#[test]
fn tool_ids_are_prefixed_and_validated() {
    let binding = McpServerBinding::streamable_http(server("crm"), "https://mcp.example.test/mcp")
        .with_tool("search_contacts", policy())
        .expect("valid tool");
    assert_eq!(
        binding.tool_id("search_contacts").expect("id").as_str(),
        "mcp.crm.search_contacts"
    );
    let error = McpServerBinding::streamable_http(server("crm"), "https://mcp.example.test/mcp")
        .with_tool("bad|name", policy())
        .expect_err("a persistence separator is refused");
    assert_eq!(error.code(), "mcp-binding-invalid");
}

#[test]
fn tool_names_may_contain_dots() {
    // MCP servers commonly name tools `a.b`; nothing in this crate splits a
    // tool id on `.` — the sync and executor key by the full `AgentToolId`.
    // Only the server id itself refuses `.`, so `mcp.<server>.` stays a
    // stable, unambiguous prefix.
    let binding = McpServerBinding::streamable_http(server("crm"), "https://mcp.example.test/mcp")
        .with_tool("search.contacts", policy())
        .expect("a dotted tool name is accepted");
    assert_eq!(
        binding.tool_id("search.contacts").expect("id").as_str(),
        "mcp.crm.search.contacts"
    );
}

#[test]
fn the_url_rule_accepts_http_and_https_and_refuses_userinfo_and_fragments() {
    for ok in ["http://127.0.0.1:1/mcp", "https://h/mcp?x=1"] {
        McpServerBinding::streamable_http(server("s"), ok)
            .with_tool("t", policy())
            .expect("tool")
            .validate()
            .expect(ok);
    }
    for bad in [
        "ftp://h/mcp",
        "https://u:p@h/mcp",
        "https://h/mcp#f",
        "not a url",
        "https:///mcp",
    ] {
        let error = McpServerBinding::streamable_http(server("s"), bad)
            .with_tool("t", policy())
            .expect("tool")
            .validate()
            .expect_err(bad);
        assert_eq!(error.code(), "mcp-binding-invalid", "{bad}");
        assert!(
            matches!(error, McpRegistrationError::InvalidUrl { .. }),
            "{bad}"
        );
    }
}

#[test]
fn defaults_are_manual_refresh_the_two_versions_and_one_inline_attempt() {
    let binding = McpServerBinding::streamable_http(server("s"), "https://h/mcp")
        .with_tool("t", policy())
        .expect("tool");
    assert_eq!(binding.refresh, McpDescriptorRefresh::Manual);
    assert_eq!(
        binding.protocol_versions,
        MCP_DEFAULT_PROTOCOL_VERSIONS.map(str::to_string).to_vec()
    );
    let policy = &binding.tools["t"];
    assert_eq!(
        (policy.max_attempts, policy.timeout_ms, policy.honor_hints),
        (1, None, false)
    );
    assert!(binding.credential_binding.is_none());
    let empty = McpServerBinding::streamable_http(server("s"), "https://h/mcp");
    assert!(matches!(
        empty
            .validate()
            .expect_err("a binding lists at least one tool"),
        McpRegistrationError::NoTools { .. }
    ));
    let no_versions = binding.clone().with_protocol_versions(vec![]);
    assert!(matches!(
        no_versions.validate().expect_err("versions"),
        McpRegistrationError::ProtocolVersionsEmpty { .. }
    ));
}

#[test]
fn a_protocol_version_must_be_well_shaped_and_one_the_sdk_knows() {
    let binding = McpServerBinding::streamable_http(server("s"), "https://h/mcp")
        .with_tool("t", policy())
        .expect("tool");
    binding
        .clone()
        .with_protocol_versions(MCP_DEFAULT_PROTOCOL_VERSIONS.map(str::to_string).to_vec())
        .validate()
        .expect("the defaults are known versions");

    // Malformed: refused on shape.
    let malformed = binding
        .clone()
        .with_protocol_versions(vec!["2026-7-28".to_string()]);
    let error = malformed.validate().expect_err("shape");
    assert_eq!(error.code(), "mcp-binding-invalid");
    assert!(
        matches!(error, McpRegistrationError::ProtocolVersionInvalid { .. }),
        "{error}"
    );

    // Well-shaped but unknown to the pinned SDK: still a binding refusal, and
    // a distinct one, so the operator is not left reading it as a network
    // failure at sync time.
    let unknown = binding.with_protocol_versions(vec!["2099-01-01".to_string()]);
    let error = unknown.validate().expect_err("unknown");
    assert_eq!(error.code(), "mcp-binding-invalid");
    assert!(
        matches!(error, McpRegistrationError::ProtocolVersionUnknown { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("2099-01-01"),
        "the refusal names the value: {error}"
    );
}

#[test]
fn with_max_attempts_no_longer_clamps_and_validate_is_the_gate() {
    // `with_max_attempts` stores the value as given; only `validate` refuses
    // a zero.
    let built = McpServerBinding::streamable_http(server("s"), "https://h/mcp")
        .with_tool("t", policy().with_max_attempts(0))
        .expect("tool");
    assert_eq!(built.tools["t"].max_attempts, 0);
    let error = built.validate().expect_err("zero attempts is refused");
    assert_eq!(error.code(), "mcp-binding-invalid");
    assert!(matches!(
        error,
        McpRegistrationError::MaxAttemptsInvalid { .. }
    ));
}

#[test]
fn a_decoded_binding_with_zero_max_attempts_fails_validate() {
    // `McpToolPolicy`'s fields are `pub` and it derives a plain `Deserialize`,
    // so a hand-built or decoded policy can carry `max_attempts: 0` without
    // going through `with_max_attempts` at all; `validate` must still catch
    // it.
    let binding = McpServerBinding::streamable_http(server("s"), "https://h/mcp")
        .with_tool("t", policy())
        .expect("tool");
    let mut encoded: serde_json::Value =
        serde_json::to_value(&binding).expect("binding encodes to a JSON value");
    encoded["tools"]["t"]["max_attempts"] = serde_json::json!(0);
    let decoded: McpServerBinding =
        serde_json::from_value(encoded).expect("a zero max_attempts decodes fine");
    assert_eq!(decoded.tools["t"].max_attempts, 0);
    let error = decoded
        .validate()
        .expect_err("a decoded zero max_attempts is refused");
    assert_eq!(error.code(), "mcp-binding-invalid");
    assert!(matches!(
        error,
        McpRegistrationError::MaxAttemptsInvalid { .. }
    ));
}

#[test]
fn a_binding_round_trips_and_carries_no_secret_shaped_field() {
    let binding = McpServerBinding::streamable_http(server("s"), "https://h/mcp")
        .with_tool("t", policy())
        .expect("tool");
    let encoded = serde_json::to_string(&binding).expect("encodes");
    let decoded: McpServerBinding = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, binding);
    for forbidden in ["token", "secret", "api_key", "password"] {
        assert!(
            !encoded.contains(forbidden),
            "{forbidden} appears in a binding's encoding: {encoded}"
        );
    }
}
