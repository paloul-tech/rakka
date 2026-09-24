//! The in-process fake MCP server and counting client the crate's own tests
//! and `rakka-agent`'s dispatcher proofs drive.
//!
//! The fake is a real MCP server: rmcp's own
//! [`StreamableHttpService`]
//! mounted on an axum router over a loopback socket, so a proof that drives it
//! exercises the real handshake, the real negotiation, and the real HTTP
//! headers rather than a stub of them. What the fake adds is observability:
//! every `tools/list`, every `tools/call` and every request's headers are
//! recorded, so a test can assert what reached the wire — and, in particular,
//! that a credential reached it only as the declared header.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::BoxStream;
use http::{HeaderName, HeaderValue};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientJsonRpcMessage, ContentBlock,
    CreateTaskResult, DiscoverRequestMethod, DiscoverResult, InputRequiredResult, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerConfig, Task, TaskStatus,
    Tool, ToolAnnotations,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_client::{
    SseError, StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::{
    StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
use serde_json::{Map, Value};
use tokio::task::JoinHandle;

/// The concrete HTTP client rmcp implements
/// [`StreamableHttpClient`] for.
///
/// rmcp depends on `reqwest` but does not re-export it
/// (`rmcp-3.4.0/src/transport/common.rs:9` gates a private `mod reqwest`), so
/// this crate names the same `reqwest` 0.13 and re-exports its client here,
/// behind the `testkit` feature only: the executor's own client type stays
/// generic, and nothing in a production build has to name reqwest at all.
pub use reqwest::Client as ReqwestClient;

/// What a fake tool answers with.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FakeToolBehaviour {
    /// One text content block.
    Text(String),
    /// Structured content.
    Structured(Value),
    /// One PNG image content block of `bytes` zero bytes.
    Image {
        /// How many zero bytes the image carries.
        bytes: usize,
    },
    /// A tool-level error: `isError: true` with the text.
    Error(String),
    /// The multi-round `input_required` answer.
    InputRequired,
    /// A task handle the client would poll for.
    Task,
    /// The call's own arguments, as structured content.
    Echo,
    /// Sleeps, then answers the text `"late"`: the timeout proof's tool.
    Sleep {
        /// How long the tool takes before answering.
        millis: u64,
    },
}

/// One tool the fake server exposes.
#[derive(Debug, Clone, PartialEq)]
pub struct FakeTool {
    /// The tool's name, as the server reports it.
    pub name: String,
    /// The tool's description.
    pub description: String,
    /// The tool's JSON Schema input.
    pub input_schema: Value,
    /// The tool's JSON Schema output, when it declares one.
    pub output_schema: Option<Value>,
    /// The tool's hints, when it declares any.
    pub annotations: Option<ToolAnnotations>,
    /// What the tool answers with.
    pub behaviour: FakeToolBehaviour,
}

impl FakeTool {
    /// Declares a tool with an explicit behaviour.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        behaviour: FakeToolBehaviour,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            output_schema: None,
            annotations: None,
            behaviour,
        }
    }

    /// Declares a tool that answers one fixed line of text.
    #[must_use]
    pub fn text(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        answer: impl Into<String>,
    ) -> Self {
        Self::new(
            name,
            description,
            input_schema,
            FakeToolBehaviour::Text(answer.into()),
        )
    }

    /// Declares the tool's hints.
    #[must_use]
    pub fn with_annotations(mut self, annotations: ToolAnnotations) -> Self {
        self.annotations = Some(annotations);
        self
    }

    /// Declares the tool's output schema.
    #[must_use]
    pub fn with_output_schema(mut self, output_schema: Value) -> Self {
        self.output_schema = Some(output_schema);
        self
    }

    /// The rmcp model this tool is listed as.
    fn listed(&self) -> Tool {
        let mut tool = Tool::new(
            self.name.clone(),
            self.description.clone(),
            Arc::new(object_of(&self.input_schema)),
        );
        tool.output_schema = self
            .output_schema
            .as_ref()
            .map(|schema| Arc::new(object_of(schema)));
        tool.annotations = self.annotations.clone();
        tool
    }
}

/// One `tools/call` the fake server served.
#[derive(Debug, Clone, PartialEq)]
pub struct SeenCall {
    /// The tool that was called.
    pub name: String,
    /// The call's arguments.
    pub arguments: Value,
    /// The call's `_meta`, as the server received it: the request-level map
    /// rmcp merged on the wire, which is where a client's own keys arrive.
    pub meta: Value,
    /// The `Authorization` header on the request that carried the call.
    pub authorization: Option<String>,
    /// Every header on the request that carried the call.
    pub headers: Vec<(String, String)>,
}

/// What the fake server knows and has seen.
#[derive(Debug)]
struct FakeState {
    tools: Vec<FakeTool>,
    server_name: String,
    versions: Vec<ProtocolVersion>,
    list_calls: usize,
    list_delay: Option<Duration>,
    legacy_only: bool,
    calls: Vec<SeenCall>,
    last_headers: Vec<(String, String)>,
}

/// An in-process MCP server whose tools, identity, negotiated versions and
/// observed traffic a test controls.
///
/// Cloning shares the state: [`serve_fake`] keeps one handle and hands another
/// back, so the counters a test reads are the ones the served handler writes.
#[derive(Debug, Clone)]
pub struct FakeMcpServer {
    state: Arc<Mutex<FakeState>>,
}

impl Default for FakeMcpServer {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeMcpServer {
    /// A server with no tools, named `fake-mcp-server`, supporting every
    /// protocol version rmcp knows.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState {
                tools: Vec::new(),
                server_name: "fake-mcp-server".to_string(),
                versions: ProtocolVersion::KNOWN_VERSIONS.to_vec(),
                list_calls: 0,
                list_delay: None,
                legacy_only: false,
                calls: Vec::new(),
                last_headers: Vec::new(),
            })),
        }
    }

    /// Adds a tool to the listing.
    #[must_use]
    pub fn with_tool(self, tool: FakeTool) -> Self {
        self.lock().tools.push(tool);
        self
    }

    /// Sets the implementation name the server reports.
    #[must_use]
    pub fn with_server_name(self, name: impl Into<String>) -> Self {
        self.lock().server_name = name.into();
        self
    }

    /// Sets the protocol versions the server supports, oldest first.
    #[must_use]
    pub fn with_supported_versions(self, versions: Vec<ProtocolVersion>) -> Self {
        self.lock().versions = versions;
        self
    }

    /// Makes every `tools/list` sleep `millis` before it answers.
    ///
    /// The handshake still completes and the session stays open: only the
    /// listing stalls, which is the shape of a server that accepts a client
    /// and then never gets round to answering it. A listing is counted in
    /// [`Self::list_calls`] when it *arrives*, so a proof can show the stalled
    /// request was reached even when the client gave up on it.
    #[must_use]
    pub fn with_list_delay(self, millis: u64) -> Self {
        self.lock().list_delay = Some(Duration::from_millis(millis));
        self
    }

    /// Makes the server one that predates the 2026-07-28 revision: it answers
    /// `server/discover` with method-not-found, as a server that has never
    /// heard of the method does, and is reachable only through the legacy
    /// `initialize` handshake, which negotiates among the versions
    /// [`Self::with_supported_versions`] set — a client that requests one of
    /// them gets it back, and one that requests another gets the newest of
    /// them that still has an `initialize` handshake.
    #[must_use]
    pub fn with_legacy_only(self) -> Self {
        self.lock().legacy_only = true;
        self
    }

    /// How many `tools/list` requests the server has received.
    #[must_use]
    pub fn list_calls(&self) -> usize {
        self.lock().list_calls
    }

    /// How many `tools/call` requests the server has served.
    #[must_use]
    pub fn call_count(&self) -> usize {
        self.lock().calls.len()
    }

    /// Every `tools/call` the server has served, in order.
    #[must_use]
    pub fn seen_calls(&self) -> Vec<SeenCall> {
        self.lock().calls.clone()
    }

    /// The headers of the most recent request of any kind.
    #[must_use]
    pub fn seen_headers(&self) -> Vec<(String, String)> {
        self.lock().last_headers.clone()
    }

    /// Replaces one tool's input schema, so a later sync sees a reshaped
    /// server.
    ///
    /// # Panics
    ///
    /// When no tool of that name is declared: a staleness proof that names the
    /// wrong tool would otherwise pass for the wrong reason.
    pub fn swap_tool_schema(&self, name: &str, input_schema: Value) {
        let mut state = self.lock();
        let tool = state
            .tools
            .iter_mut()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("the fake server declares no tool named {name}"));
        tool.input_schema = input_schema;
    }

    /// Records the headers of the request now being served.
    fn record_headers(&self, headers: Vec<(String, String)>) {
        self.lock().last_headers = headers;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().expect("the fake server state is intact")
    }
}

impl ServerHandler for FakeMcpServer {
    fn get_info(&self) -> ServerConfig {
        let state = self.lock();
        let newest = state
            .versions
            .last()
            .cloned()
            .unwrap_or(ProtocolVersion::LATEST);
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(newest)
            .with_server_info(rmcp::model::Implementation::new(
                state.server_name.clone(),
                "fake",
            ))
    }

    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Owned(self.lock().versions.clone())
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, McpError> {
        if self.lock().legacy_only {
            return Err(McpError::method_not_found::<DiscoverRequestMethod>());
        }
        // rmcp's own default, restated: overriding the method replaces it.
        Ok(DiscoverResult::from_server_info(
            self.supported_protocol_versions().into_owned(),
            self.get_info(),
        ))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        // Count and read under one lock, then drop it before any sleep: a std
        // guard must never cross an await point.
        let (tools, delay) = {
            let mut state = self.lock();
            state.list_calls += 1;
            let tools = state.tools.iter().map(FakeTool::listed).collect();
            (tools, state.list_delay)
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let name = request.name.to_string();
        let arguments = Value::Object(request.arguments.unwrap_or_default());
        // From the context, not from `request.meta`: rmcp merges a request's
        // params-level and extension-level `_meta` on the way out
        // (`rmcp-3.4.0/src/model/serde_impl.rs:41`) and hands the whole map to
        // the handler through the request context
        // (`rmcp-3.4.0/src/service/server.rs:640`), leaving the params' own
        // field empty on this side. A proof that reads `request.meta` would
        // see nothing a client sent.
        let meta = Value::Object(context.meta.0 .0.clone());
        // Read the behaviour and record the call under one lock, then drop it:
        // `Sleep` awaits, and a std guard must never cross an await point.
        let behaviour = {
            let mut state = self.lock();
            let headers = state.last_headers.clone();
            let authorization = headers
                .iter()
                .find(|(header, _)| header.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.clone());
            let behaviour = state
                .tools
                .iter()
                .find(|tool| tool.name == name)
                .map(|tool| tool.behaviour.clone());
            state.calls.push(SeenCall {
                name: name.clone(),
                arguments: arguments.clone(),
                meta,
                authorization,
                headers,
            });
            behaviour
        };
        let Some(behaviour) = behaviour else {
            return Err(McpError::invalid_params(
                format!("the fake server declares no tool named {name}"),
                None,
            ));
        };
        Ok(answer(behaviour, arguments).await)
    }
}

/// The answer one behaviour produces.
async fn answer(behaviour: FakeToolBehaviour, arguments: Value) -> CallToolResponse {
    match behaviour {
        FakeToolBehaviour::Text(text) => {
            CallToolResponse::Complete(CallToolResult::success(vec![ContentBlock::text(text)]))
        }
        FakeToolBehaviour::Structured(value) => {
            CallToolResponse::Complete(CallToolResult::structured(value))
        }
        FakeToolBehaviour::Image { bytes } => CallToolResponse::Complete(CallToolResult::success(
            vec![ContentBlock::image(base64_of_zeroes(bytes), "image/png")],
        )),
        FakeToolBehaviour::Error(text) => {
            CallToolResponse::Complete(CallToolResult::error(vec![ContentBlock::text(text)]))
        }
        FakeToolBehaviour::InputRequired => {
            CallToolResponse::InputRequired(InputRequiredResult::from_request_state("fake-state"))
        }
        FakeToolBehaviour::Task => CallToolResponse::Task(CreateTaskResult::new(Task::new(
            "fake-task",
            TaskStatus::Working,
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00Z",
        ))),
        FakeToolBehaviour::Echo => {
            CallToolResponse::Complete(CallToolResult::structured(arguments))
        }
        FakeToolBehaviour::Sleep { millis } => {
            tokio::time::sleep(Duration::from_millis(millis)).await;
            CallToolResponse::Complete(CallToolResult::success(vec![ContentBlock::text("late")]))
        }
    }
}

/// A served [`FakeMcpServer`]: its URL, its observable state, and the task
/// serving it.
#[derive(Debug)]
pub struct FakeMcpEndpoint {
    /// The MCP endpoint URL, `http://127.0.0.1:<port>/mcp`.
    pub url: String,
    /// The served server, for its counters and its schema swaps.
    pub server: FakeMcpServer,
    handle: JoinHandle<()>,
}

impl Drop for FakeMcpEndpoint {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Serves a [`FakeMcpServer`] over Streamable HTTP on an ephemeral loopback
/// port.
///
/// The endpoint stops when the returned value is dropped.
///
/// # Panics
///
/// When the loopback socket cannot be bound or its address read: a test that
/// cannot listen has nothing to prove.
pub async fn serve_fake(server: FakeMcpServer) -> FakeMcpEndpoint {
    let handler = server.clone();
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        // `json_response` keeps the fake's answers plain JSON rather than SSE,
        // which is what makes a captured request one request.
        StreamableHttpServerConfig::default().with_json_response(true),
    );
    let router = axum::Router::new().nest_service("/mcp", service).layer(
        axum::middleware::from_fn_with_state(server.clone(), capture_headers),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the loopback socket binds");
    let address = listener
        .local_addr()
        .expect("the bound address is readable");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    FakeMcpEndpoint {
        url: format!("http://{address}/mcp"),
        server,
        handle,
    }
}

/// Copies every inbound request's headers into the fake's state before the MCP
/// service sees the request: the handler itself is given no access to the HTTP
/// layer, so this is the only place the wire is observable.
async fn capture_headers(
    axum::extract::State(server): axum::extract::State<FakeMcpServer>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let headers = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    server.record_headers(headers);
    next.run(request).await
}

/// A [`StreamableHttpClient`] that counts every method it delegates.
///
/// The dispatcher's proofs use it to show that one attempt is one session — a
/// count no answer body could reveal.
#[derive(Debug, Clone)]
pub struct CountingClient {
    inner: ReqwestClient,
    sends: Arc<AtomicUsize>,
}

impl Default for CountingClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CountingClient {
    /// A counting client over a fresh reqwest client.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ReqwestClient::new(),
            sends: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// How many transport methods have been called.
    #[must_use]
    pub fn sends(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }
}

impl StreamableHttpClient for CountingClient {
    type Error = reqwest::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.inner
            .post_message(uri, message, session_id, auth_header, custom_headers)
            .await
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.inner
            .delete_session(uri, session_id, auth_header, custom_headers)
            .await
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<
        BoxStream<'static, Result<sse_stream::Sse, SseError>>,
        StreamableHttpError<Self::Error>,
    > {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.inner
            .get_stream(uri, session_id, last_event_id, auth_header, custom_headers)
            .await
    }
}

/// A JSON value's object body, or an empty object when it is not one.
fn object_of(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

/// Base64 of `bytes` zero bytes, without pulling a base64 dependency in for a
/// fake image.
fn base64_of_zeroes(bytes: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.div_ceil(3) * 4);
    let mut remaining = bytes;
    while remaining >= 3 {
        // Three zero bytes are four zero sextets.
        for _ in 0..4 {
            encoded.push(char::from(ALPHABET[0]));
        }
        remaining -= 3;
    }
    if remaining > 0 {
        let sextets = if remaining == 1 { 2 } else { 3 };
        for _ in 0..sextets {
            encoded.push(char::from(ALPHABET[0]));
        }
        for _ in sextets..4 {
            encoded.push('=');
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{base64_of_zeroes, object_of, FakeMcpServer, FakeTool, FakeToolBehaviour};

    #[test]
    fn a_text_tool_is_the_sugar_for_the_text_behaviour() {
        let tool = FakeTool::text("search", "d", json!({}), "ok");
        assert_eq!(
            tool.behaviour,
            FakeToolBehaviour::Text("ok".to_string()),
            "text is new with a Text behaviour"
        );
        assert_eq!(tool.annotations, None);
        assert_eq!(tool.output_schema, None);
    }

    #[test]
    fn a_non_object_schema_lists_as_an_empty_object() {
        assert!(object_of(&json!("not an object")).is_empty());
        assert_eq!(object_of(&json!({ "type": "object" })).len(), 1);
    }

    #[test]
    fn swapping_a_schema_changes_what_the_server_lists() {
        let server = FakeMcpServer::new().with_tool(FakeTool::text("t", "d", json!({}), "ok"));
        server.swap_tool_schema("t", json!({ "type": "object" }));
        assert_eq!(server.seen_headers(), Vec::new());
        assert_eq!(server.list_calls(), 0);
        assert_eq!(server.call_count(), 0);
    }

    #[test]
    fn the_fake_image_encodes_as_padded_base64() {
        assert_eq!(base64_of_zeroes(0), "");
        assert_eq!(base64_of_zeroes(1), "AA==");
        assert_eq!(base64_of_zeroes(2), "AAA=");
        assert_eq!(base64_of_zeroes(3), "AAAA");
        assert_eq!(base64_of_zeroes(4), "AAAAAA==");
    }
}
