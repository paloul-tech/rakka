use prost::{Enumeration, Message};

#[derive(Clone, PartialEq, Message)]
pub(crate) struct ProtoRemoteEnvelope {
    #[prost(string, optional, tag = "1")]
    pub(crate) source: Option<String>,
    #[prost(message, optional, tag = "2")]
    pub(crate) destination: Option<ProtoRemoteDestination>,
    #[prost(string, tag = "3")]
    pub(crate) message_type_id: String,
    #[prost(uint32, tag = "4")]
    pub(crate) schema_version: u32,
    #[prost(string, tag = "5")]
    pub(crate) codec_id: String,
    #[prost(string, optional, tag = "6")]
    pub(crate) trace_context: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub(crate) request_id: Option<String>,
    #[prost(bytes, tag = "8")]
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct ProtoRemoteDestination {
    #[prost(enumeration = "ProtoDestinationKind", tag = "1")]
    pub(crate) kind: i32,
    #[prost(string, tag = "2")]
    pub(crate) path: String,
    #[prost(string, tag = "3")]
    pub(crate) entity_type: String,
    #[prost(string, tag = "4")]
    pub(crate) entity_id: String,
    #[prost(string, tag = "5")]
    pub(crate) service_key: String,
    #[prost(string, tag = "6")]
    pub(crate) route_key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Enumeration)]
pub(crate) enum ProtoDestinationKind {
    Unspecified = 0,
    ActorPath = 1,
    Entity = 2,
    Service = 3,
    RouteKey = 4,
}
