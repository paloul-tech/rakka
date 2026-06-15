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
pub(crate) struct ProtoRemoteHandshake {
    #[prost(string, tag = "1")]
    pub(crate) node_id: String,
    #[prost(uint32, tag = "2")]
    pub(crate) protocol_major: u32,
    #[prost(uint32, tag = "3")]
    pub(crate) protocol_minor: u32,
    #[prost(uint32, tag = "4")]
    pub(crate) compatible_min_major: u32,
    #[prost(uint32, tag = "5")]
    pub(crate) compatible_min_minor: u32,
    #[prost(uint32, tag = "6")]
    pub(crate) compatible_max_major: u32,
    #[prost(uint32, tag = "7")]
    pub(crate) compatible_max_minor: u32,
    #[prost(uint32, tag = "8")]
    pub(crate) envelope_version: u32,
    #[prost(string, repeated, tag = "9")]
    pub(crate) capabilities: Vec<String>,
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
    #[prost(string, tag = "7")]
    pub(crate) request_id: String,
    #[prost(string, tag = "8")]
    pub(crate) actor_node_id: String,
    #[prost(string, tag = "9")]
    pub(crate) actor_system_name: String,
    #[prost(uint64, tag = "10")]
    pub(crate) actor_uid: u64,
    #[prost(string, tag = "11")]
    pub(crate) actor_message_type: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Enumeration)]
pub(crate) enum ProtoDestinationKind {
    Unspecified = 0,
    ActorPath = 1,
    Entity = 2,
    Service = 3,
    RouteKey = 4,
    Reply = 5,
    ActorRef = 6,
}
