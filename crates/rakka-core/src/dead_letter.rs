//! Dead-letter telemetry for undeliverable local messages.

use serde::{Deserialize, Serialize};

use crate::ActorPath;

/// Reason a local message could not be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeadLetterReason {
    /// Destination mailbox was full.
    MailboxFull,
    /// Destination actor was already stopped or its mailbox was closed.
    MailboxClosed,
}

/// Metadata for an undeliverable local message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Destination actor path.
    pub recipient: ActorPath,
    /// Rust type name of the attempted message.
    pub message_type: String,
    /// Delivery failure reason.
    pub reason: DeadLetterReason,
}
