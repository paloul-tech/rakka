//! The name a model provider is shown for each tool a request offers.
//!
//! An [`AgentToolId`](crate::definition::AgentToolId) admits up to 256 bytes
//! of almost anything — no control character, no `/`, no `|`. The providers
//! accept far less for a tool's name, and refuse the **whole request** when
//! one declared name is outside their rule: Anthropic
//! `^[a-zA-Z0-9_-]{1,128}$` (Claude API, "Define tools"), OpenAI
//! `[a-zA-Z0-9_-]` up to 64 bytes (`openai/openai-openapi`,
//! `FunctionObject.name`). Every MCP tool id is `mcp.<server>.<tool>`, so a
//! request offering an MCP tool under its id is refused at the first turn,
//! and so is one offering a function tool whose id carries a `.`, a space,
//! or a non-ASCII character.
//!
//! So an adapter declares each tool under [`provider_tool_name`] and reads
//! each call the provider makes back through [`AgentProviderToolNames`],
//! built from the same request's tools. The name is a pure function of the
//! id, so a replayed turn declares the same names:
//!
//! - an id already inside both providers' rules
//!   ([`is_provider_safe_tool_name`]) is its own name, so a request whose ids
//!   all are declares byte-identical names to an adapter that never renamed;
//! - any other id is renamed: each character outside `[A-Za-z0-9_-]` becomes
//!   `_`, the result keeps its last characters to leave room, and `_` plus
//!   the first [`AGENT_PROVIDER_TOOL_NAME_SUFFIX_HEX`] hex characters of the
//!   id's SHA-256 is appended. Two ids that sanitize alike (`a.b` and `a_b`)
//!   still get different names, and `mcp.crm.lookup` stays readable to the
//!   model as `mcp_crm_lookup_c41f30c9`. The cut keeps the **tail** because
//!   every tool of one MCP server shares the head (`mcp_<server>_`): a long
//!   server id cut from the end would leave the model siblings it could tell
//!   apart by description alone.
//!
//! The target is the **intersection** of the two rules — at most
//! [`AGENT_PROVIDER_TOOL_NAME_MAX_BYTES`] bytes — so one name serves both
//! providers and a profile can move between them without renaming anything.
//! A safe id longer than that is renamed too: Anthropic would accept it, but
//! OpenAI would not.
//!
//! Two different ids can still name alike: a tool deliberately named like
//! another tool's rename, a result tool spelled that way, or two renamed ids
//! whose kept characters and digest prefixes both coincide. A request
//! offering both is refused ([`AgentToolNameCollision`]) before anything is
//! sent, because a call under that name could not be read back to one of
//! them.
//!
//! What the provider is shown is the only thing that moves: the durable
//! turn, the authority, the grant, every record and every digest keep the
//! tool id. The encoding is shared with `rakka.host`, whose adapter declares
//! tools under the same names; the literal in this module's tests pins it.
//!
//! Specification: section 10.1. Filled by issue #85 (ask 16).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use crate::task::AgentContentDigest;

/// The longest name a tool is declared to a provider under: OpenAI's bound,
/// the narrower of the two providers' (Anthropic accepts 128).
pub const AGENT_PROVIDER_TOOL_NAME_MAX_BYTES: usize = 64;

/// How many hex characters of the id's SHA-256 a renamed tool carries:
/// enough that two different ids sharing a sanitized tail are told apart,
/// short enough to leave the model a readable name. A shared suffix is not a
/// misroute: [`AgentProviderToolNames::new`] refuses it.
pub const AGENT_PROVIDER_TOOL_NAME_SUFFIX_HEX: usize = 8;

/// Whether `name` is inside both providers' rules as it stands: non-empty,
/// at most [`AGENT_PROVIDER_TOOL_NAME_MAX_BYTES`] bytes, every byte in
/// `[A-Za-z0-9_-]`.
#[must_use]
pub fn is_provider_safe_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= AGENT_PROVIDER_TOOL_NAME_MAX_BYTES
        && name.bytes().all(allowed)
}

/// The name a provider is shown for the tool `tool`: `tool` itself when it
/// is [`is_provider_safe_tool_name`], otherwise its sanitized, cut,
/// digest-suffixed rename (see the module documentation).
#[must_use]
pub fn provider_tool_name(tool: &str) -> Cow<'_, str> {
    if is_provider_safe_tool_name(tool) {
        return Cow::Borrowed(tool);
    }
    let digest = AgentContentDigest::sha256_of_bytes(tool.as_bytes());
    // Every character maps to exactly one ASCII byte, so the byte slice
    // below always falls on a character boundary.
    let sanitized: String = tool
        .chars()
        .map(|character| match u8::try_from(character) {
            Ok(byte) if allowed(byte) => character,
            _ => '_',
        })
        .collect();
    // The tail, not the head: see the module documentation.
    let keep = AGENT_PROVIDER_TOOL_NAME_MAX_BYTES - 1 - AGENT_PROVIDER_TOOL_NAME_SUFFIX_HEX;
    let mut name = sanitized[sanitized.len().saturating_sub(keep)..].to_string();
    name.push('_');
    name.push_str(&digest.value[..AGENT_PROVIDER_TOOL_NAME_SUFFIX_HEX]);
    Cow::Owned(name)
}

/// The bytes both providers accept in a tool name.
const fn allowed(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

/// Two different tools one request offers would be declared under one
/// provider name.
///
/// Names both ids, in the order they were offered. Its stable code is
/// [`Self::code`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentToolNameCollision {
    /// The id declared first under the shared name.
    pub first: String,
    /// The different id offered after it under the same name.
    pub second: String,
}

impl AgentToolNameCollision {
    /// Stable code: `model-tool-name-collision`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        "model-tool-name-collision"
    }
}

impl Display for AgentToolNameCollision {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the tools {:?} and {:?} would both be declared to the provider as {:?}",
            self.first,
            self.second,
            provider_tool_name(&self.first)
        )
    }
}

impl std::error::Error for AgentToolNameCollision {}

/// The provider names one request declares, mapped back to the tool ids they
/// stand for.
///
/// Built from exactly the tools the request offers — the model-visible
/// descriptors and the result tool — so the map is per request and never
/// outlives the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProviderToolNames {
    /// Provider name → tool id.
    ids: BTreeMap<String, String>,
}

impl AgentProviderToolNames {
    /// The names `tools` are declared under.
    ///
    /// The same id offered twice is one entry, not a collision.
    ///
    /// # Errors
    ///
    /// [`AgentToolNameCollision`] when two different ids share a provider
    /// name, naming the one declared first and the one offered after it.
    pub fn new<'a>(
        tools: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, AgentToolNameCollision> {
        let mut ids: BTreeMap<String, String> = BTreeMap::new();
        for tool in tools {
            let name = provider_tool_name(tool).into_owned();
            match ids.get(&name) {
                Some(existing) if existing != tool => {
                    return Err(AgentToolNameCollision {
                        first: existing.clone(),
                        second: tool.to_string(),
                    });
                }
                Some(_) => {}
                None => {
                    ids.insert(name, tool.to_string());
                }
            }
        }
        Ok(Self { ids })
    }

    /// The tool id a provider's call names, read back through this request's
    /// declarations.
    ///
    /// A name the request never declared is answered as itself: an adapter
    /// never filters a call the model made — the authority refuses a tool it
    /// does not know, and a silently dropped call would hide the ask.
    #[must_use]
    pub fn tool<'a>(&'a self, name: &'a str) -> &'a str {
        self.ids.get(name).map_or(name, String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both providers' rules at once, written out independently of the code
    /// under test: the narrower length, the shared character set.
    fn inside_both_rules(name: &str) -> bool {
        (1..=64).contains(&name.len())
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    }

    #[test]
    fn an_id_already_inside_both_rules_is_its_own_name() {
        for id in [
            "echo",
            "submit_result",
            "invoice_lookup",
            "a-b_C9",
            &"x".repeat(64),
        ] {
            assert!(is_provider_safe_tool_name(id), "{id:?} is safe");
            assert!(
                matches!(provider_tool_name(id), Cow::Borrowed(name) if name == id),
                "{id:?} keeps its name"
            );
        }
    }

    #[test]
    fn an_mcp_id_is_renamed_readably_and_deterministically() {
        // A literal, hand-checked (`printf %s mcp.crm.lookup | shasum -a 256`
        // begins `c41f30c9`) and identical to `rakka.host`'s own pin, so a
        // change to the cut, the suffix length or the hash fails here rather
        // than passing against its own recomputation — and the two repos'
        // names cannot drift apart unnoticed.
        assert!(!is_provider_safe_tool_name("mcp.crm.lookup"));
        assert_eq!(
            provider_tool_name("mcp.crm.lookup"),
            "mcp_crm_lookup_c41f30c9"
        );
        assert_eq!(
            provider_tool_name("mcp.crm.lookup"),
            "mcp_crm_lookup_c41f30c9"
        );
    }

    #[test]
    fn a_long_id_keeps_its_tail_so_one_servers_tools_stay_apart() {
        let server = "s".repeat(60);
        let lookup = provider_tool_name(&format!("mcp.{server}.lookup")).into_owned();
        let create = provider_tool_name(&format!("mcp.{server}.create")).into_owned();
        assert_eq!(lookup.len(), 64);
        assert_eq!(create.len(), 64);
        // 64 bytes = 55 kept + `_` + 8 hex.
        assert!(lookup[..55].ends_with("_lookup"), "{lookup}");
        assert!(create[..55].ends_with("_create"), "{create}");
    }

    #[test]
    fn a_safe_id_longer_than_the_narrower_rule_is_renamed() {
        let long = "y".repeat(65);
        assert!(!is_provider_safe_tool_name(&long));
        let name = provider_tool_name(&long);
        assert_eq!(name.len(), 64);
        assert!(name.starts_with(&"y".repeat(55)), "{name}");
    }

    #[test]
    fn every_renamed_id_is_inside_both_rules() {
        let long_safe = "y".repeat(65);
        let longest = "z.".repeat(128);
        for id in [
            "mcp.crm.lookup",
            "mcp.github.repos.list",
            "with space",
            "ünïcödé",
            "名前",
            long_safe.as_str(),
            longest.as_str(),
            "a:b@c",
        ] {
            let name = provider_tool_name(id);
            assert!(inside_both_rules(&name), "{id:?} → {name:?}");
            assert_ne!(name, id, "{id:?} needed a rename");
        }
    }

    #[test]
    fn two_ids_that_sanitize_alike_still_get_different_names() {
        assert_ne!(
            provider_tool_name("mcp.crm.a_b"),
            provider_tool_name("mcp.crm.a.b")
        );
        let a = format!("{}.a", "p".repeat(80));
        let b = format!("{}.b", "p".repeat(80));
        assert_ne!(provider_tool_name(&a), provider_tool_name(&b));
    }

    #[test]
    fn a_call_reads_back_to_the_id_and_an_undeclared_name_to_itself() {
        let names = AgentProviderToolNames::new(["echo", "mcp.crm.lookup", "submit_result"])
            .expect("no collision");
        assert_eq!(names.tool("mcp_crm_lookup_c41f30c9"), "mcp.crm.lookup");
        assert_eq!(names.tool("echo"), "echo");
        assert_eq!(names.tool("submit_result"), "submit_result");
        assert_eq!(names.tool("never-declared"), "never-declared");
    }

    #[test]
    fn two_ids_under_one_name_are_refused_and_one_id_twice_is_not() {
        let collision = AgentProviderToolNames::new(["mcp.crm.lookup", "mcp_crm_lookup_c41f30c9"])
            .expect_err("a lookalike shares the rename");
        assert_eq!(
            collision,
            AgentToolNameCollision {
                first: "mcp.crm.lookup".to_string(),
                second: "mcp_crm_lookup_c41f30c9".to_string(),
            },
            "the collision names both ids, in the order offered"
        );
        assert_eq!(collision.code(), "model-tool-name-collision");
        assert!(AgentProviderToolNames::new(["mcp.crm.lookup", "mcp.crm.lookup", "echo"]).is_ok());
    }
}
