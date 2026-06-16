//! REST and remote-message data model.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CounterAction {
    Initiate,
    Get,
    Increase,
    Decrease,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterOperation {
    pub name: String,
    pub amount: i64,
    pub action: CounterAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterValue {
    pub name: String,
    pub value: i64,
    pub revision: u64,
    pub initialized: bool,
    pub created: bool,
    pub owner_node: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitiateCounterRequest {
    #[serde(default)]
    pub initial_value: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeCounterRequest {
    #[serde(default = "default_change_amount")]
    pub amount: i64,
}

pub fn default_change_amount() -> i64 {
    1
}
