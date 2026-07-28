// ─────────────────────────────────────────────────────────────────────────────
// message.rs — the vendor-neutral, protocol-neutral currency of the bridge.
//
// Everything that flows through the router is a `BridgeMessage`: an OSC-style
// address plus a list of typed `Arg`s. This shape is deliberately generic —
// it maps cleanly onto OSC, MIDI (via mapping), WebSocket JSON, plain UDP, etc.
// The core knows NOTHING about EEG, PiEEG, or any specific producer.
// ─────────────────────────────────────────────────────────────────────────────

use serde::{Deserialize, Serialize};

/// A single typed argument. Untagged so it round-trips naturally to/from JSON
/// (`0.5`, `3`, `true`, `"hi"`), which is what cloud producers emit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Arg {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

/// One protocol-neutral message: an address and its arguments.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BridgeMessage {
    pub address: String,
    #[serde(default)]
    pub args: Vec<Arg>,
}

impl BridgeMessage {
    pub fn new(address: impl Into<String>, args: Vec<Arg>) -> Self {
        Self { address: address.into(), args }
    }
}
