// ─────────────────────────────────────────────────────────────────────────────
// adapters/mod.rs — the pluggable protocol adapter interface.
//
// An Adapter is a sink: it receives protocol-neutral BridgeMessages and speaks
// them onto the local network in its own protocol (OSC, MIDI, WebSocket, …).
// Adapters are the ONLY place that knows about a specific wire protocol; adding
// a new one never touches the transport, router, or control layers.
// ─────────────────────────────────────────────────────────────────────────────

pub mod osc;

use crate::message::BridgeMessage;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Adapter: Send + Sync {
    /// Stable identifier, e.g. "osc".
    fn id(&self) -> &str;

    /// Deliver a batch of messages. Implementations should be cheap and
    /// non-blocking; a slow adapter must not stall the transport.
    async fn deliver(&self, messages: &[BridgeMessage]) -> Result<()>;
}
