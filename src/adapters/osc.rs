// ─────────────────────────────────────────────────────────────────────────────
// adapters/osc.rs — OSC 1.0 over UDP.
//
// Translates protocol-neutral BridgeMessages into OSC packets and sends them to
// a configurable host:port. The destination is hot-swappable at runtime (guarded
// by an RwLock) so the control UI can retarget without reconnecting the relay.
// ─────────────────────────────────────────────────────────────────────────────

use super::Adapter;
use crate::message::{Arg, BridgeMessage};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rosc::{encoder, OscMessage, OscPacket, OscType};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

pub struct OscAdapter {
    socket: UdpSocket,
    target: RwLock<SocketAddr>,
}

impl OscAdapter {
    /// Bind an ephemeral local UDP socket and set the initial target.
    pub async fn new(target: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .context("binding OSC UDP socket")?;
        Ok(Self {
            socket,
            target: RwLock::new(target),
        })
    }

    /// Retarget at runtime (called when the user edits the OSC destination).
    pub async fn set_target(&self, target: SocketAddr) {
        *self.target.write().await = target;
    }
}

fn to_osc_type(arg: &Arg) -> OscType {
    match arg {
        Arg::Bool(b) => OscType::Bool(*b),
        Arg::Int(i) => OscType::Int(*i as i32),
        Arg::Float(f) => OscType::Float(*f as f32),
        Arg::Str(s) => OscType::String(s.clone()),
    }
}

#[async_trait]
impl Adapter for OscAdapter {
    fn id(&self) -> &str {
        "osc"
    }

    async fn deliver(&self, messages: &[BridgeMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let target = *self.target.read().await;
        for m in messages {
            let packet = OscPacket::Message(OscMessage {
                addr: m.address.clone(),
                args: m.args.iter().map(to_osc_type).collect(),
            });
            let buf = match encoder::encode(&packet) {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!("skip un-encodable OSC message {}: {e}", m.address);
                    continue;
                }
            };
            if let Err(e) = self.socket.send_to(&buf, target).await {
                tracing::debug!("OSC send to {target} failed: {e}");
            }
        }
        Ok(())
    }
}
