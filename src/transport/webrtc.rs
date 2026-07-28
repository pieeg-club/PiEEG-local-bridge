// ─────────────────────────────────────────────────────────────────────────────
// transport/webrtc.rs — direct peer-to-peer WebRTC data channel.
//
// The browser producer connects DIRECTLY to this bridge over a DTLS-encrypted
// WebRTC data channel. No stream data ever transits a server. The only thing
// that touches the network besides the peer is a one-time SDP exchange through a
// stateless signaling endpoint:
//
//   1. The browser (offerer) POSTs its SDP offer to the signaling server, keyed
//      by the shared session code.
//   2. This bridge (answerer) GETs that offer, produces an SDP answer, and POSTs
//      it back.
//   3. ICE is gathered non-trickle (candidates are embedded in the SDP), so no
//      further signaling round-trips are needed — ideal for the same-machine /
//      same-LAN case where host candidates resolve immediately.
//   4. The data channel opens; JSON frames flow browser → bridge → router → OSC.
//
// Signaling HTTP contract (base = `signaling_url`, `{s}` = session code):
//   GET  {base}/v1/signal/{s}/offer   → 200 {"sdp": "..."} once available, else
//                                        204/404 (bridge keeps polling)
//   POST {base}/v1/signal/{s}/answer  ← {"sdp": "..."}
// ─────────────────────────────────────────────────────────────────────────────

use crate::adapters::Adapter;
use crate::router;
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Deserialize)]
struct OfferResp {
    sdp: String,
}

#[derive(Serialize)]
struct AnswerReq<'a> {
    sdp: &'a str,
}

enum SessionEnd {
    /// The peer closed cleanly — stop reconnecting.
    Ended,
    /// The connection dropped or failed to establish — reconnect if allowed.
    Dropped(Option<String>),
}

/// Run the WebRTC transport until the task is aborted. Reconnects with capped
/// backoff when `auto_reconnect` is set; otherwise exits after one session.
pub async fn run(state: Arc<AppState>, session_id: String) {
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(10);

    loop {
        let (signaling_url, auto_reconnect) = {
            let cfg = state.config.read().await;
            (cfg.signaling_url.clone(), cfg.auto_reconnect)
        };

        match run_session(&state, &signaling_url, &session_id).await {
            SessionEnd::Ended => {
                let mut s = state.status.write().await;
                s.connected = false;
                s.ended = true;
                break;
            }
            SessionEnd::Dropped(err) => {
                {
                    let mut s = state.status.write().await;
                    s.connected = false;
                    s.last_error = err;
                }
                if !auto_reconnect {
                    break;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

async fn run_session(
    state: &Arc<AppState>,
    signaling_url: &str,
    session_id: &str,
) -> SessionEnd {
    // ── Build the peer connection (data-channel only) ───────────────────────
    let mut media = MediaEngine::default();
    if let Err(e) = media.register_default_codecs() {
        return SessionEnd::Dropped(Some(format!("media engine: {e}")));
    }
    let api = APIBuilder::new().with_media_engine(media).build();

    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let pc = match api.new_peer_connection(config).await {
        Ok(pc) => Arc::new(pc),
        Err(e) => return SessionEnd::Dropped(Some(format!("peer connection: {e}"))),
    };

    // ── Lifecycle signalling from callbacks back to this task ───────────────
    let (end_tx, mut end_rx) = mpsc::channel::<SessionEnd>(1);

    {
        let end_tx = end_tx.clone();
        let state = state.clone();
        pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            let end_tx = end_tx.clone();
            let state = state.clone();
            Box::pin(async move {
                match s {
                    RTCPeerConnectionState::Connected => {
                        let mut st = state.status.write().await;
                        st.connected = true;
                        st.ended = false;
                        st.last_error = None;
                    }
                    RTCPeerConnectionState::Disconnected
                    | RTCPeerConnectionState::Failed => {
                        let _ = end_tx
                            .try_send(SessionEnd::Dropped(Some("peer connection lost".into())));
                    }
                    RTCPeerConnectionState::Closed => {
                        let _ = end_tx.try_send(SessionEnd::Ended);
                    }
                    _ => {}
                }
            })
        }));
    }

    // ── Route inbound data-channel frames to the OSC adapter ────────────────
    {
        let state = state.clone();
        pc.on_data_channel(Box::new(move |dc| {
            let state = state.clone();
            Box::pin(async move {
                let state_open = state.clone();
                dc.on_open(Box::new(move || {
                    let state = state_open.clone();
                    Box::pin(async move {
                        state.status.write().await.connected = true;
                    })
                }));

                dc.on_message(Box::new(move |msg: DataChannelMessage| {
                    let state = state.clone();
                    Box::pin(async move {
                        handle_frame(&state, &msg.data).await;
                    })
                }));
            })
        }));
    }

    // ── Signaling: fetch offer → answer → post answer ───────────────────────
    let http = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => return SessionEnd::Dropped(Some(format!("http client: {e}"))),
    };
    let base = signaling_url.trim_end_matches('/');

    let offer_sdp = match fetch_offer(&http, base, session_id).await {
        Ok(sdp) => sdp,
        Err(e) => return SessionEnd::Dropped(Some(format!("fetch offer: {e}"))),
    };

    let offer = match RTCSessionDescription::offer(offer_sdp) {
        Ok(o) => o,
        Err(e) => return SessionEnd::Dropped(Some(format!("bad offer: {e}"))),
    };
    if let Err(e) = pc.set_remote_description(offer).await {
        return SessionEnd::Dropped(Some(format!("set remote: {e}")));
    }

    let answer = match pc.create_answer(None).await {
        Ok(a) => a,
        Err(e) => return SessionEnd::Dropped(Some(format!("create answer: {e}"))),
    };

    // Gather all ICE candidates before sending the answer (non-trickle).
    let mut gather_complete = pc.gathering_complete_promise().await;
    if let Err(e) = pc.set_local_description(answer).await {
        return SessionEnd::Dropped(Some(format!("set local: {e}")));
    }
    let _ = gather_complete.recv().await;

    let local = match pc.local_description().await {
        Some(d) => d,
        None => return SessionEnd::Dropped(Some("no local description".into())),
    };
    if let Err(e) = post_answer(&http, base, session_id, &local.sdp).await {
        return SessionEnd::Dropped(Some(format!("post answer: {e}")));
    }

    // ── Wait until the connection ends ──────────────────────────────────────
    let end = end_rx.recv().await.unwrap_or(SessionEnd::Dropped(None));
    let _ = pc.close().await;
    end
}

/// Decode one data-channel frame (JSON) and forward it through the router.
async fn handle_frame(state: &Arc<AppState>, data: &[u8]) {
    let json: Value = match serde_json::from_slice(data) {
        Ok(v) => v,
        Err(_) => return,
    };

    let osc_cfg = { state.config.read().await.osc.clone() };
    if !osc_cfg.enabled {
        return;
    }

    let messages = router::map_inbound(&json, &osc_cfg);
    let out_count = messages.len() as u64;
    if out_count > 0 {
        let _ = state.osc.deliver(&messages).await;
    }

    let mut s = state.status.write().await;
    s.frames_in += 1;
    s.messages_out += out_count;
    s.last_frame_ms = Some(now_ms());
}

/// Poll the signaling endpoint until the browser's SDP offer is available.
async fn fetch_offer(
    http: &reqwest::Client,
    base: &str,
    session_id: &str,
) -> anyhow::Result<String> {
    let url = format!("{base}/v1/signal/{session_id}/offer");
    // Poll for up to ~2 minutes; the user has just entered the code.
    for _ in 0..240 {
        let resp = http.get(&url).send().await?;
        if resp.status().is_success() {
            // 204 No Content → not ready yet.
            if resp.status().as_u16() != 204 {
                if let Ok(offer) = resp.json::<OfferResp>().await {
                    if !offer.sdp.is_empty() {
                        return Ok(offer.sdp);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("timed out waiting for offer")
}

/// POST the SDP answer back to the signaling endpoint.
async fn post_answer(
    http: &reqwest::Client,
    base: &str,
    session_id: &str,
    sdp: &str,
) -> anyhow::Result<()> {
    let url = format!("{base}/v1/signal/{session_id}/answer");
    let resp = http.post(&url).json(&AnswerReq { sdp }).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("signaling rejected answer ({})", resp.status());
    }
    Ok(())
}
