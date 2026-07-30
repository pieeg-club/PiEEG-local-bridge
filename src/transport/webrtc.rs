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
use crate::state::{AppState, PeerInfo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
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
    /// Optional metadata the peer supplies about itself through signaling.
    /// Absent on older/minimal signaling servers — we then fall back to
    /// whatever we can read from the SDP alone.
    #[serde(default)]
    peer: Option<PeerMeta>,
}

/// Self-declared peer identity carried alongside the offer. Advisory only —
/// treated as untrusted display text, never used for authorization.
#[derive(Deserialize, Default)]
struct PeerMeta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    origin: Option<String>,
    #[serde(default, rename = "userAgent", alias = "user_agent")]
    user_agent: Option<String>,
}

#[derive(Serialize)]
struct AnswerReq<'a> {
    sdp: &'a str,
}

/// One ICE server as returned by the cloud (`urls` may be a single string or a
/// list; `username`/`credential` present only for TURN).
#[derive(Deserialize)]
struct IceServerCfg {
    urls: StringOrVec,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    credential: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrVec {
    One(String),
    Many(Vec<String>),
}

impl From<IceServerCfg> for RTCIceServer {
    fn from(cfg: IceServerCfg) -> Self {
        let urls = match cfg.urls {
            StringOrVec::One(s) => vec![s],
            StringOrVec::Many(v) => v,
        };
        RTCIceServer {
            urls,
            username: cfg.username.unwrap_or_default(),
            credential: cfg.credential.unwrap_or_default(),
        }
    }
}

/// Response body of `POST /v1/webrtc/session`.
#[derive(Deserialize)]
struct CreateSessionResp {
    code: String,
    #[serde(rename = "iceServers", default)]
    ice_servers: Vec<IceServerCfg>,
    #[serde(rename = "expiresAt", default)]
    expires_at: u64,
}

/// A freshly created cloud rendezvous session: the share code plus the ICE
/// servers to use for the peer connection.
pub struct SessionInfo {
    pub code: String,
    pub ice_servers: Vec<RTCIceServer>,
    #[allow(dead_code)]
    pub expires_at: u64,
}

/// Create a new WebRTC rendezvous session on the cloud signaling server.
///
/// `POST {signaling_url}/v1/webrtc/session` → `{ code, iceServers, expiresAt }`.
/// The returned `code` is the 6-character string the user shares with the web
/// app; the browser resolves it via `GET /v1/webrtc/session/{code}` to obtain
/// the same ICE configuration before starting the SDP exchange.
pub async fn create_session(signaling_url: &str) -> anyhow::Result<SessionInfo> {
    let base = signaling_url.trim_end_matches('/');
    let url = format!("{base}/v1/webrtc/session");
    let http = reqwest::Client::builder().build()?;
    let resp = http.post(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("create session failed ({})", resp.status());
    }
    let body: CreateSessionResp = resp.json().await?;
    Ok(SessionInfo {
        code: body.code,
        ice_servers: body.ice_servers.into_iter().map(Into::into).collect(),
        expires_at: body.expires_at,
    })
}

enum SessionEnd {
    /// The peer closed cleanly — stop reconnecting.
    Ended,
    /// The user declined the connection — stop and clear status.
    Rejected,
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
            SessionEnd::Rejected => {
                clear_pending(&state).await;
                let mut s = state.status.write().await;
                s.connected = false;
                s.ended = true;
                s.last_error = Some("connection declined".into());
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

async fn run_session(state: &Arc<AppState>, signaling_url: &str, session_id: &str) -> SessionEnd {
    // ── Build the peer connection (data-channel only) ───────────────────────
    let mut media = MediaEngine::default();
    if let Err(e) = media.register_default_codecs() {
        return SessionEnd::Dropped(Some(format!("media engine: {e}")));
    }
    let api = APIBuilder::new().with_media_engine(media).build();

    // Use the ICE servers handed back by the cloud session; fall back to a
    // public STUN server when none were provided.
    let ice_servers = {
        let servers = state.ice_servers.read().await;
        if servers.is_empty() {
            vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_string()],
                ..Default::default()
            }]
        } else {
            servers.clone()
        }
    };
    let config = RTCConfiguration {
        ice_servers,
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
                    RTCPeerConnectionState::Disconnected | RTCPeerConnectionState::Failed => {
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

    let (offer_sdp, peer_meta) = match fetch_offer(&http, base, session_id).await {
        Ok(v) => v,
        Err(e) => return SessionEnd::Dropped(Some(format!("fetch offer: {e}"))),
    };

    // ── Human-in-the-loop confirmation ──────────────────────────────────────
    // Before we complete the handshake, let the user see who is trying to
    // connect and explicitly accept or decline. We only prompt once per
    // session: a confirmed session that later drops reconnects silently.
    let already_confirmed = state.status.read().await.confirmed;
    if !already_confirmed {
        match await_confirmation(state, &offer_sdp, peer_meta).await {
            Confirmation::Accepted => {
                state.status.write().await.confirmed = true;
            }
            Confirmation::Declined => return SessionEnd::Rejected,
        }
    }

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
/// Returns the offer SDP together with any peer metadata the signaling server
/// attached to it.
async fn fetch_offer(
    http: &reqwest::Client,
    base: &str,
    session_id: &str,
) -> anyhow::Result<(String, Option<PeerMeta>)> {
    let url = format!("{base}/v1/signal/{session_id}/offer");
    // Poll for up to ~3 minutes; the user has just been shown the code and needs
    // time to open the web app and enter it. Polling at 1 Hz keeps signaling
    // load low (well within the server's per-IP rate limit).
    for _ in 0..180 {
        let resp = http.get(&url).send().await?;
        if resp.status().is_success() {
            // 204 No Content → not ready yet.
            if resp.status().as_u16() != 204 {
                if let Ok(offer) = resp.json::<OfferResp>().await {
                    if !offer.sdp.is_empty() {
                        return Ok((offer.sdp, offer.peer));
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    anyhow::bail!("timed out waiting for offer")
}

/// Outcome of the accept/decline prompt.
enum Confirmation {
    Accepted,
    Declined,
}

/// How long the prompt stays open before we auto-decline. Keeps a forgotten
/// prompt from parking the session forever.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(120);

/// Publish the pending peer's details, then block until the user accepts or
/// declines from the control UI (or the prompt times out).
async fn await_confirmation(
    state: &Arc<AppState>,
    offer_sdp: &str,
    meta: Option<PeerMeta>,
) -> Confirmation {
    let peer = build_peer_info(offer_sdp, meta);
    tracing::info!(
        "connection request from {} — awaiting user confirmation",
        peer.origin.as_deref().or(peer.name.as_deref()).unwrap_or("unknown peer")
    );

    let (tx, rx) = oneshot::channel::<bool>();
    {
        *state.pending_decision.lock().await = Some(tx);
        let mut s = state.status.write().await;
        s.awaiting_confirmation = true;
        s.pending_peer = Some(peer);
    }

    let accepted = tokio::select! {
        decision = rx => decision.unwrap_or(false),
        _ = tokio::time::sleep(CONFIRM_TIMEOUT) => {
            tracing::warn!("connection request timed out — auto-declining");
            false
        }
    };

    clear_pending(state).await;

    if accepted {
        Confirmation::Accepted
    } else {
        Confirmation::Declined
    }
}

/// Clear any pending-confirmation state (called after a decision, timeout, or
/// when a session is torn down).
async fn clear_pending(state: &Arc<AppState>) {
    *state.pending_decision.lock().await = None;
    let mut s = state.status.write().await;
    s.awaiting_confirmation = false;
    s.pending_peer = None;
}

/// Assemble the peer description shown to the user: self-declared metadata
/// (advisory) plus network facts parsed from the offer's ICE candidates.
fn build_peer_info(offer_sdp: &str, meta: Option<PeerMeta>) -> PeerInfo {
    let mut info = PeerInfo {
        requested_ms: now_ms(),
        ..Default::default()
    };
    if let Some(m) = meta {
        info.name = m.name.filter(|s| !s.trim().is_empty());
        info.origin = m.origin.filter(|s| !s.trim().is_empty());
        info.user_agent = m.user_agent.filter(|s| !s.trim().is_empty());
    }

    // Parse `a=candidate:` lines for remote IPs and candidate types.
    //   a=candidate:<foundation> <component> <transport> <priority> <ip> <port> typ <type> ...
    for line in offer_sdp.lines() {
        let line = line.trim();
        let rest = match line.strip_prefix("a=candidate:") {
            Some(r) => r,
            None => continue,
        };
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        if let Some(ip) = tokens.get(4) {
            let ip = ip.to_string();
            // Skip mDNS-obfuscated candidates (`.local`) — not user-meaningful.
            if !ip.ends_with(".local") && !info.ip_addresses.contains(&ip) {
                info.ip_addresses.push(ip);
            }
        }
        if let Some(pos) = tokens.iter().position(|t| *t == "typ") {
            if let Some(kind) = tokens.get(pos + 1) {
                let kind = kind.to_string();
                if !info.candidate_types.contains(&kind) {
                    info.candidate_types.push(kind);
                }
            }
        }
    }

    info
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
