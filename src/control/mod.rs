// ─────────────────────────────────────────────────────────────────────────────
// control/mod.rs — the local control plane.
//
// A tiny HTTP server on 127.0.0.1 that serves the embedded connection UI and a
// small JSON API. Everything a non-technical user needs happens here: enter the
// session code, pick an OSC destination, watch the live status. No terminal.
//
// CORS is driven entirely by config (`allowed_origins`) — the core bakes in NO
// vendor origins. Operators opt specific sites in via config or `--allow-origin`.
// ─────────────────────────────────────────────────────────────────────────────

use crate::adapters::Adapter;
use crate::state::{AppState, Ctrl};
use axum::{
    extract::State,
    http::{HeaderValue, Method, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

const UI_HTML: &str = include_str!("ui.html");
const TEST_HTML: &str = include_str!("test.html");

/// Generate a random 6-character pairing code (uppercase alphanumeric, no ambiguous chars).
pub fn generate_pairing_code() -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // No O/0, I/1
    let mut rng = rand::thread_rng();
    (0..6)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

pub fn router(state: Arc<AppState>, allowed_origins: Vec<String>) -> Router {
    let origins: Vec<HeaderValue> = allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET, Method::POST, Method::PUT])
        .allow_headers([axum::http::header::CONTENT_TYPE]);

    Router::new()
        .route("/", get(index))
        .route("/test", get(test_page))
        .route("/icon.png", get(icon))
        .route("/api/health", get(health))
        .route("/api/status", get(status))
        .route("/api/regenerate", post(regenerate))
        .route("/api/confirm", post(confirm))
        .route("/api/reject", post(reject))
        .route("/api/disconnect", post(disconnect))
        .route("/api/osc", put(update_osc))
        .route("/signal", post(signal))
        .layer(cors)
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(UI_HTML)
}

async fn test_page() -> Html<&'static str> {
    Html(TEST_HTML)
}

async fn icon() -> impl IntoResponse {
    use axum::http::header;
    let bytes = include_bytes!("../../icon.png");
    ([(header::CONTENT_TYPE, "image/png")], bytes.as_slice())
}

async fn health() -> impl IntoResponse {
    Json(json!({ "ok": true, "app": "pieeg-local-bridge" }))
}

async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let status = state.snapshot_status().await;
    let config = state.config.read().await.clone();
    let discovered = state.discovered.read().await.clone();
    Json(json!({
        "status": status,
        "config": config,
        "discovered": discovered,
    }))
}

async fn regenerate(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.ctrl.send(Ctrl::RegenerateCode).await.is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "orchestrator unavailable" })),
        );
    }
    (StatusCode::OK, Json(json!({ "ok": true })))
}

async fn disconnect(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let _ = state.ctrl.send(Ctrl::Disconnect).await;
    Json(json!({ "ok": true }))
}

/// Accept the pending connection request, letting the WebRTC handshake proceed.
async fn confirm(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.resolve_pending(true).await {
        (StatusCode::OK, Json(json!({ "ok": true })))
    } else {
        (
            StatusCode::CONFLICT,
            Json(json!({ "error": "no pending request" })),
        )
    }
}

/// Decline the pending connection request; the peer is dropped.
async fn reject(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.resolve_pending(false).await {
        (StatusCode::OK, Json(json!({ "ok": true })))
    } else {
        (
            StatusCode::CONFLICT,
            Json(json!({ "error": "no pending request" })),
        )
    }
}

#[derive(Deserialize)]
struct SignalReq {
    sdp: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    sdp_type: String,
    session_id: String,
}

async fn signal(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SignalReq>,
) -> impl IntoResponse {
    // Validate pairing code
    let expected_code = {
        let status = state.status.read().await;
        status.pairing_code.clone()
    };

    if req.session_id.trim().to_uppercase() != expected_code.to_uppercase() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "invalid pairing code" })),
        );
    }

    // Mark as paired
    {
        let mut s = state.status.write().await;
        s.paired = true;
        s.session_id = Some(req.session_id.clone());
    }

    // Create WebRTC API
    let media_engine = MediaEngine::default();
    let api = APIBuilder::new().with_media_engine(media_engine).build();

    // Create peer connection (no STUN/TURN needed for localhost)
    let config = RTCConfiguration {
        ice_servers: vec![],
        ..Default::default()
    };

    let peer = match api.new_peer_connection(config).await {
        Ok(p) => Arc::new(p),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("failed to create peer connection: {}", e) })),
            );
        }
    };

    // Clone for the data channel handler
    let state_clone = Arc::clone(&state);

    // Handle incoming data channel
    peer.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
        let state = Arc::clone(&state_clone);
        Box::pin(async move {
            tracing::info!("data channel opened: {}", dc.label());

            // Update connection status
            {
                let mut s = state.status.write().await;
                s.connected = true;
            }

            // Read messages and forward to router
            dc.on_message(Box::new(move |msg| {
                let state = Arc::clone(&state);
                Box::pin(async move {
                    if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            let cfg = state.config.read().await;
                            let messages = crate::router::map_inbound(&value, &cfg.osc);

                            // Forward to OSC adapter
                            let _ = state.osc.as_ref().deliver(&messages).await;
                        }
                    }
                })
            }));
        })
    }));

    // Set remote description (offer from browser)
    let offer = RTCSessionDescription::offer(req.sdp).unwrap();
    if let Err(e) = peer.set_remote_description(offer).await {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("invalid SDP offer: {}", e) })),
        );
    }

    // Create answer
    let answer = match peer.create_answer(None).await {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("failed to create answer: {}", e) })),
            );
        }
    };

    // Set local description
    if let Err(e) = peer.set_local_description(answer.clone()).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to set local description: {}", e) })),
        );
    }

    // Wait for ICE gathering to complete (optional, best-effort)
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let state = peer.ice_gathering_state();
            if format!("{:?}", state).contains("Complete") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await;

    // Return the answer with all ICE candidates embedded
    let final_sdp = peer.local_description().await.unwrap().sdp;

    (
        StatusCode::OK,
        Json(json!({
            "type": "answer",
            "sdp": final_sdp
        })),
    )
}

#[derive(Deserialize)]
struct OscReq {
    enabled: Option<bool>,
    host: Option<String>,
    port: Option<u16>,
    prefix: Option<String>,
    flatten: Option<bool>,
}

async fn update_osc(
    State(state): State<Arc<AppState>>,
    Json(req): Json<OscReq>,
) -> impl IntoResponse {
    {
        let mut cfg = state.config.write().await;
        if let Some(v) = req.enabled {
            cfg.osc.enabled = v;
        }
        if let Some(v) = req.host {
            cfg.osc.host = v;
        }
        if let Some(v) = req.port {
            cfg.osc.port = v;
        }
        if let Some(v) = req.prefix {
            cfg.osc.prefix = v;
        }
        if let Some(v) = req.flatten {
            cfg.osc.flatten = v;
        }
        let _ = cfg.save();
    }
    let _ = state.ctrl.send(Ctrl::ReloadOsc).await;
    let cfg = state.config.read().await.clone();
    Json(json!({ "ok": true, "osc": cfg.osc }))
}
