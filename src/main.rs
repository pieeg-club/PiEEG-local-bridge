// ─────────────────────────────────────────────────────────────────────────────
// Pieeg Local Bridge
//
// An ultra-small, ultra-fast local gateway that connects a browser producer to
// services on the user's own network. It is deliberately vendor- and
// protocol-neutral: a direct WebRTC data channel feeds a router, the router
// emits protocol-neutral messages, and pluggable adapters speak them onto the
// wire.
//
//   transport (WebRTC P2P) ──► router (generic mapping) ──► adapters (OSC, …)
//
// Nothing in the core knows about EEG or any specific producer. Adding a new
// output protocol means adding one Adapter — nothing else changes.
// ─────────────────────────────────────────────────────────────────────────────

// Hide console window on Windows (run as tray-only GUI app)
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod adapters;
mod config;
mod control;
mod discovery;
mod message;
mod router;
mod state;
mod transport;
mod update;

use crate::adapters::osc::OscAdapter;
use crate::adapters::Adapter;
use crate::config::Config;
use crate::state::{AppState, Ctrl, Status};
use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tray_icon::{
    menu::{Menu, MenuEvent, MenuItem},
    Icon, TrayIconBuilder,
};

#[derive(Debug, Clone, PartialEq)]
enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
}

impl ConnectionStatus {
    fn as_str(&self) -> &str {
        match self {
            ConnectionStatus::Disconnected => "● Disconnected",
            ConnectionStatus::Connecting => "◐ Connecting...",
            ConnectionStatus::Connected => "● Connected",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "pieeg-local-bridge",
    version,
    about = "Vendor-neutral browser-to-local-network bridge (OSC)."
)]
struct Cli {
    /// Control-UI port (overrides saved config).
    #[arg(long)]
    port: Option<u16>,
    /// Signaling base URL for the one-time WebRTC handshake (overrides config).
    #[arg(long)]
    signaling_url: Option<String>,
    /// Connect immediately with this session code and skip the browser.
    #[arg(long)]
    connect: Option<String>,
    /// Extra cross-origin site allowed to call the local control API (repeatable).
    #[arg(long = "allow-origin")]
    allow_origin: Vec<String>,
    /// Do not open the control UI in a browser on startup.
    #[arg(long, default_value_t = false)]
    no_open: bool,
}

/// Resolve `host:port` to a socket address (works for IPs and hostnames).
async fn resolve_target(host: &str, port: u16) -> Option<SocketAddr> {
    tokio::net::lookup_host((host, port))
        .await
        .ok()
        .and_then(|mut it| it.next())
}

/// Create tray icon from embedded icon.png.
fn create_tray_icon() -> Result<Icon> {
    // Load embedded icon.png
    let icon_bytes = include_bytes!("../icon.png");
    let img = image::load_from_memory(icon_bytes)
        .context("loading icon.png")?
        .to_rgba8();
    
    let width = img.width();
    let height = img.height();
    let rgba = img.into_raw();

    Icon::from_rgba(rgba, width, height).context("creating tray icon")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();

    // Install the process-wide rustls crypto provider (ring). reqwest is built
    // with `rustls-no-provider`, so this must happen before any TLS use.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // ── Config ──────────────────────────────────────────────────────────────
    let mut cfg = Config::load();
    if let Some(p) = cli.port {
        cfg.control_port = p;
    }
    if let Some(url) = cli.signaling_url.clone() {
        cfg.signaling_url = url;
    }
    for origin in &cli.allow_origin {
        if !cfg.allowed_origins.contains(origin) {
            cfg.allowed_origins.push(origin.clone());
        }
    }
    let control_port = cfg.control_port;
    let allowed_origins = cfg.allowed_origins.clone();

    // ── OSC adapter (initial target from config) ────────────────────────────
    let initial_target = resolve_target(&cfg.osc.host, cfg.osc.port)
        .await
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], cfg.osc.port)));
    let osc = Arc::new(
        OscAdapter::new(initial_target)
            .await
            .context("initialising OSC adapter")?,
    );
    tracing::info!("adapter ready: {} → {initial_target}", osc.id());

    // ── Shared state + control channel ──────────────────────────────────────
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Ctrl>(32);

    // Create a rendezvous session on the cloud signaling server to obtain the
    // share code (and ICE servers). Fall back to a locally generated code if
    // the cloud is unreachable so the bridge still starts.
    let signaling_url = cfg.signaling_url.clone();
    let (initial_pairing_code, initial_ice_servers, cloud_session_ok) =
        match transport::webrtc::create_session(&signaling_url).await {
            Ok(info) => {
                tracing::info!("cloud session created — share this code: {}", info.code);
                (info.code, info.ice_servers, true)
            }
            Err(e) => {
                tracing::warn!("could not create cloud session ({e:#}); using local code");
                (control::generate_pairing_code(), Vec::new(), false)
            }
        };

    let status = Status {
        pairing_code: initial_pairing_code.clone(),
        ..Default::default()
    };

    let state = Arc::new(AppState {
        config: RwLock::new(cfg),
        status: RwLock::new(status),
        osc,
        discovered: RwLock::new(Vec::new()),
        ctrl: ctrl_tx.clone(),
        ice_servers: RwLock::new(initial_ice_servers),
    });

    // ── Background: mDNS discovery ──────────────────────────────────────────
    tokio::spawn(discovery::run(state.clone()));

    // ── Control HTTP server ─────────────────────────────────────────────────
    let app = control::router(state.clone(), allowed_origins);
    let addr = SocketAddr::from(([127, 0, 0, 1], control_port));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding control server on {addr}"))?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("control server stopped: {e}");
        }
    });

    let ui_url = format!("http://127.0.0.1:{control_port}");
    tracing::info!("Pieeg Local Bridge ready → {ui_url}");
    tracing::info!("Pairing code: {}", initial_pairing_code);
    if !cli.no_open {
        let _ = webbrowser::open(&ui_url);
    }

    // ── System tray ─────────────────────────────────────────────────────────
    let (tray_tx, mut tray_rx) = mpsc::channel::<String>(8);
    let (tray_status_tx, tray_status_rx) = std::sync::mpsc::channel::<ConnectionStatus>();
    let (update_tx, mut update_rx) = mpsc::channel::<(String, String)>(1);
    let ui_url_clone = ui_url.clone();

    std::thread::spawn(move || {
        use tray_icon::menu::MenuId;

        let icon = create_tray_icon().expect("failed to create tray icon");

        let tray_menu = Menu::new();
        let status_item = MenuItem::with_id(
            MenuId::new("status"),
            ConnectionStatus::Disconnected.as_str(),
            false, // disabled - not clickable
            None,
        );
        let show_item = MenuItem::with_id(MenuId::new("show"), "Show Control UI", true, None);
        let regenerate_item = MenuItem::with_id(
            MenuId::new("regenerate"),
            "Regenerate Pairing Code",
            true,
            None,
        );
        let update_item = MenuItem::with_id(MenuId::new("update"), "Check for Updates", true, None);
        let quit_item = MenuItem::with_id(MenuId::new("quit"), "Quit", true, None);

        tray_menu.append(&status_item).unwrap();
        tray_menu.append(&show_item).unwrap();
        tray_menu.append(&regenerate_item).unwrap();
        tray_menu.append(&update_item).unwrap();
        tray_menu.append(&quit_item).unwrap();

        let _tray = TrayIconBuilder::new()
            .with_menu(Box::new(tray_menu))
            .with_tooltip("PiEEG Local Bridge")
            .with_icon(icon)
            .build()
            .expect("failed to create tray icon");

        let menu_channel = MenuEvent::receiver();

        // Windows event loop
        #[cfg(windows)]
        {
            use windows::Win32::UI::WindowsAndMessaging::{
                DispatchMessageW, GetMessageW, TranslateMessage, MSG,
            };

            loop {
                // Check for status updates
                while let Ok(new_status) = tray_status_rx.try_recv() {
                    status_item.set_text(new_status.as_str());
                }

                // Check for menu events
                while let Ok(event) = menu_channel.try_recv() {
                    let id_str = event.id.0;
                    match id_str.as_str() {
                        "show" => {
                            let _ = webbrowser::open(&ui_url_clone);
                        }
                        "regenerate" => {
                            let _ = tray_tx.blocking_send("regenerate".to_string());
                        }
                        "update" => {
                            let _ = tray_tx.blocking_send("check_update".to_string());
                        }
                        "quit" => {
                            let _ = tray_tx.blocking_send("quit".to_string());
                            return;
                        }
                        _ => {}
                    }
                }

                // Pump Windows messages
                unsafe {
                    let mut msg = MSG::default();
                    if GetMessageW(&mut msg, None, 0, 0).0 > 0 {
                        TranslateMessage(&msg);
                        DispatchMessageW(&msg);
                    }
                }
            }
        }

        // Non-Windows platforms
        #[cfg(not(windows))]
        {
            loop {
                // Check for status updates
                if let Ok(new_status) = tray_status_rx.try_recv() {
                    status_item.set_text(new_status.as_str());
                }

                if let Ok(event) = menu_channel.recv() {
                    let id_str = event.id.0;
                    match id_str.as_str() {
                        "show" => {
                            let _ = webbrowser::open(&ui_url_clone);
                        }
                        "regenerate" => {
                            let _ = tray_tx.blocking_send("regenerate".to_string());
                        }
                        "update" => {
                            let _ = tray_tx.blocking_send("check_update".to_string());
                        }
                        "quit" => {
                            let _ = tray_tx.blocking_send("quit".to_string());
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    tracing::info!("System tray icon active (right-click for menu)");

    // ── Background status watcher ───────────────────────────────────────────
    // Monitors connection state and updates tray menu item
    let status_watcher_state = state.clone();
    let status_watcher_tx = tray_status_tx.clone();
    tokio::spawn(async move {
        let mut last_status = ConnectionStatus::Disconnected;
        let _ = status_watcher_tx.send(last_status.clone());

        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            
            let status = status_watcher_state.status.read().await;
            let current_status = if status.ended {
                ConnectionStatus::Disconnected
            } else if status.connected {
                ConnectionStatus::Connected
            } else if status.paired {
                ConnectionStatus::Connecting
            } else {
                ConnectionStatus::Disconnected
            };

            if current_status != last_status {
                let _ = status_watcher_tx.send(current_status.clone());
                last_status = current_status;
            }
        }
    });

    // ── Background update check ─────────────────────────────────────────────
    let update_tx_clone = update_tx.clone();
    tokio::spawn(async move {
        // Wait 5 seconds after startup before checking
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        match update::check_for_update().await {
            Ok(Some((version, url))) => {
                tracing::info!("Update available: {version}");
                let _ = update_tx_clone.send((version, url)).await;
            }
            Ok(None) => {
                tracing::debug!("No updates available");
            }
            Err(e) => {
                tracing::debug!("Update check failed: {e:#}");
            }
        }
    });

    // ── Optional immediate connect (CLI or restored from config) ────────────
    if let Some(id) = cli.connect {
        let _ = ctrl_tx.send(Ctrl::Connect(id)).await;
    } else if cloud_session_ok {
        // Start polling the signaling server for the browser's offer so the
        // shared code is immediately live — no manual "connect" step needed.
        let _ = ctrl_tx
            .send(Ctrl::Connect(initial_pairing_code.clone()))
            .await;
    }

    // ── Orchestrator loop ───────────────────────────────────────────────────
    let mut transport: Option<JoinHandle<()>> = None;
    loop {
        tokio::select! {
            Some((version, url)) = update_rx.recv() => {
                tracing::info!("📦 Update available: {version} — {url}");
                // Open releases page in browser
                let _ = webbrowser::open(&url);
            }
            Some(tray_event) = tray_rx.recv() => {
                match tray_event.as_str() {
                    "regenerate" => {
                        let _ = ctrl_tx.send(Ctrl::RegenerateCode).await;
                    }
                    "check_update" => {
                        // Spawn immediate update check
                        let update_tx_manual = update_tx.clone();
                        tokio::spawn(async move {
                            match update::check_for_update().await {
                                Ok(Some((version, url))) => {
                                    tracing::info!("Update available: {version}");
                                    let _ = update_tx_manual.send((version, url)).await;
                                }
                                Ok(None) => {
                                    tracing::info!("You are running the latest version");
                                }
                                Err(e) => {
                                    tracing::warn!("Update check failed: {e:#}");
                                }
                            }
                        });
                    }
                    "quit" => {
                        tracing::info!("shutting down from tray");
                        if let Some(h) = transport.take() { h.abort(); }
                        break;
                    }
                    _ => {}
                }
            }
            maybe_cmd = ctrl_rx.recv() => {
                let Some(cmd) = maybe_cmd else { break };
                match cmd {
                    Ctrl::Connect(id) => {
                        if let Some(h) = transport.take() { h.abort(); }
                        {
                            let mut cfg = state.config.write().await;
                            cfg.session_id = Some(id.clone());
                            let _ = cfg.save();
                        }
                        {
                            let mut s = state.status.write().await;
                            // Preserve the share code shown to the user — it is
                            // also the rendezvous session id we connect with.
                            let code = s.pairing_code.clone();
                            *s = Status::default();
                            s.pairing_code = code;
                            s.paired = true;
                            s.session_id = Some(id.clone());
                        }
                        tracing::info!("connecting session {id}");
                        transport = Some(tokio::spawn(
                            transport::webrtc::run(state.clone(), id),
                        ));
                    }
                    Ctrl::Disconnect => {
                        if let Some(h) = transport.take() { h.abort(); }
                        {
                            let mut cfg = state.config.write().await;
                            cfg.session_id = None;
                            let _ = cfg.save();
                        }
                        *state.status.write().await = Status::default();
                        tracing::info!("disconnected");
                    }
                    Ctrl::ReloadOsc => {
                        let (host, port) = {
                            let cfg = state.config.read().await;
                            (cfg.osc.host.clone(), cfg.osc.port)
                        };
                        if let Some(t) = resolve_target(&host, port).await {
                            state.osc.set_target(t).await;
                            tracing::info!("OSC target → {t}");
                        }
                    }
                    Ctrl::RegenerateCode => {
                        // Disconnect any active session first.
                        if let Some(h) = transport.take() { h.abort(); }

                        let signaling_url = { state.config.read().await.signaling_url.clone() };
                        let (new_code, new_ice, cloud_ok) =
                            match transport::webrtc::create_session(&signaling_url).await {
                                Ok(info) => (info.code, info.ice_servers, true),
                                Err(e) => {
                                    tracing::warn!(
                                        "could not create cloud session ({e:#}); using local code"
                                    );
                                    (control::generate_pairing_code(), Vec::new(), false)
                                }
                            };
                        *state.ice_servers.write().await = new_ice;
                        {
                            let mut s = state.status.write().await;
                            *s = Status::default();
                            s.pairing_code = new_code.clone();
                        }
                        tracing::info!("new pairing code: {}", new_code);
                        // Start polling for the new session's offer immediately.
                        if cloud_ok {
                            let _ = ctrl_tx.send(Ctrl::Connect(new_code)).await;
                        }
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
                if let Some(h) = transport.take() { h.abort(); }
                break;
            }
        }
    }

    Ok(())
}
