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

mod adapters;
mod config;
mod control;
mod discovery;
mod message;
mod router;
mod state;
mod transport;

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

/// Create a simple tray icon (32x32 green square with "P" text).
fn create_tray_icon() -> Result<Icon> {
    use image::{Rgba, RgbaImage};

    let mut img = RgbaImage::from_pixel(32, 32, Rgba([0, 150, 136, 255])); // Teal background

    // Draw a simple "P" shape (very basic)
    for y in 8..24 {
        for x in 10..12 {
            img.put_pixel(x, y, Rgba([255, 255, 255, 255]));
        }
    }
    for y in 8..10 {
        for x in 10..18 {
            img.put_pixel(x, y, Rgba([255, 255, 255, 255]));
        }
    }
    for y in 10..16 {
        for x in 16..18 {
            img.put_pixel(x, y, Rgba([255, 255, 255, 255]));
        }
    }
    for y in 14..16 {
        for x in 10..18 {
            img.put_pixel(x, y, Rgba([255, 255, 255, 255]));
        }
    }

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

    // Generate initial pairing code
    let initial_pairing_code = control::generate_pairing_code();
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
    let ui_url_clone = ui_url.clone();

    std::thread::spawn(move || {
        use tray_icon::menu::MenuId;

        let icon = create_tray_icon().expect("failed to create tray icon");

        let tray_menu = Menu::new();
        let show_item = MenuItem::with_id(MenuId::new("show"), "Show Control UI", true, None);
        let regenerate_item = MenuItem::with_id(
            MenuId::new("regenerate"),
            "Regenerate Pairing Code",
            true,
            None,
        );
        let quit_item = MenuItem::with_id(MenuId::new("quit"), "Quit", true, None);

        tray_menu.append(&show_item).unwrap();
        tray_menu.append(&regenerate_item).unwrap();
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
                // Check for menu events first
                while let Ok(event) = menu_channel.try_recv() {
                    let id_str = event.id.0;
                    match id_str.as_str() {
                        "show" => {
                            let _ = webbrowser::open(&ui_url_clone);
                        }
                        "regenerate" => {
                            let _ = tray_tx.blocking_send("regenerate".to_string());
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
                if let Ok(event) = menu_channel.recv() {
                    let id_str = event.id.0;
                    match id_str.as_str() {
                        "show" => {
                            let _ = webbrowser::open(&ui_url_clone);
                        }
                        "regenerate" => {
                            let _ = tray_tx.blocking_send("regenerate".to_string());
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

    // ── Optional immediate connect (CLI or restored from config) ────────────
    if let Some(id) = cli.connect {
        let _ = ctrl_tx.send(Ctrl::Connect(id)).await;
    }

    // ── Orchestrator loop ───────────────────────────────────────────────────
    let mut transport: Option<JoinHandle<()>> = None;
    loop {
        tokio::select! {
            Some(tray_event) = tray_rx.recv() => {
                match tray_event.as_str() {
                    "regenerate" => {
                        let _ = ctrl_tx.send(Ctrl::RegenerateCode).await;
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
                            *s = Status::default();
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
                        let new_code = control::generate_pairing_code();
                        {
                            let mut s = state.status.write().await;
                            s.pairing_code = new_code.clone();
                            // Clear any existing pairing
                            s.paired = false;
                            s.session_id = None;
                            s.connected = false;
                        }
                        tracing::info!("new pairing code: {}", new_code);
                        // Disconnect any active session
                        if let Some(h) = transport.take() { h.abort(); }
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
