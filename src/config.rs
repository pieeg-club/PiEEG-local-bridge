// ─────────────────────────────────────────────────────────────────────────────
// config.rs — persisted, hot-swappable bridge configuration.
//
// Stored as JSON under the OS config dir so the bridge needs zero CLI knowledge:
//   Windows : %APPDATA%\pieeg\LocalBridge\config.json
//   macOS   : ~/Library/Application Support/com.pieeg.LocalBridge/config.json
//   Linux   : ~/.config/pieeg-local-bridge/config.json
// ─────────────────────────────────────────────────────────────────────────────

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Base URL of the stateless signaling endpoint used **only** to exchange the
/// one-time WebRTC offer/answer. No stream data ever transits it — once the peer
/// connection is up, everything flows browser↔bridge directly.
pub const DEFAULT_SIGNALING_URL: &str = "https://pieeg-cloud.fly.dev";
pub const DEFAULT_CONTROL_PORT: u16 = 47800;

fn default_signaling_url() -> String { DEFAULT_SIGNALING_URL.to_string() }
fn default_control_port() -> u16 { DEFAULT_CONTROL_PORT }
fn default_true() -> bool { true }

/// Configuration for the built-in OSC adapter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OscConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "OscConfig::default_host")]
    pub host: String,
    #[serde(default = "OscConfig::default_port")]
    pub port: u16,
    /// Base address prefix for the generic JSON→OSC flattener.
    #[serde(default = "OscConfig::default_prefix")]
    pub prefix: String,
    /// When true, generic JSON frames are flattened into OSC addresses.
    /// When false, only explicit `{"osc": ...}` envelopes are forwarded.
    #[serde(default = "default_true")]
    pub flatten: bool,
}

impl OscConfig {
    fn default_host() -> String { "127.0.0.1".to_string() }
    fn default_port() -> u16 { 9000 }
    fn default_prefix() -> String { "/pieeg".to_string() }
}

impl Default for OscConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: Self::default_host(),
            port: Self::default_port(),
            prefix: Self::default_prefix(),
            flatten: true,
        }
    }
}

/// Top-level persisted configuration.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Config {
    /// Signaling endpoint for the one-time WebRTC handshake (offer/answer).
    #[serde(default = "default_signaling_url", alias = "relay_url")]
    pub signaling_url: String,
    /// The current session code (rendezvous room shared with the browser peer).
    #[serde(default, alias = "relay_id")]
    pub session_id: Option<String>,
    #[serde(default = "default_control_port")]
    pub control_port: u16,
    #[serde(default = "default_true")]
    pub auto_reconnect: bool,
    /// Cross-origin sites permitted to call the local control API. Empty by
    /// default — the core ships with NO vendor origins baked in. Operators add
    /// their own via config or `--allow-origin`.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub osc: OscConfig,
}

impl Config {
    pub fn with_defaults() -> Self {
        Self {
            signaling_url: default_signaling_url(),
            session_id: None,
            control_port: default_control_port(),
            auto_reconnect: true,
            allowed_origins: Vec::new(),
            osc: OscConfig::default(),
        }
    }

    pub fn config_dir() -> Result<PathBuf> {
        let dirs = ProjectDirs::from("com", "pieeg", "LocalBridge")
            .context("could not resolve OS config directory")?;
        Ok(dirs.config_dir().to_path_buf())
    }

    pub fn config_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join("config.json"))
    }

    /// Load config, falling back to defaults if the file does not exist or is
    /// unreadable. Never fails hard — a broken config should not block startup.
    pub fn load() -> Self {
        match Self::try_load() {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!("using defaults, could not load config: {e:#}");
                Self::with_defaults()
            }
        }
    }

    fn try_load() -> Result<Self> {
        let path = Self::config_path()?;
        if !path.exists() {
            return Ok(Self::with_defaults());
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = serde_json::from_str(&raw).context("parsing config.json")?;
        Ok(cfg)
    }

    pub fn save(&self) -> Result<()> {
        let dir = Self::config_dir()?;
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating {}", dir.display()))?;
        let path = dir.join("config.json");
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}
