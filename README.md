# Pieeg Local Bridge

An ultra-fast, secure local gateway that connects the browser-based **Pieeg Cloud**
to services on your own network. It receives a real-time data stream from a browser
via **WebRTC P2P data channel** and forwards it locally over protocols such as **OSC**
— with minimal latency and near-zero setup.

The bridge is deliberately **vendor- and protocol-neutral**. Its core knows
nothing about EEG or any specific producer: a direct WebRTC data channel feeds a
generic router, the router emits protocol-neutral messages, and pluggable
adapters speak them onto the wire.

```
Browser (WebRTC P2P) ──► router (generic mapping) ──► adapters (OSC, …)
```

Adding a new output protocol means adding **one adapter** — nothing else changes.

---

## Why it exists

Pieeg Cloud runs entirely in the browser and has no backend of its own. To reach
local software (VRChat, TouchDesigner, Max/MSP, Ableton via OSC, game engines, …),
it establishes a **WebRTC peer-to-peer data channel** with the Local Bridge running
on your machine. Data flows **directly** between browser and bridge (via loopback
or LAN) — nothing is relayed through the cloud. The bridge turns the incoming
JSON stream into real OSC packets on `127.0.0.1` (or any host on your LAN).

## Features

- **Single standalone binary** — ~5 MB, no runtime, no dependencies to install.
- **Instant startup, low memory** — pure Rust / Tokio.
- **System tray** — runs quietly in background; right-click to show UI, regenerate code, or quit.
- **Direct P2P connection** — WebRTC data channel; all data stays local (loopback/LAN).
- **Reversed pairing flow** — bridge generates a 6-digit code; you enter it in Pieeg Cloud.
- **Encrypted transport** — DTLS for the P2P data channel; signaling uses HTTPS.
- **Automatic discovery** — finds local OSC apps via mDNS (`_osc._udp`).
- **Protocol-agnostic core** — pluggable adapters; OSC ships first.
- **Cross-platform** — Windows, macOS, Linux.
- **No cloud relay for data** — WebRTC handshake uses cloud signaling, then data is P2P.

---

## Installation

### 🚀 Quick Start — Download & Run (1 minute)

**No installation required.** Download a single executable for your platform and run it.

#### Windows
1. **[Download for Windows](https://github.com/pieeg-club/PiEEG-local-bridge/releases/latest)** — get `pieeg-local-bridge-x86_64-pc-windows-msvc.zip`
2. Extract the ZIP file anywhere
3. Double-click `pieeg-local-bridge.exe`
4. ✅ Done! Look for the **tray icon** (bottom-right, near clock)
5. Right-click the tray icon → **Show Control UI** to see your pairing code

#### macOS
```bash
# Choose your Mac type:

# Apple Silicon (M1/M2/M3/M4) — Most modern Macs
curl -L https://github.com/pieeg-club/PiEEG-local-bridge/releases/latest/download/pieeg-local-bridge-aarch64-apple-darwin.tar.gz | tar xz
cd pieeg-local-bridge-aarch64-apple-darwin && ./pieeg-local-bridge

# Intel Macs — 2020 and earlier
curl -L https://github.com/pieeg-club/PiEEG-local-bridge/releases/latest/download/pieeg-local-bridge-x86_64-apple-darwin.tar.gz | tar xz
cd pieeg-local-bridge-x86_64-apple-darwin && ./pieeg-local-bridge
```

First time: macOS may show "unidentified developer" warning.  
**Fix**: Right-click → **Open**, then click **Open** in the dialog.

#### Linux
```bash
# Download and run (x86_64)
curl -L https://github.com/pieeg-club/PiEEG-local-bridge/releases/latest/download/pieeg-local-bridge-x86_64-unknown-linux-gnu.tar.gz | tar xz
cd pieeg-local-bridge-x86_64-unknown-linux-gnu && ./pieeg-local-bridge
```

**What happens next:**
- ✅ Control UI opens automatically: `http://127.0.0.1:47800`
- ✅ 6-digit pairing code displayed (e.g. `KHSP3W`)
- ✅ System tray icon appears — right-click for menu

---

### 🛠️ Build from Source (Optional)

Only needed if you want to modify the code or target an unsupported platform.

```bash
git clone https://github.com/pieeg-club/PiEEG-local-bridge.git
cd PiEEG-local-bridge
cargo build --release
./target/release/pieeg-local-bridge
```

**Requirements:** [Rust](https://rustup.rs/) 1.70+

---

## How it works

1. **Run the Local Bridge**. It generates a 6-digit pairing code (e.g. `KHSP3W`)
   and displays it at `http://127.0.0.1:47800`.
2. In **Pieeg Cloud**, click **Connect to Local Bridge** and enter the pairing code.
3. The browser and bridge perform a **one-time WebRTC handshake** (SDP/ICE exchange
   via a cloud signaling server).
4. A **direct P2P data channel** opens between browser and bridge. All data flows
   **locally** (loopback or LAN) — nothing is relayed through the cloud.
5. Incoming data frames are mapped to OSC and sent to your chosen destination.

**Pairing code regeneration**: Right-click the system tray icon and select
"Regenerate Pairing Code" to create a new code and disconnect any active session.

The whole setup takes under a minute and requires no technical knowledge.

## Data mapping

The router applies two ordered, generic rules — no producer field names are
hardcoded:

1. **Explicit envelope** — any frame containing an `osc` field is forwarded
   verbatim, giving producers precise control:

   ```json
   { "osc": { "address": "/avatar/parameters/EEG_Alpha", "args": [0.42] } }
   { "osc": [ { "address": "/a", "args": [1] }, { "address": "/b", "args": [true] } ] }
   ```

2. **Generic flatten** — any other JSON object is walked recursively; every
   number / bool / string leaf becomes an OSC address built from its JSON path.
   Transport metadata keys (`type`, `t`, `ts`, …) are skipped.

   ```json
   { "channels": [0.1, 0.2, 0.3], "t": 123 }
   ```

   →

   ```
   /pieeg/channels/0  0.1
   /pieeg/channels/1  0.2
   /pieeg/channels/2  0.3
   ```

Flattening can be disabled in the UI to forward only explicit `osc` envelopes.

## Architecture

| Layer     | File                                                   | Role                                                                 |
| --------- | ------------------------------------------------------ | -------------------------------------------------------------------- |
| Transport | [`transport/webrtc.rs`](src/transport/webrtc.rs)       | WebRTC P2P data channel, SDP/ICE handshake via HTTP signaling        |
| Currency  | [`message.rs`](src/message.rs)                         | `BridgeMessage { address, args }` — protocol-neutral                 |
| Router    | [`router.rs`](src/router.rs)                           | Generic JSON → `BridgeMessage` mapping (envelope + flatten)          |
| Adapters  | [`adapters/`](src/adapters/)                           | Pluggable `Adapter` trait; OSC/UDP sink with hot-swappable target    |
| Control   | [`control/`](src/control/)                             | Local HTTP API + embedded UI; pairing code generation; WebRTC signaling endpoint |
| Discovery | [`discovery.rs`](src/discovery.rs)                     | Best-effort mDNS `_osc._udp` scan                                    |
| Config    | [`config.rs`](src/config.rs)                           | Persisted, hot-reloadable JSON config; vendor-agnostic CORS origins  |
| Tray      | [`main.rs`](src/main.rs) (tray setup)                  | System tray icon with Show UI / Regenerate Code / Quit menu          |

---

## Development

### Build & Run from Source

For development or custom builds. Requires Rust 1.70+.

```sh
# Run in development (opens the control UI automatically)
cargo run

# Optimized standalone binary (~5 MB)
cargo build --release
# → target/release/pieeg-local-bridge[.exe]
# Note: Binary size increased from ~2MB due to WebRTC stack (DTLS, STUN, ICE, SRTP)

# Run the router tests
cargo test
```

### CLI options

| Flag                      | Description                                                     |
| ------------------------- | -------------------------------------------------------------- |
| `--port <PORT>`           | Control-UI port (default `47800`).                             |
| `--signaling-url <URL>`   | Signaling server URL for WebRTC handshake (default `https://pieeg-cloud.fly.dev`). |
| `--connect <SESSION_ID>`  | Connect immediately with this session code and skip the browser. |
| `--allow-origin <ORIGIN>` | Add a cross-origin site allowed to call the control API (repeatable). |
| `--no-open`               | Do not open the control UI in a browser on startup.            |

## Configuration

Settings are persisted as JSON and edited from the UI (no manual editing needed):

| OS      | Path                                                              |
| ------- | ---------------------------------------------------------------- |
| Windows | `%APPDATA%\pieeg\LocalBridge\config.json`                        |
| macOS   | `~/Library/Application Support/com.pieeg.LocalBridge/config.json` |
| Linux   | `~/.config/pieeg-local-bridge/config.json`                       |

## Security

- **P2P data channel encrypted with DTLS** — WebRTC data flows directly between
  browser and bridge (loopback/LAN), not through the cloud.
- **One-time signaling handshake** — SDP/ICE exchange uses HTTPS; only occurs at
  connection setup.
- **Pairing code as secret** — the 6-digit code is the trust anchor; regenerate
  to revoke access.
- **CORS agnostic** — no vendor origins baked in; configure allowed origins via
  `--allow-origin` or JSON config.
- **Local-only control API** — listens only on `127.0.0.1`.
- **No cloud relay for data** — after handshake, all data stays local.

## System Tray

The bridge runs quietly in your system tray (Windows notification area). Right-click
the tray icon for:

- **Show Control UI** — opens `http://127.0.0.1:47800` in your browser.
- **Regenerate Pairing Code** — creates a new 6-digit code and disconnects any
  active session.
- **Quit** — stops the bridge cleanly.

## License

MIT — see [`LICENSE`](../LICENSE).
