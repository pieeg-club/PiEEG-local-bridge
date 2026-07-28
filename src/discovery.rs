// ─────────────────────────────────────────────────────────────────────────────
// discovery.rs — best-effort mDNS discovery of local OSC destinations.
//
// Browses for `_osc._udp.local.` services and keeps `AppState.discovered` in
// sync. The control UI offers these as one-click OSC targets. Discovery is
// entirely optional — failure here never affects forwarding.
// ─────────────────────────────────────────────────────────────────────────────

use crate::state::{AppState, Discovered};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use std::sync::Arc;

const SERVICE_TYPE: &str = "_osc._udp.local.";

pub async fn run(state: Arc<AppState>) {
    let daemon = match ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            tracing::info!("mDNS discovery unavailable: {e}");
            return;
        }
    };

    let receiver = match daemon.browse(SERVICE_TYPE) {
        Ok(r) => r,
        Err(e) => {
            tracing::info!("mDNS browse failed: {e}");
            return;
        }
    };

    while let Ok(event) = receiver.recv_async().await {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                let host = info
                    .get_addresses()
                    .iter()
                    .next()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| info.get_hostname().trim_end_matches('.').to_string());
                let entry = Discovered {
                    name: pretty_name(info.get_fullname()),
                    host,
                    port: info.get_port(),
                };
                let mut list = state.discovered.write().await;
                if let Some(existing) = list.iter_mut().find(|d| d.name == entry.name) {
                    *existing = entry;
                } else {
                    list.push(entry);
                }
            }
            ServiceEvent::ServiceRemoved(_ty, fullname) => {
                let name = pretty_name(&fullname);
                let mut list = state.discovered.write().await;
                list.retain(|d| d.name != name);
            }
            _ => {}
        }
    }
}

/// "MyApp._osc._udp.local." → "MyApp".
fn pretty_name(fullname: &str) -> String {
    fullname
        .split_once("._osc._udp")
        .map(|(n, _)| n.to_string())
        .unwrap_or_else(|| fullname.to_string())
}
