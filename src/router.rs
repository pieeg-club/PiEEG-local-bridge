// ─────────────────────────────────────────────────────────────────────────────
// router.rs — turns arbitrary inbound JSON into protocol-neutral BridgeMessages.
//
// This is where "agnosticism" lives. The router has NO knowledge of EEG or any
// specific producer. It applies two ordered, generic rules:
//
//   1. Explicit envelope:  {"osc": {"address": "/x", "args": [..]}}
//                          {"osc": [ {..}, {..} ]}
//      → forwarded verbatim. Any cloud producer can opt into precise control.
//
//   2. Generic flatten:    any other JSON object is walked recursively; every
//                          number/bool/string leaf becomes an OSC address built
//                          from its JSON path (e.g. {"channels":[1,2]} →
//                          /prefix/channels/0, /prefix/channels/1). Control
//                          keys (type, t, ts, …) are skipped.
//
// Rule 2 makes the bridge work for ANY producer — EEG channels, sensor arrays,
// game telemetry — without hardcoding a single field name.
// ─────────────────────────────────────────────────────────────────────────────

use crate::config::OscConfig;
use crate::message::{Arg, BridgeMessage};
use serde_json::Value;

/// JSON keys that are transport/control metadata, never payload.
const SKIP_KEYS: &[&str] = &[
    "type",
    "t",
    "ts",
    "viewerId",
    "viewer_id",
    "from",
    "to",
    "color",
    "name",
    "list",
    "viewers",
    "data",
];

/// Map one inbound JSON value into zero or more protocol-neutral messages.
pub fn map_inbound(json: &Value, osc: &OscConfig) -> Vec<BridgeMessage> {
    // Rule 1 — explicit OSC envelope wins and is always honoured.
    if let Some(osc_field) = json.get("osc") {
        if let Some(msgs) = parse_osc_envelope(osc_field) {
            return msgs;
        }
    }

    // Rule 2 — generic flatten (opt-out via config).
    if osc.flatten {
        let mut out = Vec::new();
        flatten(&osc.prefix, json, &mut out);
        return out;
    }

    Vec::new()
}

/// Parse `{"address": "/x", "args": [..]}` or an array of the same.
fn parse_osc_envelope(v: &Value) -> Option<Vec<BridgeMessage>> {
    match v {
        Value::Object(_) => serde_json::from_value::<BridgeMessage>(v.clone())
            .ok()
            .map(|m| vec![m]),
        Value::Array(_) => serde_json::from_value::<Vec<BridgeMessage>>(v.clone()).ok(),
        _ => None,
    }
}

/// Recursively flatten a JSON value into `/prefix/path` addressed messages.
fn flatten(prefix: &str, v: &Value, out: &mut Vec<BridgeMessage>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                if SKIP_KEYS.contains(&k.as_str()) {
                    continue;
                }
                flatten(&join(prefix, k), child, out);
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                flatten(&join(prefix, &i.to_string()), child, out);
            }
        }
        Value::Number(n) => {
            let arg = n
                .as_f64()
                .map(Arg::Float)
                .or_else(|| n.as_i64().map(Arg::Int))
                .unwrap_or(Arg::Float(0.0));
            out.push(BridgeMessage::new(prefix.to_string(), vec![arg]));
        }
        Value::Bool(b) => out.push(BridgeMessage::new(prefix.to_string(), vec![Arg::Bool(*b)])),
        Value::String(s) => out.push(BridgeMessage::new(
            prefix.to_string(),
            vec![Arg::Str(s.clone())],
        )),
        Value::Null => {}
    }
}

fn join(prefix: &str, seg: &str) -> String {
    format!("{}/{}", prefix.trim_end_matches('/'), seg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn osc() -> OscConfig {
        OscConfig::default()
    }

    #[test]
    fn explicit_envelope_object() {
        let msgs = map_inbound(&json!({"osc": {"address": "/a", "args": [1.0]}}), &osc());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].address, "/a");
    }

    #[test]
    fn explicit_envelope_array() {
        let msgs = map_inbound(
            &json!({"osc": [{"address": "/a", "args": []}, {"address": "/b", "args": [true]}]}),
            &osc(),
        );
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn flatten_channels_array_skips_metadata() {
        let msgs = map_inbound(
            &json!({"channels": [0.1, 0.2, 0.3], "t": 123.0, "type": "x"}),
            &osc(),
        );
        let addrs: Vec<_> = msgs.iter().map(|m| m.address.as_str()).collect();
        assert!(addrs.contains(&"/pieeg/channels/0"));
        assert!(addrs.contains(&"/pieeg/channels/2"));
        // metadata keys are never emitted
        assert!(!addrs.iter().any(|a| a.contains("/t") || a.contains("type")));
    }

    #[test]
    fn flatten_disabled_yields_nothing() {
        let mut c = osc();
        c.flatten = false;
        assert!(map_inbound(&json!({"channels": [1.0]}), &c).is_empty());
    }
}
