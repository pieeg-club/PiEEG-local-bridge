// ─────────────────────────────────────────────────────────────────────────────
// transport/mod.rs — inbound transports feed the router.
//
// A Transport establishes a channel to the remote producer and turns it into a
// stream of raw JSON values for the router. The only transport is a WebRTC data
// channel: the browser peer connects directly to this bridge (peer-to-peer,
// DTLS-encrypted) and the stream never transits any server. A stateless
// signaling endpoint is used once, only to exchange the SDP offer/answer.
// ─────────────────────────────────────────────────────────────────────────────

pub mod webrtc;
