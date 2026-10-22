//! Generic WebSocket streaming STT backend.
//!
//! Connects to any local STT server implementing the "generic streaming
//! STT WebSocket" protocol (see docs/WS_PROTOCOL.md):
//!
//!   ws://host:port/v1/audio/stream
//!
//! Client → Server:
//!   Text   {"type":"start","sample_rate":16000,"encoding":"f32","language":"zh"}
//!   Binary [raw audio frames]
//!   Text   {"type":"stop"}
//!   Text   {"type":"cancel"}
//!
//! Server → Client:
//!   Text   {"type":"ready"}
//!   Text   {"type":"partial","text":"...","segment_id":1}
//!   Text   {"type":"final","text":"...","segment_id":1}
//!   Text   {"type":"error","message":"..."}
//!   Text   {"type":"ended"}
//!
//! This enables any local STT engine (Python, whisper.cpp server, custom
//! model server) to serve voxtype with true streaming partial results.

use super::{StreamHandle, StreamingEvent, StreamingTranscriber, TranscribeError, Transcriber};
use crate::config::WsStreamConfig;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Generic WebSocket streaming transcriber
pub struct GenericWsTranscriber {
    /// WebSocket endpoint URL (ws:// or wss://)
    pub endpoint: String,
    /// Audio encoding: "f32" or "s16le"
    pub encoding: String,
    /// Language hint (e.g. "zh", "en", "auto")
    pub language: String,
    /// Sample rate (Hz)
    pub sample_rate: u32,
}

impl GenericWsTranscriber {
    pub fn new(config: &WsStreamConfig) -> Result<Self, TranscribeError> {
        let endpoint = config
            .endpoint
            .as_ref()
            .ok_or_else(|| {
                TranscribeError::ConfigError(
                    "ws_stream.endpoint is required (e.g. \"ws://127.0.0.1:8090/v1/audio/stream\")"
                        .into(),
                )
            })?
            .clone();

        if !endpoint.starts_with("ws://") && !endpoint.starts_with("wss://") {
            return Err(TranscribeError::ConfigError(format!(
                "ws_stream.endpoint must start with ws:// or wss://, got: {}",
                endpoint
            )));
        }

        Ok(Self {
            endpoint,
            encoding: config.encoding.clone().unwrap_or_else(|| "f32".into()),
            language: config.language.clone().unwrap_or_else(|| "auto".into()),
            sample_rate: config.sample_rate.unwrap_or(16000),
        })
    }

    /// Encode f32 samples to binary bytes for WebSocket transmission
    fn encode_samples(&self, samples: &[f32]) -> Vec<u8> {
        match self.encoding.as_str() {
            "s16le" => {
                let mut buf = Vec::with_capacity(samples.len() * 2);
                for &s in samples {
                    let v = (s.clamp(-1.0, 1.0) * 32767.0) as i16;
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                buf
            }
            _ => {
                // f32 (default)
                let mut buf = Vec::with_capacity(samples.len() * 4);
                for &s in samples {
                    buf.extend_from_slice(&s.to_le_bytes());
                }
                buf
            }
        }
    }
}

/// JSON messages from the server
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMessage {
    Ready,
    Partial { text: String, segment_id: Option<u64> },
    Final { text: String, segment_id: Option<u64> },
    Error { message: Option<String> },
    Ended,
}

/// Run the WebSocket streaming session (blocking, called from spawn_blocking)
async fn run_ws_session(
    endpoint: String,
    encoding: String,
    language: String,
    sample_rate: u32,
    mut samples_rx: mpsc::Receiver<Vec<f32>>,
    events_tx: mpsc::Sender<StreamingEvent>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), TranscribeError> {
    // Connect
    let (ws_stream, _) = connect_async(&endpoint)
        .await
        .map_err(|e| TranscribeError::NetworkError(format!("WS connect failed: {}", e)))?;

    let (mut sink, mut stream) = ws_stream.split();

    // Send start frame
    let start_msg = serde_json::json!({
        "type": "start",
        "sample_rate": sample_rate,
        "encoding": encoding,
        "language": language,
    });
    sink.send(Message::Text(start_msg.to_string()))
        .await
        .map_err(|e| TranscribeError::NetworkError(format!("WS send start: {}", e)))?;

    // Wait for ready
    // (some servers send it, some don't — don't block on it)

    let mut segment_id: u64 = 1;
    let mut got_final = false;

    loop {
        tokio::select! {
            // Cancel signal
            _ = &mut cancel_rx => {
                let _ = sink.send(Message::Text(
                    serde_json::json!({"type": "cancel"}).to_string()
                )).await;
                break;
            }

            // Audio chunk from daemon
            chunk = samples_rx.recv() => {
                match chunk {
                    Some(samples) => {
                        let binary = encode_samples_static(&samples, &encoding);
                        if let Err(e) = sink.send(Message::Binary(binary)).await {
                            tracing::warn!("WS send audio failed: {}", e);
                            break;
                        }
                    }
                    None => {
                        // samples_rx dropped → user released hotkey → send stop
                        let _ = sink.send(Message::Text(
                            serde_json::json!({"type": "stop"}).to_string()
                        )).await;
                    }
                }
            }

            // Server message
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ServerMessage>(&text) {
                            Ok(ServerMessage::Partial { text, segment_id: sid }) => {
                                let _ = events_tx.send(StreamingEvent::Partial {
                                    text,
                                    segment_id: sid.unwrap_or(segment_id),
                                }).await;
                            }
                            Ok(ServerMessage::Final { text, segment_id: sid }) => {
                                let _ = events_tx.send(StreamingEvent::Final {
                                    text,
                                    segment_id: sid.unwrap_or(segment_id),
                                }).await;
                                got_final = true;
                                segment_id += 1;
                            }
                            Ok(ServerMessage::Error { message }) => {
                                let _ = events_tx.send(StreamingEvent::Error(
                                    TranscribeError::InferenceFailed(
                                        message.unwrap_or_else(|| "unknown".into())
                                    )
                                )).await;
                            }
                            Ok(ServerMessage::Ended) | Ok(ServerMessage::Ready) => {
                                // Ready: ignore; Ended: drain below
                            }
                            Err(e) => {
                                tracing::warn!("WS JSON parse: {} — {}", e, text);
                            }
                        }
                    }
                    Some(Ok(_)) => {} // Binary/ping/pong from server — ignore
                    Some(Err(e)) => {
                        tracing::warn!("WS error: {}", e);
                        break;
                    }
                    None => break, // Server closed
                }
            }
        }

        // If we got a final and samples are done, we're finished
        if got_final {
            // Check if samples_rx is also closed
            if samples_rx.is_closed() {
                break;
            }
        }
    }

    // Send Ended
    let _ = events_tx.send(StreamingEvent::Ended).await;
    Ok(())
}

impl StreamingTranscriber for GenericWsTranscriber {
    fn start_stream(
        &self,
        samples_rx: mpsc::Receiver<Vec<f32>>,
    ) -> Result<StreamHandle, TranscribeError> {
        let (events_tx, events_rx) = mpsc::channel::<StreamingEvent>(64);
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

        let endpoint = self.endpoint.clone();
        let encoding = self.encoding.clone();
        let language = self.language.clone();
        let sample_rate = self.sample_rate;

        let task: JoinHandle<Result<(), TranscribeError>> = tokio::task::spawn(async move {
            run_ws_session(endpoint, encoding, language, sample_rate, samples_rx, events_tx, cancel_rx).await
        });

        Ok(StreamHandle {
            events: events_rx,
            cancel: cancel_tx,
            task,
        })
    }
}

/// Batch transcription via WebSocket (one-shot session: start → binary → stop → final)
impl Transcriber for GenericWsTranscriber {
    fn transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        if samples.is_empty() {
            return Ok(String::new());
        }

        // Use tokio's block_on to run the async WebSocket session
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let (ws_stream, _) = connect_async(&self.endpoint)
                    .await
                    .map_err(|e| {
                        TranscribeError::NetworkError(format!("WS connect: {}", e))
                    })?;

                let (mut sink, mut stream) = ws_stream.split();

                // Start
                let start = serde_json::json!({
                    "type": "start",
                    "sample_rate": self.sample_rate,
                    "encoding": self.encoding,
                    "language": self.language,
                });
                sink.send(Message::Text(start.to_string())).await.ok();

                // Send all audio as binary frames (chunked to ~1s each)
                let chunk_size = self.sample_rate as usize; // 1 second
                for chunk in samples.chunks(chunk_size) {
                    let binary = encode_samples_static(chunk, &self.encoding);
                    sink.send(Message::Binary(binary)).await.ok();
                }

                // Stop
                sink.send(Message::Text(
                    serde_json::json!({"type": "stop"}).to_string()
                )).await.ok();

                // Wait for final
                while let Some(msg) = stream.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(m) = serde_json::from_str::<serde_json::Value>(&text) {
                                match m["type"].as_str() {
                                    Some("final") => {
                                        return Ok(m["text"].as_str().unwrap_or("").to_string());
                                    }
                                    Some("error") => {
                                        return Err(TranscribeError::InferenceFailed(
                                            m["message"].as_str().unwrap_or("unknown").into()
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        _ => {}
                    }
                }

                Err(TranscribeError::InferenceFailed("No final received".into()))
            })
        })
    }
}

/// Static helper for batch encoding
fn encode_samples_static(samples: &[f32], encoding: &str) -> Vec<u8> {
    match encoding {
        "s16le" => {
            let mut buf = Vec::with_capacity(samples.len() * 2);
            for &s in samples {
                let v = (s.clamp(-1.0, 1.0) * 32767.0) as i16;
                buf.extend_from_slice(&v.to_le_bytes());
            }
            buf
        }
        _ => {
            let mut buf = Vec::with_capacity(samples.len() * 4);
            for &s in samples {
                buf.extend_from_slice(&s.to_le_bytes());
            }
            buf
        }
    }
}
