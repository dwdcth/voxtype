//! Generic WebSocket streaming STT configuration.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WsStreamConfig {
    pub endpoint: Option<String>,
    pub encoding: Option<String>,
    pub language: Option<String>,
    pub sample_rate: Option<u32>,
}
