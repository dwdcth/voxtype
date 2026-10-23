# Generic Streaming STT WebSocket Protocol

Version: 1.0

A minimal WebSocket protocol for local streaming speech-to-text.
Designed for voxtype but usable by any client.

## Endpoint

`ws://host:port/v1/audio/stream`

## Message Types

Audio is sent as **Binary** frames. All control and results are **Text** frames containing JSON.

### Client → Server

#### start (Text/JSON, required first message)

```json
{"type": "start", "sample_rate": 16000, "encoding": "f32", "language": "zh"}
```

| Field | Type | Default | Description |
|---|---|---|---|
| sample_rate | int | 16000 | Audio sample rate in Hz |
| encoding | string | "f32" | Audio encoding: `"f32"` (32-bit LE float) or `"s16le"` (16-bit LE signed int) |
| language | string | "auto" | BCP-47 language hint (e.g. `"zh"`, `"en"`) |

#### Binary frames (audio data)

Raw audio bytes in the negotiated encoding. No headers, no framing — just concatenated samples.

#### stop (Text/JSON)

```json
{"type": "stop"}
```

Signals end of audio. Server will process remaining audio and send final result.

#### cancel (Text/JSON)

```json
{"type": "cancel"}
```

Abort the current session. Server discards buffered audio.

### Server → Client

#### ready (Text/JSON)

```json
{"type": "ready"}
```

Server is loaded and ready to accept audio. Sent immediately after WebSocket accept.

#### partial (Text/JSON)

```json
{"type": "partial", "text": "今天天气", "segment_id": 1}
```

In-progress transcription. Each partial **replaces** the previous partial for the same `segment_id`.
May be sent zero or more times before the final.

#### final (Text/JSON)

```json
{"type": "final", "text": "今天天气真不错，我想出去爬山。", "segment_id": 1}
```

Committed result for this segment. Immutable — sent exactly once per segment.

#### error (Text/JSON)

```json
{"type": "error", "message": "model not loaded"}
```

#### ended (Text/JSON)

```json
{"type": "ended"}
```

Server has finished processing. Connection may be closed by either party after this.

## Segment

A segment is one continuous utterance. In push-to-talk mode there is always
exactly one segment (`segment_id = 1`). In continuous/meeting mode the server
may increment `segment_id` when it detects an utterance boundary (VAD).

## Example Session (push-to-talk)

```
Client                          Server
  │                                │
  │◄── {"type":"ready"} ──────────│  (after WS accept)
  │                                │
  ├── {"type":"start",...} ──────▶│
  ├── [binary audio chunk] ──────▶│
  │◄── {"type":"partial",...} ────│  (as user speaks)
  ├── [binary audio chunk] ──────▶│
  │◄── {"type":"partial",...} ────│
  ├── [binary audio chunk] ──────▶│
  ├── {"type":"stop"} ───────────▶│  (user released key)
  │◄── {"type":"final",...} ──────│
  │◄── {"type":"ended"} ──────────│
  │                                │
```

## Reference Implementations

- **Server**: `engine/server.py` (Python, FastAPI/Starlette + uvicorn)
- **Client**: `voxtype-patch/src/transcribe/generic_ws.rs` (Rust, tokio-tungstenite)
