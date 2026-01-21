use crate::assets::{self, AssetsLayout};
use anyhow::{Context, Result, anyhow};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::oneshot;

#[derive(Clone)]
struct DiagnosticsState {
    network_name: Arc<str>,
}

pub struct DiagnosticsProxyHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl DiagnosticsProxyHandle {
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

pub async fn spawn(layout: &AssetsLayout) -> Result<DiagnosticsProxyHandle> {
    let addr = SocketAddr::from(([127, 0, 0, 1], assets::diagnostics_proxy_port() as u16));
    let state = DiagnosticsState {
        network_name: Arc::from(layout.network_name()),
    };

    let app = Router::new()
        .route(
            "/diagnostics/{node}",
            get(diagnostics_ws_handler).post(diagnostics_post_handler),
        )
        .route(
            "/diagnostics/{node}/",
            get(diagnostics_ws_handler).post(diagnostics_post_handler),
        )
        .with_state(state);

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind diagnostics proxy to {}", addr))?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = axum::serve(listener, app).with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
    });

    tokio::spawn(async move {
        if let Err(err) = server.await {
            eprintln!("warning: diagnostics proxy exited: {}", err);
        }
    });

    Ok(DiagnosticsProxyHandle {
        shutdown_tx: Some(shutdown_tx),
    })
}

async fn diagnostics_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<DiagnosticsState>,
    Path(node_label): Path<String>,
) -> impl IntoResponse {
    let Some(node_id) = parse_node_id(&node_label) else {
        return (StatusCode::BAD_REQUEST, "invalid node id").into_response();
    };

    ws.on_upgrade(move |socket| handle_ws(socket, state, node_id))
}

fn parse_node_id(label: &str) -> Option<u32> {
    label.strip_prefix("node-")?.parse::<u32>().ok()
}

async fn diagnostics_post_handler(
    State(state): State<DiagnosticsState>,
    Path(node_label): Path<String>,
    body: Bytes,
) -> Response {
    let Some(node_id) = parse_node_id(&node_label) else {
        return (StatusCode::BAD_REQUEST, "invalid node id").into_response();
    };

    let socket_path = assets::diagnostics_socket_path(&state.network_name, node_id);
    let stream = match UnixStream::connect(&socket_path).await {
        Ok(stream) => stream,
        Err(err) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("diagnostics socket unavailable: {}", err),
            )
                .into_response();
        }
    };

    let (unix_reader, mut unix_writer) = stream.into_split();
    if let Err(err) = write_upstream(&mut unix_writer, b"set --quiet false --output json").await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("diagnostics setup failed: {}", err),
        )
            .into_response();
    }

    let mut unix_reader = BufReader::new(unix_reader);
    let mut unix_buf = Vec::new();
    if let Err(err) = await_setup_ack(&mut unix_reader, &mut unix_buf).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("diagnostics setup failed: {}", err),
        )
            .into_response();
    }

    if let Err(err) = write_upstream(&mut unix_writer, &body).await {
        return (
            StatusCode::BAD_GATEWAY,
            format!("diagnostics socket write failed: {}", err),
        )
            .into_response();
    }

    let stream = futures::stream::unfold(
        (unix_reader, unix_buf),
        |(mut reader, mut buf)| async move {
            loop {
                match parse_first_json(&buf) {
                    Ok(Some((value, consumed))) => {
                        buf.drain(0..consumed);
                        let mut line = value.to_string();
                        line.push('\n');
                        return Some((Ok(Bytes::from(line)), (reader, buf)));
                    }
                    Ok(None) => {
                        let mut read_buf = [0u8; 4096];
                        match reader.read(&mut read_buf).await {
                            Ok(0) => return None,
                            Ok(bytes_read) => buf.extend_from_slice(&read_buf[..bytes_read]),
                            Err(err) => return Some((Err(err), (reader, buf))),
                        }
                    }
                    Err(err) => {
                        return Some((
                            Err(io::Error::new(io::ErrorKind::InvalidData, err)),
                            (reader, buf),
                        ));
                    }
                }
            }
        },
    );

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/x-ndjson"),
    );
    response
}

async fn handle_ws(mut socket: WebSocket, state: DiagnosticsState, node_id: u32) {
    let socket_path = assets::diagnostics_socket_path(&state.network_name, node_id);
    let stream = match UnixStream::connect(&socket_path).await {
        Ok(stream) => stream,
        Err(err) => {
            let _ = socket
                .send(Message::Text(
                    format!("diagnostics socket unavailable: {}", err).into(),
                ))
                .await;
            let _ = socket.close().await;
            return;
        }
    };

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (unix_reader, mut unix_writer) = stream.into_split();
    if let Err(err) = write_upstream(&mut unix_writer, b"set --quiet false --output json").await {
        let _ = ws_sender
            .send(Message::Text(
                format!("diagnostics setup failed: {}", err).into(),
            ))
            .await;
        let _ = ws_sender.send(Message::Close(None)).await;
        return;
    }

    let mut unix_reader = BufReader::new(unix_reader);
    let mut unix_buf = Vec::new();
    let mut read_buf = [0u8; 4096];
    if let Err(err) = await_setup_ack(&mut unix_reader, &mut unix_buf).await {
        let _ = ws_sender
            .send(Message::Text(
                format!("diagnostics setup failed: {}", err).into(),
            ))
            .await;
        let _ = ws_sender.send(Message::Close(None)).await;
        return;
    }

    if let Err(err) = process_json_buffer(&mut ws_sender, &mut unix_buf).await {
        let _ = ws_sender
            .send(Message::Text(
                format!("diagnostics parse error: {}", err).into(),
            ))
            .await;
        let _ = ws_sender.send(Message::Close(None)).await;
        return;
    }

    loop {
        tokio::select! {
            message = ws_receiver.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if write_upstream(&mut unix_writer, text.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if write_upstream(&mut unix_writer, &data).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if ws_sender.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Err(_)) => {
                        break;
                    }
                }
            }
            read = unix_reader.read(&mut read_buf) => {
                match read {
                    Ok(0) => break,
                    Ok(bytes_read) => {
                        unix_buf.extend_from_slice(&read_buf[..bytes_read]);
                        if let Err(err) = process_json_buffer(&mut ws_sender, &mut unix_buf).await {
                            let _ = ws_sender
                                .send(Message::Text(format!("diagnostics parse error: {}", err).into()))
                                .await;
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

async fn write_upstream(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    payload: &[u8],
) -> Result<()> {
    if payload.ends_with(b"\n") {
        writer.write_all(payload).await?;
    } else {
        writer.write_all(payload).await?;
        writer.write_all(b"\n").await?;
    }
    Ok(())
}

async fn process_json_buffer(
    ws_sender: &mut SplitSink<WebSocket, Message>,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let mut consumed = 0usize;
    let mut stream = serde_json::Deserializer::from_slice(buffer).into_iter::<Value>();

    while let Some(item) = stream.next() {
        match item {
            Ok(value) => {
                consumed = stream.byte_offset();
                ws_sender
                    .send(Message::Text(value.to_string().into()))
                    .await
                    .map_err(|_| anyhow!("websocket send failed"))?;
            }
            Err(err) => {
                if err.is_eof() {
                    break;
                }
                return Err(err.into());
            }
        }
    }

    if consumed > 0 {
        buffer.drain(0..consumed);
    }

    Ok(())
}

async fn await_setup_ack(
    reader: &mut BufReader<OwnedReadHalf>,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    let mut read_buf = [0u8; 4096];
    loop {
        if let Some((value, consumed)) = parse_first_json(buffer)? {
            buffer.drain(0..consumed);
            return validate_setup_response(value);
        }

        let bytes_read = reader.read(&mut read_buf).await?;
        if bytes_read == 0 {
            return Err(anyhow!("diagnostics socket closed before setup response"));
        }
        buffer.extend_from_slice(&read_buf[..bytes_read]);
    }
}

fn parse_first_json(buffer: &[u8]) -> Result<Option<(Value, usize)>> {
    if buffer.is_empty() {
        return Ok(None);
    }

    let mut stream = serde_json::Deserializer::from_slice(buffer).into_iter::<Value>();
    match stream.next() {
        None => Ok(None),
        Some(Ok(value)) => Ok(Some((value, stream.byte_offset()))),
        Some(Err(err)) => {
            if err.is_eof() {
                Ok(None)
            } else {
                Err(err.into())
            }
        }
    }
}

fn validate_setup_response(value: Value) -> Result<()> {
    let Value::Object(map) = value else {
        return Err(anyhow!("unexpected diagnostics response"));
    };

    if map.contains_key("Success") {
        return Ok(());
    }

    if let Some(failure) = map.get("Failure") {
        if let Some(reason) = failure.get("reason").and_then(|value| value.as_str()) {
            return Err(anyhow!("diagnostics setup rejected: {}", reason));
        }
        return Err(anyhow!("diagnostics setup rejected: {}", failure));
    }

    Err(anyhow!("unexpected diagnostics response"))
}
