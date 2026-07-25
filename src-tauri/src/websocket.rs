use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tauri::{AppHandle, Emitter, Manager};
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{header::SEC_WEBSOCKET_PROTOCOL, HeaderValue},
        Message,
    },
};

use crate::{error::AppError, model::ClientState, state::AppState};

const SUBPROTOCOL: &str = "mngquest.v1";

pub async fn connect(app: AppHandle, url: String) -> Result<(), AppError> {
    let state = app.state::<AppState>().inner().clone();
    if state.ws_writer.lock().await.is_some() {
        return Ok(());
    }

    let mut request = url
        .clone()
        .into_client_request()
        .map_err(|error| {
            AppError::Protocol(format!(
                "URL WebSocket non valida: {error}"
            ))
        })?;

    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(SUBPROTOCOL),
    );

    let (stream, response) = connect_async(request)
        .await
        .map_err(|error| {
            AppError::Protocol(format!(
                "connessione WebSocket fallita: {error}"
            ))
        })?;

    let selected = response
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok());

    if selected != Some(SUBPROTOCOL) {
        return Err(AppError::Protocol(format!(
            "subprotocol WebSocket inatteso: {selected:?}"
        )));
    }

    let (writer, mut reader) = stream.split();
    *state.ws_writer.lock().await = Some(writer);

    {
        let mut snapshot = state.snapshot.write().await;
        snapshot.websocket_connected = true;
        snapshot.state = ClientState::Connected;
        snapshot.last_error = None;
    }
    state.persist().await?;

    let _ = app.emit(
        "transport-status",
        serde_json::json!({
            "connected": true,
            "url": url,
            "subprotocol": SUBPROTOCOL
        }),
    );

    let task_app = app.clone();
    let task_state = state.clone();

    tauri::async_runtime::spawn(async move {
        let disconnect_reason = loop {
            match reader.next().await {
                Some(Ok(Message::Text(text))) => {
                    handle_text_message(
                        &task_app,
                        &task_state,
                        text.as_ref(),
                    )
                    .await;
                }
                Some(Ok(Message::Ping(payload))) => {
                    let send_result = {
                        let mut writer_guard =
                            task_state.ws_writer.lock().await;

                        match writer_guard.as_mut() {
                            Some(writer) => {
                                writer.send(Message::Pong(payload)).await
                            }
                            None => {
                                break "writer WebSocket assente".to_string();
                            }
                        }
                    };

                    if let Err(error) = send_result {
                        break format!(
                            "invio pong fallito: {error}"
                        );
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    break format!("close WebSocket: {frame:?}");
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    break format!(
                        "lettura WebSocket fallita: {error}"
                    );
                }
                None => {
                    break "stream WebSocket terminato".to_string();
                }
            }
        };

        *task_state.ws_writer.lock().await = None;
        drain_pending(&task_state, &disconnect_reason).await;

        {
            let mut snapshot = task_state.snapshot.write().await;
            snapshot.websocket_connected = false;
            snapshot.last_error = Some(disconnect_reason.clone());
            snapshot.state = match snapshot.state {
                ClientState::Running
                | ClientState::Starting
                | ClientState::Submitting => ClientState::Offline,
                ClientState::Submitted => ClientState::Submitted,
                _ => ClientState::Disconnected,
            };
        }

        let _ = task_state.persist().await;
        let _ = task_app.emit(
            "transport-status",
            serde_json::json!({
                "connected": false,
                "reason": disconnect_reason
            }),
        );
    });

    Ok(())
}

pub async fn disconnect(state: &AppState) -> Result<(), AppError> {
    let writer = state.ws_writer.lock().await.take();

    if let Some(mut writer) = writer {
        let _ = writer.send(Message::Close(None)).await;
    }

    Ok(())
}

pub async fn request(
    state: &AppState,
    envelope: &Value,
    timeout_duration: Duration,
) -> Result<Value, AppError> {
    let request_id = envelope
        .get("request_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::Protocol(
                "request_id assente nell'envelope".to_string(),
            )
        })?
        .to_string();

    let command = envelope
        .get("cmd")
        .and_then(Value::as_str)
        .unwrap_or("<cmd-assente>")
        .to_string();

    let text = serde_json::to_string(envelope)?;
    let (reply_sender, reply_receiver) =
        tokio::sync::oneshot::channel();

    state
        .pending
        .lock()
        .await
        .insert(request_id.clone(), reply_sender);

    eprintln!(
        "[mngquest] websocket: scrittura diretta cmd={} request_id={}",
        command,
        request_id
    );

    let send_result = {
        let mut writer_guard = state.ws_writer.lock().await;
        let writer = match writer_guard.as_mut() {
            Some(writer) => writer,
            None => {
                state.pending.lock().await.remove(&request_id);
                return Err(AppError::Disconnected);
            }
        };

        timeout(
            timeout_duration,
            writer.send(Message::Text(text.into())),
        )
        .await
    };

    match send_result {
        Ok(Ok(())) => {
            eprintln!(
                "[mngquest] websocket: frame inviato cmd={} request_id={}",
                command,
                request_id
            );
        }
        Ok(Err(error)) => {
            state.pending.lock().await.remove(&request_id);
            return Err(AppError::Protocol(format!(
                "scrittura WebSocket fallita: {error}"
            )));
        }
        Err(_) => {
            state.pending.lock().await.remove(&request_id);
            return Err(AppError::Timeout(format!(
                "send:{request_id}"
            )));
        }
    }

    let received = timeout(timeout_duration, reply_receiver).await;

    eprintln!(
        "[mngquest] websocket: attesa risposta terminata cmd={} request_id={}",
        command,
        request_id
    );

    match received {
        Ok(Ok(Ok(value))) => {
            eprintln!(
                "[mngquest] websocket: oneshot ricevuto cmd={} request_id={}",
                command,
                request_id
            );
            Ok(value)
        }
        Ok(Ok(Err(message))) => {
            eprintln!(
                "[mngquest] websocket: oneshot errore cmd={} request_id={} errore={}",
                command,
                request_id,
                message
            );
            Err(AppError::Protocol(message))
        }
        Ok(Err(_)) => {
            eprintln!(
                "[mngquest] websocket: oneshot cancellato cmd={} request_id={}",
                command,
                request_id
            );
            Err(AppError::Disconnected)
        }
        Err(_) => {
            state.pending.lock().await.remove(&request_id);
            eprintln!(
                "[mngquest] websocket: timeout risposta cmd={} request_id={}",
                command,
                request_id
            );
            Err(AppError::Timeout(request_id))
        }
    }
}

async fn handle_text_message(
    app: &AppHandle,
    state: &AppState,
    text: &str,
) {
    let value = match serde_json::from_str::<Value>(text) {
        Ok(value) => value,
        Err(error) => {
            let _ = app.emit(
                "server-message",
                serde_json::json!({
                    "text": text,
                    "parseError": error.to_string()
                }),
            );
            return;
        }
    };

    if value.get("type").and_then(Value::as_str)
        == Some("response")
    {
        let request_id = value
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::to_owned);

        if let Some(request_id) = request_id {
            let reply = {
                let mut pending = state.pending.lock().await;
                pending.remove(&request_id)
            };

            if let Some(reply) = reply {
                eprintln!(
                    "[mngquest] websocket: risposta instradata request_id={}",
                    request_id
                );

                let delivered = reply.send(Ok(value)).is_ok();

                eprintln!(
                    "[mngquest] websocket: oneshot consegnato={} request_id={}",
                    delivered,
                    request_id
                );
                return;
            }

            eprintln!(
                "[mngquest] websocket: risposta senza richiesta pendente request_id={}",
                request_id
            );
        }
    }

    if value.get("type").and_then(Value::as_str) == Some("event") {
        let _ = app.emit("server-event", value);
    } else {
        let _ = app.emit("server-message", value);
    }
}

async fn drain_pending(state: &AppState, reason: &str) {
    let pending = {
        let mut guard = state.pending.lock().await;
        std::mem::take(&mut *guard)
    };

    for (_, reply) in pending {
        let _ = reply.send(Err(reason.to_string()));
    }
}
