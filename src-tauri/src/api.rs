use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{error::AppError, state::AppState, websocket};

#[derive(Debug, Serialize)]
struct CommandAuth<'a> {
    token: &'a str,
}

#[derive(Debug, Serialize)]
struct CommandRequest<'a> {
    version: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    request_id: &'a str,
    cmd: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<CommandAuth<'a>>,
    data: &'a Value,
}

#[derive(Debug, Deserialize)]
struct CommandError {
    code: String,
    message: String,
    #[serde(default)]
    retryable: bool,
}

#[derive(Debug, Deserialize)]
struct CommandResponse<T> {
    version: String,
    #[serde(rename = "type")]
    message_type: String,
    request_id: String,
    cmd: String,
    ok: bool,
    data: Option<T>,
    error: Option<CommandError>,
}

pub async fn send_command<T: DeserializeOwned>(
    state: &AppState,
    cmd: &str,
    data: Value,
    attempts: usize,
) -> Result<T, AppError> {
    let request_id = Uuid::new_v4().to_string();
    send_command_with_request_id(
        state,
        cmd,
        data,
        attempts,
        &request_id,
    )
    .await
}

pub async fn send_command_with_request_id<T: DeserializeOwned>(
    state: &AppState,
    cmd: &str,
    data: Value,
    attempts: usize,
    request_id: &str,
) -> Result<T, AppError> {
    let token = state
        .access_token
        .read()
        .await
        .clone()
        .ok_or_else(|| {
            AppError::InvalidState("token JWT assente".to_string())
        })?;

    send_command_internal(
        state,
        cmd,
        data,
        attempts,
        request_id,
        Some(token),
    )
    .await
}

pub async fn send_public_command<T: DeserializeOwned>(
    state: &AppState,
    cmd: &str,
    data: Value,
    attempts: usize,
) -> Result<T, AppError> {
    let request_id = Uuid::new_v4().to_string();
    send_command_internal(
        state,
        cmd,
        data,
        attempts,
        &request_id,
        None,
    )
    .await
}

async fn send_command_internal<T: DeserializeOwned>(
    state: &AppState,
    cmd: &str,
    data: Value,
    attempts: usize,
    request_id: &str,
    token: Option<String>,
) -> Result<T, AppError> {
    let timeout_ms = state.config.read().await.request_timeout_ms;
    let envelope = serde_json::to_value(CommandRequest {
        version: "1.0",
        message_type: "request",
        request_id,
        cmd,
        auth: token
            .as_deref()
            .map(|value| CommandAuth { token: value }),
        data: &data,
    })?;

    let total_attempts = attempts.max(1);
    let mut last_error = None;

    for attempt in 1..=total_attempts {
        match websocket::request(
            state,
            &envelope,
            Duration::from_millis(timeout_ms),
        )
        .await
        {
            Ok(value) => {
                eprintln!(
                    "[mngquest] api: envelope ricevuto cmd={} request_id={} ok={:?} response_cmd={:?} has_data={} error_code={:?}",
                    cmd,
                    request_id,
                    value.get("ok").and_then(Value::as_bool),
                    value.get("cmd").and_then(Value::as_str),
                    value.get("data").is_some(),
                    value
                        .get("error")
                        .and_then(|error| error.get("code"))
                        .and_then(Value::as_str)
                );

                let decoded = decode_response(value, request_id, cmd);

                match &decoded {
                    Ok(_) => eprintln!(
                        "[mngquest] api: decodifica completata cmd={} request_id={}",
                        cmd,
                        request_id
                    ),
                    Err(error) => eprintln!(
                        "[mngquest] api: errore decodifica cmd={} request_id={} errore={}",
                        cmd,
                        request_id,
                        error
                    ),
                }

                return decoded;
            }
            Err(error @ AppError::Timeout(_))
            | Err(error @ AppError::Disconnected) => {
                last_error = Some(error);
                if attempt == total_attempts {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        AppError::Protocol(
            "richiesta fallita senza dettaglio".to_string(),
        )
    }))
}

fn decode_response<T: DeserializeOwned>(
    value: Value,
    request_id: &str,
    cmd: &str,
) -> Result<T, AppError> {
    let response = serde_json::from_value::<CommandResponse<T>>(value)?;
    if response.version != "1.0"
        || response.message_type != "response"
    {
        return Err(AppError::Protocol(
            "envelope di risposta non valido".to_string(),
        ));
    }
    if response.request_id != request_id {
        return Err(AppError::Protocol(format!(
            "request_id inatteso: {}",
            response.request_id
        )));
    }
    if response.cmd != cmd {
        return Err(AppError::Protocol(format!(
            "cmd inatteso: {}",
            response.cmd
        )));
    }
    if !response.ok {
        let error = response.error.unwrap_or(CommandError {
            code: "UNKNOWN_ERROR".to_string(),
            message: "errore server senza descrizione".to_string(),
            retryable: false,
        });
        return Err(AppError::Server {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
        });
    }

    response.data.ok_or_else(|| {
        AppError::Protocol("data assente nella risposta".to_string())
    })
}
