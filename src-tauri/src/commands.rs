use tauri::{AppHandle, State, WebviewWindow};
use uuid::Uuid;

use crate::{
    api,
    error::AppError,
    model::{
        ActivateAttemptData, ActivateAttemptInput, AnswerSyncState,
        ClientConfig, ClientRuntime, ClientSnapshot, ClientState,
        ConfigureClientInput, LocalAnswer, Questionnaire,
        QuestionnaireReadData, SaveAnswerInput, SaveAnswerOutcome,
        StartAttemptData, SubmitData, SubmitResult, SyncSummary,
    },
    outbox,
    state::AppState,
    websocket,
};

#[tauri::command]
pub async fn initialize_client(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    let monitor_count = window.available_monitors()?.len();
    state.snapshot.write().await.monitor_count = Some(monitor_count);
    state.persist().await?;
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn get_runtime(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn configure_client(
    input: ConfigureClientInput,
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    if state.ws_writer.lock().await.is_some() {
        return Err(AppError::InvalidState(
            "disconnettere il WebSocket prima di modificare la configurazione"
                .to_string(),
        ));
    }
    if input.server_url.trim().is_empty()
        || input.installation_id.trim().is_empty()
        || input.session_id.trim().is_empty()
        || input.compilation_id.trim().is_empty()
    {
        return Err(AppError::InvalidState(
            "URL e identificativi sono obbligatori".to_string(),
        ));
    }

    let config = ClientConfig {
        server_url: input.server_url.trim().to_string(),
        installation_id: input.installation_id.trim().to_string(),
        session_id: input.session_id.trim().to_string(),
        compilation_id: input.compilation_id.trim().to_string(),
        request_timeout_ms: input
            .request_timeout_ms
            .unwrap_or(8_000)
            .clamp(1_000, 60_000),
    };

    state.db.clear_attempt_data()?;
    *state.config.write().await = config.clone();
    *state.answers.write().await = Default::default();
    *state.snapshot.write().await = ClientSnapshot::from_config(&config);

    *state.access_token.write().await = if input.token.trim().is_empty() {
        None
    } else {
        Some(input.token.trim().to_string())
    };

    state.persist().await?;
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn connect_server(
    app: AppHandle,
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    let url = state.config.read().await.server_url.clone();
    websocket::connect(app, url).await?;
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn disconnect_server(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    websocket::disconnect(state.inner()).await?;
    {
        let mut snapshot = state.snapshot.write().await;
        snapshot.websocket_connected = false;
        snapshot.state = match snapshot.state {
            ClientState::Running | ClientState::Offline => {
                ClientState::Offline
            }
            ClientState::Submitted => ClientState::Submitted,
            _ => ClientState::Disconnected,
        };
    }
    state.persist().await?;
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn activate_attempt(
    input: ActivateAttemptInput,
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    ensure_connected(state.inner()).await?;
    if input.username.trim().is_empty() || input.password.is_empty() {
        return Err(AppError::InvalidState(
            "username e password sono obbligatori".to_string(),
        ));
    }

    let config = state.config.read().await.clone();
    let previous_compilation =
        state.snapshot.read().await.compilation_id.clone();
    state.snapshot.write().await.state = ClientState::Starting;

    eprintln!(
    "[mngquest] auth.activate payload: username={:?} installation_id={:?} sessione_id={:?} compilazione_id={:?} password_len={}",
    input.username.trim(),
    config.installation_id,
    config.session_id,
    config.compilation_id,
    input.password.chars().count()
);
    eprintln!("[mngquest] auth.activate: invio richiesta");
    let response = api::send_public_command::<ActivateAttemptData>(
        state.inner(),
        "auth.activate",
        serde_json::json!({
            "username": input.username.trim(),
            "password": input.password,
            "sessione_id": config.session_id,
            "installation_id": config.installation_id,
            "compilazione_id": config.compilation_id
        }),
        2,
    )
    .await;

    let response = match response {
        Ok(response) => {
            eprintln!("[mngquest] auth.activate: risposta ricevuta");
            response
        }
        Err(error) => {
            eprintln!(
                "[mngquest] auth.activate: errore restituito dall'API: {}",
                error
            );
            {
                let mut snapshot = state.snapshot.write().await;
                snapshot.state = ClientState::Connected;
                snapshot.last_error = Some(error.to_string());
            }
            return Err(error);
        }
    };

    if response.state != "RUNNING" || response.token_type != "Bearer" {
        state.snapshot.write().await.state = ClientState::Connected;
        return Err(AppError::Protocol(
            "risposta auth.activate non valida".to_string(),
        ));
    }

    eprintln!(
        "[mngquest] auth.activate: rebind compilazione {} -> {}",
        previous_compilation,
        response.compilazione_id
    );
    state.db.rebind_compilation(
        &previous_compilation,
        &response.compilazione_id,
    )?;
    eprintln!("[mngquest] auth.activate: rebind completato");
    *state.access_token.write().await = Some(response.access_token);
    window.set_fullscreen(true)?;

    {
        let mut snapshot = state.snapshot.write().await;
        snapshot.state = ClientState::Running;
        snapshot.compilation_id = response.compilazione_id;
        snapshot.server_revision = response.server_revision;
        snapshot.expires_at = Some(response.expires_at);
        snapshot.receipt = None;
        snapshot.submitted_at = None;
        snapshot.last_error = None;
    }

    state.reload_answers().await?;
    eprintln!("[mngquest] auth.activate: risposte SQLite caricate");
    state.persist().await?;
    eprintln!("[mngquest] auth.activate: stato persistito, ritorno alla UI");

    // La sincronizzazione dell'outbox non fa parte dell'autenticazione:
    // viene avviata separatamente dal frontend dopo il ritorno alla UI.
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn start_attempt(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ClientRuntime, AppError> {
    ensure_connected(state.inner()).await?;
    let config = state.config.read().await.clone();
    let previous_compilation =
        state.snapshot.read().await.compilation_id.clone();
    state.snapshot.write().await.state = ClientState::Starting;

    let response = api::send_command::<StartAttemptData>(
        state.inner(),
        "compilazione.start",
        serde_json::json!({
            "sessione_id": config.session_id,
            "installation_id": config.installation_id,
            "compilazione_id": config.compilation_id
        }),
        2,
    )
    .await;

    let response = match response {
        Ok(response) => response,
        Err(error) => {
            {
                let mut snapshot = state.snapshot.write().await;
                snapshot.state = ClientState::Connected;
                snapshot.last_error = Some(error.to_string());
            }
            state.persist().await?;
            return Err(error);
        }
    };

    if response.state != "RUNNING" {
        return Err(AppError::Protocol(format!(
            "stato inatteso dopo compilazione.start: {}",
            response.state
        )));
    }

    state.db.rebind_compilation(
        &previous_compilation,
        &response.compilazione_id,
    )?;
    window.set_fullscreen(true)?;

    {
        let mut snapshot = state.snapshot.write().await;
        snapshot.state = ClientState::Running;
        snapshot.compilation_id = response.compilazione_id;
        snapshot.server_revision = response.server_revision;
        snapshot.expires_at = Some(response.expires_at);
        snapshot.receipt = None;
        snapshot.submitted_at = None;
        snapshot.last_error = None;
    }

    state.reload_answers().await?;
    state.persist().await?;
    runtime(&window, state.inner()).await
}

#[tauri::command]
pub async fn load_questionnaire(
    state: State<'_, AppState>,
) -> Result<Questionnaire, AppError> {
    ensure_attempt_active(state.inner()).await?;
    let snapshot = state.snapshot.read().await.clone();
    let compilation_id = snapshot.compilation_id;

    if snapshot.websocket_connected
        && state.access_token.read().await.is_some()
    {
        let response = api::send_command::<QuestionnaireReadData>(
            state.inner(),
            "compilazione.read",
            serde_json::json!({
                "compilazione_id": compilation_id.clone()
            }),
            3,
        )
        .await;

        match response {
            Ok(response) => {
                state.db.reconcile_server_answers(
                    &compilation_id,
                    &response.answers,
                )?;
                state.db.cache_questionnaire(
                    &compilation_id,
                    &response.questionario,
                )?;
                state.reload_answers().await?;
                {
                    let mut snapshot = state.snapshot.write().await;
                    snapshot.server_revision = response.server_revision;
                    snapshot.expires_at = Some(response.expires_at);
                    snapshot.last_error = None;
                }
                state.persist().await?;
                return Ok(response.questionario);
            }
            Err(error) => {
                if let Some(cached) = state
                    .db
                    .load_cached_questionnaire(&compilation_id)?
                {
                    state.snapshot.write().await.last_error =
                        Some(error.to_string());
                    state.persist().await?;
                    return Ok(cached);
                }
                return Err(error);
            }
        }
    }

    state
        .db
        .load_cached_questionnaire(&compilation_id)?
        .ok_or_else(|| {
            AppError::InvalidState(
                "questionario non disponibile nella cache SQLite".to_string(),
            )
        })
}

#[tauri::command]
pub async fn save_answer(
    input: SaveAnswerInput,
    state: State<'_, AppState>,
) -> Result<SaveAnswerOutcome, AppError> {
    ensure_attempt_active(state.inner()).await?;

    let compilation_id =
        state.snapshot.read().await.compilation_id.clone();
    let request_id = Uuid::new_v4().to_string();
    let previous_server_revision = state
        .answers
        .read()
        .await
        .get(&input.question_id)
        .and_then(|answer| answer.server_revision);
    let local = LocalAnswer {
        question_id: input.question_id.clone(),
        client_revision: input.client_revision,
        server_revision: previous_server_revision,
        answer: input.answer.clone(),
        sync_state: AnswerSyncState::Pending,
        last_error: None,
    };
    let payload = serde_json::json!({
        "compilazione_id": compilation_id.clone(),
        "domanda_id": input.question_id.clone(),
        "client_revision": input.client_revision,
        "answer": input.answer.clone()
    });

    state.db.queue_answer(
        &compilation_id,
        &local,
        &request_id,
        &payload,
    )?;
    state.reload_answers().await?;

    if state.snapshot.read().await.websocket_connected
        && state.access_token.read().await.is_some()
    {
        let _ = outbox::flush_pending_answers(state.inner()).await?;
    }

    let current = state
        .answers
        .read()
        .await
        .get(&local.question_id)
        .cloned()
        .ok_or_else(|| {
            AppError::Storage(
                "risposta locale assente dopo il commit SQLite".to_string(),
            )
        })?;

    Ok(SaveAnswerOutcome {
        question_id: current.question_id,
        client_revision: current.client_revision,
        server_revision: current.server_revision,
        idempotent: None,
        queued: current.sync_state != AnswerSyncState::Synced,
    })
}

#[tauri::command]
pub async fn sync_pending_answers(
    state: State<'_, AppState>,
) -> Result<SyncSummary, AppError> {
    ensure_connected(state.inner()).await?;
    if state.snapshot.read().await.state != ClientState::Running {
        return Err(AppError::InvalidState(
            "autenticare nuovamente la compilazione prima della sincronizzazione"
                .to_string(),
        ));
    }
    state.db.retry_outbox_errors()?;
    outbox::flush_pending_answers(state.inner()).await
}

#[tauri::command]
pub async fn submit_attempt(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<SubmitResult, AppError> {
    ensure_connected(state.inner()).await?;
    if state.snapshot.read().await.state != ClientState::Running {
        return Err(AppError::InvalidState(
            "la compilazione non è nello stato RUNNING".to_string(),
        ));
    }

    let sync = outbox::flush_pending_answers(state.inner()).await?;
    if sync.pending > 0 || sync.errors > 0 {
        return Err(AppError::InvalidState(format!(
            "outbox non vuota: {} pendenti, {} errori",
            sync.pending, sync.errors
        )));
    }

    let compilation_id =
        state.snapshot.read().await.compilation_id.clone();
    state.snapshot.write().await.state = ClientState::Submitting;

    let response = api::send_command::<SubmitData>(
        state.inner(),
        "compilazione.submit",
        serde_json::json!({
            "compilazione_id": compilation_id.clone()
        }),
        3,
    )
    .await;

    let response = match response {
        Ok(response) => response,
        Err(error) => {
            {
                let mut snapshot = state.snapshot.write().await;
                snapshot.state = ClientState::Running;
                snapshot.last_error = Some(error.to_string());
            }
            state.persist().await?;
            return Err(error);
        }
    };

    if response.state != "SUBMITTED" {
        state.snapshot.write().await.state = ClientState::Running;
        return Err(AppError::Protocol(format!(
            "stato inatteso dopo compilazione.submit: {}",
            response.state
        )));
    }

    let snapshot = {
        let mut snapshot = state.snapshot.write().await;
        snapshot.state = ClientState::Submitted;
        snapshot.websocket_connected = false;
        snapshot.receipt = Some(response.receipt.clone());
        snapshot.submitted_at = Some(response.submitted_at.clone());
        snapshot.last_error = None;
        snapshot.clone()
    };
    let config = state.config.read().await.clone();
    state.db.recreate_with_state(&config, &snapshot)?;
    state.answers.write().await.clear();
    *state.access_token.write().await = None;
    websocket::disconnect(state.inner()).await?;
    window.set_fullscreen(false)?;

    Ok(SubmitResult {
        snapshot,
        receipt: response.receipt,
        submitted_at: response.submitted_at,
        idempotent: response.idempotent,
    })
}

async fn ensure_connected(state: &AppState) -> Result<(), AppError> {
    if !state.snapshot.read().await.websocket_connected {
        return Err(AppError::Disconnected);
    }
    Ok(())
}

async fn ensure_attempt_active(
    state: &AppState,
) -> Result<(), AppError> {
    let current = state.snapshot.read().await.state.clone();
    if !matches!(current, ClientState::Running | ClientState::Offline) {
        return Err(AppError::InvalidState(
            "la compilazione non è attiva".to_string(),
        ));
    }
    Ok(())
}

async fn runtime(
    window: &WebviewWindow,
    state: &AppState,
) -> Result<ClientRuntime, AppError> {
    let (pending_outbox, outbox_errors) = state.db.outbox_counts()?;

    Ok(ClientRuntime {
        snapshot: state.snapshot.read().await.clone(),
        config: state.config.read().await.clone(),
        answers: state.answers.read().await.clone(),
        token_loaded: state.access_token.read().await.is_some(),
        fullscreen: window.is_fullscreen()?,
        sqlite_path: state.db.path().display().to_string(),
        pending_outbox,
        outbox_errors,
    })
}
