use crate::{
    api,
    error::AppError,
    model::{SaveAnswerResult, SyncSummary},
    state::AppState,
};

pub async fn flush_pending_answers(
    state: &AppState,
) -> Result<SyncSummary, AppError> {
    let mut attempted = 0_u64;
    let mut synced = 0_u64;
    let mut last_error = None;

    if !state.snapshot.read().await.websocket_connected
        || state.access_token.read().await.is_none()
    {
        let (pending, errors) = state.db.outbox_counts()?;
        return Ok(SyncSummary {
            attempted,
            synced,
            pending,
            errors,
            last_error,
        });
    }

    loop {
        let mut entries = state.db.pending_outbox(1)?;
        let Some(entry) = entries.pop() else {
            break;
        };

        state.db.mark_outbox_in_flight(entry.sequence)?;
        attempted += 1;

        let result = api::send_command_with_request_id::<SaveAnswerResult>(
            state,
            &entry.command,
            entry.payload.clone(),
            1,
            &entry.request_id,
        )
        .await;

        match result {
            Ok(response) => {
                state.db.acknowledge_outbox(&entry, &response)?;
                {
                    let mut snapshot = state.snapshot.write().await;
                    snapshot.server_revision = snapshot
                        .server_revision
                        .max(response.server_revision);
                    snapshot.last_error = None;
                }
                synced += 1;
            }
            Err(error) => {
                let retryable = error.is_retryable();
                let message = error.to_string();
                state.db.fail_outbox(
                    &entry,
                    &message,
                    retryable,
                )?;
                state.snapshot.write().await.last_error =
                    Some(message.clone());
                last_error = Some(message);
                break;
            }
        }
    }

    state.reload_answers().await?;
    state.persist().await?;
    let (pending, errors) = state.db.outbox_counts()?;

    Ok(SyncSummary {
        attempted,
        synced,
        pending,
        errors,
        last_error,
    })
}
