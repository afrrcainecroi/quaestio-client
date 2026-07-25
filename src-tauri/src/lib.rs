mod api;
mod commands;
mod db;
mod error;
mod model;
mod outbox;
mod state;
mod websocket;

use commands::{
    activate_attempt, configure_client, connect_server, disconnect_server,
    get_runtime, initialize_client, load_questionnaire, save_answer,
    start_attempt, submit_attempt, sync_pending_answers,
};
use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::new())
        .invoke_handler(tauri::generate_handler![
            initialize_client,
            get_runtime,
            configure_client,
            activate_attempt,
            connect_server,
            disconnect_server,
            start_attempt,
            load_questionnaire,
            save_answer,
            sync_pending_answers,
            submit_attempt
        ])
        .run(tauri::generate_context!())
        .expect("errore durante l'esecuzione dell'applicazione Tauri");
}
