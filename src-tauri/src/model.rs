use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClientState {
    Disconnected,
    Connected,
    Starting,
    Running,
    Offline,
    Submitting,
    Submitted,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientConfig {
    pub server_url: String,
    pub installation_id: String,
    pub session_id: String,
    pub compilation_id: String,
    pub request_timeout_ms: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_url: std::env::var("MNGQUEST_WS_URL")
                .unwrap_or_else(|_| "ws://127.0.0.1:32456/hws".to_string()),
            installation_id: std::env::var("MNGQUEST_INSTALLATION_ID")
                .unwrap_or_else(|_| "http-installation-test-1".to_string()),
            session_id: std::env::var("MNGQUEST_SESSION_ID")
                .unwrap_or_else(|_| "ws-session-test-1".to_string()),
            compilation_id: std::env::var("MNGQUEST_COMPILATION_ID")
                .unwrap_or_else(|_| "tauri-compilation-test-1".to_string()),
            request_timeout_ms: 8_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigureClientInput {
    pub server_url: String,
    pub token: String,
    pub installation_id: String,
    pub session_id: String,
    pub compilation_id: String,
    pub request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientSnapshot {
    pub state: ClientState,
    pub server_url: String,
    pub installation_id: String,
    pub session_id: String,
    pub compilation_id: String,
    pub websocket_connected: bool,
    pub server_revision: u64,
    pub expires_at: Option<String>,
    pub receipt: Option<String>,
    pub submitted_at: Option<String>,
    pub monitor_count: Option<usize>,
    pub last_error: Option<String>,
}

impl ClientSnapshot {
    pub fn from_config(config: &ClientConfig) -> Self {
        Self {
            state: ClientState::Disconnected,
            server_url: config.server_url.clone(),
            installation_id: config.installation_id.clone(),
            session_id: config.session_id.clone(),
            compilation_id: config.compilation_id.clone(),
            websocket_connected: false,
            server_revision: 0,
            expires_at: None,
            receipt: None,
            submitted_at: None,
            monitor_count: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AnswerSyncState {
    Pending,
    Synced,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalAnswer {
    pub question_id: String,
    pub client_revision: u64,
    pub server_revision: Option<u64>,
    pub answer: Value,
    pub sync_state: AnswerSyncState,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientRuntime {
    pub snapshot: ClientSnapshot,
    pub config: ClientConfig,
    pub answers: BTreeMap<String, LocalAnswer>,
    pub token_loaded: bool,
    pub fullscreen: bool,
    pub sqlite_path: String,
    pub pending_outbox: u64,
    pub outbox_errors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivateAttemptInput {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivateAttemptData {
    #[serde(alias = "access_token")]
    pub access_token: String,
    #[serde(alias = "token_type")]
    pub token_type: String,
    #[serde(alias = "expires_in")]
    pub expires_in: u64,
    #[serde(alias = "compilazione_id")]
    pub compilazione_id: String,
    pub state: String,
    #[serde(alias = "server_revision")]
    pub server_revision: u64,
    #[serde(alias = "started_at")]
    pub started_at: String,
    #[serde(alias = "expires_at")]
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartAttemptData {
    #[serde(alias = "compilazione_id")]
    pub compilazione_id: String,
    pub state: String,
    #[serde(alias = "server_revision")]
    pub server_revision: u64,
    #[serde(alias = "started_at")]
    pub started_at: String,
    #[serde(alias = "expires_at")]
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionnaireReadData {
    #[serde(alias = "compilazione_id")]
    pub compilazione_id: String,
    pub state: String,
    #[serde(alias = "server_revision")]
    pub server_revision: u64,
    #[serde(alias = "expires_at")]
    pub expires_at: String,
    pub questionario: Questionnaire,
    #[serde(default)]
    pub answers: Vec<ServerAnswer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerAnswer {
    #[serde(alias = "domanda_id")]
    pub question_id: String,
    #[serde(alias = "client_revision")]
    pub client_revision: u64,
    #[serde(alias = "server_revision")]
    pub server_revision: u64,
    pub answer: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Questionnaire {
    #[serde(alias = "questionario_id")]
    pub questionnaire_id: String,
    pub revision: u64,
    pub title: String,
    pub questions: Vec<Question>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Question {
    #[serde(alias = "domanda_id")]
    pub question_id: String,
    #[serde(rename = "type")]
    pub question_type: String,
    pub text: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub options: Vec<QuestionOption>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuestionOption {
    #[serde(alias = "option_id")]
    pub option_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAnswerInput {
    pub question_id: String,
    pub client_revision: u64,
    pub answer: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAnswerResult {
    #[serde(alias = "domanda_id")]
    pub question_id: String,
    #[serde(alias = "client_revision")]
    pub client_revision: u64,
    #[serde(alias = "server_revision")]
    pub server_revision: u64,
    pub idempotent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAnswerOutcome {
    pub question_id: String,
    pub client_revision: u64,
    pub server_revision: Option<u64>,
    pub idempotent: Option<bool>,
    pub queued: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncSummary {
    pub attempted: u64,
    pub synced: u64,
    pub pending: u64,
    pub errors: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitData {
    pub state: String,
    #[serde(alias = "submitted_at")]
    pub submitted_at: String,
    pub receipt: String,
    pub idempotent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitResult {
    pub snapshot: ClientSnapshot,
    pub receipt: String,
    pub submitted_at: String,
    pub idempotent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub config: ClientConfig,
    pub snapshot: ClientSnapshot,
    pub answers: BTreeMap<String, LocalAnswer>,
}
