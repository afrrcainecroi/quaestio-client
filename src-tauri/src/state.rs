use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use futures_util::stream::SplitSink;
use tokio::{net::TcpStream, sync::{oneshot, Mutex, RwLock}};
use tokio_tungstenite::{
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};

use crate::{
    db::Database,
    error::AppError,
    model::{
        Checkpoint, ClientConfig, ClientSnapshot, ClientState, LocalAnswer,
    },
};

pub type PendingReply = oneshot::Sender<Result<Value, String>>;
pub type WebSocketWriter =
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<ClientConfig>>,
    pub snapshot: Arc<RwLock<ClientSnapshot>>,
    pub access_token: Arc<RwLock<Option<String>>>,
    pub answers: Arc<RwLock<BTreeMap<String, LocalAnswer>>>,
    pub ws_writer: Arc<Mutex<Option<WebSocketWriter>>>,
    pub pending: Arc<Mutex<HashMap<String, PendingReply>>>,
    pub db: Database,
}

impl AppState {
    pub fn new() -> Self {
        Self::try_new().unwrap_or_else(|error| {
            panic!("impossibile inizializzare SQLite mngquest: {error}")
        })
    }

    fn try_new() -> Result<Self, AppError> {
        let database = Database::open(database_path())?;

        if database.load_state()?.is_none() {
            if let Some(checkpoint) = load_legacy_checkpoint() {
                database.import_checkpoint(&checkpoint)?;
                archive_legacy_checkpoint();
            } else {
                let config = ClientConfig::default();
                let snapshot = ClientSnapshot::from_config(&config);
                database.save_state(&config, &snapshot)?;
            }
        }

        let (config, mut snapshot) = database
            .load_state()?
            .ok_or_else(|| {
                AppError::Storage(
                    "stato SQLite assente dopo l'inizializzazione".to_string(),
                )
            })?;

        if attempt_is_expired(&snapshot) {
            snapshot = ClientSnapshot::from_config(&config);
            database.recreate_with_state(&config, &snapshot)?;
        }

        snapshot.websocket_connected = false;
        if matches!(
            snapshot.state,
            ClientState::Connected
                | ClientState::Starting
                | ClientState::Running
                | ClientState::Submitting
        ) {
            snapshot.state = ClientState::Offline;
        }

        database.save_state(&config, &snapshot)?;
        let answers = database.load_answers(&snapshot.compilation_id)?;

        let token = std::env::var("MNGQUEST_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(read_default_token);

        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            snapshot: Arc::new(RwLock::new(snapshot)),
            access_token: Arc::new(RwLock::new(token)),
            answers: Arc::new(RwLock::new(answers)),
            ws_writer: Arc::new(Mutex::new(None)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            db: database,
        })
    }

    pub async fn persist(&self) -> Result<(), AppError> {
        // Cloniamo i valori protetti dai RwLock e rilasciamo subito
        // i read guard prima di entrare nel codice SQLite sincrono.
        let config = self.config.read().await.clone();
        let snapshot = self.snapshot.read().await.clone();

        self.db.save_state(&config, &snapshot)
    }

    pub async fn reload_answers(&self) -> Result<(), AppError> {
        let compilation_id = self.snapshot.read().await.compilation_id.clone();
        let answers = self.db.load_answers(&compilation_id)?;
        *self.answers.write().await = answers;
        Ok(())
    }
}

fn database_path() -> PathBuf {
    if let Ok(value) = std::env::var("MNGQUEST_SQLITE_FILE") {
        if !value.trim().is_empty() {
            return PathBuf::from(value);
        }
    }

    dirs::data_local_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("controlled-questionnaire-client")
        .join("mngquest-v1.sqlite3")
}

fn legacy_checkpoint_path() -> PathBuf {
    if let Ok(value) = std::env::var("MNGQUEST_CLIENT_STATE_FILE") {
        return PathBuf::from(value);
    }

    dirs::data_local_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("controlled-questionnaire-client")
        .join("state-v0.5.json")
}

fn load_legacy_checkpoint() -> Option<Checkpoint> {
    let path = legacy_checkpoint_path();
    let data = fs::read(path).ok()?;
    serde_json::from_slice(&data).ok()
}

fn archive_legacy_checkpoint() {
    let path = legacy_checkpoint_path();
    if !path.exists() {
        return;
    }
    let destination = path.with_extension("json.migrated");
    let _ = fs::rename(path, destination);
}

fn read_default_token() -> Option<String> {
    let path = std::env::var("MNGQUEST_TOKEN_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)?;

    let value = fs::read_to_string(path).ok()?;
    let token = value.trim().to_string();
    (!token.is_empty()).then_some(token)
}

fn attempt_is_expired(snapshot: &ClientSnapshot) -> bool {
    if snapshot.state == ClientState::Submitted {
        return false;
    }

    snapshot
        .expires_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .is_some_and(|expires_at| expires_at.with_timezone(&Utc) <= Utc::now())
}
