use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Transizione di stato non valida: {0}")]
    InvalidState(String),

    #[error("WebSocket non connesso")]
    Disconnected,

    #[error("Timeout della richiesta {0}")]
    Timeout(String),

    #[error("Errore Tauri: {0}")]
    Tauri(#[from] tauri::Error),

    #[error("Errore I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("Errore JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Errore SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Errore storage: {0}")]
    Storage(String),

    #[error("Errore protocollo: {0}")]
    Protocol(String),

    #[error("Errore server {code}: {message}")]
    Server {
        code: String,
        message: String,
        retryable: bool,
    },
}

impl AppError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Disconnected | Self::Timeout(_) => true,
            Self::Server { retryable, .. } => *retryable,
            _ => false,
        }
    }
}

impl serde::Serialize for AppError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
