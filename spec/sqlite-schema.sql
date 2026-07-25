-- Schema SQLite client mngquest v1.
-- La versione eseguibile è mantenuta in src-tauri/src/db.rs.

PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA secure_delete = ON;
PRAGMA auto_vacuum = INCREMENTAL;

CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE client_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    config_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE local_answer (
    compilation_id TEXT NOT NULL,
    question_id TEXT NOT NULL,
    client_revision INTEGER NOT NULL,
    server_revision INTEGER,
    answer_json TEXT NOT NULL,
    sync_state TEXT NOT NULL
        CHECK (sync_state IN ('PENDING', 'SYNCED', 'ERROR')),
    last_error TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (compilation_id, question_id)
);

CREATE TABLE outbox (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    compilation_id TEXT NOT NULL,
    question_id TEXT NOT NULL,
    client_revision INTEGER NOT NULL,
    command TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('PENDING', 'IN_FLIGHT', 'ERROR')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX outbox_status_sequence_idx
    ON outbox(status, sequence);

CREATE TABLE questionnaire_cache (
    compilation_id TEXT PRIMARY KEY,
    questionnaire_id TEXT NOT NULL,
    questionnaire_revision INTEGER NOT NULL,
    questionnaire_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
