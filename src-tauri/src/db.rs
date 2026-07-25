use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    error::AppError,
    model::{
        AnswerSyncState, Checkpoint, ClientConfig, ClientSnapshot, LocalAnswer,
        Questionnaire, SaveAnswerResult, ServerAnswer,
    },
};

const SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub sequence: i64,
    pub request_id: String,
    pub compilation_id: String,
    pub question_id: String,
    pub client_revision: u64,
    pub command: String,
    pub payload: Value,
}

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
    path: Arc<PathBuf>,
}

impl Database {
    pub fn open(path: PathBuf) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut connection = Connection::open(&path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "secure_delete", "ON")?;
        connection.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;

        migrate(&mut connection)?;
        connection.execute(
            "UPDATE outbox SET status = 'PENDING'
             WHERE status = 'IN_FLIGHT'",
            [],
        )?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            path: Arc::new(path),
        })
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn load_state(
        &self,
    ) -> Result<Option<(ClientConfig, ClientSnapshot)>, AppError> {
        self.with_connection(|connection| {
            let row = connection
                .query_row(
                    "SELECT config_json, snapshot_json
                     FROM client_state
                     WHERE id = 1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                        ))
                    },
                )
                .optional()?;

            match row {
                Some((config_json, snapshot_json)) => Ok(Some((
                    serde_json::from_str(&config_json)?,
                    serde_json::from_str(&snapshot_json)?,
                ))),
                None => Ok(None),
            }
        })
    }

    pub fn save_state(
        &self,
        config: &ClientConfig,
        snapshot: &ClientSnapshot,
    ) -> Result<(), AppError> {
        let config_json = serde_json::to_string(config)?;
        let snapshot_json = serde_json::to_string(snapshot)?;
        let now = now();

        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO client_state
                    (id, config_json, snapshot_json, updated_at)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    config_json = excluded.config_json,
                    snapshot_json = excluded.snapshot_json,
                    updated_at = excluded.updated_at",
                params![config_json, snapshot_json, now],
            )?;
            Ok(())
        })
    }

    pub fn import_checkpoint(
        &self,
        checkpoint: &Checkpoint,
    ) -> Result<(), AppError> {
        let config_json = serde_json::to_string(&checkpoint.config)?;
        let snapshot_json = serde_json::to_string(&checkpoint.snapshot)?;
        let now = now();

        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO client_state
                    (id, config_json, snapshot_json, updated_at)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    config_json = excluded.config_json,
                    snapshot_json = excluded.snapshot_json,
                    updated_at = excluded.updated_at",
                params![config_json, snapshot_json, now],
            )?;

            for answer in checkpoint.answers.values() {
                insert_local_answer(
                    &transaction,
                    &checkpoint.snapshot.compilation_id,
                    answer,
                )?;

                if answer.sync_state != AnswerSyncState::Synced {
                    let request_id = Uuid::new_v4().to_string();
                    let payload = json!({
                        "compilazione_id": checkpoint.snapshot.compilation_id.clone(),
                        "domanda_id": answer.question_id.clone(),
                        "client_revision": answer.client_revision,
                        "answer": answer.answer.clone()
                    });
                    insert_outbox(
                        &transaction,
                        &request_id,
                        &checkpoint.snapshot.compilation_id,
                        &answer.question_id,
                        answer.client_revision,
                        "compilazione.save-answer",
                        &payload,
                    )?;
                }
            }

            transaction.commit()?;
            Ok(())
        })
    }

    pub fn load_answers(
        &self,
        compilation_id: &str,
    ) -> Result<BTreeMap<String, LocalAnswer>, AppError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT question_id, client_revision, server_revision,
                        answer_json, sync_state, last_error
                 FROM local_answer
                 WHERE compilation_id = ?1
                 ORDER BY question_id",
            )?;

            let rows = statement
                .query_map([compilation_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;

            let mut answers = BTreeMap::new();
            for (
                question_id,
                client_revision,
                server_revision,
                answer_json,
                sync_state,
                last_error,
            ) in rows
            {
                let answer = LocalAnswer {
                    question_id: question_id.clone(),
                    client_revision: to_u64(client_revision, "client_revision")?,
                    server_revision: server_revision
                        .map(|value| to_u64(value, "server_revision"))
                        .transpose()?,
                    answer: serde_json::from_str(&answer_json)?,
                    sync_state: sync_state_from_str(&sync_state)?,
                    last_error,
                };
                answers.insert(question_id, answer);
            }
            Ok(answers)
        })
    }

    pub fn queue_answer(
        &self,
        compilation_id: &str,
        answer: &LocalAnswer,
        request_id: &str,
        payload: &Value,
    ) -> Result<(), AppError> {
        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            insert_local_answer(&transaction, compilation_id, answer)?;
            insert_outbox(
                &transaction,
                request_id,
                compilation_id,
                &answer.question_id,
                answer.client_revision,
                "compilazione.save-answer",
                payload,
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    pub fn pending_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, AppError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT sequence, request_id, compilation_id, question_id,
                        client_revision, command, payload_json
                 FROM outbox o
                 WHERE o.status = 'PENDING'
                   AND NOT EXISTS (
                       SELECT 1
                       FROM outbox previous
                       WHERE previous.sequence < o.sequence
                         AND previous.status = 'ERROR'
                   )
                 ORDER BY sequence
                 LIMIT ?1",
            )?;

            let rows = statement
                .query_map([limit as i64], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;

            rows.into_iter()
                .map(
                    |(
                        sequence,
                        request_id,
                        compilation_id,
                        question_id,
                        client_revision,
                        command,
                        payload_json,
                    )| {
                        Ok(OutboxEntry {
                            sequence,
                            request_id,
                            compilation_id,
                            question_id,
                            client_revision: to_u64(
                                client_revision,
                                "client_revision",
                            )?,
                            command,
                            payload: serde_json::from_str(&payload_json)?,
                        })
                    },
                )
                .collect()
        })
    }

    pub fn mark_outbox_in_flight(
        &self,
        sequence: i64,
    ) -> Result<(), AppError> {
        self.with_connection(|connection| {
            connection.execute(
                "UPDATE outbox
                 SET status = 'IN_FLIGHT',
                     attempts = attempts + 1,
                     updated_at = ?2
                 WHERE sequence = ?1",
                params![sequence, now()],
            )?;
            Ok(())
        })
    }

    pub fn acknowledge_outbox(
        &self,
        entry: &OutboxEntry,
        response: &SaveAnswerResult,
    ) -> Result<(), AppError> {
        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "DELETE FROM outbox WHERE sequence = ?1",
                [entry.sequence],
            )?;
            transaction.execute(
                "UPDATE local_answer
                 SET server_revision = ?4,
                     sync_state = CASE
                         WHEN client_revision = ?3 THEN 'SYNCED'
                         ELSE sync_state
                     END,
                     last_error = CASE
                         WHEN client_revision = ?3 THEN NULL
                         ELSE last_error
                     END,
                     updated_at = ?5
                 WHERE compilation_id = ?1
                   AND question_id = ?2",
                params![
                    entry.compilation_id,
                    entry.question_id,
                    to_i64(entry.client_revision)?,
                    to_i64(response.server_revision)?,
                    now(),
                ],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    pub fn fail_outbox(
        &self,
        entry: &OutboxEntry,
        message: &str,
        retryable: bool,
    ) -> Result<(), AppError> {
        let outbox_status = if retryable { "PENDING" } else { "ERROR" };
        let answer_status = if retryable { "PENDING" } else { "ERROR" };

        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE outbox
                 SET status = ?2,
                     last_error = ?3,
                     updated_at = ?4
                 WHERE sequence = ?1",
                params![entry.sequence, outbox_status, message, now()],
            )?;
            transaction.execute(
                "UPDATE local_answer
                 SET sync_state = CASE
                         WHEN client_revision = ?3 THEN ?4
                         ELSE sync_state
                     END,
                     last_error = CASE
                         WHEN client_revision = ?3 THEN ?5
                         ELSE last_error
                     END,
                     updated_at = ?6
                 WHERE compilation_id = ?1
                   AND question_id = ?2",
                params![
                    entry.compilation_id,
                    entry.question_id,
                    to_i64(entry.client_revision)?,
                    answer_status,
                    message,
                    now(),
                ],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    pub fn retry_outbox_errors(&self) -> Result<(), AppError> {
        self.with_connection(|connection| {
            connection.execute(
                "UPDATE outbox
                 SET status = 'PENDING',
                     last_error = NULL,
                     updated_at = ?1
                 WHERE status = 'ERROR'",
                [now()],
            )?;
            connection.execute(
                "UPDATE local_answer
                 SET sync_state = 'PENDING',
                     last_error = NULL,
                     updated_at = ?1
                 WHERE sync_state = 'ERROR'",
                [now()],
            )?;
            Ok(())
        })
    }

    pub fn outbox_counts(&self) -> Result<(u64, u64), AppError> {
        self.with_connection(|connection| {
            let (pending, errors) = connection.query_row(
                "SELECT
                    COALESCE(SUM(CASE
                        WHEN status IN ('PENDING', 'IN_FLIGHT') THEN 1
                        ELSE 0
                    END), 0),
                    COALESCE(SUM(CASE
                        WHEN status = 'ERROR' THEN 1
                        ELSE 0
                    END), 0)
                 FROM outbox",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            Ok((
                to_u64(pending, "pending outbox")?,
                to_u64(errors, "errori outbox")?,
            ))
        })
    }

    pub fn reconcile_server_answers(
        &self,
        compilation_id: &str,
        answers: &[ServerAnswer],
    ) -> Result<(), AppError> {
        use std::collections::HashSet;

        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            let server_question_ids: HashSet<&str> = answers
                .iter()
                .map(|answer| answer.question_id.as_str())
                .collect();

            // Le risposte restituite da compilazione.read sono autorevoli.
            // Non sovrascriviamo però modifiche locali ancora PENDING/ERROR.
            for answer in answers {
                transaction.execute(
                    "INSERT INTO local_answer
                        (compilation_id, question_id, client_revision,
                         server_revision, answer_json, sync_state,
                         last_error, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'SYNCED', NULL, ?6)
                     ON CONFLICT(compilation_id, question_id) DO UPDATE SET
                        client_revision = excluded.client_revision,
                        server_revision = excluded.server_revision,
                        answer_json = excluded.answer_json,
                        sync_state = 'SYNCED',
                        last_error = NULL,
                        updated_at = excluded.updated_at
                     WHERE local_answer.sync_state = 'SYNCED'
                       AND excluded.client_revision >=
                           local_answer.client_revision",
                    params![
                        compilation_id,
                        answer.question_id,
                        to_i64(answer.client_revision)?,
                        to_i64(answer.server_revision)?,
                        serde_json::to_string(&answer.answer)?,
                        now(),
                    ],
                )?;
            }

            // Una risposta locale marcata SYNCED ma assente dalla lettura
            // autorevole del server non è realmente sincronizzata. La
            // rimettiamo nell'outbox con la stessa revisione e lo stesso
            // contenuto, così il submit resta bloccato finché il server
            // non la riconferma.
            let stale_synced = {
                let mut statement = transaction.prepare(
                    "SELECT question_id, client_revision, answer_json
                     FROM local_answer
                     WHERE compilation_id = ?1
                       AND sync_state = 'SYNCED'
                     ORDER BY question_id",
                )?;

                let rows = statement
                    .query_map([compilation_id], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;

                rows
            };

            for (question_id, client_revision, answer_json) in stale_synced {
                if server_question_ids.contains(question_id.as_str()) {
                    continue;
                }

                let already_queued: i64 = transaction.query_row(
                    "SELECT EXISTS(
                         SELECT 1
                         FROM outbox
                         WHERE compilation_id = ?1
                           AND question_id = ?2
                           AND client_revision = ?3
                           AND status IN ('PENDING', 'IN_FLIGHT', 'ERROR')
                     )",
                    params![
                        compilation_id,
                        question_id,
                        client_revision,
                    ],
                    |row| row.get(0),
                )?;

                if already_queued == 0 {
                    let request_id = Uuid::new_v4().to_string();
                    let answer: Value =
                        serde_json::from_str(&answer_json)?;
                    let payload = json!({
                        "compilazione_id": compilation_id,
                        "domanda_id": question_id,
                        "client_revision": to_u64(
                            client_revision,
                            "client_revision"
                        )?,
                        "answer": answer
                    });

                    insert_outbox(
                        &transaction,
                        &request_id,
                        compilation_id,
                        payload
                            .get("domanda_id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                AppError::Storage(
                                    "domanda_id assente nel payload di riconciliazione"
                                        .to_string(),
                                )
                            })?,
                        to_u64(client_revision, "client_revision")?,
                        "compilazione.save-answer",
                        &payload,
                    )?;
                }

                transaction.execute(
                    "UPDATE local_answer
                     SET sync_state = 'PENDING',
                         server_revision = NULL,
                         last_error = NULL,
                         updated_at = ?4
                     WHERE compilation_id = ?1
                       AND question_id = ?2
                       AND client_revision = ?3",
                    params![
                        compilation_id,
                        question_id,
                        client_revision,
                        now(),
                    ],
                )?;
            }

            transaction.commit()?;
            Ok(())
        })
    }

    pub fn cache_questionnaire(
        &self,
        compilation_id: &str,
        questionnaire: &Questionnaire,
    ) -> Result<(), AppError> {
        let json = serde_json::to_string(questionnaire)?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO questionnaire_cache
                    (compilation_id, questionnaire_id,
                     questionnaire_revision, questionnaire_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(compilation_id) DO UPDATE SET
                    questionnaire_id = excluded.questionnaire_id,
                    questionnaire_revision = excluded.questionnaire_revision,
                    questionnaire_json = excluded.questionnaire_json,
                    updated_at = excluded.updated_at",
                params![
                    compilation_id,
                    questionnaire.questionnaire_id,
                    to_i64(questionnaire.revision)?,
                    json,
                    now(),
                ],
            )?;
            Ok(())
        })
    }

    pub fn load_cached_questionnaire(
        &self,
        compilation_id: &str,
    ) -> Result<Option<Questionnaire>, AppError> {
        self.with_connection(|connection| {
            let json = connection
                .query_row(
                    "SELECT questionnaire_json
                     FROM questionnaire_cache
                     WHERE compilation_id = ?1",
                    [compilation_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            json.map(|value| serde_json::from_str(&value))
                .transpose()
                .map_err(AppError::from)
        })
    }

    pub fn rebind_compilation(
        &self,
        old_compilation_id: &str,
        new_compilation_id: &str,
    ) -> Result<(), AppError> {
        if old_compilation_id.is_empty()
            || old_compilation_id == new_compilation_id
        {
            return Ok(());
        }

        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE OR REPLACE local_answer
                 SET compilation_id = ?2
                 WHERE compilation_id = ?1",
                params![old_compilation_id, new_compilation_id],
            )?;
            transaction.execute(
                "UPDATE outbox
                 SET compilation_id = ?2,
                     payload_json = json_set(
                         payload_json,
                         '$.compilazione_id',
                         ?2
                     ),
                     updated_at = ?3
                 WHERE compilation_id = ?1",
                params![old_compilation_id, new_compilation_id, now()],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO questionnaire_cache
                    (compilation_id, questionnaire_id,
                     questionnaire_revision, questionnaire_json, updated_at)
                 SELECT ?2, questionnaire_id,
                        questionnaire_revision, questionnaire_json, ?3
                 FROM questionnaire_cache
                 WHERE compilation_id = ?1",
                params![old_compilation_id, new_compilation_id, now()],
            )?;
            transaction.execute(
                "DELETE FROM questionnaire_cache
                 WHERE compilation_id = ?1",
                [old_compilation_id],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    pub fn clear_attempt_data(&self) -> Result<(), AppError> {
        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute("DELETE FROM outbox", [])?;
            transaction.execute("DELETE FROM local_answer", [])?;
            transaction.execute("DELETE FROM questionnaire_cache", [])?;
            transaction.commit()?;
            Ok(())
        })
    }

    pub fn complete_submit(
        &self,
        config: &ClientConfig,
        snapshot: &ClientSnapshot,
        compilation_id: &str,
    ) -> Result<(), AppError> {
        let config_json = serde_json::to_string(config)?;
        let snapshot_json = serde_json::to_string(snapshot)?;
        let timestamp = now();

        self.with_connection(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO client_state
                    (id, config_json, snapshot_json, updated_at)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    config_json = excluded.config_json,
                    snapshot_json = excluded.snapshot_json,
                    updated_at = excluded.updated_at",
                params![config_json, snapshot_json, timestamp],
            )?;
            transaction.execute(
                "DELETE FROM outbox WHERE compilation_id = ?1",
                [compilation_id],
            )?;
            transaction.execute(
                "DELETE FROM local_answer WHERE compilation_id = ?1",
                [compilation_id],
            )?;
            transaction.execute(
                "DELETE FROM questionnaire_cache WHERE compilation_id = ?1",
                [compilation_id],
            )?;
            transaction.commit()?;

            connection.execute_batch(
                "PRAGMA wal_checkpoint(TRUNCATE);
                 PRAGMA incremental_vacuum;",
            )?;
            Ok(())
        })
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, AppError>,
    ) -> Result<T, AppError> {
        let mut guard = self.connection.lock().map_err(|_| {
            AppError::Storage("mutex SQLite avvelenato".to_string())
        })?;
        operation(&mut guard)
    }
}

fn migrate(connection: &mut Connection) -> Result<(), AppError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
         );

         CREATE TABLE IF NOT EXISTS client_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            config_json TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
         );

         CREATE TABLE IF NOT EXISTS local_answer (
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

         CREATE TABLE IF NOT EXISTS outbox (
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

         CREATE INDEX IF NOT EXISTS outbox_status_sequence_idx
             ON outbox(status, sequence);

         CREATE TABLE IF NOT EXISTS questionnaire_cache (
            compilation_id TEXT PRIMARY KEY,
            questionnaire_id TEXT NOT NULL,
            questionnaire_revision INTEGER NOT NULL,
            questionnaire_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
         );",
    )?;

    let existing = connection
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    match existing {
        Some(value) if value.parse::<i64>().ok() == Some(SCHEMA_VERSION) => {}
        Some(value) => {
            return Err(AppError::Storage(format!(
                "versione schema SQLite non supportata: {value}"
            )));
        }
        None => {
            connection.execute(
                "INSERT INTO schema_meta(key, value)
                 VALUES ('schema_version', ?1)",
                [SCHEMA_VERSION.to_string()],
            )?;
        }
    }
    Ok(())
}

fn insert_local_answer(
    transaction: &Transaction<'_>,
    compilation_id: &str,
    answer: &LocalAnswer,
) -> Result<(), AppError> {
    transaction.execute(
        "INSERT INTO local_answer
            (compilation_id, question_id, client_revision,
             server_revision, answer_json, sync_state,
             last_error, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(compilation_id, question_id) DO UPDATE SET
            client_revision = excluded.client_revision,
            server_revision = excluded.server_revision,
            answer_json = excluded.answer_json,
            sync_state = excluded.sync_state,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at",
        params![
            compilation_id,
            answer.question_id,
            to_i64(answer.client_revision)?,
            answer
                .server_revision
                .map(to_i64)
                .transpose()?,
            serde_json::to_string(&answer.answer)?,
            sync_state_as_str(&answer.sync_state),
            answer.last_error,
            now(),
        ],
    )?;
    Ok(())
}

fn insert_outbox(
    transaction: &Transaction<'_>,
    request_id: &str,
    compilation_id: &str,
    question_id: &str,
    client_revision: u64,
    command: &str,
    payload: &Value,
) -> Result<(), AppError> {
    let timestamp = now();
    transaction.execute(
        "INSERT INTO outbox
            (request_id, compilation_id, question_id,
             client_revision, command, payload_json,
             status, attempts, last_error, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6,
                 'PENDING', 0, NULL, ?7, ?7)",
        params![
            request_id,
            compilation_id,
            question_id,
            to_i64(client_revision)?,
            command,
            serde_json::to_string(payload)?,
            timestamp,
        ],
    )?;
    Ok(())
}

fn sync_state_as_str(state: &AnswerSyncState) -> &'static str {
    match state {
        AnswerSyncState::Pending => "PENDING",
        AnswerSyncState::Synced => "SYNCED",
        AnswerSyncState::Error => "ERROR",
    }
}

fn sync_state_from_str(value: &str) -> Result<AnswerSyncState, AppError> {
    match value {
        "PENDING" => Ok(AnswerSyncState::Pending),
        "SYNCED" => Ok(AnswerSyncState::Synced),
        "ERROR" => Ok(AnswerSyncState::Error),
        other => Err(AppError::Storage(format!(
            "stato risposta SQLite non valido: {other}"
        ))),
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn to_i64(value: u64) -> Result<i64, AppError> {
    i64::try_from(value).map_err(|_| {
        AppError::Storage(format!("valore troppo grande per SQLite: {value}"))
    })
}

fn to_u64(value: i64, field: &str) -> Result<u64, AppError> {
    u64::try_from(value).map_err(|_| {
        AppError::Storage(format!(
            "valore negativo per {field}: {value}"
        ))
    })
}
