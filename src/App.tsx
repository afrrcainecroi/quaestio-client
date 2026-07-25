import {
  type ChangeEvent,
  useEffect,
  useMemo,
  useState,
} from "react";
import { listen } from "@tauri-apps/api/event";
import {
  activateAttempt,
  configureClient,
  connectServer,
  disconnectServer,
  getRuntime,
  initializeClient,
  loadQuestionnaire,
  saveAnswer,
  startAttempt,
  submitAttempt,
  syncPendingAnswers,
} from "./lib/backend";
import { StatusPanel } from "./components/StatusPanel";
import type {
  ActivateAttemptInput,
  ClientRuntime,
  ConfigureClientInput,
  LocalAnswer,
  Question,
  Questionnaire,
} from "./types/protocol";
import "./styles.css";

const COMMAND_TIMEOUT_MS = 25_000;

function withTimeout<T>(
  promise: Promise<T>,
  operation: string,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = window.setTimeout(() => {
      reject(new Error(`${operation}: timeout client dopo ${COMMAND_TIMEOUT_MS / 1000}s`));
    }, COMMAND_TIMEOUT_MS);

    promise.then(
      (value) => {
        window.clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        window.clearTimeout(timer);
        reject(error);
      },
    );
  });
}

const emptyRuntime: ClientRuntime = {
  snapshot: {
    state: "DISCONNECTED",
    serverUrl: "ws://127.0.0.1:32456/hws",
    installationId: "http-installation-test-1",
    sessionId: "ws-session-test-1",
    compilationId: "tauri-compilation-test-1",
    websocketConnected: false,
    serverRevision: 0,
  },
  config: {
    serverUrl: "ws://127.0.0.1:32456/hws",
    installationId: "http-installation-test-1",
    sessionId: "ws-session-test-1",
    compilationId: "tauri-compilation-test-1",
    requestTimeoutMs: 8000,
  },
  answers: {},
  tokenLoaded: false,
  fullscreen: false,
  sqlitePath: "",
  pendingOutbox: 0,
  outboxErrors: 0,
};

export default function App() {
  const [runtime, setRuntime] = useState<ClientRuntime>(emptyRuntime);
  const [form, setForm] = useState<ConfigureClientInput>({
    ...emptyRuntime.config,
    token: "",
  });
  const [auth, setAuth] = useState<ActivateAttemptInput>({
    username: "http-student-test-1",
    password: "",
  });
  const [questionnaire, setQuestionnaire] =
    useState<Questionnaire | null>(null);
  const [answers, setAnswers] =
    useState<Record<string, LocalAnswer>>({});
  const [busy, setBusy] = useState(false);
  const [message, setMessage] =
    useState<string | null>(null);

  useEffect(() => {
    void initializeClient()
      .then(applyRuntime)
      .catch(showError);

    const unlisten = listen<{
      connected: boolean;
      reason?: string;
    }>("transport-status", ({ payload }) => {
      setMessage(
        payload.connected
          ? "WebSocket connesso"
          : payload.reason ?? "WebSocket disconnesso",
      );
      void refreshRuntime();
    });

    const timer = window.setInterval(
      () => void refreshRuntime(),
      2500,
    );
    return () => {
      window.clearInterval(timer);
      void unlisten.then((fn) => fn());
    };
  }, []);

  useEffect(() => {
    if (
      !runtime.snapshot.websocketConnected
      || runtime.snapshot.state !== "RUNNING"
      || runtime.pendingOutbox === 0
    ) {
      return;
    }

    const timer = window.setTimeout(() => {
      void syncPendingAnswers()
        .then(() => refreshRuntime())
        .catch(() => {
          // Lo stato dell'outbox e l'errore restano in SQLite.
        });
    }, 5000);

    return () => window.clearTimeout(timer);
  }, [
    runtime.snapshot.websocketConnected,
    runtime.snapshot.state,
    runtime.pendingOutbox,
  ]);

  const unsynced = useMemo(
    () =>
      Object.values(answers).some(
        (answer) => answer.syncState !== "SYNCED",
      ),
    [answers],
  );

  const attemptVisible =
    runtime.snapshot.state === "RUNNING"
    || runtime.snapshot.state === "OFFLINE";

  function applyRuntime(next: ClientRuntime) {
    setRuntime(next);
    setAnswers(next.answers);
    setForm((current) => ({
      ...current,
      ...next.config,
      token: current.token,
    }));
  }

  async function refreshRuntime() {
    try {
      applyRuntime(await getRuntime());
    } catch {
      // La finestra può essere in chiusura.
    }
  }

  function showError(error: unknown) {
    setMessage(
      error instanceof Error ? error.message : String(error),
    );
    setBusy(false);
  }

  async function execute<T>(
    operation: () => Promise<T>,
    done?: (value: T) => void,
  ) {
    setBusy(true);
    setMessage(null);
    try {
      const value = await operation();
      done?.(value);
      await refreshRuntime();
    } catch (error) {
      showError(error);
      return;
    }
    setBusy(false);
  }

  function updateForm<K extends keyof ConfigureClientInput>(
    key: K,
    value: ConfigureClientInput[K],
  ) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  async function beginActivation() {
    setBusy(true);
    setMessage("auth.activate in corso…");
    try {
      const next = await withTimeout(
        activateAttempt(auth),
        "auth.activate",
      );
      applyRuntime(next);
      setAuth((current) => ({ ...current, password: "" }));

      // Da questo punto l'autenticazione è conclusa e la UI può
      // distinguere un eventuale problema di compilazione.read.
      setMessage("Autenticazione completata; caricamento questionario…");
      const loaded = await withTimeout(
        loadQuestionnaire(),
        "compilazione.read",
      );
      setQuestionnaire(loaded);
      setMessage("Autenticazione e caricamento completati");
      await refreshRuntime();
    } catch (error) {
      showError(error);
    } finally {
      setBusy(false);
    }
  }

  async function beginAttempt() {
    setBusy(true);
    setMessage(null);
    try {
      applyRuntime(await startAttempt());
      setQuestionnaire(await loadQuestionnaire());
      await refreshRuntime();
    } catch (error) {
      showError(error);
      return;
    }
    setBusy(false);
  }

  async function persistAnswer(
    questionId: string,
    answer: unknown,
  ) {
    const revision =
      (answers[questionId]?.clientRevision ?? 0) + 1;

    setAnswers((current) => ({
      ...current,
      [questionId]: {
        questionId,
        clientRevision: revision,
        answer,
        syncState: "PENDING",
      },
    }));

    try {
      const result = await saveAnswer({
        questionId,
        clientRevision: revision,
        answer,
      });
      if (result.queued) {
        setMessage(
          runtime.snapshot.websocketConnected
            ? "Risposta registrata nell'outbox SQLite"
            : "Risposta salvata localmente: sincronizzazione rinviata",
        );
      }
      await refreshRuntime();
    } catch (error) {
      showError(error);
      await refreshRuntime();
    }
  }

  function selectedOptions(questionId: string): string[] {
    const answer = answers[questionId]?.answer as
      | { selected_option_ids?: string[] }
      | undefined;
    return answer?.selected_option_ids ?? [];
  }

  function renderQuestion(question: Question) {
    const selected = selectedOptions(question.questionId);
    const local = answers[question.questionId];

    return (
      <article className="question" key={question.questionId}>
        <h3>
          {question.text}
          {question.required && (
            <span className="required"> *</span>
          )}
        </h3>

        {question.type === "SINGLE_CHOICE"
          && question.options.map((option) => (
            <label className="choice" key={option.optionId}>
              <input
                type="radio"
                name={question.questionId}
                checked={selected.includes(option.optionId)}
                onChange={() =>
                  void persistAnswer(question.questionId, {
                    selected_option_ids: [option.optionId],
                  })}
              />
              {option.text}
            </label>
          ))}

        {question.type === "MULTIPLE_CHOICE"
          && question.options.map((option) => (
            <label className="choice" key={option.optionId}>
              <input
                type="checkbox"
                checked={selected.includes(option.optionId)}
                onChange={() => {
                  const next = selected.includes(option.optionId)
                    ? selected.filter(
                        (id) => id !== option.optionId,
                      )
                    : [...selected, option.optionId];
                  void persistAnswer(question.questionId, {
                    selected_option_ids: next,
                  });
                }}
              />
              {option.text}
            </label>
          ))}

        {question.type === "TEXT" && (
          <textarea
            defaultValue={String(
              (
                local?.answer as
                  | { text?: string }
                  | undefined
              )?.text ?? "",
            )}
            onBlur={(
              event: ChangeEvent<HTMLTextAreaElement>,
            ) =>
              void persistAnswer(question.questionId, {
                text: event.target.value,
              })}
          />
        )}

        <small
          className={`save-state ${(
            local?.syncState ?? "UNSAVED"
          ).toLowerCase()}`}
        >
          Stato locale: {local?.syncState ?? "UNSAVED"}
          {local?.lastError
            ? ` — ${local.lastError}`
            : ""}
        </small>
      </article>
    );
  }

  return (
    <main className="layout">
      <header>
        <p className="eyebrow">MNGQUEST</p>
        <h1>
          Controlled Questionnaire Client v0.7.0
        </h1>
        <p>
          SQLite WAL, outbox persistente e funzionamento
          local-first.
        </p>
      </header>

      <StatusPanel runtime={runtime} />
      {message && (
        <section className="panel notice">{message}</section>
      )}

      {!runtime.snapshot.websocketConnected && !attemptVisible && (
        <section className="panel">
          <h2>Configurazione connessione</h2>
          <div className="config-grid">
            <label>
              WebSocket URL
              <input
                value={form.serverUrl}
                onChange={(
                  event: ChangeEvent<HTMLInputElement>,
                ) =>
                  updateForm(
                    "serverUrl",
                    event.target.value,
                  )}
              />
            </label>
            <label>
              Token JWT
              <input
                type="password"
                value={form.token}
                onChange={(
                  event: ChangeEvent<HTMLInputElement>,
                ) =>
                  updateForm("token", event.target.value)}
                placeholder={
                  runtime.tokenLoaded
                    ? "token già caricato"
                    : "fallback opzionale"
                }
              />
            </label>
            <label>
              Installation ID
              <input
                value={form.installationId}
                onChange={(
                  event: ChangeEvent<HTMLInputElement>,
                ) =>
                  updateForm(
                    "installationId",
                    event.target.value,
                  )}
              />
            </label>
            <label>
              Session ID
              <input
                value={form.sessionId}
                onChange={(
                  event: ChangeEvent<HTMLInputElement>,
                ) =>
                  updateForm(
                    "sessionId",
                    event.target.value,
                  )}
              />
            </label>
            <label>
              Compilation ID
              <input
                value={form.compilationId}
                onChange={(
                  event: ChangeEvent<HTMLInputElement>,
                ) =>
                  updateForm(
                    "compilationId",
                    event.target.value,
                  )}
              />
            </label>
            <label>
              Timeout ms
              <input
                type="number"
                value={form.requestTimeoutMs}
                onChange={(
                  event: ChangeEvent<HTMLInputElement>,
                ) =>
                  updateForm(
                    "requestTimeoutMs",
                    Number(event.target.value),
                  )}
              />
            </label>
          </div>
          <div className="actions">
            <button
              disabled={busy}
              onClick={() =>
                void execute(
                  () => configureClient(form),
                  (next) => {
                    applyRuntime(next);
                    setForm((current) => ({
                      ...current,
                      token: "",
                    }));
                  },
                )}
            >
              Applica configurazione
            </button>
            <button
              disabled={busy}
              onClick={() =>
                void execute(connectServer, applyRuntime)}
            >
              Connetti
            </button>
          </div>
        </section>
      )}

      {runtime.snapshot.websocketConnected
        && runtime.snapshot.state !== "RUNNING"
        && runtime.snapshot.state !== "SUBMITTED" && (
          <section className="panel">
            <h2>Autenticazione e ripresa</h2>
            <div className="config-grid">
              <label>
                Username
                <input
                  value={auth.username}
                  onChange={(
                    event: ChangeEvent<HTMLInputElement>,
                  ) =>
                    setAuth((current) => ({
                      ...current,
                      username: event.target.value,
                    }))}
                />
              </label>
              <label>
                Password
                <input
                  type="password"
                  value={auth.password}
                  onChange={(
                    event: ChangeEvent<HTMLInputElement>,
                  ) =>
                    setAuth((current) => ({
                      ...current,
                      password: event.target.value,
                    }))}
                />
              </label>
            </div>
            <div className="actions">
              <button
                disabled={
                  busy || !auth.username || !auth.password
                }
                onClick={() => void beginActivation()}
              >
                auth.activate
              </button>
              {runtime.tokenLoaded && (
                <button
                  disabled={busy}
                  onClick={() => void beginAttempt()}
                >
                  Avvia con token esistente
                </button>
              )}
              <button
                className="secondary"
                disabled={busy}
                onClick={() =>
                  void execute(
                    disconnectServer,
                    applyRuntime,
                  )}
              >
                Disconnetti
              </button>
            </div>
          </section>
        )}

      {attemptVisible && !runtime.snapshot.websocketConnected && (
        <section className="panel notice">
          <strong>Modalità offline.</strong>{" "}
          Le risposte vengono salvate in SQLite. Per
          sincronizzarle, riconnetti il server e ripeti
          <code> auth.activate</code>.
          <div className="actions reconnect-actions">
            <button
              disabled={busy}
              onClick={() =>
                void execute(connectServer, applyRuntime)}
            >
              Riconnetti
            </button>
          </div>
        </section>
      )}

      {questionnaire && attemptVisible && (
        <section className="panel questionnaire">
          <h2>{questionnaire.title}</h2>
          <p>
            Revisione questionario {questionnaire.revision}
          </p>
          {questionnaire.questions.map(renderQuestion)}

          <div className="actions">
            {(runtime.pendingOutbox > 0
              || runtime.outboxErrors > 0)
              && runtime.snapshot.websocketConnected
              && runtime.snapshot.state === "RUNNING" && (
                <button
                  className="secondary"
                  disabled={busy}
                  onClick={() =>
                    void execute(
                      syncPendingAnswers,
                      (summary) => {
                        setMessage(
                          `Outbox: ${summary.synced} sincronizzate, `
                          + `${summary.pending} pendenti, `
                          + `${summary.errors} errori`,
                        );
                      },
                    )}
                >
                  Sincronizza outbox
                </button>
              )}

            <button
              className="submit"
              disabled={
                busy
                || unsynced
                || !runtime.snapshot.websocketConnected
                || runtime.pendingOutbox > 0
                || runtime.outboxErrors > 0
              }
              onClick={() =>
                void execute(submitAttempt, (result) => {
                  setQuestionnaire(null);
                  setAnswers({});
                  setMessage(
                    `Consegna confermata: ${result.receipt}`,
                  );
                })}
            >
              compilazione.submit
            </button>
          </div>
        </section>
      )}

      {runtime.snapshot.receipt && (
        <section className="panel receipt">
          <h2>Consegna confermata</h2>
          <code>{runtime.snapshot.receipt}</code>
          <p>{runtime.snapshot.submittedAt}</p>
        </section>
      )}
    </main>
  );
}
