import type { ClientRuntime } from "../types/protocol";

interface Props {
  runtime: ClientRuntime;
}

export function StatusPanel({ runtime }: Props) {
  const { snapshot } = runtime;
  return (
    <section className="panel">
      <h2>Stato runtime</h2>
      <dl className="status-grid">
        <dt>Stato</dt><dd>{snapshot.state}</dd>
        <dt>WebSocket</dt><dd>{snapshot.websocketConnected ? "connesso" : "non connesso"}</dd>
        <dt>Server</dt><dd className="mono">{snapshot.serverUrl}</dd>
        <dt>Installation</dt><dd className="mono">{snapshot.installationId}</dd>
        <dt>Sessione</dt><dd className="mono">{snapshot.sessionId}</dd>
        <dt>Compilazione</dt><dd className="mono">{snapshot.compilationId}</dd>
        <dt>Revisione server</dt><dd>{snapshot.serverRevision}</dd>
        <dt>Scadenza</dt><dd>{snapshot.expiresAt ?? "—"}</dd>
        <dt>Token</dt><dd>{runtime.tokenLoaded ? "caricato" : "assente"}</dd>
        <dt>SQLite</dt><dd className="mono">{runtime.sqlitePath}</dd>
        <dt>Outbox pendente</dt><dd>{runtime.pendingOutbox}</dd>
        <dt>Outbox errori</dt><dd>{runtime.outboxErrors}</dd>
        <dt>Fullscreen</dt><dd>{runtime.fullscreen ? "attivo" : "non attivo"}</dd>
      </dl>
      {snapshot.lastError && <p className="error">{snapshot.lastError}</p>}
    </section>
  );
}
