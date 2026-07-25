export type ClientState =
  | "DISCONNECTED"
  | "CONNECTED"
  | "STARTING"
  | "RUNNING"
  | "OFFLINE"
  | "SUBMITTING"
  | "SUBMITTED"
  | "ERROR";

export interface ClientConfig {
  serverUrl: string;
  installationId: string;
  sessionId: string;
  compilationId: string;
  requestTimeoutMs: number;
}

export interface ConfigureClientInput extends ClientConfig {
  token: string;
}

export interface ActivateAttemptInput {
  username: string;
  password: string;
}

export interface ClientSnapshot {
  state: ClientState;
  serverUrl: string;
  installationId: string;
  sessionId: string;
  compilationId: string;
  websocketConnected: boolean;
  serverRevision: number;
  expiresAt?: string;
  receipt?: string;
  submittedAt?: string;
  monitorCount?: number;
  lastError?: string | null;
}

export type AnswerSyncState = "PENDING" | "SYNCED" | "ERROR";

export interface LocalAnswer {
  questionId: string;
  clientRevision: number;
  serverRevision?: number | null;
  answer: unknown;
  syncState: AnswerSyncState;
  lastError?: string | null;
}

export interface ClientRuntime {
  snapshot: ClientSnapshot;
  config: ClientConfig;
  answers: Record<string, LocalAnswer>;
  tokenLoaded: boolean;
  fullscreen: boolean;
  sqlitePath: string;
  pendingOutbox: number;
  outboxErrors: number;
}

export interface Questionnaire {
  questionnaireId: string;
  revision: number;
  title: string;
  questions: Question[];
}

export interface Question {
  questionId: string;
  type:
    | "SINGLE_CHOICE"
    | "MULTIPLE_CHOICE"
    | "TEXT"
    | "NUMERIC"
    | string;
  text: string;
  required: boolean;
  options: QuestionOption[];
}

export interface QuestionOption {
  optionId: string;
  text: string;
}

export interface SaveAnswerInput {
  questionId: string;
  clientRevision: number;
  answer: unknown;
}

export interface SaveAnswerOutcome {
  questionId: string;
  clientRevision: number;
  serverRevision?: number | null;
  idempotent?: boolean | null;
  queued: boolean;
}

export interface SyncSummary {
  attempted: number;
  synced: number;
  pending: number;
  errors: number;
  lastError?: string | null;
}

export interface SubmitResult {
  snapshot: ClientSnapshot;
  receipt: string;
  submittedAt: string;
  idempotent: boolean;
}
