import { invoke } from "@tauri-apps/api/core";
import type {
  ActivateAttemptInput,
  ClientRuntime,
  ConfigureClientInput,
  Questionnaire,
  SaveAnswerInput,
  SaveAnswerOutcome,
  SubmitResult,
  SyncSummary,
} from "../types/protocol";

export const initializeClient = () =>
  invoke<ClientRuntime>("initialize_client");

export const getRuntime = () =>
  invoke<ClientRuntime>("get_runtime");

export const configureClient = (input: ConfigureClientInput) =>
  invoke<ClientRuntime>("configure_client", { input });

export const connectServer = () =>
  invoke<ClientRuntime>("connect_server");

export const disconnectServer = () =>
  invoke<ClientRuntime>("disconnect_server");

export const activateAttempt = (input: ActivateAttemptInput) =>
  invoke<ClientRuntime>("activate_attempt", { input });

export const startAttempt = () =>
  invoke<ClientRuntime>("start_attempt");

export const loadQuestionnaire = () =>
  invoke<Questionnaire>("load_questionnaire");

export const saveAnswer = (input: SaveAnswerInput) =>
  invoke<SaveAnswerOutcome>("save_answer", { input });

export const syncPendingAnswers = () =>
  invoke<SyncSummary>("sync_pending_answers");

export const submitAttempt = () =>
  invoke<SubmitResult>("submit_attempt");
