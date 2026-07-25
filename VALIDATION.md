# Validazione v0.7.0 SQLite

## Build

```bash
npm install
npm run build

cd src-tauri
cargo check
```

## Verifica PRAGMA

Con il client chiuso:

```bash
sqlite3 ~/.local/share/controlled-questionnaire-client/mngquest-v1.sqlite3 \
  'PRAGMA journal_mode; PRAGMA synchronous; PRAGMA secure_delete;'
```

Valori attesi:

```text
wal
2
1
```

`2` corrisponde a `FULL`.

## Test offline

1. Avviare il backend e il client.
2. Eseguire `auth.activate`.
3. Caricare il questionario.
4. Arrestare il backend o scollegare la rete.
5. Selezionare una risposta.

L'interfaccia deve mostrare:

```text
Stato: OFFLINE
Stato locale: PENDING
Outbox pendente: 1
```

Verifica database:

```bash
sqlite3 ~/.local/share/controlled-questionnaire-client/mngquest-v1.sqlite3 \
  'SELECT sequence, request_id, command, status, attempts FROM outbox;'
```

## Test ripresa

1. Riavviare il backend.
2. Premere `Connetti`.
3. Ripetere `auth.activate`.

L'outbox deve essere sincronizzata automaticamente e la risposta deve
diventare `SYNCED`.

## Test crash

Con una risposta pendente:

```bash
pkill -9 controlled-questionnaire-client
```

Riavviare il client. Dopo la nuova autenticazione, la risposta deve essere
ancora presente e deve essere inviata.

## Test submit

Prima del submit:

```bash
sqlite3 ~/.local/share/controlled-questionnaire-client/mngquest-v1.sqlite3 \
  'SELECT count(*) FROM local_answer; SELECT count(*) FROM outbox;'
```

Dopo l'ACK del submit entrambe le tabelle devono restituire zero per la
compilazione consegnata.
