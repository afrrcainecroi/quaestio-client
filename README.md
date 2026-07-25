# Controlled Questionnaire Client v0.7.6 — SQLite

Client Tauri per `mngquest-dual-transport-v0.6.0-auth`.

## Novità

La persistenza JSON è stata sostituita da SQLite:

- `journal_mode=WAL`;
- `synchronous=FULL`;
- `secure_delete=ON`;
- schema versionato;
- risposte locali transazionali;
- outbox persistente;
- stessi `request_id` nei retry;
- ripresa dopo crash;
- compilazione utilizzabile in stato `OFFLINE`;
- cache locale del questionario;
- cancellazione di risposte, outbox e cache dopo l'ACK di submit;
- checkpoint WAL troncato dopo la consegna.

Il JWT resta esclusivamente in memoria. Dopo un riavvio dell'applicazione si
esegue nuovamente `auth.activate`: il server riprende la compilazione e il
client sincronizza automaticamente l'outbox.

## Percorso database

Predefinito Linux:

```text
~/.local/share/controlled-questionnaire-client/mngquest-v1.sqlite3
```

Override:

```bash
MNGQUEST_SQLITE_FILE=/percorso/mngquest.sqlite3 npm run tauri dev
```

## Prima esecuzione

```bash
npm install

cd src-tauri
cargo check
cd ..

npm run tauri dev
```

Non serve eseguire preventivamente `cargo build`: `tauri dev` compila il
backend Rust.

La dipendenza `rusqlite` usa la feature `bundled`, quindi SQLite viene
compilato insieme al client e non dipende dalla versione installata nel
sistema.

## Semantica local-first

Quando l'utente modifica una risposta:

1. una transazione aggiorna `local_answer`;
2. nella stessa transazione inserisce l'envelope applicativo in `outbox`;
3. soltanto dopo il commit viene tentato l'invio WebSocket;
4. l'ACK server elimina l'elemento dall'outbox e marca la revisione `SYNCED`.

Se il processo o la rete si interrompono tra i punti 3 e 4, al successivo
`auth.activate` l'elemento viene reinviato con lo stesso `request_id` e la
stessa `client_revision`.

Le revisioni vengono inviate nell'ordine della colonna `sequence`. Al primo
errore la coda si arresta per non superare una revisione precedente.

## Modalità offline

Se il WebSocket cade durante la compilazione:

```text
RUNNING → OFFLINE
```

Il questionario resta visibile e le nuove risposte continuano a essere
salvate in SQLite. Per riprendere:

1. riconnettere il WebSocket;
2. eseguire nuovamente `auth.activate`;
3. attendere lo svuotamento dell'outbox.

Il submit è disabilitato finché esistono elementi pendenti o in errore.

## Migrazione dal checkpoint JSON

Alla prima apertura di un database vuoto, il client cerca il precedente:

```text
state-v0.5.json
```

Se valido, importa configurazione, snapshot e risposte in SQLite, quindi lo
rinomina:

```text
state-v0.5.json.migrated
```

## Dopo il submit

Solo dopo l'ACK `SUBMITTED`, una singola transazione salva lo snapshot finale e rimuove:

- risposte locali;
- outbox;
- questionario in cache.

Il client esegue inoltre:

```sql
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA incremental_vacuum;
```

`secure_delete=ON` migliora la cancellazione delle pagine SQLite, ma questa
versione non cifra ancora il database. Per protezione crittografica at-rest il
passo successivo sarà SQLCipher o cifratura applicativa con chiave effimera.


## Gestione errori outbox

La sincronizzazione automatica non supera mai un elemento in stato `ERROR`,
per mantenere l'ordine delle revisioni. Il pulsante **Sincronizza outbox**
rende nuovamente `PENDING` gli elementi in errore e tenta un retry esplicito.

I retry automatici sono distanziati di cinque secondi, evitando un ciclo
aggressivo in caso di timeout con socket ancora formalmente aperto.


Lo snapshot `SUBMITTED` e la cancellazione dei dati locali sono atomici: un
crash non può lasciare lo stato precedente dopo che le righe sensibili sono
state eliminate.


## Correzione v0.7.1

`AppState::persist()` clona `ClientConfig` e `ClientSnapshot` dai rispettivi
`RwLock` prima di chiamare SQLite. In questo modo non vengono passati
`RwLockReadGuard` a `Database::save_state` e i lock non restano detenuti
durante l'operazione sincrona sul database.


## Baseline v0.7.6

Questa è una versione completa e autosufficiente. Integra:

- SQLite WAL e outbox persistente;
- `AppState::persist()` senza guard `RwLock` passati al database;
- rilascio dei lock prima di ogni `persist()`;
- autenticazione non bloccata dalla sincronizzazione outbox;
- writer WebSocket diretto, senza canale `mpsc`;
- routing della risposta con rilascio esplicito del mutex `pending`;
- `request_id` copiato in una `String` prima di spostare il JSON della risposta;
- diagnostica sicura del trasporto, senza stampa del JWT.

Per preservare la cache Cargo, copiare i sorgenti nella directory di lavoro
stabile senza eliminare `src-tauri/target`, `node_modules` o `Cargo.lock`.
