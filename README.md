# Quaestio Client v0.7.6

Client desktop open source del sistema **Quaestio**, destinato alla compilazione controllata di questionari tramite un server remoto.

## Natura e finalità

Quaestio non costituisce un prodotto destinato alla vendita. È uno strumento di supporto tecnico-didattico e di ricerca, sviluppato e impiegato nell'ambito dell'attività professionale di consulenza e formazione ICT di Franco Arcieri.

Nell'ambito della documentazione professionale e contabile dell'autore, il software è classificato come immobilizzazione immateriale strumentale all'attività professionale.

## Autore e licenza

Quaestio è stato ideato, progettato e sviluppato da **Franco Arcieri**.

Il codice sorgente è distribuito secondo i termini della **Apache License 2.0**. Le attribuzioni applicabili sono riportate anche nel file [`NOTICE`](NOTICE).

Copyright © 2022–2026 Franco Arcieri.

## Repository ufficiali

- Quaestio Client: https://github.com/afrrcainecroi/quaestio-client
- Quaestio Server: https://github.com/afrrcainecroi/quaestio-server
- Distributed Cooperative Systems Security Framework — DCSSF: https://github.com/afrrcainecroi/dcssf

Il repository del server sarà pubblicato all'indirizzo indicato sopra.

## Architettura

Quaestio Client è un'applicazione Tauri con frontend React/TypeScript e backend Rust. Comunica con Quaestio Server tramite WebSocket e subprotocollo applicativo `mngquest.v1`.

Il nome `mngquest` resta utilizzato esclusivamente come identificatore tecnico del protocollo e della compatibilità con le versioni server esistenti.

Caratteristiche principali:

- persistenza locale SQLite;
- `journal_mode=WAL`;
- `synchronous=FULL`;
- `secure_delete=ON`;
- salvataggio transazionale delle risposte;
- outbox persistente e retry idempotenti;
- funzionamento local-first e ripresa dopo interruzioni;
- cache locale temporanea del questionario;
- riconciliazione delle risposte con il server;
- eliminazione dei dati della compilazione dopo submit o scadenza;
- JWT conservato esclusivamente in memoria.

## Percorso del database

Il percorso predefinito è ottenuto dalla directory dati locale dell'utente.

Su Windows:

```text
%LOCALAPPDATA%\controlled-questionnaire-client\mngquest-v1.sqlite3
```

Su Linux:

```text
~/.local/share/controlled-questionnaire-client/mngquest-v1.sqlite3
```

La directory conserva per compatibilità il nome storico `controlled-questionnaire-client`. Potrà essere migrata in una versione successiva senza perdere eventuali compilazioni temporaneamente pendenti.

È possibile specificare un percorso diverso con la variabile d'ambiente `MNGQUEST_SQLITE_FILE`.

## Sviluppo

Requisiti principali:

- Node.js e npm;
- Rust;
- toolchain Tauri;
- su Windows, MSVC e Windows SDK.

Avvio:

```text
npm install
npm run tauri dev
```

Endpoint server di sviluppo:

```text
MNGQUEST_WS_URL=ws://192.168.122.1:32456/hws
```

In PowerShell:

```powershell
$env:MNGQUEST_WS_URL = "ws://192.168.122.1:32456/hws"
npm run tauri dev
```

## Installer Windows

L'installer NSIS è configurato per l'utente corrente e non richiede un'installazione per macchina:

```text
npm run bundle:windows
```

Il pacchetto viene generato sotto:

```text
src-tauri\target\release\bundle\nsis\
```

## Semantica local-first

Quando l'utente modifica una risposta:

1. una transazione aggiorna `local_answer`;
2. la stessa transazione inserisce l'envelope applicativo nell'outbox;
3. soltanto dopo il commit viene tentato l'invio WebSocket;
4. l'ACK del server elimina l'elemento dall'outbox e marca la revisione `SYNCED`.

Se il processo o la rete si interrompono, la richiesta viene ritentata con lo stesso `request_id` e la stessa `client_revision`.

Il submit resta disabilitato finché esistono elementi pendenti o in errore.

## Dati locali dopo il submit

Dopo la conferma `SUBMITTED` del server, il client:

- elimina risposte, outbox e questionario in cache;
- chiude la connessione SQLite;
- rimuove il database precedente e i file WAL/SHM;
- ricrea un database minimo con il solo stato finale necessario;
- elimina il token dalla memoria;
- esce dalla modalità fullscreen.

Una compilazione scaduta viene bonificata automaticamente al successivo avvio.

## Stato del progetto

La versione `0.7.6` costituisce la baseline Windows iniziale del client Quaestio. Il software è uno strumento di supporto tecnico-didattico e di ricerca e non un prodotto commerciale destinato alla vendita.
