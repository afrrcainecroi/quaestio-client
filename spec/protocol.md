# Protocollo client MNGQUEST v0.5

Il backend Rust mantiene una singola connessione WebSocket con subprotocol
`mngquest.v1`. Ogni richiesta e risposta è un frame text UTF-8 contenente
l’envelope canonico JSON.

## Correlazione

Il backend genera un UUID `request_id`, inserisce un canale `oneshot` nella
mappa delle richieste pendenti e invia il frame. Il reader WebSocket estrae
`request_id` dalla risposta e consegna il valore al chiamante corretto.

## Comandi

1. `compilazione.start`
2. `compilazione.read`
3. `compilazione.save-answer`
4. `compilazione.submit`

I retry riutilizzano lo stesso envelope e lo stesso `request_id`. Le operazioni
di salvataggio sono inoltre idempotenti per `client_revision`; il submit è
idempotente sul server.

## Stato locale

Configurazione pubblica, snapshot e risposte vengono salvati con sostituzione
atomica di un checkpoint JSON. Il JWT non viene scritto nel checkpoint.
