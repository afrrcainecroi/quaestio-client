# MNGQUEST command API — client v0.5

Il client usa esclusivamente il trasporto WebSocket validato dal server.
Ogni frame text contiene l'envelope canonico `version/type/request_id/cmd/auth/data`.

Comandi implementati:

- `compilazione.start`
- `compilazione.read`
- `compilazione.save-answer`
- `compilazione.submit`

Il `request_id` è generato nel backend Rust e resta invariato durante gli
eventuali retry della stessa operazione.
