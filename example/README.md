# Local chat proof of concept

This Docker Compose example starts PostgreSQL, RabbitMQ, the `freestate-pi` worker, and a small HTTP backend/page. The browser only reaches the backend on **127.0.0.1:8081**; it never accesses the broker, database, or Pi auth file directly. The backend submits durable AMQP requests, replays text from the RabbitMQ stream over SSE, and saves final results to its own Docker volume before acknowledging them.

## Start

The requested copy of the local Pi credentials is at `example/auth.json`. It is ignored by Git and excluded from the worker image build context. If you're running this elsewhere, supply your _own_ Pi auth file at that path; OAuth refresh may update the copy, not the original.

From this directory, start the example without setting any password variables:

```bash
cd example
# Optional if your auth supports a different Pi provider/model:
# export EXAMPLE_MODEL='provider/model'
docker compose up --build -d
```

The Compose file includes generated **public, demo-only** PostgreSQL and RabbitMQ password defaults. These are not private credentials: the database and broker are only on the internal Compose network, but this setup must never be used as a production deployment. To override them, set `EXAMPLE_DB_PASSWORD` and `EXAMPLE_AMQP_PASSWORD` before the **first** start and keep the same values for that demo's volumes.

Open <http://127.0.0.1:8081>. Submit a prompt and watch text appear. Follow up with another prompt in the same conversation: the browser reuses its `threadId`, so Pi should remember the earlier turn. Refresh during a run to replay earlier text and continue live streaming. Conversation history and the active run ID are kept in browser local storage for this demo. On completion, the final result replaces the partial text. Use **New conversation** to start a separate thread.

This demo's backend stores completed results in the `web-results` Docker volume so a page refresh or backend restart can reload the last final answer. The worker stores per-thread workspace files in the `workspace` volume. RabbitMQ stream text is retained for up to 1 day or 1 GB, whichever limit removes it first; after expiry only the final result is available. The shared event stream is scanned from its first retained event and filtered by run ID—appropriate for a local demonstration, not a high-traffic deployment.

To view logs or stop the example:

```bash
docker compose logs -f runner web
docker compose down
```

`docker compose down` keeps the demo volumes. If you previously started with custom passwords, keep using those values; changing passwords does not update existing PostgreSQL/RabbitMQ volumes. You can explicitly delete **only the demo's data** with `docker compose down -v` before starting fresh, but this erases its history.

**Security:** This page has no login or rate limiting; Pi can execute shell commands and use the mounted provider credentials. It is for local development only. Never publish its port, broker, or auth file to the internet; do not put `auth.json` or passwords in Git or an image. A real model response depends on a valid Pi auth file and a model accessible with it.
