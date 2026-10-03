# dlq-triage

A small service for working through an SQS dead-letter queue. It reads the dead-lettered messages, groups them by the failure that put them there, and lets an operator replay one group at a time, with a dry run first, a rate limit, and an audit trail. It has a GraphQL API and a web dashboard, both served from a single Go binary.

I built it because a dead-letter queue with a few thousand messages is hard to act on: the messages look alike, the useful question is "how many distinct failures are in here", and a blind "redrive all" can re-trigger the same problem at full speed.

## What it does

- **Groups failures.** Each message gets a fingerprint from a normalised error signature (numbers, UUIDs and similar are replaced with placeholders) plus the structural shape of its JSON payload (field names and types, never values). 24 messages with 3 distinct causes show up as 3 rows.
- **Replays per group.** A replay moves one group's messages to a destination queue. It supports a dry run, a messages-per-second limit, a maximum count, cancellation, and refuses to run twice at once for the same group.
- **Keeps an audit log** of who started, finished or cancelled what.
- **Optional AI summaries.** If you configure an OpenAI-compatible endpoint, an operator can ask for a plain-language description of a group. The model sees only the error signature and payload shape, never message bodies or attribute values. Off by default.
- **Two roles.** `viewer` can look but sees no message bodies or attribute values. `operator` can replay, cancel and summarise.
- **Ops basics.** `/healthz`, `/readyz`, and Prometheus metrics at `/metrics`.

## Running it

```sh
cd web && npm ci && npm run build && cd ..
go build -o dlq-triage ./cmd/dlq-triage

export DLQ_TRIAGE_OPERATOR_TOKEN=...   # at least 16 characters
export DLQ_TRIAGE_VIEWER_TOKEN=...
./dlq-triage -check -config examples/config.example.json   # validate only
./dlq-triage -config examples/config.example.json
```

AWS credentials come from the standard AWS SDK chain. See [examples/config.example.json](examples/config.example.json) for every option. Open `http://localhost:8080` and sign in with a token.

To try it without AWS, run an SQS emulator such as `moto_server -p 5055`, then `python scripts/seed_local.py` and use [examples/config.local.json](examples/config.local.json).

## Design notes

- **Ingest is non-destructive.** Scanning receives messages with a short visibility timeout and never deletes them. Messages are stored by id, so rescanning is idempotent.
- **Replay order of operations:** send to the destination, mark replayed in the local store, then delete from the dead-letter queue. A crash between steps can cause a duplicate send but never a lost message.
- **Types are preserved.** `String`, `Number` and `Binary` message attributes are re-sent with their original type. Replayed messages gain a marker attribute so consumers can tell them apart.
- **Secrets.** Config values written as `env:NAME` are read from the environment. Tokens are compared in constant time. Unknown config fields are rejected.
- **Restarts.** A job left "running" by a crash or restart is marked failed on startup, so it doesn't block that group forever.

## Limitations

Please read these before relying on it.

- **At-least-once delivery.** A replayed message can occasionally be delivered more than once. The consuming service must be idempotent. On FIFO queues the destination's deduplication also applies.
- **Scanning changes receive counts.** SQS has no read-only peek, so every scan increments `ApproximateReceiveCount` on messages in the dead-letter queue. If you use a `maxReceiveCount` on the DLQ itself, or alarm on receive counts, set the scan interval accordingly.
- **SQS and SQLite only.** One queue backend and one storage backend. SQLite means a single instance; it is not built for running several replicas.
- **Authentication is static bearer tokens.** There is no SSO, token rotation or per-user identity beyond the token's name. Run it behind your own network controls and TLS; it does not terminate TLS itself.
- **Fingerprinting is heuristic.** Two different problems with similar error text and payload shape will be grouped together; one problem with highly variable error text may be split. Groups are a triage aid, not proof of a common cause.
- **AI summaries can be wrong.** They are labelled as machine-generated in the UI and should be checked.
- **Tested against an emulator, not AWS.** The integration and end-to-end tests run against [moto](https://github.com/getmoto/moto). I have not yet run it against real SQS, so behaviours that differ between the two (timing of visibility, FIFO details) are unverified.
- The Dockerfile has not been built yet in the environment this was developed in. CI runs the tests with the race detector; I could not run it locally.

## Development

```sh
go vet ./... && go test -count=1 ./...

# integration tests, with an SQS emulator on :5055
DLQ_TRIAGE_SQS_ENDPOINT=http://127.0.0.1:5055 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  go test -tags integration ./internal/queue/... ./internal/e2e/...

# dashboard with hot reload (proxies /graphql to a local service on :8099)
cd web && npm run dev
```

Layout: `internal/fingerprint` (grouping), `internal/store` (SQLite), `internal/queue` (SQS adapter and in-memory fake), `internal/ingest`, `internal/replay`, `internal/api` (GraphQL, auth, metrics), `internal/ai`, `web/` (React and TypeScript dashboard).

## License

MIT
