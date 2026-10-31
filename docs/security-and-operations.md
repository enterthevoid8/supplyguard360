# Operational readiness and incident runbooks

## Security model

Local authentication is deliberately restricted to one configured tenant and a single `x-api-key`. Do **not** expose it publicly as a production identity solution. For production: authenticate end-users with OIDC (e.g. Entra ID or Cognito), validate JWT audience/issuer/scopes at API Gateway/Node, bind tenant from identity claims rather than request payload, use per-role DB grants, TLS for all hops, workload identities/service principals, encrypted storage, AWS Secrets Manager/UC service credentials and secret rotation. Apply CORS policy and CSRF controls appropriately. Secure the producer outbox, TLS Kafka brokers, network routes and UC catalog permissions. Avoid personal data in event payloads.

## Instrumentation to add

- **Kafka:** consumer lag, failed messages, rebalance rate, topic throughput, retries, ISR, disk retention.
- **Outbox:** oldest unpublished age, count waiting/unleased, repeated publish attempts, broker availability.
- **Lakeflow:** refresh success/fail, last completed update, dropped expectation counts, streaming state size, late event counts, records Bronze/Silver/Gold, data freshness.
- **Serving:** age of most recent `serving_snapshots`, export duration, Postgres transactions/locks, p95 query latency, row counts, failures.
- **Security:** suspicious API authentication failures, forbidden tenant access, secret access, UC audit logs.

## Incident A: inventory dashboard looks stale

1. Verify API health; inspect last successful serving snapshot time.
2. Check outbox oldest unpublished age and Kafka producer errors.
3. Verify Kafka topic offsets and Databricks Lakeflow job state.
4. Confirm Gold table freshness and expectation-drop rates; do not force a full refresh blindly.
5. Verify exporter job and PostgreSQL transaction promotion. Recover publisher or exporter, then confirm synthetic known-record values. Incident resolved after freshness SLO recovers.

## Incident B: duplicate or out-of-order inventory snapshot

1. Fetch same `tenant_id`, SKU, warehouse, sequence from event log and Bronze/Silver.
2. Ensure unique `event_id` and monotonic `sequence` from upstream source.
3. Gold selects largest sequence, with deterministic event-time/id tie breaking. Local worker updates state only when incoming sequence is greater.
4. For duplicate *sequence* with different values, pause source and audit producer bug. Such conflicts are a data-contract violation and should alert before customer-facing decisions.

## Incident C: export fails after first staging table

1. Do not execute manual serving table deletes. The previous active snapshot still serves.
2. Inspect staging rows by `snapshot_id`, then rerun exporter with a fresh UUID.
3. Restore DB from PITR if necessary and replay from governed Gold, not from customer app request logs.
4. Clean orphan staging snapshots with a controlled retention job after confirming no exporter is active.

## Delivery guarantees

- API 202: event durably persisted in Postgres, **not** proof it reached Kafka or Gold.
- Kafka ack: event persisted according to broker replication configuration, not proof it reached Databricks.
- Local preview consumer commit: transaction applied, offset committed after DB commit.
- Lakeflow Gold: refresh completed; no claim of 1-second streaming latency in scheduled mode.
- Serving: export snapshot promoted transactionally (both tables, or neither).

## Release gates

- Unit/schema test; cross-source state and ordering cases; integration test with actual Kafka and PostgreSQL; Spark integration test on designated workspace; vulnerability scan; generated synthetic load suite; private connectivity; DLQ alerts; backup/restore; OIDC; compliance/data retention; SLOs verified under load.
