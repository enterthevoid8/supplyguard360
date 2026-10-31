# How to present SupplyGuard 360 in a senior engineer / Databricks interview

## 60-second explanation

> “I built a reference platform that helps retailers identify stockouts and overdue orders across warehouses. I used a Node.js API with a transactional outbox so accepted business events couldn't silently disappear before Kafka publish. Databricks Lakeflow ingests raw Kafka events into Bronze, validates and deduplicates them into Silver, then resolves latest inventory and order state into Gold, including a seven-day demand-run-rate replenishment gap signal. I built a PostgreSQL serving projection and a Next.js operations UI. I designed around replay, out-of-order versions, data quality, tenant boundaries and atomic serving promotion. The local demo is fully synthetic, and the cloud Databricks deployment requires workspace-specific configuration and testing.”

## What recruiters can ask you

1. Why choose snapshot events, not inventory deltas? Explain non-idempotent subtraction and replay safety.
2. What guarantees does a transactional outbox provide? Postgres durability then at-least-once publish; not end-to-end exactly-once.
3. What happens if events arrive in the wrong order? Stable key partition, sequence-aware current state and separate order/shipment state.
4. What does Bronze retain? Raw payload and Kafka provenance, including parsing-invalid events.
5. What does Lakeflow expectation-drop mean? Invalid records are excluded from Silver and counters are surfaced; audit/quarantine needed for semantic failures.
6. Why watermark deduplication? Bounded state; duplicate outside retention may exist, and Gold version logic still resolves state.
7. When use a materialized view vs streaming table? Stateful streaming ETL for incremental arrival; derived current-state analytics as managed MVs.
8. Why export into Postgres? Low-latency application endpoints decoupled from Spark/workspace demand; additional cost and consistency boundaries.
9. What is Unity Catalog for? Central data access policy, identity, lineage, metadata and governed datasets.
10. How do you prove value? Instrument detection lag, exception closure, late delivery outcome and business holdout metrics.

## Databricks Data Engineer Associate relevance

- Ingestion and Structured Streaming; Kafka source semantics
- Delta / Bronze-Silver-Gold modeling
- Managed Lakeflow pipelines; materialized views vs streaming tables
- Data-quality expectations and deduplication
- SQL transforms and window-function latest-state reconciliation
- Jobs scheduling and bundle-based CI/CD
- Unity Catalog governance and troubleshooting

## Honest resume line after you run/test the relevant scope

**Developed a supply-chain risk intelligence reference platform** with a Kafka-backed Node.js transactional outbox, Databricks Lakeflow Medallion pipelines, PySpark data-quality and latest-state reconciliation, PostgreSQL serving snapshots and a Next.js analytics dashboard. Designed replay/idempotency, deployment automation and operational controls. (*Synthetic workload; do not invent production scale or impact metrics.*)

## Phase 2: what would genuinely warrant MLflow?

Add 12+ months of anonymized daily SKU demand, promotions, holidays and inbound supplier receipts. Compare seasonal naive forecasts against gradient-boosted quantile models; use time-based cross-validation and weighted absolute percentage error, probabilistic coverage intervals, model registry, batch inference, drift checks and operator feedback. Do **not** describe the current run-rate baseline as a trained predictive model.
