# AWS + Databricks production deployment blueprint

## Resource layout

- **Public edge:** CloudFront / WAF / API Gateway or ALB, OIDC auth, rate limiting.
- **App:** Next.js + Fastify containers deployed on ECS Fargate (separate task services).
- **Streaming:** Amazon MSK private brokers (or managed Confluent Cloud with private networking). 3 availability zones, retention/replication strategy and monitoring.
- **Transactional/serving:** Amazon RDS for PostgreSQL Multi-AZ. Operational outbox tables and serving tables ideally segregated by database/schema/privileges; the local demo co-locates them for simplicity.
- **Lakehouse:** Unity Catalog enabled Databricks on AWS with Lakeflow triggered refresh, managed Delta Bronze/Silver/Gold. Private network access to Kafka. Managed Databricks storage is not exposed as public S3 buckets.
- **Export:** Lakeflow success -> Databricks Spark Python task -> PostgreSQL staging -> atomic promotion transaction.
- **Operations:** Databricks pipeline event log, CloudWatch + OpenTelemetry, Kafka lag alerts, outbox age, failed refresh count, snapshot age.

## Cloud rollout in six gates

1. **Networking/IAM:** VPC endpoints or private connectivity, UC grants for a service principal, RDS private subnets. Choose compute compatible with broker networking. Configure TLS and credentials via secret manager or Databricks secret facilities.
2. **Source contracts:** Establish warehouse/OMS/carrier event owners, guarantees for per-entity sequence and time, retention, backfill horizon. Set schema registry/versioning and data classification.
3. **Ingest:** Deploy API / publisher. Test outbox relay recovery by stopping publisher before POST; resume and verify exactly one downstream effect from duplicate Kafka sends.
4. **Lakeflow:** Deploy `databricks.yml`; validate expectation counts and quarantine. Verify Gold against sample SQL. Test late, duplicate, and out-of-order sequences, plus replays beyond the 7-day watermark.
5. **Serving:** Execute `export_serving.py` manually against a staging RDS first. Set `PG_JDBC_URL`, `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `UC_CATALOG`, `UC_SCHEMA`, `PG_SECRET_SCOPE`. Create Databricks secret keys `pg_user` and `pg_password`. Require the Job to begin ONLY after successful Lakeflow refresh. Grant staging writes / serving updates to an export-only role, read-only to the API role. The atomic transaction deletes/reloads the two serving tables together. For a very large dataset, change to versioned snapshots with a pointer switch rather than long-lived deletes.
6. **Go-live:** Synthetic load test, DLQ handling, observability/alerts, database PITR restore drill, Kafka replay drill, tenant separation, security scan, change approval.

## Example downstream job task (add to `resources/pipeline.yml` after credentials and network exist)

```yaml
- task_key: publish_postgres_serving
  depends_on:
    - task_key: refresh_lakehouse
  spark_python_task:
    python_file: ../databricks/export_serving.py
  new_cluster:
    spark_version: 16.4.x-scala2.12  # validate available runtime in region
    node_type_id: i3.xlarge         # change as appropriate for AWS region/policy
    num_workers: 2
    spark_env_vars:
      UC_CATALOG: main
      UC_SCHEMA: supplyguard_prod
      PG_JDBC_URL: jdbc:postgresql://PRIVATE-RDS:5432/supplyguard?sslmode=require
      PG_HOST: PRIVATE-RDS
      PG_PORT: '5432'
      PG_DATABASE: supplyguard
      PG_SECRET_SCOPE: supplyguard
  libraries:
    - pypi:
        package: psycopg2-binary==2.9.10
```

**This is an adaptation example, not automatically deployed.** Job compute, cluster policies, Spark/Databricks versions, secret syntax and SDK invocation must be validated against your actual workspace. The Spark Python task should initialize `SparkSession` and `DBUtils` itself.

## Correctness caveats and backlog

- Kafka delivery is at least once. The project guarantees idempotent application on the *local state* via a Postgres primary key, not globally exactly-once side effects.
- 7-day watermark bounds Spark state but can allow extremely late replay beyond watermark. Gold latest-sequence resolution prevents older values overwriting newer state if both are retained; build a separate historic backfill runbook and dedup policy for long retention.
- The local worker is not a replacement for Databricks. Keep it **disabled in cloud**, otherwise both writers would compete for serving tables.
- Shipment state is separate from order creation, tolerating status arriving before order creation.
- A materialized view over potentially large event history might be expensive. Optimize with AUTO CDC keyed state tables, liquid clustering/partition strategy where justified, and cost measurements.
- The example's hourly/daily capacity is unknown until measured on the intended Kafka/MSK, Databricks compute, and RDS plan.
- Prefer time-partitioned Gold snapshot + atomic metadata pointer for large volumes; the supplied DELETE/INSERT transaction is pedagogically transparent but not a scale-out architecture.
- Add a true dead-letter stream and quarantine for malformed business payloads (Bronze retains invalid raw JSON and a basic parse quarantine exists now).
- Never load customer names, emails, addresses or payments into this demo stream. Only artificial IDs and stock data are necessary.
