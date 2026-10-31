"""SupplyGuard Lakeflow Spark Declarative Pipeline.

Run inside a Unity Catalog-enabled Databricks workspace. `spark` is supplied by Databricks.
This pipeline computes historical/latest business state; dashboards evaluate SLA against now().
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType)

payload_schema = StructType([
    StructField('sku', StringType()),
    StructField('warehouse_id', StringType()),
    StructField('on_hand', IntegerType()),
    StructField('reserved', IntegerType()),
    StructField('reorder_point', IntegerType()),
    StructField('order_id', StringType()),
    StructField('quantity', IntegerType()),
    StructField('promised_at', StringType()),
    StructField('status', StringType()),
    StructField('eta_days', IntegerType()),
])
event_schema = StructType([
    StructField('version', IntegerType()),
    StructField('event_id', StringType()),
    StructField('tenant_id', StringType()),
    StructField('sequence', LongType()),
    StructField('occurred_at', StringType()),
    StructField('event_type', StringType()),
    StructField('payload', payload_schema),
])

@dp.table(comment='Immutable Kafka event landing zone; invalid JSON retained for audit and repair')
def bronze_events():
    return (spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', spark.conf.get('supplyguard.kafka_bootstrap'))
        .option('subscribe', spark.conf.get('supplyguard.kafka_topic', 'supplyguard.events.v1'))
        .option('startingOffsets', 'earliest')
        .load()
        .select(F.col('value').cast('string').alias('raw_json'),
                F.col('topic').alias('kafka_topic'),
                F.col('partition').alias('kafka_partition'),
                F.col('offset').alias('kafka_offset'),
                F.col('timestamp').alias('kafka_timestamp'),
                F.current_timestamp().alias('ingested_at')))

@dp.table(comment='Validated, typed, event-id-deduplicated stream')
@dp.expect_all_or_drop({
    'valid_envelope': "version = 1 AND event_id IS NOT NULL AND tenant_id IS NOT NULL AND sequence >= 0 AND event_time IS NOT NULL",
    'valid_type': "event_type IN ('inventory_snapshot','order_created','shipment_status','supplier_eta_updated')",
    'valid_payload': """
        CASE
          WHEN event_type='inventory_snapshot' THEN
            sku IS NOT NULL AND warehouse_id IS NOT NULL AND on_hand >= 0
            AND reserved >= 0 AND reorder_point >= 0
          WHEN event_type='order_created' THEN
            order_id IS NOT NULL AND sku IS NOT NULL AND warehouse_id IS NOT NULL
            AND quantity > 0 AND promised_at IS NOT NULL
          WHEN event_type='shipment_status' THEN
            order_id IS NOT NULL AND status IN ('created','shipped','delivered','cancelled')
          WHEN event_type='supplier_eta_updated' THEN
            sku IS NOT NULL AND warehouse_id IS NOT NULL AND eta_days BETWEEN 0 AND 365
          ELSE false
        END
    """,
})
def silver_events():
    parsed = (spark.readStream.table('bronze_events')
        .select(F.from_json('raw_json', event_schema).alias('e'),
                'kafka_partition','kafka_offset','ingested_at'))
    return (parsed.select('e.*','kafka_partition','kafka_offset','ingested_at')
        .select('*','payload.*')
        .drop('payload')
        .withColumn('event_time', F.to_timestamp('occurred_at'))
        .withColumn('promised_at', F.to_timestamp('promised_at'))
        .withWatermark('event_time', '7 days')
        .dropDuplicatesWithinWatermark(['tenant_id','event_id']))

@dp.materialized_view(comment='Rejected parsing/validation candidates for data quality triage')
def quarantine_candidates():
    d = spark.read.table('bronze_events')
    e = F.from_json('raw_json', event_schema)
    return d.filter(e.isNull() | e['event_id'].isNull() | e['tenant_id'].isNull())

# Gold tables resolve multiple events for an entity using its monotonic sequence.
# Crucial: sequence numbers are per (tenant, entity, event_type), NOT globally across Kafka.
@dp.materialized_view(comment='Current inventory availability and supplier lead time by SKU and warehouse')
def gold_inventory_risks():
    return spark.sql('''
      WITH inventory_ranked AS (
        SELECT *, row_number() OVER (
          PARTITION BY tenant_id,sku,warehouse_id
          ORDER BY sequence DESC,event_time DESC,event_id DESC) AS rn
        FROM silver_events WHERE event_type='inventory_snapshot'
      ), suppliers_ranked AS (
        SELECT tenant_id,sku,warehouse_id,eta_days,
          row_number() OVER (PARTITION BY tenant_id,sku,warehouse_id
            ORDER BY sequence DESC,event_time DESC,event_id DESC) AS rn
        FROM silver_events WHERE event_type='supplier_eta_updated'
      )
      SELECT i.tenant_id,i.sku,i.warehouse_id,i.on_hand,i.reserved,i.reorder_point,
        greatest(i.on_hand-i.reserved,0) AS available_units,
        s.eta_days AS supplier_eta_days,
        i.event_time AS updated_at
      FROM inventory_ranked i
      LEFT JOIN suppliers_ranked s
        ON i.tenant_id=s.tenant_id AND i.sku=s.sku AND i.warehouse_id=s.warehouse_id AND s.rn=1
      WHERE i.rn=1
    ''')

@dp.materialized_view(comment='Current order + last shipment status; overdue evaluated by serving API')
def gold_order_risks():
    return spark.sql('''
      WITH orders_ranked AS (
        SELECT *, row_number() OVER (PARTITION BY tenant_id,order_id
          ORDER BY sequence DESC,event_time DESC,event_id DESC) AS rn
        FROM silver_events WHERE event_type='order_created'
      ), shipping_ranked AS (
        SELECT tenant_id,order_id,status,
          row_number() OVER (PARTITION BY tenant_id,order_id
            ORDER BY sequence DESC,event_time DESC,event_id DESC) AS rn
        FROM silver_events WHERE event_type='shipment_status'
      )
      SELECT o.tenant_id,o.order_id,o.sku,o.warehouse_id,o.quantity,o.promised_at,
        coalesce(s.status,'created') AS status,o.event_time AS updated_at
      FROM orders_ranked o LEFT JOIN shipping_ranked s
        ON o.tenant_id=s.tenant_id AND o.order_id=s.order_id AND s.rn=1
      WHERE o.rn=1
    ''')

@dp.materialized_view(comment='Tenant-level operational inventory KPI snapshot')
def gold_inventory_kpis():
    return spark.sql('''
      SELECT tenant_id,
        count(*) AS tracked_skus,
        sum(CASE WHEN available_units=0 THEN 1 ELSE 0 END) AS stockouts,
        sum(CASE WHEN available_units>0 AND available_units<reorder_point THEN 1 ELSE 0 END) AS low_stock
      FROM gold_inventory_risks GROUP BY tenant_id
    ''')

@dp.materialized_view(comment='Explainable seven-day order run-rate vs supplier replenishment ETA')
def gold_replenishment_signals():
    # This is a transparent statistical baseline, NOT a trained ML forecast.
    # Suitable for an initial operations pilot; graduate to MLflow models only
    # once you have enough demand history and measured forecast error.
    return spark.sql('''
      WITH recent_orders AS (
        SELECT tenant_id,sku,warehouse_id,
          sum(quantity) / CAST(7.0 AS DOUBLE) AS avg_daily_demand
        FROM silver_events
        WHERE event_type='order_created'
          AND event_time >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY tenant_id,sku,warehouse_id
      )
      SELECT i.tenant_id,i.sku,i.warehouse_id,i.available_units,
        i.supplier_eta_days,
        coalesce(d.avg_daily_demand,0.0) AS avg_daily_demand,
        CASE WHEN d.avg_daily_demand > 0
          THEN round(i.available_units / d.avg_daily_demand,1)
          ELSE CAST(NULL AS DOUBLE) END AS days_of_cover,
        CASE
          WHEN i.available_units = 0 THEN 'CRITICAL_STOCKOUT'
          WHEN d.avg_daily_demand > 0 AND i.supplier_eta_days IS NOT NULL
            AND i.available_units / d.avg_daily_demand < i.supplier_eta_days
            THEN 'REPLENISHMENT_GAP'
          ELSE 'MONITOR'
        END AS action_priority
      FROM gold_inventory_risks i
      LEFT JOIN recent_orders d
        ON i.tenant_id=d.tenant_id AND i.sku=d.sku AND i.warehouse_id=d.warehouse_id
    ''')
