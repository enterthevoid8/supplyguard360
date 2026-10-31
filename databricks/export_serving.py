"""Optional production serving export: Databricks Gold -> PostgreSQL.

Run as a Databricks Python/Spark job after a SUCCESSFUL Lakeflow refresh.
Required job environment: PG_JDBC_URL=jdbc:postgresql://host:5432/db?sslmode=require,
PG_HOST, PG_PORT, PG_DATABASE, PG_SECRET_SCOPE; UC_CATALOG, UC_SCHEMA.
Databricks secrets: pg_user, pg_password. Add `psycopg2-binary` to job environment.
Network routing, TLS and roles must be configured out of band. Avoid using public DB IPs.
"""
import os
import uuid
import psycopg2
from pyspark.sql import functions as F, SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

catalog = os.environ['UC_CATALOG']
schema = os.environ['UC_SCHEMA']
scope = os.environ.get('PG_SECRET_SCOPE', 'supplyguard')
username = dbutils.secrets.get(scope=scope, key='pg_user')
password = dbutils.secrets.get(scope=scope, key='pg_password')
snapshot = str(uuid.uuid4())
jdbc_url = os.environ['PG_JDBC_URL']

# Load both tables into UUID-scoped staging. Any failure BEFORE promotion leaves the
# currently served risk snapshot untouched. Restrict Spark write concurrency if required.
for gold_table, stage_table in [
    ('gold_inventory_risks','stage_inventory_risks'),
    ('gold_order_risks','stage_order_risks'),
]:
    (spark.table(f'{catalog}.{schema}.{gold_table}')
        .withColumn('snapshot_id', F.lit(snapshot))
        .write.format('jdbc').mode('append')
        .option('url', jdbc_url).option('dbtable', stage_table)
        .option('user', username).option('password', password)
        .option('driver', 'org.postgresql.Driver')
        .option('batchsize', '1000')
        .save())

# All-or-nothing visibility to API readers. A completed export has identical
# snapshot semantics for inventory and orders; API transactions use READ COMMITTED.
conn = psycopg2.connect(host=os.environ['PG_HOST'], port=int(os.getenv('PG_PORT', '5432')),
                        dbname=os.environ['PG_DATABASE'], user=username,
                        password=password, sslmode='require')
try:
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(38299125)')
            cur.execute('DELETE FROM inventory_risks')
            cur.execute('''INSERT INTO inventory_risks
              (tenant_id,sku,warehouse_id,on_hand,reserved,reorder_point,
               available_units,supplier_eta_days,updated_at)
              SELECT tenant_id,sku,warehouse_id,on_hand,reserved,reorder_point,
                     available_units,supplier_eta_days,updated_at
              FROM stage_inventory_risks WHERE snapshot_id=%s''', (snapshot,))
            cur.execute('DELETE FROM order_risks')
            cur.execute('''INSERT INTO order_risks
              (tenant_id,order_id,sku,warehouse_id,quantity,promised_at,status,updated_at)
              SELECT tenant_id,order_id,sku,warehouse_id,quantity,promised_at,status,updated_at
              FROM stage_order_risks WHERE snapshot_id=%s''', (snapshot,))
            cur.execute('INSERT INTO serving_snapshots(snapshot_id) VALUES (%s)', (snapshot,))
            cur.execute('DELETE FROM stage_inventory_risks WHERE snapshot_id=%s', (snapshot,))
            cur.execute('DELETE FROM stage_order_risks WHERE snapshot_id=%s', (snapshot,))
finally:
    conn.close()
print('Promoted serving snapshot:', snapshot)
