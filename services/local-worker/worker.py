"""Local deterministic preview worker. Production analytics run in Databricks, NOT here."""
import json
import os
import time
from confluent_kafka import Consumer, KafkaError
import psycopg

DB_URL = os.environ['DATABASE_URL']
TOPIC = os.getenv('KAFKA_TOPIC', 'supplyguard.events.v1')

def refresh_inventory(cur, tenant, sku, wh):
    cur.execute('''INSERT INTO inventory_risks
      (tenant_id,sku,warehouse_id,on_hand,reserved,reorder_point,available_units,supplier_eta_days,updated_at)
      SELECT i.tenant_id,i.sku,i.warehouse_id,i.on_hand,i.reserved,i.reorder_point,
             GREATEST(i.on_hand-i.reserved,0),s.eta_days,now()
      FROM inventory_state i LEFT JOIN supplier_state s
        ON s.tenant_id=i.tenant_id AND s.sku=i.sku AND s.warehouse_id=i.warehouse_id
      WHERE i.tenant_id=%s AND i.sku=%s AND i.warehouse_id=%s
      ON CONFLICT(tenant_id,sku,warehouse_id) DO UPDATE SET
       on_hand=EXCLUDED.on_hand,reserved=EXCLUDED.reserved,
       reorder_point=EXCLUDED.reorder_point, available_units=EXCLUDED.available_units,
       supplier_eta_days=EXCLUDED.supplier_eta_days,updated_at=EXCLUDED.updated_at''',
       (tenant,sku,wh))

def refresh_order(cur, tenant, order_id):
    cur.execute('''INSERT INTO order_risks
      (tenant_id,order_id,sku,warehouse_id,quantity,promised_at,status,updated_at)
      SELECT o.tenant_id,o.order_id,o.sku,o.warehouse_id,o.quantity,
             o.promised_at,COALESCE(s.status,'created'),now()
      FROM orders_state o LEFT JOIN shipment_state s
        ON s.tenant_id=o.tenant_id AND s.order_id=o.order_id
      WHERE o.tenant_id=%s AND o.order_id=%s
      ON CONFLICT(tenant_id,order_id) DO UPDATE SET
       sku=EXCLUDED.sku,warehouse_id=EXCLUDED.warehouse_id,
       quantity=EXCLUDED.quantity,promised_at=EXCLUDED.promised_at,
       status=EXCLUDED.status,updated_at=EXCLUDED.updated_at''', (tenant,order_id))

def process(conn, event):
    p = event['payload']
    typ = event['event_type']
    t = event['tenant_id']
    s = event['sequence']
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute('INSERT INTO processed_events(event_id) VALUES (%s) ON CONFLICT DO NOTHING RETURNING event_id',
                        (event['event_id'],))
            if not cur.fetchone():
                return False
            if typ == 'inventory_snapshot':
                cur.execute('''INSERT INTO inventory_state
                  (tenant_id,sku,warehouse_id,on_hand,reserved,reorder_point,sequence,occurred_at)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                  ON CONFLICT(tenant_id,sku,warehouse_id) DO UPDATE SET
                   on_hand=EXCLUDED.on_hand,reserved=EXCLUDED.reserved,
                   reorder_point=EXCLUDED.reorder_point,sequence=EXCLUDED.sequence,
                   occurred_at=EXCLUDED.occurred_at
                  WHERE inventory_state.sequence<EXCLUDED.sequence''',
                  (t,p['sku'],p['warehouse_id'],p['on_hand'],p['reserved'],p['reorder_point'],s,event['occurred_at']))
                refresh_inventory(cur,t,p['sku'],p['warehouse_id'])
            elif typ == 'supplier_eta_updated':
                cur.execute('''INSERT INTO supplier_state(tenant_id,sku,warehouse_id,eta_days,sequence)
                  VALUES (%s,%s,%s,%s,%s)
                  ON CONFLICT(tenant_id,sku,warehouse_id) DO UPDATE SET
                   eta_days=EXCLUDED.eta_days,sequence=EXCLUDED.sequence
                  WHERE supplier_state.sequence<EXCLUDED.sequence''',
                  (t,p['sku'],p['warehouse_id'],p['eta_days'],s))
                refresh_inventory(cur,t,p['sku'],p['warehouse_id'])
            elif typ == 'order_created':
                cur.execute('''INSERT INTO orders_state
                  (tenant_id,order_id,sku,warehouse_id,quantity,promised_at,sequence)
                  VALUES (%s,%s,%s,%s,%s,%s,%s)
                  ON CONFLICT(tenant_id,order_id) DO UPDATE SET
                   sku=EXCLUDED.sku,warehouse_id=EXCLUDED.warehouse_id,
                   quantity=EXCLUDED.quantity,promised_at=EXCLUDED.promised_at,
                   sequence=EXCLUDED.sequence WHERE orders_state.sequence<EXCLUDED.sequence''',
                  (t,p['order_id'],p['sku'],p['warehouse_id'],p['quantity'],p['promised_at'],s))
                refresh_order(cur,t,p['order_id'])
            elif typ == 'shipment_status':
                cur.execute('''INSERT INTO shipment_state(tenant_id,order_id,status,sequence)
                  VALUES (%s,%s,%s,%s)
                  ON CONFLICT(tenant_id,order_id) DO UPDATE SET
                   status=EXCLUDED.status,sequence=EXCLUDED.sequence
                  WHERE shipment_state.sequence<EXCLUDED.sequence''',
                  (t,p['order_id'],p['status'],s))
                refresh_order(cur,t,p['order_id'])
            else:
                raise ValueError('Unsupported type: '+typ)
    return True

def main():
    c = Consumer({'bootstrap.servers':os.environ['KAFKA_BOOTSTRAP'],
                  'group.id':'supplyguard-local-preview-v1',
                  'auto.offset.reset':'earliest','enable.auto.commit':False})
    c.subscribe([TOPIC])
    conn = None
    try:
        while True:
            if conn is None or conn.closed:
                try: conn = psycopg.connect(DB_URL, autocommit=True)
                except psycopg.OperationalError: time.sleep(2); continue
            msg = c.poll(1)
            if msg is None: continue
            if msg.error():
                print('Kafka poll error:', msg.error(), flush=True)
                time.sleep(1)
                continue
            try:
                event = json.loads(msg.value())
                process(conn,event)
                c.commit(message=msg, asynchronous=False)
            except Exception as exc:
                print('Worker failed; message will be retried:', exc, flush=True)
                # Do not commit failed messages: re-read from last committed offset.
                time.sleep(2)
                raise  # restart, consumer resumes from committed offset
    finally:
        c.close()
        if conn: conn.close()

if __name__ == '__main__': main()
