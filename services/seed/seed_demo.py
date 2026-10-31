"""Send deterministic, synthetic business events to the HTTP ingestion API.
Run on host: python3 services/seed/seed_demo.py
"""
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

random.seed(42)
api = os.getenv('SEED_API_URL', 'http://localhost:4000')
secret = os.getenv('DEMO_API_KEY', 'replace-with-a-long-random-local-secret')
tenant = os.getenv('TENANT_ID', 'demo-retailer')
now = datetime.now(timezone.utc)
iso = lambda dt: dt.isoformat().replace('+00:00','Z')
# Deterministic IDs make re-running the seed idempotent at the API/outbox layer.
def event_id(name): return str(uuid.uuid5(uuid.NAMESPACE_DNS, f'supplyguard-demo:{tenant}:{name}'))

def send(name, typ, sequence, payload):
    event = dict(version=1,event_id=event_id(name),tenant_id=tenant,sequence=sequence,
                 occurred_at=iso(now),event_type=typ,payload=payload)
    req = Request(api + '/v1/events', data=json.dumps(event).encode(),
                  headers={'content-type':'application/json','x-api-key':secret},method='POST')
    with urlopen(req, timeout=10) as r: r.read()

warehouses=['WH-PUNE','WH-MUMBAI','WH-BENGALURU']
for n in range(36):
    sku=f'SKU-{n+1:03d}'
    wh=warehouses[n%3]
    on_hand=random.choice([0,4,6,12,25,55,90])
    reserved=random.randint(0,max(on_hand,1))
    send(f'inventory:{sku}', 'inventory_snapshot', 1,
         dict(sku=sku,warehouse_id=wh,on_hand=on_hand,reserved=reserved,reorder_point=15))
    send(f'supplier:{sku}', 'supplier_eta_updated', 1,
         dict(sku=sku,warehouse_id=wh,eta_days=random.randint(1,17)))
for n in range(70):
    order=f'ORD-{n+1:04d}'
    ix=n%36
    sku=f'SKU-{ix+1:03d}'
    wh=warehouses[ix%3]
    promised=now+timedelta(hours=random.choice([-72,-48,-24,-4,8,24,48]))
    send(f'order:{order}', 'order_created', 1,
         dict(order_id=order,sku=sku,warehouse_id=wh,
              quantity=random.randint(1,6),promised_at=iso(promised)))
    if n % 4 == 0:
        status='delivered'
    elif n % 3 == 0:
        status='shipped'
    else:
        status='created'
    send(f'shipment:{order}', 'shipment_status', 2,dict(order_id=order,status=status))
print('Seed complete: 36 inventory snapshots, 36 supplier updates, 70 orders, 70 statuses.')
print('Open http://localhost:3000 once publisher/worker have processed the queue.')
