"""External local smoke check after demo seeding: HTTP API + resulting projections."""
import json
import os
import sys
import time
from urllib.request import Request, urlopen

base = os.getenv('SEED_API_URL', 'http://localhost:4000')
secret = os.getenv('DEMO_API_KEY', 'replace-with-a-long-random-local-secret')
def request(path):
    with urlopen(Request(base+path, headers={'x-api-key': secret}),timeout=8) as res:
        return json.load(res)

for attempt in range(35):
    try:
        status = request('/v1/kpis')
        if status['total_orders'] >= 70 and status['tracked_skus'] >= 36:
            inv = request('/v1/inventory-risks')['items']
            orders = request('/v1/order-risks')['items']
            assert len(inv) >= 36 and len(orders) >= 70
            assert all(x['available_units'] >= 0 for x in inv)
            print('PASS: seeded 70 orders / 36 inventory records and risk API is consistent.')
            print(json.dumps(status,indent=2))
            sys.exit(0)
        if attempt % 5 == 0: print('Awaiting outbox + Kafka + local worker:', status)
    except Exception as exc:
        if attempt % 5 == 0: print('Waiting for API...', exc)
    time.sleep(2)
print('FAIL: events not yet processed; inspect docker compose logs api publisher local-worker',file=sys.stderr)
sys.exit(1)
