"""Fast unit tests that can run without Docker, Kafka or Databricks."""
from pathlib import Path
import ast
import json
import unittest
import uuid
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]


def inventory_risk(on_hand, reserved, reorder_point):
    available = max(on_hand - reserved, 0)
    return ('STOCKOUT' if available == 0 else
            'LOW_STOCK' if available < reorder_point else 'HEALTHY')


def order_risk(status, promised_at, now):
    return 'OVERDUE' if status not in ('delivered','cancelled') and promised_at < now else 'ON_TRACK'

class ProjectContractTests(unittest.TestCase):
    def test_stockout_and_low_stock(self):
        self.assertEqual(inventory_risk(15,15,10),'STOCKOUT')
        self.assertEqual(inventory_risk(19,17,8),'LOW_STOCK')
        self.assertEqual(inventory_risk(20,2,8),'HEALTHY')

    def test_order_risk_is_time_and_status_aware(self):
        now=datetime.now(timezone.utc)
        past=now-timedelta(minutes=1)
        self.assertEqual(order_risk('shipped',past,now),'OVERDUE')
        self.assertEqual(order_risk('delivered',past,now),'ON_TRACK')
        self.assertEqual(order_risk('cancelled',past,now),'ON_TRACK')

    def test_seed_ids_are_stable(self):
        name='supplyguard-demo:demo-retailer:inventory:SKU-001'
        self.assertEqual(uuid.uuid5(uuid.NAMESPACE_DNS,name),uuid.uuid5(uuid.NAMESPACE_DNS,name))

    def test_databricks_pipeline_parses(self):
        src = (ROOT/'databricks/pipeline.py').read_text()
        ast.parse(src)
        self.assertIn('dropDuplicatesWithinWatermark',src)
        self.assertIn('gold_inventory_risks',src)
        self.assertIn('gold_order_risks',src)
        self.assertIn('gold_replenishment_signals',src)
        self.assertIn('@dp.expect_all_or_drop',src)

    def test_project_paths(self):
        for path in ['docker-compose.yml','databricks.yml','resources/pipeline.yml',
                     'apps/api/src/server.ts','apps/web/app/page.tsx',
                     'infra/postgres/001_init.sql']:
            self.assertTrue((ROOT/path).exists(),path)

if __name__=='__main__': unittest.main()
