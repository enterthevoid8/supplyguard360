import test from 'node:test';
import assert from 'node:assert/strict';
import { EventSchema, routingKey } from './schema.js';
const good = { version: 1, event_id: 'd640ef67-d6b7-4793-ae0c-f65ab33c7646',
 tenant_id: 'demo-retailer', sequence: 3, occurred_at: '2026-10-22T15:00:00Z',
 event_type: 'inventory_snapshot', payload: {sku:'SKU-1', warehouse_id:'WH-1',
 on_hand:12, reserved:5, reorder_point:10} };
test('valid snapshot produces a stable Kafka partition key', () => {
 const parsed = EventSchema.parse(good);
 assert.equal(routingKey(parsed), 'demo-retailer:sku:SKU-1:WH-1');
});
test('invalid negative inventory rejected', () => {
 assert.equal(EventSchema.safeParse({...good, payload: {...good.payload, on_hand:-1}}).success, false);
});
