import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { timingSafeEqual } from 'node:crypto';
import { db } from './db.js';
import { EventSchema } from './schema.js';

const app = Fastify({ logger: true, bodyLimit: 64 * 1024 });
const tenant = process.env.TENANT_ID || 'demo-retailer';
const secret = process.env.DEMO_API_KEY || '';
if (secret.length < 24) throw new Error('DEMO_API_KEY must have >= 24 characters');
const equal = (a: string, b: string) => {
  const x = Buffer.from(a), y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
};

async function main() {
  await app.register(rateLimit, { max: 120, timeWindow: '1 minute' });
  app.addHook('onRequest', async (request, reply) => {
    if (request.url === '/health') return;
    if (!equal(String(request.headers['x-api-key'] || ''), secret)) {
      return reply.code(401).send({ error: 'unauthorized' });
    }
  });
  app.get('/health', async (_req, reply) => {
    try { await db.query('SELECT 1'); return { ok: true }; }
    catch { return reply.code(503).send({ ok: false }); }
  });
  app.post('/v1/events', async (req, reply) => {
    const event = EventSchema.safeParse(req.body);
    if (!event.success) return reply.code(400).send({ error: 'invalid_event', issues: event.error.flatten() });
    if (event.data.tenant_id !== tenant) return reply.code(403).send({ error: 'tenant_mismatch' });
    const { rowCount } = await db.query(
      `INSERT INTO event_outbox(event_id,tenant_id,event_type,event_body)
       VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT(event_id) DO NOTHING`,
      [event.data.event_id, tenant, event.data.event_type, JSON.stringify(event.data)]);
    return reply.code(rowCount ? 202 : 200).send({ accepted: true, duplicate: rowCount === 0,
      event_id: event.data.event_id });
  });
  app.get('/v1/kpis', async () => {
    const { rows } = await db.query(`
      SELECT
        (SELECT COUNT(*)::int FROM inventory_risks WHERE tenant_id=$1) AS tracked_skus,
        (SELECT COUNT(*)::int FROM inventory_risks WHERE tenant_id=$1 AND available_units=0) AS stockouts,
        (SELECT COUNT(*)::int FROM inventory_risks WHERE tenant_id=$1 AND available_units>0 AND available_units<reorder_point) AS low_stock,
        (SELECT COUNT(*)::int FROM order_risks WHERE tenant_id=$1) AS total_orders,
        (SELECT COUNT(*)::int FROM order_risks WHERE tenant_id=$1 AND status NOT IN ('delivered','cancelled') AND promised_at<now()) AS overdue_orders,
        (SELECT COUNT(*)::int FROM order_risks WHERE tenant_id=$1 AND status='delivered') AS delivered_orders
      `, [tenant]);
    return rows[0];
  });
  app.get('/v1/inventory-risks', async (req, reply) => {
    const q = req.query as Record<string, string>;
    const limit = Math.min(200, Math.max(1, Number(q.limit) || 100));
    const { rows } = await db.query(`SELECT sku, warehouse_id, on_hand, reserved, reorder_point,
      available_units, supplier_eta_days, updated_at,
      CASE WHEN available_units=0 THEN 'STOCKOUT'
           WHEN available_units<reorder_point THEN 'LOW_STOCK' ELSE 'HEALTHY' END AS risk_level
      FROM inventory_risks WHERE tenant_id=$1
      ORDER BY CASE WHEN available_units=0 THEN 0 WHEN available_units<reorder_point THEN 1 ELSE 2 END,
        available_units ASC, sku LIMIT $2`, [tenant, limit]);
    return { items: rows };
  });
  app.get('/v1/order-risks', async (req) => {
    const q = req.query as Record<string,string>;
    const limit = Math.min(200, Math.max(1, Number(q.limit) || 100));
    const { rows } = await db.query(`SELECT order_id, sku, warehouse_id, quantity, promised_at,
      status, updated_at,
      CASE WHEN status NOT IN ('delivered','cancelled') AND promised_at<now()
        THEN 'OVERDUE' ELSE 'ON_TRACK' END AS risk_level
      FROM order_risks WHERE tenant_id=$1
      ORDER BY CASE WHEN status NOT IN ('delivered','cancelled') AND promised_at<now()
        THEN 0 ELSE 1 END, promised_at ASC LIMIT $2`, [tenant, limit]);
    return { items: rows };
  });
  app.addHook('onClose', async () => { await db.end(); });
  await app.listen({ host: '0.0.0.0', port: Number(process.env.API_PORT || 4000) });
}
main().catch(err => { app.log.error(err); process.exit(1); });
