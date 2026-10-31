import { Kafka } from 'kafkajs';
import { db } from './db.js';
import { EventSchema, routingKey } from './schema.js';

const kafka = new Kafka({ clientId: 'supplyguard-outbox', brokers: (process.env.KAFKA_BOOTSTRAP || '').split(',') });
const producer = kafka.producer({ allowAutoTopicCreation: true });
const topic = process.env.KAFKA_TOPIC || 'supplyguard.events.v1';
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
let running = true;
process.on('SIGTERM', () => { running = false; });
process.on('SIGINT', () => { running = false; });
async function publish() {
  await producer.connect();
  while (running) {
    try {
      const client = await db.connect();
      let rows: { event_id: string; event_body: unknown }[];
      try {
        await client.query('BEGIN');
        const claimed = await client.query<{ event_id: string; event_body: unknown }>(`
          UPDATE event_outbox SET leased_until=now()+interval '60 seconds'
          WHERE event_id IN (
            SELECT event_id FROM event_outbox
            WHERE published_at IS NULL AND (leased_until IS NULL OR leased_until<now())
            ORDER BY created_at LIMIT 50 FOR UPDATE SKIP LOCKED
          ) RETURNING event_id,event_body`);
        rows = claimed.rows;
        await client.query('COMMIT');
      } catch (e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
      for (const row of rows) {
        const event = EventSchema.parse(row.event_body);
        await producer.send({ topic, acks: -1, messages: [{
          key: routingKey(event), value: JSON.stringify(event),
          headers: { event_id: event.event_id },
        }] });
        await db.query('UPDATE event_outbox SET published_at=now(),leased_until=NULL WHERE event_id=$1', [row.event_id]);
      }
      if (!rows.length) await sleep(800);
    } catch (error) { console.error('Outbox publisher retry:', error); await sleep(3000); }
  }
  await producer.disconnect();
  await db.end();
}
publish().catch(err => { console.error(err); process.exit(1); });
