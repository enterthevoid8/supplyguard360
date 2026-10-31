import { z } from 'zod';

const header = z.object({
  version: z.literal(1),
  event_id: z.string().uuid(),
  tenant_id: z.string().min(1).max(80),
  sequence: z.number().int().nonnegative(),
  occurred_at: z.string().datetime({ offset: true }),
});
const identifier = z.string().min(1).max(100).regex(/^[a-zA-Z0-9_.:-]+$/);
const location = { sku: identifier, warehouse_id: identifier };
export const EventSchema = z.discriminatedUnion('event_type', [
  header.extend({ event_type: z.literal('inventory_snapshot'), payload: z.object({
    ...location, on_hand: z.number().int().nonnegative(),
    reserved: z.number().int().nonnegative(),
    reorder_point: z.number().int().nonnegative(),
  }).strict() }),
  header.extend({ event_type: z.literal('order_created'), payload: z.object({
    order_id: identifier, ...location, quantity: z.number().int().positive(),
    promised_at: z.string().datetime({ offset: true }),
  }).strict() }),
  header.extend({ event_type: z.literal('shipment_status'), payload: z.object({
    order_id: identifier,
    status: z.enum(['created', 'shipped', 'delivered', 'cancelled']),
  }).strict() }),
  header.extend({ event_type: z.literal('supplier_eta_updated'), payload: z.object({
    ...location, eta_days: z.number().int().nonnegative().max(365),
  }).strict() }),
]);
export type BusinessEvent = z.infer<typeof EventSchema>;
export function routingKey(event: BusinessEvent): string {
  const id = event.event_type === 'shipment_status' || event.event_type === 'order_created'
    ? `order:${event.payload.order_id}`
    : `sku:${event.payload.sku}:${event.payload.warehouse_id}`;
  return `${event.tenant_id}:${id}`;
}
