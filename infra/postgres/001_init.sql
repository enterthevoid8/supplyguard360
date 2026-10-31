CREATE TABLE IF NOT EXISTS event_outbox (
  event_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_body JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  leased_until TIMESTAMPTZ,
  published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox(created_at)
 WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
  event_id UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS inventory_state (
  tenant_id TEXT NOT NULL, sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  on_hand INTEGER NOT NULL CHECK (on_hand >= 0),
  reserved INTEGER NOT NULL CHECK (reserved >= 0),
  reorder_point INTEGER NOT NULL CHECK (reorder_point >= 0),
  sequence BIGINT NOT NULL, occurred_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(tenant_id, sku, warehouse_id)
);
CREATE TABLE IF NOT EXISTS supplier_state (
  tenant_id TEXT NOT NULL, sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  eta_days INTEGER NOT NULL CHECK (eta_days >= 0), sequence BIGINT NOT NULL,
  PRIMARY KEY(tenant_id, sku, warehouse_id)
);
CREATE TABLE IF NOT EXISTS orders_state (
  tenant_id TEXT NOT NULL, order_id TEXT NOT NULL,
  sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  promised_at TIMESTAMPTZ NOT NULL, sequence BIGINT NOT NULL,
  PRIMARY KEY(tenant_id, order_id)
);
CREATE TABLE IF NOT EXISTS shipment_state (
  tenant_id TEXT NOT NULL, order_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('created','shipped','delivered','cancelled')),
  sequence BIGINT NOT NULL,
  PRIMARY KEY(tenant_id, order_id)
);
-- Serving tables: local worker updates these; the Databricks exporter replaces them transactionally.
CREATE TABLE IF NOT EXISTS inventory_risks (
  tenant_id TEXT NOT NULL, sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  on_hand INTEGER NOT NULL, reserved INTEGER NOT NULL,
  reorder_point INTEGER NOT NULL, available_units INTEGER NOT NULL,
  supplier_eta_days INTEGER,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(tenant_id, sku, warehouse_id)
);
CREATE TABLE IF NOT EXISTS order_risks (
  tenant_id TEXT NOT NULL, order_id TEXT NOT NULL,
  sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  quantity INTEGER NOT NULL, promised_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(tenant_id, order_id)
);
CREATE INDEX IF NOT EXISTS idx_inventory_tenant ON inventory_risks(tenant_id);
CREATE INDEX IF NOT EXISTS idx_orders_tenant_promised ON order_risks(tenant_id,promised_at);

-- Snapshot staging is isolated per export UUID. Staging data is never queried by the API.
CREATE TABLE IF NOT EXISTS stage_inventory_risks (
  snapshot_id UUID NOT NULL,
  tenant_id TEXT NOT NULL, sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  on_hand INTEGER NOT NULL, reserved INTEGER NOT NULL, reorder_point INTEGER NOT NULL,
  available_units INTEGER NOT NULL, supplier_eta_days INTEGER,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS stage_order_risks (
  snapshot_id UUID NOT NULL,
  tenant_id TEXT NOT NULL, order_id TEXT NOT NULL,
  sku TEXT NOT NULL, warehouse_id TEXT NOT NULL,
  quantity INTEGER NOT NULL, promised_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stage_inventory_snapshot ON stage_inventory_risks(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_stage_order_snapshot ON stage_order_risks(snapshot_id);
CREATE TABLE IF NOT EXISTS serving_snapshots (
  snapshot_id UUID PRIMARY KEY, promoted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
