import { Activity, ArrowDownRight, Boxes, CircleAlert, Clock3,
  DatabaseZap, Factory, GitBranch, Layers3, PackageCheck, ShieldAlert, ShoppingBag,
  Truck, Warehouse } from 'lucide-react';

export const dynamic = 'force-dynamic';
type KPI = { tracked_skus:number; stockouts:number; low_stock:number;
  total_orders:number; overdue_orders:number; delivered_orders:number };
type Inventory = {sku:string; warehouse_id:string;on_hand:number;reserved:number;
  reorder_point:number;available_units:number;supplier_eta_days:number|null;
  updated_at:string;risk_level:'STOCKOUT'|'LOW_STOCK'|'HEALTHY'};
type Order = {order_id:string;sku:string;warehouse_id:string;quantity:number;
  promised_at:string;status:string;updated_at:string;risk_level:'OVERDUE'|'ON_TRACK'};

async function api<T>(path:string):Promise<T> {
  const base = process.env.INTERNAL_API_URL || 'http://localhost:4000';
  const response = await fetch(base+path,{headers:{'x-api-key':process.env.DEMO_API_KEY||''},cache:'no-store'});
  if (!response.ok) throw new Error(`API ${response.status} for ${path}`);
  return response.json();
}
function tone(level: string) {
  if (level === 'STOCKOUT' || level === 'OVERDUE') return 'danger';
  if (level === 'LOW_STOCK') return 'warning';
  return 'success';
}
function card(n:string, label:string, icon: React.ReactNode, delta:string, kind='base') {
  return <div className={`metric ${kind}`}><div className="metric-top"><span>{label}</span>{icon}</div>
    <div className="metric-value">{n}</div><div className="metric-sub">{delta}</div></div>;
}
function formatted(d:string) { return new Date(d).toLocaleString('en-IN', {dateStyle:'medium',timeStyle:'short',timeZone:'Asia/Kolkata'}); }
export default async function Page() {
  let metrics:KPI={tracked_skus:0,stockouts:0,low_stock:0,total_orders:0,overdue_orders:0,delivered_orders:0};
  let inventory: Inventory[] = [], orders: Order[]=[]; let unavailable=false;
  try {
    const [m,i,o]=await Promise.all([
      api<KPI>('/v1/kpis'),api<{items:Inventory[]}>('/v1/inventory-risks'),
      api<{items:Order[]}>('/v1/order-risks')]);
    metrics=m;inventory=i.items;orders=o.items;
  } catch { unavailable=true; }
  const stockout=inventory.filter(i=>i.risk_level==='STOCKOUT').length;
  const low=inventory.filter(i=>i.risk_level==='LOW_STOCK').length;
  const healthy=Math.max(metrics.tracked_skus-stockout-low,0);
  const total=Math.max(metrics.tracked_skus,1);
  const overdue=orders.filter(o=>o.risk_level==='OVERDUE');
  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-icon"><Layers3 size={23}/></div><div><strong>SUPPLYGUARD</strong><small>360 · Intelligence</small></div></div>
      <div className="nav-label">WORKSPACE</div>
      <a href="#overview" className="nav-item active"><Activity size={18}/> Command Center</a>
      <a href="#inventory" className="nav-item"><Boxes size={18}/> Inventory Monitor</a>
      <a href="#fulfillment" className="nav-item"><Truck size={18}/> Fulfillment Risk</a>
      <a href="#pipeline" className="nav-item"><GitBranch size={18}/> Data Pipeline</a>
      <div className="sidebar-footer"><div className="status-dot"/> LAKEHOUSE CONNECTOR<div className="sidebar-caption">LOCAL PREVIEW MODE</div></div>
    </aside>
    <main className="main">
      <header className="topbar"><div><span className="breadcrumb">Operations / </span> Command Center</div>
       <span className="environment"><span className="status-dot"/> Operational visibility</span></header>
      <section className="hero" id="overview"><div><div className="eyebrow"><DatabaseZap size={15}/> SUPPLY CHAIN INTELLIGENCE</div>
         <h1>Know the risk.<br/><span>Before it becomes a loss.</span></h1>
         <p>Unified inventory availability, fulfillment delays and supplier intelligence — across every warehouse.</p>
         <div className="hero-tags"><span><Warehouse size={14}/> Multi-warehouse</span><span><Clock3 size={14}/> Event-driven</span><span><ShieldAlert size={14}/> Risk-first</span></div>
        </div><div className="hero-art"><div className="orb orb-one"/><div className="orb orb-two"/><div className="hero-symbol"><Activity size={62} strokeWidth={1.1}/></div></div></section>
      {unavailable && <div className="error-banner">API unavailable. Check <code>docker compose ps</code> and API logs.</div>}
      {!unavailable && metrics.total_orders===0 && <div className="hint">No demo events processed yet. Run <code>python3 services/seed/seed_demo.py</code>, then refresh.</div>}
      <div className="section-heading"><div><h2>Current operating pulse</h2><p>Your critical operational signals at a glance</p></div><span className="time-badge">CURRENT VIEW</span></div>
      <section className="metrics">
       {card(String(metrics.tracked_skus),'Tracked inventory units',<Boxes size={20}/>, 'Warehouse / SKU combinations')}
       {card(String(metrics.stockouts),'Stockout alerts',<CircleAlert size={20}/>, 'Requires immediate replenishment','red')}
       {card(String(metrics.low_stock),'Low stock warnings',<ArrowDownRight size={20}/>, 'Below replenishment threshold','amber')}
       {card(String(metrics.overdue_orders),'Overdue orders',<Clock3 size={20}/>,`Of ${metrics.total_orders} tracked orders`,'purple')}
      </section>
      <section className="insight-grid">
        <div className="panel health-panel"><div className="panel-head"><div><h3>Inventory health</h3><p>Current risk distribution</p></div><span className="panel-icon"><Factory size={18}/></span></div>
         <div className="distribution"><div className="donut" style={{background:`conic-gradient(#f46e71 0 ${stockout/total*100}%, #f6bb62 ${stockout/total*100}% ${(stockout+low)/total*100}%, #55d9b1 ${(stockout+low)/total*100}% 100%)`}}><div className="donut-center"><strong>{metrics.tracked_skus}</strong><span>LOCATIONS</span></div></div>
         <div className="legend"><div><span className="legend-dot red-dot"/> Stockout <strong>{metrics.stockouts}</strong></div><div><span className="legend-dot amber-dot"/> Low stock <strong>{metrics.low_stock}</strong></div><div><span className="legend-dot green-dot"/> Healthy <strong>{healthy}</strong></div></div></div>
         <div className="panel-note">Available units = max(on-hand − reserved, 0). Warnings trigger below each SKU's reorder point.</div>
        </div>
        <div className="panel priority-panel"><div className="panel-head"><div><h3>Priority fulfillment risks</h3><p>Orders needing attention</p></div><span className="panel-icon"><ShoppingBag size={18}/></span></div>
         <div className="priority-number">{metrics.overdue_orders}<span> / {metrics.total_orders} orders</span></div>
         <div className="progress"><div style={{width:`${metrics.total_orders ? metrics.overdue_orders/metrics.total_orders*100:0}%`}}/></div>
         <div className="priority-content"><div className="priority-icon"><Truck size={24}/></div><div><strong>Recover delayed fulfillment</strong><p>Review promised delivery time, shipment progress, and supplier replenishment.</p></div></div>
         <div className="tiny-caption"><PackageCheck size={15}/> {metrics.delivered_orders} delivered orders in current dataset</div>
        </div>
      </section>
      <section className="panel table-panel" id="inventory"><div className="panel-head"><div><h3>Inventory risk watchlist</h3><p>Highest urgency first · Snapshot-aware stock signals</p></div><span className="panel-icon"><Boxes size={18}/></span></div>
        <div className="table-scroll"><table><thead><tr><th>PRODUCT</th><th>WAREHOUSE</th><th>ON HAND</th><th>RESERVED</th><th>AVAILABLE</th><th>SUPPLIER ETA</th><th>STATUS</th></tr></thead><tbody>
          {inventory.slice(0,9).map(item=><tr key={`${item.sku}:${item.warehouse_id}`}><td className="primary-cell">{item.sku}</td><td>{item.warehouse_id}</td><td>{item.on_hand}</td><td>{item.reserved}</td><td><strong>{item.available_units}</strong></td><td>{item.supplier_eta_days===null?'—':`${item.supplier_eta_days} days`}</td><td><span className={`pill ${tone(item.risk_level)}`}>{item.risk_level.replace('_',' ')}</span></td></tr>)}
          {!inventory.length && <tr><td colSpan={7} className="empty">No inventory data yet</td></tr>}
        </tbody></table></div></section>
      <section className="panel table-panel" id="fulfillment"><div className="panel-head"><div><h3>Fulfillment exception queue</h3><p>At-risk delivery promises</p></div><span className="panel-icon"><Clock3 size={18}/></span></div>
        <div className="table-scroll"><table><thead><tr><th>ORDER</th><th>PRODUCT</th><th>WAREHOUSE</th><th>QUANTITY</th><th>PROMISED DELIVERY (IST)</th><th>SHIPMENT</th><th>RISK</th></tr></thead><tbody>
          {orders.slice(0,9).map(order=><tr key={order.order_id}><td className="primary-cell">{order.order_id}</td><td>{order.sku}</td><td>{order.warehouse_id}</td><td>{order.quantity}</td><td>{formatted(order.promised_at)}</td><td>{order.status}</td><td><span className={`pill ${tone(order.risk_level)}`}>{order.risk_level.replace('_',' ')}</span></td></tr>)}
          {!orders.length && <tr><td colSpan={7} className="empty">No order data yet</td></tr>}
        </tbody></table></div></section>
      <section className="panel pipeline-panel" id="pipeline"><div className="panel-head"><div><h3>Data platform design</h3><p>Deployed to Databricks separately from this local preview</p></div><span className="panel-icon"><DatabaseZap size={18}/></span></div>
        <div className="pipeline-steps"><div><strong>BRONZE</strong><span>Raw Kafka events with offsets</span></div><div><strong>SILVER</strong><span>Typed, validated, deduplicated</span></div><div><strong>GOLD</strong><span>Inventory and fulfillment models</span></div></div></section>
      <footer>SUPPLYGUARD 360 · Synthetic data only · Next.js + Kafka + PostgreSQL + Databricks Lakeflow</footer>
    </main>
  </div>;
}
