// Quick end-to-end smoke test (1 VU): supplier -> stock -> booking -> carrier journey -> status.
//   k6 run -e BASE_URL=http://<alb-dns> loadtest/k6/smoke.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const H = { headers: { 'Content-Type': 'application/json' } };
export const options = { vus: 1, iterations: 1, thresholds: { checks: ['rate==1.0'] } };

export default function () {
  const sup = http.post(`${BASE}/api/suppliers`, JSON.stringify({ name: 'Smoke Supplier', country: 'India', contact_email: 'smoke@test.example' }), H);
  check(sup, { 'supplier 201': (r) => r.status === 201 });
  const supplierId = sup.json('supplier_id');
  const sku = `SKU-SMOKE-${Date.now()}`;
  check(http.post(`${BASE}/api/inventory/items`, JSON.stringify({ sku, supplier_id: supplierId, name: 'Smoke', quantity: 10 }), H),
    { 'stock 201': (r) => r.status === 201 });
  const ship = http.post(`${BASE}/api/shipments`, JSON.stringify({ supplier_id: supplierId, retailer_id: 'RET-SMOKE', carrier: 'DHL', items: [{ sku, quantity: 2 }] }), H);
  check(ship, { 'booking 201 (REST+gRPC)': (r) => r.status === 201 });
  const tid = ship.json('tracking_id');
  const now = Date.now();
  for (const [st, dt] of [['PICKED_UP', -20], ['IN_TRANSIT', -10], ['OUT_FOR_DELIVERY', -2], ['DELIVERED', 0]]) {
    check(http.post(`${BASE}/api/tracking/events`, JSON.stringify({ tracking_id: tid, carrier: 'DHL', status: st, location: 'Somewhere', event_time: now + dt * 3600e3 }), H),
      { [`${st} 202`]: (r) => r.status === 202 });
  }
  let status = '';
  for (let i = 0; i < 20 && status !== 'DELIVERED'; i++) {
    sleep(1);
    const r = http.get(`${BASE}/api/status/${tid}`);
    if (r.status === 200) status = r.json('state.status');
  }
  check(status, { 'derived status DELIVERED (outbox -> MSK -> Dynamo store)': (s) => s === 'DELIVERED' });
}
