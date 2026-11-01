// Sustained-load test that drives the HPAs (tracking-ingestion, booking-service, inventory-service)
// to scale out, while asserting latency/error SLOs.
//
//   k6 run -e BASE_URL=http://<alb-dns> loadtest/k6/hpa-load.js
//   (or: scripts/load-test.sh)
//
// Tunables: RATE (carrier events/s, default 250), BOOK_RATE (bookings/s, default 15),
//           READ_RATE (status reads/s, default 40), DURATION (default 6m)
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const DURATION = __ENV.DURATION || '6m';
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };
const CARRIERS = ['DELHIVERY', 'BLUEDART', 'DHL', 'FEDEX', 'ECOMEXPRESS'];
const STATUSES = ['PICKED_UP', 'IN_TRANSIT', 'AT_HUB', 'IN_TRANSIT', 'AT_HUB', 'OUT_FOR_DELIVERY', 'DELIVERED'];
const CITIES = ['Mumbai DC', 'Pune Hub', 'Nagpur Hub', 'Hyderabad Hub', 'Chennai Hub', 'Bengaluru Hub', 'Delhi Hub'];

const accepted = new Counter('carrier_events_accepted');
const bookLatency = new Trend('booking_latency', true);

export const options = {
  setupTimeout: '5m',
  discardResponseBodies: false,
  scenarios: {
    carrier_events: {
      executor: 'ramping-arrival-rate', exec: 'carrierEvents',
      startRate: 20, timeUnit: '1s', preAllocatedVUs: 80, maxVUs: 600,
      stages: [
        { target: Number(__ENV.RATE || 250), duration: '1m' },   // ramp
        { target: Number(__ENV.RATE || 250), duration: DURATION }, // sustain -> HPA scales out
        { target: 0, duration: '30s' },
      ],
    },
    bookings: {
      executor: 'constant-arrival-rate', exec: 'book', startTime: '30s',
      rate: Number(__ENV.BOOK_RATE || 15), timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 20, maxVUs: 150,
    },
    status_reads: {
      executor: 'constant-arrival-rate', exec: 'readStatus', startTime: '30s',
      rate: Number(__ENV.READ_RATE || 40), timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 20, maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    'http_req_duration{scenario:carrier_events}': ['p(95)<800'],
    'http_req_duration{scenario:status_reads}': ['p(95)<500'],
    booking_latency: ['p(95)<1500'],
  },
};

export function setup() {
  const sup = http.post(`${BASE}/api/suppliers`, JSON.stringify({
    name: `LoadTest Supplier ${Date.now()}`, country: 'India', contact_email: 'load@test.example' }), JSON_HDR).json();
  const sku = `SKU-LOAD-${Date.now()}`;
  http.post(`${BASE}/api/inventory/items`, JSON.stringify({
    sku, supplier_id: sup.supplier_id, name: 'Load test widget', quantity: 100000000, reorder_level: 0 }), JSON_HDR);
  // Spread load over many shipments (realistic ~tens of scans per parcel), booked in parallel batches.
  const shipments = [];
  const POOL = Number(__ENV.SHIPMENTS || 600);
  for (let b = 0; b < POOL / 50; b++) {
    const reqs = [];
    for (let i = 0; i < 50; i++) {
      reqs.push(['POST', `${BASE}/api/shipments`, JSON.stringify({
        supplier_id: sup.supplier_id, retailer_id: `RET-LOAD-${i % 20}`, carrier: CARRIERS[(b * 50 + i) % CARRIERS.length],
        items: [{ sku, quantity: 1 }] }), JSON_HDR]);
    }
    for (const r of http.batch(reqs)) {
      if (r.status === 201) shipments.push({ id: r.json('tracking_id'), carrier: r.json('carrier') });
    }
  }
  console.log(`setup: supplier ${sup.supplier_id}, sku ${sku}, ${shipments.length} shipments`);
  return { supplier: sup.supplier_id, sku, shipments };
}

export function carrierEvents(data) {
  const s = data.shipments[Math.floor(Math.random() * data.shipments.length)];
  const status = STATUSES[Math.floor(Math.random() * STATUSES.length)];
  const res = http.post(`${BASE}/api/tracking/events`, JSON.stringify({
    tracking_id: s.id, carrier: s.carrier, status,
    location: CITIES[Math.floor(Math.random() * CITIES.length)],
    event_time: Date.now() - Math.floor(Math.random() * 3600e3),
  }), Object.assign({ tags: { name: 'POST /api/tracking/events' } }, JSON_HDR));
  if (check(res, { 'event accepted (202)': (r) => r.status === 202 })) accepted.add(1);
}

export function book(data) {
  const res = http.post(`${BASE}/api/shipments`, JSON.stringify({
    supplier_id: data.supplier, retailer_id: `RET-LOAD-${Math.floor(Math.random() * 50)}`,
    carrier: CARRIERS[Math.floor(Math.random() * CARRIERS.length)], items: [{ sku: data.sku, quantity: 1 }],
  }), Object.assign({ tags: { name: 'POST /api/shipments' } }, JSON_HDR));
  bookLatency.add(res.timings.duration);
  check(res, { 'booked (201)': (r) => r.status === 201 });
}

export function readStatus(data) {
  const s = data.shipments[Math.floor(Math.random() * data.shipments.length)];
  const res = http.get(`${BASE}/api/status/${s.id}`, { tags: { name: 'GET /api/status/:id' } });
  check(res, { 'status 200/404': (r) => r.status === 200 || r.status === 404 });
}

export function teardown(data) {
  console.log(`done - check HPA: kubectl -n supplychain get hpa -w`);
}
