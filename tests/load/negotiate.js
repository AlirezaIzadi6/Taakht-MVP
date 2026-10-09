// Write-path mix: each iteration runs a complete swap through the REST edge:
//   publish 2 ads -> open negotiation -> approve ad -> approve proposal x2 (full agreement)
//   -> negotiation AGREED -> swap created -> both ads CLOSED (in-person terms: nobody owes a fee, so the swap completes at once)
//
//   k6 run negotiate.js                       (VUS=10 DURATION=60s)
//   k6 run -e VUS=30 -e DURATION=90s negotiate.js
//
// Custom metrics:
//   time_to_agreed         ms from sending the final approval until GET /v1/negotiations/{id} shows AGREED (poll every 100 ms)
//   time_to_ads_closed     ms from sending the final approval until GET /v1/ads/{id} shows the target ad CLOSED
//                          = end-to-end event propagation: outbox -> Kafka -> swap -> LockAds -> SwapCompleted -> ad
//   iteration_ok           rate of iterations that reached the end state
// Thresholds are recorded, not gated: this is not CI.
import { Trend, Rate } from 'k6/metrics';
import { USERS, api, json, pick, adSpec, publishAd, hideRunAds, runId, check, sleep, slimSummary, textLine, resultFile } from './common.js';

const VUS = parseInt(__ENV.VUS || '10');
const DURATION = __ENV.DURATION || '60s';
const POLL_MS = parseInt(__ENV.POLL_MS || '100');
const POLL_MAX_MS = parseInt(__ENV.POLL_MAX_MS || '30000');
const THINK = parseFloat(__ENV.THINK || '0.2');

const timeToAgreed = new Trend('time_to_agreed', true);
const timeToClosed = new Trend('time_to_ads_closed', true);
const iterationOk = new Rate('iteration_ok');

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    negotiate: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP || '15s', target: VUS },
        { duration: DURATION, target: VUS },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '60s',
      gracefulStop: '60s',
    },
  },
  thresholds: {
    unexpected_status: ['rate<0.01'],
    http_req_failed: ['rate<0.01'],
    iteration_ok: ['rate>0.99'],
    time_to_agreed: ['p(95)<2000'],
    time_to_ads_closed: ['p(95)<5000'],
    'http_req_duration{name:open negotiation}': ['p(95)<1000'],
  },
};

export function setup() {
  return { run: runId() };
}

// Polls GET path until `done(json)` is true; returns elapsed ms since t0 or -1 on timeout.
function pollUntil(user, path, name, done, t0) {
  while (Date.now() - t0 < POLL_MAX_MS) {
    const r = api(user, 'GET', path, undefined, name);
    if (r.status === 200 && done(json(r))) return Date.now() - t0;
    sleep(POLL_MS / 1000);
  }
  return -1;
}

export default function (data) {
  const ua = pick(USERS);
  let ub = pick(USERS);
  while (ub === ua) ub = pick(USERS);
  const tag = data.run + '-' + __VU + '-' + __ITER;

  const A = publishAd(ua, adSpec('neg target ' + tag + ' [' + data.run + ']', 'books', ['tools'], ['n-valiasr']));
  const B = publishAd(ub, adSpec('neg requester ' + tag + ' [' + data.run + ']', 'tools', ['books'], ['n-valiasr']));
  if (!A || !B || A.failed || B.failed) return iterationOk.add(false);

  const open = api(ub, 'POST', '/v1/negotiations', { requesterAdId: B.id, targetAdId: A.id }, 'open negotiation');
  if (!check(open, { 'open negotiation 200': (r) => r.status === 200 })) return iterationOk.add(false);
  const nid = json(open).id;

  const ad = api(ua, 'POST', '/v1/negotiations/' + nid + ':approve-ad', { adVersion: B.version }, 'approve ad');
  if (!check(ad, { 'approve ad 200': (r) => r.status === 200 })) return iterationOk.add(false);

  const p1 = api(ua, 'POST', '/v1/negotiations/' + nid + ':approve-proposal', { proposalNumber: 1 }, 'approve proposal');
  if (!check(p1, { 'approve proposal (1st) 200': (r) => r.status === 200 })) return iterationOk.add(false);

  const t0 = Date.now();
  const p2 = api(ub, 'POST', '/v1/negotiations/' + nid + ':approve-proposal', { proposalNumber: 1 }, 'approve proposal');
  if (!check(p2, {
    'approve proposal (final) 200': (r) => r.status === 200,
    'final approval -> AGREEMENT_PENDING or AGREED': (r) => ['NEGOTIATION_STATUS_AGREEMENT_PENDING', 'NEGOTIATION_STATUS_AGREED'].includes(json(r).status),
  })) return iterationOk.add(false);

  const dAgreed = pollUntil(ub, '/v1/negotiations/' + nid, 'poll negotiation', (n) => n.status === 'NEGOTIATION_STATUS_AGREED', t0);
  check(null, { 'negotiation reached AGREED': () => dAgreed >= 0 });
  if (dAgreed >= 0) timeToAgreed.add(dAgreed);

  const dClosed = pollUntil(ua, '/v1/ads/' + A.id, 'poll ad', (a) => a.status === 'AD_STATUS_CLOSED', t0);
  check(null, { 'target ad reached CLOSED': () => dClosed >= 0 });
  if (dClosed >= 0) timeToClosed.add(dClosed);

  iterationOk.add(dAgreed >= 0 && dClosed >= 0);
  sleep(THINK);
}

export function teardown(data) {
  hideRunAds(data.run);
}

export function handleSummary(data) {
  const out = slimSummary(data, 'negotiate', { VUS, DURATION, POLL_MS, THINK });
  return { [resultFile('negotiate')]: JSON.stringify(out, null, 1), stdout: textLine(out) };
}
