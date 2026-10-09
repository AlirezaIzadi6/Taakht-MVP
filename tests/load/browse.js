// Read-path load: many virtual users searching and listing against a pre-seeded set of published ads.
//
//   k6 run browse.js                         (VUS=20 DURATION=60s ADS=200)
//   k6 run -e VUS=50 -e DURATION=90s browse.js
//
// Thresholds are recorded, not gated: this is not CI.
import { TOKENS, USERS, CATEGORIES, NEIGHBORHOODS, api, json, pick, adSpec, publishAd, hideRunAds, runId, check, sleep, slimSummary, textLine, resultFile } from './common.js';

const VUS = parseInt(__ENV.VUS || '20');
const DURATION = __ENV.DURATION || '60s';
const ADS = parseInt(__ENV.ADS || '200');
const THINK = parseFloat(__ENV.THINK || '0.1'); // seconds between iterations

export const options = {
  setupTimeout: '600s',
  teardownTimeout: '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    browse: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP || '10s', target: VUS },
        { duration: DURATION, target: VUS },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    unexpected_status: ['rate<0.01'],
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:search}': ['p(95)<500'],
    'http_req_duration{name:list my ads}': ['p(95)<500'],
  },
};

export function setup() {
  const run = runId();
  let created = 0;
  for (let i = 0; i < ADS; i++) {
    const user = USERS[i % USERS.length];
    const have = CATEGORIES[i % CATEGORIES.length];
    const wants = [CATEGORIES[(i + 1 + (i % 3)) % CATEGORIES.length], CATEGORIES[(i + 3) % CATEGORIES.length]];
    const hood = NEIGHBORHOODS[Math.floor(i / 2) % NEIGHBORHOODS.length];
    const ad = publishAd(user, adSpec('browse ad ' + i + ' [' + run + ']', have, wants, [hood]));
    if (ad && !ad.failed) created++;
  }
  console.log('setup: published ' + created + ' of ' + ADS + ' ads, run ' + run);
  // Matching is fed through Kafka: wait until the index returns results for a broad search (max 60 s).
  const t0 = Date.now();
  let found = 0;
  while (Date.now() - t0 < 60000) {
    const r = api('user-1', 'POST', '/v1/matching/search', { criteria: { wantCategories: CATEGORIES }, limit: 100 }, 'setup search');
    found = (json(r).candidates || []).length;
    if (found >= Math.min(100, Math.floor(created * 0.7))) break;
    sleep(1);
  }
  console.log('setup: search returns ' + found + ' candidates after ' + (Date.now() - t0) + ' ms');
  return { run: run, created: created };
}

export default function () {
  const user = pick(USERS);
  // 3 of 4 iterations search (random criteria, limit 20/50), 1 of 4 lists the caller's own ads.
  if (Math.random() < 0.75) {
    const criteria = { wantCategories: [pick(CATEGORIES)], neighborhoodIds: Math.random() < 0.5 ? [pick(NEIGHBORHOODS)] : [] };
    const r = api(user, 'POST', '/v1/matching/search', { criteria: criteria, limit: pick([20, 50]) }, 'search');
    check(r, { 'search 200': (x) => x.status === 200 });
  } else {
    const r = api(user, 'GET', '/v1/ads', undefined, 'list my ads');
    check(r, { 'list my ads 200': (x) => x.status === 200 });
  }
  sleep(THINK);
}

export function teardown(data) {
  hideRunAds(data.run);
}

export function handleSummary(data) {
  const out = slimSummary(data, 'browse', { VUS, DURATION, ADS, THINK });
  return { [resultFile('browse')]: JSON.stringify(out, null, 1), stdout: textLine(out) };
}
