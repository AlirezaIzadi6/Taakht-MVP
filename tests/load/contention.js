// Contention on ONE hot ad (owner user-1), three scenarios in one run:
//
//  1. cap:    REQUESTERS virtual users (default 40, each with its own published ad owned by user-2..4) open a
//             negotiation on the hot ad at the same instant. NEGOTIATION_CAP is 10 open negotiations per ad, so
//             exactly 10 must succeed (200) and the rest must get 429 (RESOURCE_EXHAUSTED).
//  2. race:   the 10 negotiations are driven to agreement concurrently: the hot ad owner approves the requester ad
//             and the proposal, then all 10 requesters send the FINAL approval at the same wall-clock instant.
//  3. verify: counts the swaps for the hot ad through GET /v1/swaps; exactly one must be not REJECTED/CANCELLED.
//
//   k6 run contention.js                       (REQUESTERS=40)
//   k6 run -e REQUESTERS=100 contention.js
//
// Thresholds are recorded, not gated: this is not CI.
import { Counter, Gauge } from 'k6/metrics';
import exec from 'k6/execution';
import { USERS, api, json, adSpec, publishAd, hideRunAds, runId, check, sleep, slimSummary, textLine, resultFile } from './common.js';

const REQUESTERS = parseInt(__ENV.REQUESTERS || '40');
const CAP = parseInt(__ENV.CAP || '10');
const RACE_START_S = 25; // seconds after the start of the test
const VERIFY_START_S = 50;

const openedOk = new Counter('opened_ok');
const opened429 = new Counter('opened_429');
const openedOther = new Counter('opened_other');
const raceFinalOk = new Counter('race_final_approval_200');
const raceFinalRejected = new Counter('race_final_approval_rejected');
const negsOnHot = new Gauge('negotiations_on_hot_ad');
const swapsWinning = new Gauge('swaps_not_rejected_or_cancelled');
const swapsRejected = new Gauge('swaps_rejected');
const swapsTotal = new Gauge('swaps_total_for_hot_ad');

export const options = {
  setupTimeout: '300s',
  teardownTimeout: '300s',
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    cap: { executor: 'shared-iterations', vus: REQUESTERS, iterations: REQUESTERS, maxDuration: '20s', exec: 'openNegotiation' },
    race: { executor: 'per-vu-iterations', vus: CAP, iterations: 1, startTime: RACE_START_S + 's', maxDuration: '40s', exec: 'raceToAgreement' },
    verify: { executor: 'per-vu-iterations', vus: 1, iterations: 1, startTime: VERIFY_START_S + 's', maxDuration: '120s', exec: 'verify' },
  },
  thresholds: {
    opened_ok: ['count==' + CAP],
    opened_429: ['count==' + (REQUESTERS - CAP)],
    opened_other: ['count==0'],
    negotiations_on_hot_ad: ['value==' + CAP],
    swaps_not_rejected_or_cancelled: ['value==1'],
    unexpected_status: ['rate<0.01'],
  },
};

export function setup() {
  const run = runId();
  const hot = publishAd('user-1', adSpec('hot ad [' + run + ']', 'books', ['tools'], ['n-valiasr']));
  if (!hot || hot.failed) throw new Error('could not publish the hot ad');
  const reqs = [];
  for (let i = 0; i < REQUESTERS; i++) {
    const user = USERS[1 + (i % 3)];
    const ad = publishAd(user, adSpec('contender ' + i + ' [' + run + ']', 'tools', ['books'], ['n-valiasr']));
    if (!ad || ad.failed) throw new Error('could not publish contender ad ' + i);
    reqs.push({ user: user, adId: ad.id, version: ad.version });
  }
  const now = Date.now();
  // wall-clock barriers (the test starts right after setup)
  return { run: run, hot: hot, reqs: reqs, raceAt: now + (RACE_START_S + 12) * 1000, capAt: now + 1500 };
}

function waitUntil(ts) {
  const ms = ts - Date.now();
  if (ms > 0) sleep(ms / 1000);
}

export function openNegotiation(data) {
  const r = data.reqs[exec.scenario.iterationInTest];
  waitUntil(data.capAt);
  const res = api(r.user, 'POST', '/v1/negotiations', { requesterAdId: r.adId, targetAdId: data.hot.id }, 'open negotiation', [200, 429]);
  if (res.status === 200) openedOk.add(1);
  else if (res.status === 429) opened429.add(1);
  else {
    openedOther.add(1);
    console.log('unexpected open status ' + res.status + ': ' + res.body);
  }
}

export function raceToAgreement(data) {
  const list = api('user-1', 'GET', '/v1/negotiations?pageSize=200&adId=' + data.hot.id, undefined, 'list negotiations');
  const negs = (json(list).negotiations || []).slice().sort((a, b) => (a.id < b.id ? -1 : 1));
  negsOnHot.add(negs.length);
  const n = negs[exec.scenario.iterationInTest % Math.max(negs.length, 1)];
  if (!n) return;
  const req = data.reqs.find((x) => x.adId === n.requesterAdId);

  const ad = api('user-1', 'POST', '/v1/negotiations/' + n.id + ':approve-ad', { adVersion: req.version }, 'approve ad');
  check(ad, { 'approve ad 200': (r) => r.status === 200 });
  const p1 = api('user-1', 'POST', '/v1/negotiations/' + n.id + ':approve-proposal', { proposalNumber: 1 }, 'approve proposal');
  check(p1, { 'approve proposal (owner) 200': (r) => r.status === 200 });

  waitUntil(data.raceAt); // all final approvals leave within a few ms of each other
  // the 4th valid approval; a loser may already be CANCELLED by the winner's lock, so 400/409 are legitimate here
  const fin = api(n.requesterUserId, 'POST', '/v1/negotiations/' + n.id + ':approve-proposal', { proposalNumber: 1 }, 'final approval (race)', [200, 400, 409]);
  if (fin.status === 200) raceFinalOk.add(1);
  else raceFinalRejected.add(1);
}

// Swaps of the hot ad: GET /v1/swaps is paginated, newest first, so walk pages (200 each) until a page
// holds no swap of the hot ad after at least one was found, or the pages run out (hard stop after 20).
function hotSwaps(hotId) {
  const found = [];
  let token = '';
  for (let page = 0; page < 20; page++) {
    const r = json(api('user-1', 'GET', '/v1/swaps?pageSize=200' + (token ? '&pageToken=' + encodeURIComponent(token) : ''), undefined, 'list swaps'));
    const mine = (r.swaps || []).filter((s) => s.adAId === hotId || s.adBId === hotId);
    mine.forEach((s) => found.push(s));
    token = r.nextPageToken || '';
    if (!token || (found.length > 0 && mine.length === 0)) break;
  }
  return found;
}

export function verify(data) {
  const t0 = Date.now();
  let swaps = [];
  let negs = [];
  let stable = 0;
  while (Date.now() - t0 < 90000) {
    negs = json(api('user-1', 'GET', '/v1/negotiations?pageSize=200&adId=' + data.hot.id, undefined, 'list negotiations')).negotiations || [];
    swaps = hotSwaps(data.hot.id);
    const pending = negs.filter((x) => x.status === 'NEGOTIATION_STATUS_AGREEMENT_PENDING' || x.status === 'NEGOTIATION_STATUS_OPEN').length;
    const locking = swaps.filter((s) => s.status === 'SWAP_STATUS_LOCKING').length;
    stable = pending === 0 && locking === 0 && swaps.length > 0 ? stable + 1 : 0;
    if (stable >= 2) break;
    sleep(1);
  }
  const by = {};
  swaps.forEach((s) => (by[s.status] = (by[s.status] || 0) + 1));
  const negBy = {};
  negs.forEach((x) => (negBy[x.status] = (negBy[x.status] || 0) + 1));
  swapsTotal.add(swaps.length);
  swapsRejected.add(swaps.filter((s) => s.status === 'SWAP_STATUS_REJECTED').length);
  swapsWinning.add(swaps.filter((s) => s.status !== 'SWAP_STATUS_REJECTED' && s.status !== 'SWAP_STATUS_CANCELLED').length);
  console.log('verify: swaps by status ' + JSON.stringify(by) + '; negotiations by status ' + JSON.stringify(negBy) + '; settled after ' + (Date.now() - t0) + ' ms');
  const hot = json(api('user-1', 'GET', '/v1/ads/' + data.hot.id, undefined, 'get ad'));
  console.log('verify: hot ad status ' + hot.status);
}

export function teardown(data) {
  hideRunAds(data.run);
}

export function handleSummary(data) {
  const out = slimSummary(data, 'contention', { REQUESTERS, CAP });
  return { [resultFile('contention')]: JSON.stringify(out, null, 1), stdout: textLine(out) };
}
