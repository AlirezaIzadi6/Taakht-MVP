// Shared helpers for the k6 load scripts. See README.md.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';

export const BASE = __ENV.GATEWAY || 'http://localhost:8080';
export const TOKENS = JSON.parse(open('./.tokens.json')); // written by gen-tokens.sh
export const USERS = ['user-1', 'user-2', 'user-3', 'user-4'];

// Values from config/eligibility.json (the only ones the services accept).
export const CATEGORIES = ['books', 'tools', 'furniture', 'sports', 'electronics-small', 'kitchen'];
export const NEIGHBORHOODS = ['n-valiasr', 'n-tajrish', 'n-sadeghieh', 'n-narmak', 'n-shahrak-gharb', 'n-karaj-gohardasht'];

// Share of API calls whose HTTP status was not one the scenario expected for that call.
export const unexpected = new Rate('unexpected_status');
export const apiCalls = new Counter('api_calls');

export function runId() {
  return (__ENV.RUN_ID || Date.now().toString(36) + Math.floor(Math.random() * 1296).toString(36));
}

export function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export function nowDateUtc() {
  return new Date().toISOString().slice(0, 10);
}

// api(user, method, path, body, name, expected) -> response.
// `name` groups the URL for metrics ({name:...} tag); `expected` is the list of statuses that are not failures.
export function api(user, method, path, body, name, expected) {
  const exp = expected || [200];
  const params = {
    headers: { Authorization: 'Bearer ' + TOKENS[user], 'Content-Type': 'application/json' },
    tags: { name: name || method + ' ' + path },
    responseCallback: http.expectedStatuses(...exp),
  };
  let payload = body;
  if (payload === undefined && method === 'POST') payload = {};
  const res = http.request(method, BASE + path, payload === undefined ? null : JSON.stringify(payload), params);
  apiCalls.add(1);
  unexpected.add(!exp.includes(res.status));
  return res;
}

export function json(res) {
  try {
    return res.json();
  } catch (e) {
    return {};
  }
}

export function adSpec(title, have, wants, hoods) {
  return {
    title: title,
    description: 'created by tests/load',
    haveCategory: have,
    wantCategories: wants,
    neighborhoodIds: hoods,
    valueEstimate: '500000',
  };
}

// Creates and publishes an ad. Returns {id, version} or null on failure.
export function publishAd(user, spec) {
  const c = api(user, 'POST', '/v1/ads', spec, 'create ad');
  if (!check(c, { 'create ad 200': (r) => r.status === 200 })) return null;
  const id = json(c).id;
  const p = api(user, 'POST', '/v1/ads/' + id + ':publish', undefined, 'publish ad');
  if (!check(p, { 'publish ad 200': (r) => r.status === 200 })) return { id: id, version: json(c).version, failed: true };
  return { id: id, version: json(p).version };
}

// Hides every ad of the given run that is still PUBLISHED (ads of a finished swap are LOCKED/CLOSED and stay).
// Ads are found through GET /v1/ads (the caller's own ads) by the run id in the title.
export function hideRunAds(run) {
  let hidden = 0;
  for (const u of USERS) {
    // The list is paginated, newest first: walk pages (200 per page) until a page holds no ad of this run
    // after at least one was found, or the pages run out (hard stop after 20 pages).
    const ads = [];
    let token = '';
    for (let page = 0; page < 20; page++) {
      const l = json(api(u, 'GET', '/v1/ads?pageSize=200' + (token ? '&pageToken=' + encodeURIComponent(token) : ''), undefined, 'cleanup list ads'));
      const mine = (l.ads || []).filter((a) => a.spec && a.spec.title.indexOf('[' + run + ']') >= 0);
      mine.filter((a) => a.status === 'AD_STATUS_PUBLISHED').forEach((a) => ads.push(a));
      token = l.nextPageToken || '';
      if (!token || (mine.length === 0 && page > 0)) break;
    }
    for (let i = 0; i < ads.length; i += 20) {
      const reqs = ads.slice(i, i + 20).map((a) => ({
        method: 'POST',
        url: BASE + '/v1/ads/' + a.id + ':hide',
        body: '{}',
        params: { headers: { Authorization: 'Bearer ' + TOKENS[u], 'Content-Type': 'application/json' }, tags: { name: 'cleanup hide ad' } },
      }));
      http.batch(reqs);
      hidden += reqs.length;
    }
  }
  console.log('teardown: hid ' + hidden + ' published ads of run ' + run);
  return hidden;
}

// Keeps the handleSummary output small: only the metric values, thresholds and options of interest.
export function slimSummary(data, script, extra) {
  const out = { script: script, date: new Date().toISOString(), params: extra || {}, metrics: {}, thresholds: {} };
  for (const [name, m] of Object.entries(data.metrics)) {
    if (name.indexOf('{') >= 0 && name.indexOf('name:') < 0 && name.indexOf('scenario:') < 0) continue;
    out.metrics[name] = m.values;
    if (m.thresholds) {
      for (const [t, r] of Object.entries(m.thresholds)) out.thresholds[name + ' ' + t] = r.ok ? 'pass' : 'FAIL';
    }
  }
  out.root_group_checks = {};
  const walk = (g) => {
    for (const c of g.checks || []) out.root_group_checks[c.name] = { passes: c.passes, fails: c.fails };
    for (const sub of g.groups || []) walk(sub);
  };
  if (data.root_group) walk(data.root_group);
  return out;
}

export function textLine(out) {
  const m = out.metrics;
  const f = (x) => (x === undefined ? 'n/a' : x.toFixed(1));
  const d = m.http_req_duration || {};
  return [
    `script=${out.script} iterations=${(m.iterations || {}).count} requests=${(m.http_reqs || {}).count} rate=${f((m.http_reqs || {}).rate)}/s`,
    `http_req_failed=${f(100 * ((m.http_req_failed || {}).rate || 0))}% unexpected_status=${f(100 * ((m.unexpected_status || {}).rate || 0))}%`,
    `latency ms p50=${f(d.med)} p95=${f(d['p(95)'])} p99=${f(d['p(99)'])} max=${f(d.max)}`,
    'thresholds: ' + (Object.keys(out.thresholds).map((k) => k + '=' + out.thresholds[k]).join('; ') || 'none'),
    '',
  ].join('\n');
}

export function resultFile(script) {
  const label = __ENV.LABEL ? '-' + __ENV.LABEL : '';
  return 'results/' + nowDateUtc() + '-' + script + label + '.json';
}

export { sleep, check, http };
