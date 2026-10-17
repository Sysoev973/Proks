// Цепочка «карточка книги -> авторы» для A/B-замера предзагрузки.
//
// Каждая итерация: GET /books/{id}, пауза THINK_MS, GET /books/{id}/authors
// с Referer на карточку. Переход предсказуем, поэтому при PREFETCH_ENABLED=true
// прокси успевает загрузить авторов, пока «пользователь думает».
//
// Метрики считаются только в фазе measure (после WARMUP). Счётчики прокси
// (/metrics) снимаются в конце прогрева и в teardown, поэтому RPS к upstream,
// hit ratio и доля бесполезных предзагрузок тоже относятся только к замеру.
//
// Параметры (-e NAME=value):
//   PROXY_URLS  список прокси через запятую; запрос уходит на случайную
//               реплику (балансировщик без привязки к ключу). По умолчанию
//               http://localhost:8080
//   METRICS_URLS реплики, с которых снимать /metrics, если запросы идут через
//               балансировщик (например, http://localhost:8080,http://localhost:8082
//               при PROXY_URLS=http://localhost:8090). По умолчанию PROXY_URLS
//   BOOKS       размер keyspace, книги 1001..1000+BOOKS (migrations/bench_seed.sql)
//   SKEW        перекос популярности, 1 = равномерно, больше = сильнее
//   RATE        цепочек в секунду
//   WARMUP, DURATION, THINK_MS, LABEL, RESULT_FILE
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Gauge, Rate } from 'k6/metrics';

const PROXIES = (__ENV.PROXY_URLS || 'http://localhost:8080').split(',').map((s) => s.trim()).filter(Boolean);
const METRICS = (__ENV.METRICS_URLS || PROXIES.join(',')).split(',').map((s) => s.trim()).filter(Boolean);
const FIRST_BOOK = 1001;
const BOOKS = parseInt(__ENV.BOOKS || '2000', 10);
const SKEW = parseFloat(__ENV.SKEW || '2');
const RATE = parseInt(__ENV.RATE || '40', 10);
const WARMUP = __ENV.WARMUP || '30s';
const DURATION = __ENV.DURATION || '2m';
const THINK_MS = parseInt(__ENV.THINK_MS || '150', 10);
const LABEL = __ENV.LABEL || '';

const COUNTERS = {
  hits: 'proxy_cache_hits_total',
  prefetch_hits: 'proxy_cache_prefetch_hits_total',
  misses: 'proxy_cache_misses_total',
  bypasses: 'proxy_cache_bypasses_total',
  upstream_client: 'proxy_upstream_requests_total{kind="client"}',
  upstream_prefetch: 'proxy_upstream_requests_total{kind="prefetch"}',
  prefetch_executed: 'proxy_prefetch_executed_total',
};

const cacheHit = new Rate('cache_hit');
const startGauges = {};
const endGauges = {};
for (const name of Object.keys(COUNTERS)) {
  startGauges[name] = new Gauge(`srv_start_${name}`);
  endGauges[name] = new Gauge(`srv_end_${name}`);
}

const load = {
  executor: 'constant-arrival-rate',
  rate: RATE,
  timeUnit: '1s',
  preAllocatedVUs: Math.max(10, RATE),
  maxVUs: Math.max(50, RATE * 5),
  exec: 'chain',
};

export const options = {
  scenarios: {
    warmup: { ...load, duration: WARMUP, tags: { phase: 'warmup' } },
    snapshot: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      startTime: WARMUP,
      exec: 'snapshot',
      tags: { phase: 'snapshot' },
    },
    measure: { ...load, startTime: WARMUP, duration: DURATION, tags: { phase: 'measure' } },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  // Thresholds on tagged sub-metrics make k6 report them separately; only the
  // error rate is a real pass/fail condition.
  thresholds: {
    'http_req_failed{phase:measure}': ['rate<0.01'],
    'http_req_duration{phase:measure}': ['p(99)>=0'],
    'http_req_duration{phase:measure,step:card}': ['p(99)>=0'],
    'http_req_duration{phase:measure,step:authors}': ['p(99)>=0'],
    'cache_hit{phase:measure,step:card}': ['rate>=0'],
    'cache_hit{phase:measure,step:authors}': ['rate>=0'],
  },
};

function pickBook() {
  return FIRST_BOOK + Math.floor(BOOKS * Math.pow(Math.random(), SKEW));
}

function pickProxy() {
  return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

function isHit(res) {
  const status = res.headers['X-Cache-Status'];
  return status === 'HIT' || status === 'PREFETCH';
}

export function chain() {
  const id = pickBook();
  const card = http.get(`${pickProxy()}/books/${id}`, { tags: { step: 'card', name: 'card' } });
  check(card, { 'card 200': (r) => r.status === 200 });
  cacheHit.add(isHit(card), { step: 'card' });

  sleep(THINK_MS / 1000);

  const authors = http.get(`${pickProxy()}/books/${id}/authors`, {
    headers: { Referer: `/books/${id}` },
    tags: { step: 'authors', name: 'authors' },
  });
  check(authors, { 'authors 200': (r) => r.status === 200 });
  cacheHit.add(isHit(authors), { step: 'authors' });
}

function scrapeCounters() {
  const totals = {};
  for (const name of Object.keys(COUNTERS)) totals[name] = 0;
  for (const proxy of METRICS) {
    const res = http.get(`${proxy}/metrics`, { tags: { name: 'metrics' } });
    if (res.status !== 200) throw new Error(`scrape ${proxy}/metrics: HTTP ${res.status}`);
    const values = {};
    for (const line of res.body.split('\n')) {
      if (line === '' || line.startsWith('#')) continue;
      const cut = line.lastIndexOf(' ');
      values[line.slice(0, cut)] = parseFloat(line.slice(cut + 1));
    }
    for (const [name, series] of Object.entries(COUNTERS)) totals[name] += values[series] || 0;
  }
  return totals;
}

export function snapshot() {
  const totals = scrapeCounters();
  for (const name of Object.keys(COUNTERS)) startGauges[name].add(totals[name]);
}

export function teardown() {
  // proxy_prefetch_executed_total is flushed by a 2s ticker in the proxy.
  sleep(3);
  const totals = scrapeCounters();
  for (const name of Object.keys(COUNTERS)) endGauges[name].add(totals[name]);
}

function seconds(duration) {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(duration);
  if (!match) return NaN;
  const factor = { ms: 0.001, s: 1, m: 60, h: 3600 }[match[2]];
  return parseFloat(match[1]) * factor;
}

function metricValue(data, name, stat) {
  const metric = data.metrics[name];
  return metric && metric.values ? metric.values[stat] : undefined;
}

function fmtMs(value) {
  return value === undefined ? 'n/a' : `${value.toFixed(2)}ms`;
}

function fmtPct(value) {
  return value === undefined || Number.isNaN(value) ? 'n/a' : `${(value * 100).toFixed(1)}%`;
}

export function handleSummary(data) {
  const delta = {};
  for (const name of Object.keys(COUNTERS)) {
    delta[name] = (metricValue(data, `srv_end_${name}`, 'value') || 0) - (metricValue(data, `srv_start_${name}`, 'value') || 0);
  }
  const served = delta.hits + delta.prefetch_hits + delta.misses + delta.bypasses;
  const upstreamTotal = delta.upstream_client + delta.upstream_prefetch;
  const window = seconds(DURATION);
  const latency = (tags, stat) => metricValue(data, `http_req_duration{${tags}}`, stat);

  const result = {
    label: LABEL,
    proxies: METRICS.length,
    books: BOOKS,
    rate: RATE,
    duration_s: window,
    latency_ms: {
      all: { p50: latency('phase:measure', 'p(50)'), p95: latency('phase:measure', 'p(95)'), p99: latency('phase:measure', 'p(99)') },
      card: { p50: latency('phase:measure,step:card', 'p(50)'), p95: latency('phase:measure,step:card', 'p(95)'), p99: latency('phase:measure,step:card', 'p(99)') },
      authors: { p50: latency('phase:measure,step:authors', 'p(50)'), p95: latency('phase:measure,step:authors', 'p(95)'), p99: latency('phase:measure,step:authors', 'p(99)') },
    },
    client_hit_ratio: {
      card: metricValue(data, 'cache_hit{phase:measure,step:card}', 'rate'),
      authors: metricValue(data, 'cache_hit{phase:measure,step:authors}', 'rate'),
    },
    server: {
      ...delta,
      hit_ratio: served > 0 ? (delta.hits + delta.prefetch_hits) / served : null,
      upstream_rps: window > 0 ? upstreamTotal / window : null,
      upstream_prefetch_rps: window > 0 ? delta.upstream_prefetch / window : null,
      useless_prefetch_share: delta.prefetch_executed > 0 ? 1 - delta.prefetch_hits / delta.prefetch_executed : null,
    },
    failed_rate: metricValue(data, 'http_req_failed{phase:measure}', 'rate'),
  };

  const l = result.latency_ms;
  const s = result.server;
  const text = [
    `== prefetch-chain ${LABEL} (proxies=${METRICS.length}, entry=${PROXIES.length}, books=${BOOKS}, rate=${RATE}/s, measure=${DURATION})`,
    `latency all      p50=${fmtMs(l.all.p50)} p95=${fmtMs(l.all.p95)} p99=${fmtMs(l.all.p99)}`,
    `latency card     p50=${fmtMs(l.card.p50)} p95=${fmtMs(l.card.p95)} p99=${fmtMs(l.card.p99)}`,
    `latency authors  p50=${fmtMs(l.authors.p50)} p95=${fmtMs(l.authors.p95)} p99=${fmtMs(l.authors.p99)}`,
    `client hit ratio card=${fmtPct(result.client_hit_ratio.card)} authors=${fmtPct(result.client_hit_ratio.authors)}`,
    `server hit ratio ${fmtPct(s.hit_ratio)}, upstream ${s.upstream_rps === null ? 'n/a' : s.upstream_rps.toFixed(1)} rps ` +
      `(prefetch ${s.upstream_prefetch_rps === null ? 'n/a' : s.upstream_prefetch_rps.toFixed(1)} rps)`,
    `prefetch executed=${s.prefetch_executed} hits=${s.prefetch_hits} useless=${fmtPct(s.useless_prefetch_share === null ? undefined : s.useless_prefetch_share)}`,
    `failed requests  ${fmtPct(result.failed_rate)}`,
    '',
  ].join('\n');

  const out = { stdout: text };
  if (__ENV.RESULT_FILE) out[__ENV.RESULT_FILE] = JSON.stringify(result, null, 2);
  return out;
}
