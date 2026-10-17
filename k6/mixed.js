import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const P95_MS = Number(__ENV.P95_MS || 50);
// Books seeded by csv_files/books.csv (see cmd/gendata).
const BOOKS = Number(__ENV.BOOKS || 1000);
const GENRES = 5;
const READERS = 5;
const WORDS = ['мир', 'война', 'основание', 'шерлок', 'go', 'том', 'история', 'город'];

export const options = {
  stages: [
    { duration: '1m', target: 10 },
    { duration: '2m', target: 25 },
    { duration: '1m', target: 5 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{kind:read}': [`p(95)<${P95_MS}`],
  },
};

const headers = {
  'X-Tenant': 'acme',
  'X-Locale': 'ru',
};

// zipfID picks small IDs much more often than large ones, like real traffic.
function zipfID(n) {
  return Math.min(n, Math.floor(Math.pow(n + 1, Math.random())));
}

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function get(path, kind) {
  return http.get(`${BASE_URL}${path}`, { headers, tags: { kind, endpoint: path.split('?')[0].replace(/\/\d+/g, '/{id}') } });
}

export default function () {
  const roll = Math.random();
  let res;
  if (roll < 0.35) {
    res = get(`/books/${zipfID(BOOKS)}`, 'read');
  } else if (roll < 0.5) {
    res = get(`/books?page=${zipfID(20)}&genre=${zipfID(GENRES)}`, 'read');
  } else if (roll < 0.6) {
    // Browsing: a book page, then its authors. The Referer teaches the
    // proxy's Markov chain the transition, so it starts prefetching it.
    const id = zipfID(BOOKS);
    get(`/books/${id}`, 'read');
    res = http.get(`${BASE_URL}/books/${id}/authors`, {
      headers: { ...headers, Referer: `${BASE_URL}/books/${id}` },
      tags: { kind: 'read', endpoint: '/books/{id}/authors' },
    });
  } else if (roll < 0.7) {
    res = Math.random() < 0.5 ? get('/genres', 'read') : get(`/genres/${zipfID(GENRES)}`, 'read');
  } else if (roll < 0.85) {
    // Many unique keys: puts pressure on admission.
    res = get(`/search?q=${encodeURIComponent(pick(WORDS) + (Math.random() < 0.5 ? '' : ` ${__ITER}`))}`, 'read');
  } else if (roll < 0.97) {
    // Personal response: Cache-Control: private, expected BYPASS.
    res = get(`/readers/${zipfID(READERS)}/loans`, 'personal');
    check(res, { 'personal response bypasses cache': (r) => r.headers['X-Cache-Status'] === 'BYPASS' });
  } else {
    const id = zipfID(BOOKS);
    res = http.put(`${BASE_URL}/books/${id}`, JSON.stringify({ title: `Книга ${id} (ред. ${__ITER})` }), {
      headers: { ...headers, 'Content-Type': 'application/json' },
      tags: { kind: 'write' },
    });
  }
  check(res, { 'status in 200..299': (r) => r.status >= 200 && r.status < 300 });
  sleep(0.2);
}
