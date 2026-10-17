import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const ADMIN_TOKEN = __ENV.ADMIN_TOKEN || 'dev-admin-token';
const P95_MS = Number(__ENV.P95_MS || 50);

export const options = {
  vus: 25,
  duration: '2m',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{kind:read}': [`p(95)<${P95_MS}`],
  },
};

export default function () {
  if (Math.random() < 0.9) {
    const res = http.get(`${BASE_URL}/books/1?tenant=acme&locale=ru`, { tags: { kind: 'read' } });
    check(res, { 'status is 200': (r) => r.status === 200 });
    return;
  }
  // Без version: purge без версии чистит кэш, но не трогает версии тегов.
  // Синтетическая версия (раньше VU*1e6+ITER) сохранялась бы в state.json
  // реплик и потом отклоняла настоящие события book:1 из outbox.
  const payload = JSON.stringify({ tags: ['book:1', 'list:books'] });
  const res = http.post(`${BASE_URL}/admin/purge`, payload, {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${ADMIN_TOKEN}`,
    },
    tags: { kind: 'purge' },
  });
  check(res, { 'purge accepted': (r) => r.status === 202 });
}
