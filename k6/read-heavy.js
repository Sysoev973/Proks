import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const P95_MS = Number(__ENV.P95_MS || 50);

export const options = {
  stages: [
    { duration: '1m', target: 20 },
    { duration: '2m', target: 40 },
    { duration: '1m', target: 10 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: [`p(95)<${P95_MS}`],
  },
};

export default function () {
  const bookID = Math.random() < 0.7 ? '1' : '2';
  const res = http.get(`${BASE_URL}/books/${bookID}`, {
    headers: {
      'X-User-ID': `user-${__VU}`,
      'X-Tenant': 'acme',
      'X-Locale': 'ru',
    },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
  sleep(0.1);
}
