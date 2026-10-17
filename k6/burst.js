import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const P95_MS = Number(__ENV.P95_MS || 50);

export const options = {
  scenarios: {
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { target: 80, duration: '30s' },
        { target: 20, duration: '30s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: [`p(95)<${P95_MS}`],
  },
};

export default function () {
  const res = http.get(`${BASE_URL}/books/1`, {
    headers: {
      'X-User-ID': `user-${__VU}`,
      'X-Tenant': 'acme',
      'X-Locale': 'ru',
    },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
}
