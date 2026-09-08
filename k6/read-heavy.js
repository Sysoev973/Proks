import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '1m', target: 20 },
    { duration: '2m', target: 40 },
    { duration: '1m', target: 10 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

export default function () {
  const res = http.get('http://localhost:8080/books/42', {
    headers: {
      'X-User-ID': `user-${__VU}`,
      'X-Tenant': 'acme',
      'X-Locale': 'ru',
    },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
  sleep(0.1);
}
