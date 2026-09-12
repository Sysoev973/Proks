import http from 'k6/http';
import { check } from 'k6';

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
}
