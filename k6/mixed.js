import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '1m', target: 10 },
    { duration: '2m', target: 25 },
    { duration: '1m', target: 5 },
  ],
};

export default function () {
  const path = Math.random() < 0.7 ? '/books/42' : '/books/99';
  const res = http.get('http://localhost:8080/books/42', {
    headers: {
      'X-User-ID': `user-${__VU}`,
      'X-Tenant': 'acme',
      'X-Locale': 'ru',
    },
  });
  check(res, { 'status in 200..299': (r) => r.status >= 200 && r.status < 300 });
  sleep(0.2);
}
