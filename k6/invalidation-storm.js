import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 25,
  duration: '2m',
};

export default function () {
  const mode = Math.random();
  if (mode < 0.9) {
    const res = http.get('http://proxy:8080/books/1?tenant=acme&locale=ru');
    check(res, { 'status is 200': (r) => r.status === 200 });
    return;
  }
  const version = __VU * 1000000 + __ITER + 1;
  const payload = JSON.stringify({ book_id: '1', version, type: 'book.updated' });
  const res = http.post('http://proxy:8080/internal/events/book.updated', payload, {
    headers: {
      'Content-Type': 'application/json',
    },
  });
  check(res, { 'event accepted': (r) => r.status === 202 });
}
