import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 25,
  duration: '2m',
};

export default function () {
  const mode = Math.random();
  if (mode < 0.9) {
    const res = http.get('http://localhost:8080/books/42?tenant=acme&locale=ru');
    check(res, { 'status is 200': (r) => r.status === 200 });
    return;
  }
  const payload = JSON.stringify({ book_id: '42', version: 1000, type: 'book.updated' });
  const res = http.post('http://localhost:8080/internal/events/book.updated', payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'event accepted': (r) => r.status >= 200 && r.status < 300 });
}
