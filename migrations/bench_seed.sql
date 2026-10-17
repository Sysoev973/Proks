-- Синтетические книги для нагрузочного стенда (k6/prefetch-chain.js).
-- В демо-данных 1000 книг (cmd/gendata). Для замера предзагрузки нужен
-- keyspace больше кэша, иначе всё сразу лежит в кэше и эффект не измерить.
-- Скрипт только добавляет строки (book_id 1001..10000) и безопасен для
-- повторного запуска.
--
--   docker compose exec -T postgres psql -U library_user -d library_db < migrations/bench_seed.sql

INSERT INTO books (book_id, genre_id, title, isbn, publication_year, is_rare, has_ebook)
SELECT id,
       (SELECT genre_id FROM genres ORDER BY genre_id LIMIT 1),
       'Bench book ' || id,
       '978-0-' || lpad(id::text, 8, '0'),
       1900 + id % 125,
       id % 10 = 0,
       id % 2 = 0
FROM generate_series(1001, 10000) AS id
ON CONFLICT (book_id) DO NOTHING;

INSERT INTO book_authors (book_id, author_id)
SELECT b.book_id, a.author_id
FROM generate_series(1001, 10000) AS b(book_id)
JOIN LATERAL (
    SELECT author_id FROM authors ORDER BY (author_id + b.book_id) % 7 LIMIT 1 + b.book_id % 2
) a ON TRUE
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('books', 'book_id'), (SELECT MAX(book_id) FROM books));
