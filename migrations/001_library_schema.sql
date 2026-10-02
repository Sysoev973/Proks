-- 1. Филиалы
CREATE TABLE IF NOT EXISTS branches (
                          branch_id BIGSERIAL PRIMARY KEY,
                          name VARCHAR(255) NOT NULL,
                          address VARCHAR(500) NOT NULL,
                          phone VARCHAR(50)
);

-- 2. Жанры
CREATE TABLE IF NOT EXISTS genres (
                        genre_id BIGSERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL UNIQUE
);

-- 3. Авторы
CREATE TABLE IF NOT EXISTS authors (
                         author_id BIGSERIAL PRIMARY KEY,
                         full_name VARCHAR(255) NOT NULL
);

-- 4. Читатели
CREATE TABLE IF NOT EXISTS readers (
                         reader_id BIGSERIAL PRIMARY KEY,
                         card_number VARCHAR(50) NOT NULL UNIQUE,
                         full_name VARCHAR(255) NOT NULL,
                         phone VARCHAR(50),
                         email VARCHAR(100) UNIQUE,
                         status VARCHAR(20) NOT NULL DEFAULT 'Active',
                         created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 5. Книги
CREATE TABLE IF NOT EXISTS books (
                       book_id BIGSERIAL PRIMARY KEY,
                       genre_id BIGINT REFERENCES genres(genre_id),
                       title VARCHAR(255) NOT NULL,
                       isbn VARCHAR(20),
                       publication_year INT,
                       is_rare BOOLEAN NOT NULL DEFAULT FALSE,
                       has_ebook BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_books_genre_id ON books (genre_id);

-- 6. Связь книг и авторов (многие-ко-многим)
CREATE TABLE IF NOT EXISTS book_authors (
                              book_id BIGINT NOT NULL REFERENCES books(book_id) ON DELETE CASCADE,
                              author_id BIGINT NOT NULL REFERENCES authors(author_id) ON DELETE CASCADE,
                              PRIMARY KEY (book_id, author_id)
);
CREATE INDEX IF NOT EXISTS idx_book_authors_author_id ON book_authors (author_id);

-- 7. Сотрудники (Библиотекари)
CREATE TABLE IF NOT EXISTS librarians (
                            librarian_id BIGSERIAL PRIMARY KEY,
                            branch_id BIGINT REFERENCES branches(branch_id),
                            full_name VARCHAR(255) NOT NULL,
                            position VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_librarians_branch_id ON librarians (branch_id);

-- 8. Физические экземпляры книг
CREATE TABLE IF NOT EXISTS book_items (
                            item_id BIGSERIAL PRIMARY KEY,
                            book_id BIGINT NOT NULL REFERENCES books(book_id),
                            branch_id BIGINT NOT NULL REFERENCES branches(branch_id),
                            inventory_number VARCHAR(100) NOT NULL UNIQUE,
                            status VARCHAR(20) NOT NULL DEFAULT 'Available'
);
CREATE INDEX IF NOT EXISTS idx_book_items_book_id ON book_items (book_id);
CREATE INDEX IF NOT EXISTS idx_book_items_branch_id ON book_items (branch_id);

-- 9. Выдачи (аренда) книг
CREATE TABLE IF NOT EXISTS loans (
                       loan_id BIGSERIAL PRIMARY KEY,
                       reader_id BIGINT NOT NULL REFERENCES readers(reader_id),
                       item_id BIGINT NOT NULL REFERENCES book_items(item_id),
                       librarian_id BIGINT NOT NULL REFERENCES librarians(librarian_id),
                       issue_date TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       due_date TIMESTAMPTZ NOT NULL,
                       actual_return_date TIMESTAMPTZ,
                       renewals_count INT NOT NULL DEFAULT 0,
                       state_on_return VARCHAR(255),
                       status VARCHAR(20) NOT NULL DEFAULT 'Active'
);
CREATE INDEX IF NOT EXISTS idx_loans_reader_id ON loans (reader_id);
CREATE INDEX IF NOT EXISTS idx_loans_item_id ON loans (item_id);
CREATE INDEX IF NOT EXISTS idx_loans_librarian_id ON loans (librarian_id);

-- 10. Штрафы
CREATE TABLE IF NOT EXISTS fines (
                       fine_id BIGSERIAL PRIMARY KEY,
                       loan_id BIGINT NOT NULL REFERENCES loans(loan_id),
                       amount NUMERIC(10, 2) NOT NULL,
                       status VARCHAR(20) NOT NULL DEFAULT 'Unpaid',
                       calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       paid_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_fines_loan_id ON fines (loan_id);

-- 11. Бронирования
CREATE TABLE IF NOT EXISTS reservations (
                              reservation_id BIGSERIAL PRIMARY KEY,
                              reader_id BIGINT NOT NULL REFERENCES readers(reader_id),
                              book_id BIGINT NOT NULL REFERENCES books(book_id),
                              branch_id BIGINT NOT NULL REFERENCES branches(branch_id),
                              created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                              expires_at TIMESTAMPTZ,
                              status VARCHAR(20) NOT NULL DEFAULT 'Pending'
);
CREATE INDEX IF NOT EXISTS idx_reservations_reader_id ON reservations (reader_id);
CREATE INDEX IF NOT EXISTS idx_reservations_book_id ON reservations (book_id);
CREATE INDEX IF NOT EXISTS idx_reservations_branch_id ON reservations (branch_id);
CREATE INDEX IF NOT EXISTS idx_reservations_reader_id_book_id ON reservations (reader_id, book_id);

-- 12. Доступ к электронным изданиям
CREATE TABLE IF NOT EXISTS ebook_access (
                              access_id BIGSERIAL PRIMARY KEY,
                              reader_id BIGINT NOT NULL REFERENCES readers(reader_id),
                              book_id BIGINT NOT NULL REFERENCES books(book_id),
                              granted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                              expires_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_ebook_access_reader_id ON ebook_access (reader_id);
CREATE INDEX IF NOT EXISTS idx_ebook_access_book_id ON ebook_access (book_id);

-- ============================================================================
-- 3. Импорт данных из CSV-файлов через COPY
-- ============================================================================

COPY branches(branch_id, name, address, phone)
    FROM '/tmp/csv_files/branches.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY genres(genre_id, name)
    FROM '/tmp/csv_files/genres.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY authors(author_id, full_name)
    FROM '/tmp/csv_files/authors.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY readers(reader_id, card_number, full_name, phone, email, status, created_at)
    FROM '/tmp/csv_files/readers.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY books(book_id, genre_id, title, isbn, publication_year, is_rare, has_ebook)
    FROM '/tmp/csv_files/books.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY book_authors(book_id, author_id)
    FROM '/tmp/csv_files/book_authors.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY librarians(librarian_id, branch_id, full_name, position)
    FROM '/tmp/csv_files/librarians.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY book_items(item_id, book_id, branch_id, inventory_number, status)
    FROM '/tmp/csv_files/book_items.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY loans(loan_id, reader_id, item_id, librarian_id, issue_date, due_date, actual_return_date, renewals_count, state_on_return, status)
    FROM '/tmp/csv_files/loans.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY fines(fine_id, loan_id, amount, status, calculated_at, paid_at)
    FROM '/tmp/csv_files/fines.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY reservations(reservation_id, reader_id, book_id, branch_id, created_at, expires_at, status)
    FROM '/tmp/csv_files/reservations.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY ebook_access(access_id, reader_id, book_id, granted_at, expires_at)
    FROM '/tmp/csv_files/ebook_access.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- ============================================================================
-- 4. Выравнивание автоинкрементных счетчиков
-- ============================================================================
SELECT setval(pg_get_serial_sequence('branches', 'branch_id'), COALESCE(MAX(branch_id), 1)) FROM branches;
SELECT setval(pg_get_serial_sequence('genres', 'genre_id'), COALESCE(MAX(genre_id), 1)) FROM genres;
SELECT setval(pg_get_serial_sequence('authors', 'author_id'), COALESCE(MAX(author_id), 1)) FROM authors;
SELECT setval(pg_get_serial_sequence('readers', 'reader_id'), COALESCE(MAX(reader_id), 1)) FROM readers;
SELECT setval(pg_get_serial_sequence('books', 'book_id'), COALESCE(MAX(book_id), 1)) FROM books;
SELECT setval(pg_get_serial_sequence('librarians', 'librarian_id'), COALESCE(MAX(librarian_id), 1)) FROM librarians;
SELECT setval(pg_get_serial_sequence('book_items', 'item_id'), COALESCE(MAX(item_id), 1)) FROM book_items;
SELECT setval(pg_get_serial_sequence('loans', 'loan_id'), COALESCE(MAX(loan_id), 1)) FROM loans;
SELECT setval(pg_get_serial_sequence('fines', 'fine_id'), COALESCE(MAX(fine_id), 1)) FROM fines;
SELECT setval(pg_get_serial_sequence('reservations', 'reservation_id'), COALESCE(MAX(reservation_id), 1)) FROM reservations;
SELECT setval(pg_get_serial_sequence('ebook_access', 'access_id'), COALESCE(MAX(access_id), 1)) FROM ebook_access;

-- ============================================================================
-- 5. Запросы (10 штук с условиями)
-- ============================================================================
SELECT reader_id, card_number, full_name, email, status FROM readers WHERE status = 'Active';
SELECT book_id, title, publication_year, isbn FROM books WHERE is_rare = TRUE;
SELECT fine_id, loan_id, amount, status, calculated_at FROM fines WHERE status = 'Unpaid' AND amount > 200.00;
SELECT title, publication_year, isbn FROM books WHERE publication_year < 2000;
SELECT item_id, book_id, branch_id, inventory_number FROM book_items WHERE status = 'Available';
SELECT reservation_id, reader_id, book_id, branch_id, created_at FROM reservations WHERE status = 'Pending' ORDER BY created_at DESC;
SELECT loan_id, reader_id, item_id, issue_date, renewals_count FROM loans WHERE renewals_count > 0;
SELECT reader_id, card_number, full_name, phone FROM readers WHERE status = 'Blocked';
SELECT access_id, reader_id, book_id, granted_at FROM ebook_access WHERE granted_at >= '2024-01-01 00:00:00+00' AND granted_at <= '2024-12-31 23:59:59+00';
SELECT librarian_id, full_name, position FROM librarians WHERE branch_id = 1;