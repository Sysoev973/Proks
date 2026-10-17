-- Версия карточки и transactional outbox для инвалидации кэша.
-- Скрипт идемпотентен: upstream применяет его при каждом старте, поэтому он
-- работает и на уже существующем volume pgdata, где 001 больше не выполняется.

ALTER TABLE books ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

-- Строка пишется в той же транзакции, что и UPDATE books. Фоновый relay
-- публикует её в RabbitMQ как событие invalidate и удаляет только после
-- publisher confirm. version = 0 означает сброс без версии (например,
-- list:books при смене жанра): списки не несут версию одной книги.
CREATE TABLE IF NOT EXISTS invalidation_outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_id   TEXT        NOT NULL UNIQUE DEFAULT gen_random_uuid()::text,
    tags       TEXT[]      NOT NULL CHECK (cardinality(tags) > 0),
    version    BIGINT      NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempts   INT         NOT NULL DEFAULT 0,
    last_error TEXT
);
