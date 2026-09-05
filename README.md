# Cервис-прокси кеширования
---
### Основной Этап перед разработкой:
1. Cache интерфейс + InMemoryCache (LRU, TTL, Delete, Snapshot).

2. proxy/handler:
2.1 Build cache key (user_id, tenant, locale, route-template, query).
2.2 HIT/MISS/BYPASS флоу.
2.3 Заголовки X-Cache-Status, X-Cache-Reason.

3. Upstream client с таймаутами/connection pooling.

4. main.go wiring + graceful shutdown.

5. Базовые тесты: key normalization, HIT/MISS/BYPASS, TTL expiry.
>Это и будет являться первым этапом
