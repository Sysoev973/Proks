# Cервис-прокси кеширования

В данном сервисе-прокси handler обрабатывает попадание в кэш за ~5 мкс (бенчмарк на Intel Core i7-10870H, без строки access-лога; лог добавляет ещё 2–5 мкс в зависимости от машины), а p95 по HTTP на локальном стенде — 1–2 мс (k6, см. «Результаты измерений»), что в больших высоконагруженных системах с иерархическими или связанными данными предоставляет возможность значительно увеличить пропускную способность и уменьшить задержку между запросами до минимума, что всегда было и будет актуально из-за большого количества пользователей, которые увлекаются чтением(книг, манги и т.п).

В дальнейшем данный прокси реализуется, как универсальный, для быстрой обработки данных в ваших проектах без каких либо задержек, зависаний сайтов, и долгой загрузки страниц, чтобы дать вам возможность максимально эффективно распоряжаться вашим временем, которое вы уделяете на свое любимое дело.

### План запуска и тестирования проекта:

---

## 🛠 Предварительные требования

- **Go**: версия 1.25 или выше.
- **Docker & Docker Compose & Docker daemon**: для контейнеризации и запуска полного стека.
- **k6**: (опционально) для проведения нагрузочных тестов.

  ## Установка зависимостей проекта:

  ```bash
  winget install GoLang.Go
  go mod download
  ```

  ## Установка k6:
  - **Windows**

  ```powershell
  winget install k6
  ```

  - **MacOS**

  ```bash
  brew install k6
  ```

  - **Linux**

  Универсальный(Snap)

  ```bash
  sudo snap install k6
  ```

  Debian / Ubuntu (APT)

  ```bash
  sudo gpg -k
  sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
  echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
  sudo apt-get update
  sudo apt-get install k6
  ```

# Windows (PowerShell):

## **1. Подготовка:**

- **1.1. Скачайте архив проекта.**

- **1.2. Запустите приложение Docker Desktop.**

## **2. Переход в директорию и запуск терминалов:**

- **2.1. Перейдите в папку проекта:**

```powershell
cd C:\(Путь до директории проекта)
```

- **2.2. Откройте 3 терминала PowerShell.**

- **2.3. (Опционально) Запустите контейнер RabbitMQ:**
  
```powershell
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
  
## **3. Запуск сервисов:**

- **3.1. Терминал 1 (Прокси-сервер):**

```powershell
$env:RABBITMQ_URL="amqp://guest:guest@localhost:5672/"  # без него используется in-memory брокер
$env:ADMIN_TOKEN="dev-admin-token"
$env:PROXY_REPLICA_ID="proxy-local"
$env:EVENT_VERSION_STATE_PATH="$PWD\data\events\state.json"
$env:EVENT_LOG_PATH="$PWD\data\events\events.log"
go run main.go
```

- **3.2. Терминал 2 (Upstream):**

```powershell
$env:DB_DSN="postgres://library_user:library_password@localhost:5432/library_db?sslmode=disable"
$env:LISTEN_ADDR=":8081"
go run .\cmd\upstream
```

Для локального запуска upstream без Docker PostgreSQL должен быть доступен на `localhost:5432`.
Проверяйте готовность отдельным endpoint, а не запросом к конкретной книге:

```powershell
curl.exe -i http://localhost:8081/health/live
curl.exe -i http://localhost:8081/health/ready
curl.exe -i http://localhost:8080/health/live
curl.exe -i http://localhost:8080/health/ready
```

## Политика кэширования

Политика задаётся через `proxy.CachePolicy` и передаётся в `NewHandlerWithPolicy`. По умолчанию кэшируются только успешные `GET`-ответы с TTL 5 минут. Запросы с `Authorization` или `Cookie`, а также ответы с `Set-Cookie`, `Cache-Control: private/no-store` и неподдерживаемым `Vary` обходят кэш и всегда идут в upstream. При явном разрешении персонализированного кэша значения `Authorization` и `Cookie` представлены в ключе только SHA-256 fingerprint, а не исходным секретом.

RabbitMQ опционален. Если `RABBITMQ_URL` не задан, прокси использует in-memory брокер: инвалидация работает только внутри текущей реплики. Если `RABBITMQ_URL` задан, но RabbitMQ недоступен в течение `RABBITMQ_CONNECT_TIMEOUT` (по умолчанию `60s`), прокси пишет предупреждение и тоже переходит на in-memory брокер; чтобы вместо этого завершаться с ошибкой, задайте `RABBITMQ_REQUIRED=true` (так настроен `docker-compose.yml`).

Endpoint `POST /internal/events/book.updated` защищён:

- `ADMIN_TOKEN` — требует заголовок `Authorization: Bearer <token>`;
- `ADMIN_LISTEN_ADDR` (например, `127.0.0.1:9091`) — endpoint обслуживается только на отдельном admin-порту, а на публичном порту отвечает `404`. Токен при этом тоже проверяется, если задан;
- если не задано ни то, ни другое, endpoint отключён (`404`).

```powershell
curl.exe -i -X POST http://localhost:8080/internal/events/book.updated -H "Authorization: Bearer dev-admin-token" -H "Content-Type: application/json" -d '{\"type\":\"book.updated\",\"book_id\":\"1\",\"version\":2}'
```

Для RabbitMQ каждая реплика использует собственную durable-очередь. Обязательная переменная `PROXY_REPLICA_ID` должна быть стабильной и уникальной для реплики; без неё RabbitMQ-брокер не запускается. Для сохранения deduplication-версий между перезапусками задайте `EVENT_VERSION_STATE_PATH` на постоянный том.

Publisher использует persistent messages и RabbitMQ publisher confirms. `EVENT_LOG_PATH` и `EVENT_VERSION_STATE_PATH` обязательны для RabbitMQ-брокера: событие перед публикацией добавляется в durable append-only event log, а обработанные `event_id` и версии сохраняются атомарно. Event log повторно проигрывается при старте consumer-а и покрывает события, опубликованные до создания очереди. Для нескольких независимых узлов event log должен находиться на общем durable-хранилище либо быть заменён PostgreSQL outbox/RabbitMQ Stream.

Publisher имеет отдельное AMQP-соединение и автоматически переподключает channel/connection после `NotifyClose` или ошибки подтверждения. Публикация сериализована, поэтому confirmation однозначно относится к текущему `event_id`; после переподключения событие повторяется с тем же идентификатором.

## **4. Проверка и тестирование (Терминал 3):**

- **4.1. Проверка работоспособности (Healthcheck):**

```powershell
curl.exe -i http://localhost:8080/books/1
```

- **4.2. Базовые unit-тесты и проверка на состояния гонки (Race Detector):**

```powershell
go test -v .\...
go test -count=1 ./...
```

```
$env:CGO_ENABLED="1"
go test -race ./...
```
Третья команда -race, может выдавать ошибки

- **4.3. Проверка покрытия кода (Code Coverage):**

```powershell
go test "-coverprofile=coverage.out" .\internal\...
go tool cover -html="coverage.out"
```

- **4.4. Тестирование отдельных пакетов:**

```powershell
go test -v .\internal\cache\...
go test -v .\internal\predictor\...
go test -v .\internal\proxy\...
```

- **4.5. Оценка производительности (Бенчмарки):**

```powershell
go test "-bench=." -benchmem .\...
```

- **4.6. Нагрузочное тестирование (k6):**

Скрипты берут адрес из `BASE_URL` (по умолчанию `http://localhost:8080`) и токен из `ADMIN_TOKEN` (по умолчанию `dev-admin-token`), поэтому запускаются без правок двумя способами:

```
# с хоста (прокси слушает localhost:8080)
k6 run k6\read-heavy.js
k6 run k6\burst.js
k6 run k6\invalidation-storm.js
k6 run k6\mixed.js

# внутри docker-сети (сервис k6 в профиле load, BASE_URL=http://proxy:8080)
docker compose --profile load run --rm k6 run /scripts/burst.js
```

| Скрипт | Что проверяет |
|---|---|
| `read-heavy.js` | чтение двух горячих ключей |
| `burst.js` | всплеск до 80 запросов/с |
| `invalidation-storm.js` | чтение на фоне `POST /admin/purge` |
| `mixed.js` | все эндпоинты upstream: карточки, списки, авторы, жанры, поиск, личные выдачи (`BYPASS`), `PUT /books/{id}` |

В каждом скрипте есть `thresholds` (`http_req_failed < 1%`, `p(95) < 50ms`, порог меняется через `P95_MS`), при деградации k6 завершается с ненулевым кодом. Длительность можно сократить флагами, например `k6 run --duration 10s --vus 10 k6\mixed.js`.

# Linux / macOS (Bash / Zsh):

## **1. Подготовка:**

- **1.1. Скачайте архив проекта.**

- **1.2. Запустите приложение Docker Daemon/Docker Deskstop:**

```bash
sudo systemctl start docker
```

## **2. Перейдите в папку проекта:**

```bash
cd /path/to/your/directory
```

- **3. (Опционально) Запустите контейнер RabbitMQ:**

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

- **3.1. Откройте 3 терминала (или вкладки / tmux).**

## **4. Запуск сервисов:**

- **4.1. Терминал 1 (Прокси-сервер):**

```bash
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"  # без него используется in-memory брокер
export ADMIN_TOKEN="dev-admin-token"
export PROXY_REPLICA_ID="proxy-local"
export EVENT_VERSION_STATE_PATH="$PWD/data/events/state.json"
export EVENT_LOG_PATH="$PWD/data/events/events.log"
go run main.go
```

- **4.2. Терминал 2 (Upstream):**

```bash
export DB_DSN="postgres://library_user:library_password@localhost:5432/library_db?sslmode=disable"
export LISTEN_ADDR=":8081"
go run ./cmd/upstream
```

Миграция `migrations/001_library_schema.sql` выполняется PostgreSQL только при
первом создании пустого volume. Изменение этого файла не применяет его к уже
существующему `pgdata`. Для чистого demo/reset запуска:

```powershell
docker compose down -v
docker compose up -d --build postgres
```

`migrations/reset_demo.sql` является разрушительным reset-скриптом и не
подключается обычным `docker compose` запуском.

Интеграционный тест PostgreSQL:

```powershell
$env:TEST_DATABASE_URL="postgres://library_user:library_password@localhost:5432/library_db?sslmode=disable"
go test .\cmd\upstream -run TestUpstreamAgainstPostgres -v
```

## **5. Проверка и тестирование (Терминал 3):**

- **5.1. Проверка работоспособности (Healthcheck):**

```bash
curl -i http://localhost:8080/books/1
```

- **5.2. Базовые unit-тесты и проверка на состояния гонки (Race Detector):**

```bash
go test -v ./...
go test -race ./...
```

- **5.3. Проверка покрытия кода (Code Coverage):**

```bash
go test -coverprofile=coverage.out ./internal/...
go tool cover -html=coverage.out
```

- **5.4. Тестирование отдельных пакетов:**

```bash
go test -v ./internal/cache/...
go test -v ./internal/predictor/...
go test -v ./internal/proxy/...
```

- **5.5. Оценка производительности (Бенчмарки):**

```bash
go test -bench=. -benchmem ./...
```

- **5.6. Нагрузочное тестирование (k6):**

Скрипты берут адрес из `BASE_URL` (по умолчанию `http://localhost:8080`) и токен из `ADMIN_TOKEN` (по умолчанию `dev-admin-token`), поэтому запускаются без правок двумя способами:

```
# с хоста (прокси слушает localhost:8080)
k6 run k6/read-heavy.js
k6 run k6/burst.js
k6 run k6/invalidation-storm.js
k6 run k6/mixed.js

# внутри docker-сети (сервис k6 в профиле load, BASE_URL=http://proxy:8080)
docker compose --profile load run --rm k6 run /scripts/burst.js
```

| Скрипт | Что проверяет |
|---|---|
| `read-heavy.js` | чтение двух горячих ключей |
| `burst.js` | всплеск до 80 запросов/с |
| `invalidation-storm.js` | чтение на фоне `POST /admin/purge` |
| `mixed.js` | все эндпоинты upstream: карточки, списки, авторы, жанры, поиск, личные выдачи (`BYPASS`), `PUT /books/{id}` |

В каждом скрипте есть `thresholds` (`http_req_failed < 1%`, `p(95) < 50ms`, порог меняется через `P95_MS`), при деградации k6 завершается с ненулевым кодом. Длительность можно сократить флагами, например `k6 run --duration 10s --vus 10 k6/mixed.js`.

# Результаты измерений

Только измеренные значения. Железо: Intel Core i7-10870H (16 потоков), Windows 10, Go 1.27.

**Внутри прокси** (`go test -run x -bench . -benchmem ./internal/...`):

| Бенчмарк | Результат |
|---|---|
| `BenchmarkServeHTTPHit/log=info` — HIT через весь handler, без access-лога | 4.8 мкс/оп, 39 аллокаций |
| `BenchmarkServeHTTPHit/log=debug` — то же с `cache_decision` | 6.2 мкс/оп |
| `BenchmarkServeHTTPMiss/log=info` — MISS с upstream на loopback | 119 мкс/оп |
| `BenchmarkLookupParallel` — `Lookup` из 16 горутин | 52 нс/оп |
| `BenchmarkAccessLog` (корневой пакет) — строка `request_finished` | +2.2 мкс к запросу (на другой машине — ~4.7 мкс) |

**Доля попаданий** (`go test -run TestTinyLFUBeatsLRU -v ./internal/cache`, кэш 1000 ключей):

| Нагрузка | LRU | TinyLFU |
|---|---|---|
| Zipf, 50 000 ключей, s=1.1 | 0.692 | 0.728 |
| 800 горячих ключей + 50% одноразовых (scan) | 0.260 | 0.457 |

HIT вместе с access-логом здесь — около 7 мкс; на другой машине получилось 12 мкс. Сравнивайте цифры только на одном железе.

Тест падает, если TinyLFU не обгоняет LRU (на 0.02 для Zipf и на 0.10 для scan).

**По HTTP** (k6 с хоста, 10 VU, 8 с, прокси и upstream-заглушка на той же машине, без PostgreSQL):

| Скрипт | median | p95 | ошибки |
|---|---|---|---|
| `read-heavy.js` | 0.52 мс | 1.63 мс | 0% |
| `mixed.js` | 0.58 мс | 2.29 мс | 0% |

Цифры для docker-стенда с PostgreSQL ещё не сняты: `docker compose up -d --build`, затем `docker compose --profile load run --rm k6 run --summary-trend-stats "med,p(95),p(99)" /scripts/mixed.js`.

# Конфигурация прокси

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `UPSTREAM_URL`, `LISTEN_ADDR` | `http://localhost:8081`, `:8080` | адреса |
| `CACHE_CAPACITY`, `CACHE_MAX_BYTES` | `10000`, `268435456` | размер кэша |
| `ROUTES_CONFIG` | — | YAML с тегами по маршрутам, пример в `routes.example.yaml` |
| `ADMIN_TOKEN`, `ADMIN_LISTEN_ADDR` | — | доступ к `/admin/purge` и `/internal/events/book.updated` |
| `PREFETCH_ENABLED` | `false` | включает prefetch по цепи Маркова |
| `PREFETCH_RPS`, `PREFETCH_TIMEOUT`, `PREFETCH_WORKERS` | `20`, `1s`, `4` | бюджет prefetch-запросов к upstream |
| `LOG_LEVEL` | `info` | `debug` добавляет строку `cache_decision` с ключом |
| `LOG_HIT_SAMPLE` | `1` | логировать каждый N-й HIT |

Некорректное значение (например, `CACHE_CAPACITY=abc`) — ошибка запуска, а не молчаливое значение по умолчанию. Логи — JSON (`log/slog`), одна строка `request_finished` на запрос с полями `status`, `cache_status`, `reason`, `duration`.

## Инвалидация для любого API

Прокси ничего не знает о книгах. Теги ответа берутся из двух источников:

- маршруты из `ROUTES_CONFIG`: `{match: /orders/{id}, tags: ["order:{id}"]}`; `{query.name}` подставляет параметр запроса;
- заголовки upstream: `Surrogate-Key` (через пробел), `Cache-Tag` и `X-Cache-Depends-On` (через запятую).

Подходит любой префикс; тег — до 256 символов из букв, цифр и `:_-./@+=`, не больше 64 тегов на ответ.

Сброс по тегам или ключам кэша, с рассылкой по всем репликам через RabbitMQ:

```bash
curl -X POST http://localhost:8080/admin/purge -H "Authorization: Bearer dev-admin-token" \
  -d '{"tags": ["order:42", "list:orders"], "version": 2}'
```

Старый `POST /internal/events/book.updated` с `{"type":"book.updated","book_id":"1"}` работает как алиас и сбрасывает тег `book:1`.

## Prefetch

С `PREFETCH_ENABLED=true` прокси запоминает переходы между страницами (по `Referer` или `X-Prev-Route-Key`) и после запроса A заранее загружает вероятный следующий маршрут B. Prefetch идёт тем же путём, что обычный промах (singleflight, политика кэша, учёт инвалидаций), только для анонимных `GET` (без `Authorization` и `Cookie`), в рамках `PREFETCH_RPS` и приостанавливается на 5 с после ошибки upstream. Первый запрос к B после этого получает `X-Cache-Status: PREFETCH`. Метрики: `proxy_prefetch_executed_total`, `proxy_prefetch_failed_total`.

## Несколько реплик

Каждая реплика держит свой кэш в памяти; после перезапуска он пуст. Согласованность не страдает (purge доходит до всех реплик), но при N репликах upstream получает до N промахов на ключ. Самый дешёвый способ это убрать — балансировка по ключу на ingress, чтобы один и тот же URL всегда шёл в одну реплику. Пример для nginx:

```nginx
upstream proks {
    hash $request_uri consistent;
    server proxy-1:8080;
    server proxy-2:8080;
}
```

Хранилище спрятано за интерфейсом `cache.Store` (`internal/cache/cache.go`), поэтому общий L2 (Redis) или peer-кэш можно добавить без изменений в handler. Делать это стоит, только если метрики покажут, что промахи между репликами реально нагружают upstream.

# Покрытие и CI

GitHub Actions (`.github/workflows/ci.yml`) запускает `go vet`, `gofmt`, `go test -race` сначала без внешних сервисов, затем с RabbitMQ и PostgreSQL. Минимальное покрытие по пакетам задано в `.github/coverage-unit.txt` и `.github/coverage-integration.txt`; если покрытие падает ниже порога, CI падает. Отчёты (`coverage-*.out`, `.html`) публикуются как артефакт сборки и в репозитории не хранятся. Проверить локально:

```bash
go test -coverprofile=coverage.out ./...
sh scripts/check-coverage.sh coverage.out .github/coverage-unit.txt
```

Демо-каталог расширяется генератором: `go run ./cmd/gendata -books 1000` (первые 5 книг сохраняются).
