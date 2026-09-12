### План запуска и тестирования проекта:

---

## 🛠 Предварительные требования

- **Go**: версия 1.22 или выше.
- **Docker & Docker Compose & Docker daemon**: для контейнеризации и запуска полного стека.
- **k6**: (опционально) для проведения нагрузочных тестов.

  ## Установка зависимостей проекта:

  ```bash
  go mod download
  ```

  ## Установка k6:
  - **Windows**

  ```powershell
  choco install k6
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
  sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C7474FCD99C739F358E6C44836700074A3
  echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
  sudo apt-get update
  sudo apt-get install k6
  ```

# Windows (PowerShell):

## **1. Подготовка:**

- **1.1. Скачайте архив проекта.**

- **1.2. Запустите приложение Docker Desktop.**
- **1.3. Запустите контейнер RabbitMQ:**

```powershell
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## **2. Переход в директорию и запуск терминалов:**

- **2.1. Перейдите в папку проекта:**

```powershell
cd C:\(Путь до директории проекта)
```

- **2.2. Откройте 3 терминала PowerShell.**

## **3. Запуск сервисов:**

- **3.1. Терминал 1 (Прокси-сервер):**

```powershell
go run main.go
```

- **3.2. Терминал 2 (Upstream):**

```powershell
go run .\cmd\upstream
```

## **4. Проверка и тестирование (Терминал 3):**

- **4.1. Проверка работоспособности (Healthcheck):**

```powershell
curl.exe -i http://localhost:8080/books/1
```

- **4.2. Базовые unit-тесты и проверка на состояния гонки (Race Detector):**

```powershell
go test -v .\...
go test -race .\...
```

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

```powershell
k6 run .\k6\read-heavy.js
```

---

> можно провести тестирование других k6:

\*Тест всплесков трафика.

```powershell
k6 run .\k6\burst.js
```

\*Тест массовой инвалидации кэша.

```powershell
k6 run .\k6\invalidation-storm.js
```

\*Смешанный сценарий.

```powershell
k6 run .\k6\mixed.js
```

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

- **3. Запустите контейнер RabbitMQ:**

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

- **3.1. Откройте 3 терминала (или вкладки / tmux).**

## **4. Запуск сервисов:**

- **4.1. Терминал 1 (Прокси-сервер):**

```bash
go run main.go
```

- **4.2. Терминал 2 (Upstream):**

```bash
go run ./cmd/upstream
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

```bash
k6 run ./k6/read-heavy.js
```

---

> можно провести тестирование других k6:

\*Тест всплесков трафика.

```bash
k6 run ./k6/burst.js
```

\*Тест массовой инвалидации кэша.

```bash
k6 run ./k6/invalidation-storm.js
```

\*Смешанный сценарий.

```bash
k6 run ./k6/mixed.js
```
