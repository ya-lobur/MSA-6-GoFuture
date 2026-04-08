# Механизмы надёжной доставки событий

## Контекст

В распределённой системе GoFuture с 500 000+ конкурентных поездок потеря или дублирование событий может привести
к финансовым расхождениям (двойное списание, незавершённые бронирования) и ухудшению пользовательского опыта.
Необходимо обеспечить гарантии доставки на всех уровнях: от публикации событий до их потребления.

## Обзор механизмов

| Механизм                | Уровень          | Проблема                                | Гарантия                            |
|-------------------------|------------------|-----------------------------------------|-------------------------------------|
| Transactional Outbox    | Публикация       | Несогласованность БД и Kafka            | Атомарная запись + публикация       |
| Idempotent Consumers    | Потребление      | Дублирование обработки                  | Exactly-once семантика обработки    |
| Retry Topics            | Повтор обработки | Временные сбои                          | Экспоненциальный backoff            |
| Dead Letter Queue (DLQ) | Обработка ошибок | Некорректные или необработанные события | Изоляция ошибок от основного потока |
| Schema Registry         | Контракт         | Несовместимость схем                    | Обратная совместимость              |
| Kafka Producer Config   | Публикация       | Потеря при записи в брокер              | acks=all + idempotence              |

## 1. Transactional Outbox Pattern

### Принцип работы

Каждый сервис записывает доменные события в **Outbox-таблицу** в той же PostgreSQL-транзакции, что и бизнес-операцию.
Debezium CDC (Change Data Capture) считывает изменения из PostgreSQL WAL и публикует события в Kafka.

```
┌─────────────────────────────────────────────┐
│ PostgreSQL Transaction                      │
│  1. UPDATE bookings SET status = 'CONFIRMED'│
│  2. INSERT INTO outbox (event_type, payload)│
│  COMMIT                                     │
└─────────────────────┬───────────────────────┘
                      │ WAL (Write-Ahead Log)
                      ▼
┌─────────────────────────────────────────────┐
│ Debezium CDC Connector                      │
│  • Читает WAL через logical replication     │
│  • Публикует в __outbox.{service}.events    │
│  • SMT роутит в доменный топик              │
└─────────────────────┬───────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│ Apache Kafka                                │
│  booking.booking.lifecycle.v1               │
└─────────────────────────────────────────────┘
```

### Схема Outbox-таблицы

```sql
CREATE TABLE outbox
(
    id             UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   UUID         NOT NULL,
    event_type     VARCHAR(255) NOT NULL,
    region         VARCHAR(10)  NOT NULL,
    correlation_id UUID,
    payload        JSONB        NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ -- заполняется после публикации
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at)
    WHERE published_at IS NULL;
```

### Конфигурация Debezium

| Параметр                                    | Значение                                             | Описание                         |
|---------------------------------------------|------------------------------------------------------|----------------------------------|
| `connector.class`                           | `io.debezium.connector.postgresql.PostgresConnector` | Коннектор PostgreSQL             |
| `plugin.name`                               | `pgoutput`                                           | Плагин логической репликации     |
| `table.include.list`                        | `public.outbox`                                      | Только Outbox-таблица            |
| `transforms`                                | `outbox`                                             | SMT для Outbox-роутинга          |
| `transforms.outbox.type`                    | `io.debezium.transforms.outbox.EventRouter`          | Роутинг по event_type            |
| `transforms.outbox.route.topic.replacement` | `${routedByValue}.v1`                                | Динамический топик по event_type |
| `tombstones.on.delete`                      | `false`                                              | Не публиковать tombstone         |

### Какие сервисы используют Outbox

| Сервис               | Outbox нужен | Обоснование                                                      |
|----------------------|--------------|------------------------------------------------------------------|
| Booking Service      | **Да**       | Saga-события: BookingCreated, BookingConfirmed, BookingCancelled |
| Driver Service       | **Да**       | DriverAssignmentAccepted, DriverReleased — изменение состояния   |
| Payments Service     | **Да**       | PaymentAuthorized, PaymentFailed — финансовые операции           |
| Fraud Service        | **Да**       | FraudCheckPassed/Failed — результат влияет на Saga               |
| Geography Service    | **Да**       | NearbyDriversFound — результат геопоиска для Saga                |
| Pricing Service      | **Да**       | PriceQuoteCalculated — результат расчёта для Saga                |
| Notification Service | Нет          | Не публикует критичные события — потребитель (event consumer)    |
| Analytics Service    | Нет          | Потребитель — пишет в ClickHouse, не генерирует доменные события |

## 2. Idempotent Consumers

### Принцип работы

Каждый consumer ведёт таблицу `processed_events` с уникальным ограничением на `event_id`. Перед обработкой
события проверяется его наличие в таблице.

### Алгоритм обработки

```python
async def process_event(event: DomainEvent):
    # 1. Попытка записать event_id
    try:
        await db.execute(
            "INSERT INTO processed_events (event_id, processed_at) "
            "VALUES ($1, now())",
            event.event_id
        )
    except UniqueViolationError:
        # Событие уже обработано — пропускаем
        logger.info(f"Duplicate event {event.event_id}, skipping")
        return

    # 2. Бизнес-логика обработки
    await handle_business_logic(event)

    # 3. Manual commit offset
    await consumer.commit()
```

### Схема таблицы

```sql
CREATE TABLE processed_events
(
    event_id     UUID PRIMARY KEY,
    event_type   VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Периодическая очистка старых записей (retention 7 дней)
-- через pg_cron или Kubernetes CronJob
```

### Consumer offset management

| Параметр             | Значение         | Описание                                        |
|----------------------|------------------|-------------------------------------------------|
| `enable.auto.commit` | `false`          | Ручной commit после успешной обработки          |
| `auto.offset.reset`  | `earliest`       | При новом consumer group — читать с начала      |
| `isolation.level`    | `read_committed` | Читать только committed транзакции (для Outbox) |
| `max.poll.records`   | `100`            | Пакетная обработка для throughput               |

## 3. Retry Topics с экспоненциальным Backoff

### Принцип работы

При ошибке обработки сообщение перенаправляется в retry-топик с задержкой. Каждый уровень retry увеличивает
задержку экспоненциально. После исчерпания всех retry — сообщение попадает в DLQ.

```
Основной топик → Ошибка → retry.1 (1 мин) → Ошибка → retry.2 (5 мин)
    → Ошибка → retry.3 (30 мин) → Ошибка → DLQ (ручной разбор)
```

### Конфигурация retry-уровней

| Уровень | Задержка   | Топик                      | max.poll.interval.ms |
|---------|------------|----------------------------|----------------------|
| Retry 1 | 1 минута   | `{domain}.{topic}.retry.1` | 120000               |
| Retry 2 | 5 минут    | `{domain}.{topic}.retry.2` | 600000               |
| Retry 3 | 30 минут   | `{domain}.{topic}.retry.3` | 1800000 + запас      |
| DLQ     | ∞ (ручная) | `{domain}.{topic}.dlq`     | —                    |

### Реализация задержки

Для реализации задержки используется подход **delayed consumer**: отдельный consumer group для каждого retry-топика,
который при потреблении проверяет timestamp сообщения и откладывает обработку, если задержка ещё не истекла.

Альтернатива: использование Kafka headers `retry-after` и consumer-side pause.

## 4. Dead Letter Queue (DLQ)

### Формат DLQ-сообщения

| Поле                 | Описание                           |
|----------------------|------------------------------------|
| `original_topic`     | Исходный топик                     |
| `original_partition` | Исходный partition                 |
| `original_offset`    | Исходный offset                    |
| `original_key`       | Ключ партиционирования             |
| `original_value`     | Тело оригинального сообщения       |
| `error_message`      | Описание ошибки                    |
| `error_stacktrace`   | Stack trace ошибки                 |
| `retry_count`        | Количество выполненных попыток     |
| `failed_at`          | Время последнего сбоя (UTC)        |
| `consumer_group`     | Consumer group, где произошёл сбой |
| `correlation_id`     | ID корреляции для трассировки      |

### Процесс работы с DLQ

1. **Мониторинг**: алерт при `dlq_depth > 0` (Critical)
2. **Анализ**: SRE/разработчик изучает ошибку через Grafana/Loki (по correlation_id)
3. **Исправление**: устранение root cause (баг, таймаут, невалидные данные)
4. **Replay**: повторная отправка сообщений из DLQ в основной топик
5. **Очистка**: архивация или удаление обработанных DLQ-сообщений

### DLQ Replay Tool

CLI-утилита для повторной отправки сообщений из DLQ:

```bash
# Просмотр DLQ
kafka-dlq-tool --topic booking.booking.lifecycle.v1.dlq --list

# Replay одного сообщения
kafka-dlq-tool --topic booking.booking.lifecycle.v1.dlq --replay --offset 42

# Replay всех сообщений
kafka-dlq-tool --topic booking.booking.lifecycle.v1.dlq --replay-all
```

## 5. Конфигурация Kafka Producer для надёжности

### Параметры продюсера

| Параметр                                | Значение | Описание                                              |
|-----------------------------------------|----------|-------------------------------------------------------|
| `acks`                                  | `all`    | Ожидание подтверждения от всех ISR-реплик             |
| `enable.idempotence`                    | `true`   | Дедупликация на стороне брокера                       |
| `max.in.flight.requests.per.connection` | `5`      | Максимум с idempotence (поддерживает упорядоченность) |
| `retries`                               | `3`      | Автоматический retry при transient errors             |
| `retry.backoff.ms`                      | `100`    | Задержка между retry                                  |
| `delivery.timeout.ms`                   | `120000` | Максимальное время доставки                           |
| `linger.ms`                             | `5`      | Микро-батчинг для throughput                          |
| `batch.size`                            | `16384`  | Размер батча (16 KB)                                  |
| `compression.type`                      | `lz4`    | Сжатие для экономии bandwidth                         |

### Параметры брокера

| Параметр                         | Значение | Описание                                       |
|----------------------------------|----------|------------------------------------------------|
| `min.insync.replicas`            | `2`      | Минимум 2 ISR-реплики для подтверждения записи |
| `default.replication.factor`     | `3`      | 3 реплики каждого partition                    |
| `unclean.leader.election.enable` | `false`  | Запрет выбора out-of-sync реплики лидером      |

## 6. Schema Registry и контрактное тестирование

### Политика совместимости

| Режим      | Описание                                       | Применение              |
|------------|------------------------------------------------|-------------------------|
| `BACKWARD` | Новые потребители читают старые + новые версии | **По умолчанию**        |
| `FORWARD`  | Старые потребители читают новые версии         | Для некритичных топиков |
| `FULL`     | Полная совместимость в обе стороны             | Для финансовых событий  |

### Валидация в CI/CD

```yaml
# GitHub Actions step
- name: Validate Avro Schema Compatibility
  run: |
    curl -X POST \
      -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data @schemas/booking-created-v2.avsc \
      http://schema-registry:8081/compatibility/subjects/booking.booking.lifecycle.v1-value/versions/latest
```

Если валидация не проходит — pipeline блокирует деплой. Это предотвращает несовместимые изменения схем
от попадания в production.

### Правила эволюции

1. **Разрешено**: добавление optional-полей с default-значениями
2. **Запрещено**: удаление required-полей, изменение типов полей
3. **Deprecated**: старые поля помечаются `@deprecated` и удаляются через 1 мажорную версию

## Сквозная диаграмма потока надёжной доставки

```
┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│   Service    │     │   PostgreSQL    │     │   Debezium    │
│  (FastAPI)   │────>│  Transaction:   │────>│     CDC       │
│              │     │  1. Бизнес-     │ WAL │  Connector    │
│              │     │     операция    │     │               │
│              │     │  2. INSERT INTO │     │  • Читает WAL │
│              │     │     outbox      │     │  • SMT роутинг│
│              │     │  COMMIT         │     │               │
└──────────────┘     └─────────────────┘     └───────┬───────┘
                                                      │
                                                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      Apache Kafka                           │
│  ┌──────────────────┐  ┌──────────┐  ┌───────────────────┐ │
│  │  Доменный топик  │  │ Retry 1  │  │       DLQ         │ │
│  │ booking.booking.  │  │ (1 мин)  │  │ booking.booking.  │ │
│  │ lifecycle.v1      │  │          │  │ lifecycle.v1.dlq  │ │
│  └────────┬─────────┘  └────┬─────┘  └───────────────────┘ │
│           │                  │              ▲                │
│           ▼                  │              │                │
│  Schema Registry ◄───────── Валидация схем │                │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────┐
│        Consumer (aiokafka)               │
│  1. Получить событие                     │
│  2. INSERT INTO processed_events         │
│     (ON CONFLICT → skip)                 │
│  3. Бизнес-логика                        │
│  4. Manual commit offset                 │
│                                          │
│  При ошибке:                             │
│    → retry.1 → retry.2 → retry.3 → DLQ  │
└──────────────────────────────────────────┘
```

## Компромиссы

| Компромисс                       | Преимущество                           | Ограничение                            |
|----------------------------------|----------------------------------------|----------------------------------------|
| Transactional Outbox + Debezium  | Атомарность без 2PC                    | Доп. latency (CDC ~100–500ms)          |
| Idempotent Consumers             | Exactly-once обработка                 | Доп. таблица + overhead на проверку    |
| Retry с backoff                  | Устойчивость к transient errors        | Увеличенная задержка при сбоях         |
| DLQ                              | Изоляция «ядовитых» сообщений          | Требует ручного разбора и replay       |
| Schema Registry (BACKWARD)       | Эволюция без даунтайма                 | Ограничения на изменения схем          |
| acks=all + min.insync.replicas=2 | Отсутствие потерь при отказе 1 брокера | Увеличенная latency на запись (~1–5ms) |
