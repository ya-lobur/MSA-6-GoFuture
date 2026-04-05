# Схема топиков Kafka с региональным партиционированием

## Контекст

Apache Kafka является центральным брокером событий в событийно-ориентированной архитектуре GoFuture. Для обработки
500 000+ конкурентных поездок в нескольких географических регионах необходима продуманная схема топиков с региональным
партиционированием, обеспечивающая упорядоченность событий внутри агрегата и масштабируемость по регионам.

## Соглашение об именовании топиков

### Формат

```
{domain}.{aggregate}.{event-category}.v{version}
```

### Примеры

| Топик                           | Домен    | Агрегат | Категория |
|---------------------------------|----------|---------|-----------|
| `booking.booking.lifecycle.v1`  | booking  | booking | lifecycle |
| `driver.driver.location.v1`     | driver   | driver  | location  |
| `driver.driver.status.v1`       | driver   | driver  | status    |
| `pricing.quote.lifecycle.v1`    | pricing  | quote   | lifecycle |
| `payments.payment.lifecycle.v1` | payments | payment | lifecycle |

### Правила именования

- Только строчные буквы и точки как разделители
- Домен соответствует Bounded Context сервиса
- Категория группирует связанные события (`lifecycle`, `location`, `status`, `commands`)
- Версия (`v1`, `v2`) изменяется при несовместимых изменениях схемы

## Стратегия партиционирования по регионам

### Составной ключ партиционирования

```
partition_key = "{region}:{aggregate_id}"
```

**Пример:** `sea:550e8400-e29b-41d4-a716-446655440000`

### Обоснование

| Аспект                    | Решение                                                               |
|---------------------------|-----------------------------------------------------------------------|
| Упорядоченность           | Все события одного агрегата попадают в один partition → FIFO-порядок  |
| Региональная изоляция     | Events из одного региона сгруппированы → региональные consumer groups |
| Масштабируемость          | Составной ключ распределяет нагрузку равномерно по partitions         |
| Подготовка к multi-region | Ключ с регионом готовит к выделению региональных Kafka-кластеров      |

### Регионы

| Код региона | Название           | Описание                            |
|-------------|--------------------|-------------------------------------|
| `cis`       | СНГ                | Россия, Казахстан, Узбекистан и др. |
| `sea`       | Юго-Восточная Азия | Индонезия, Вьетнам, Таиланд и др.   |
| `latam`     | Южная Америка      | Бразилия, Мексика, Аргентина и др.  |

## Реестр топиков

### Основные доменные топики

| Топик                                   | Домен        | Partition Key       | Partitions | Retention | Replication Factor | Формат схемы | min.insync.replicas |
|-----------------------------------------|--------------|---------------------|------------|-----------|--------------------|--------------|---------------------|
| `booking.booking.lifecycle.v1`          | Booking      | `region:booking_id` | 64         | 7 дней    | 3                  | Avro         | 2                   |
| `driver.driver.location.v1`             | Driver       | `region:driver_id`  | 128        | 24 часа   | 3                  | Protobuf     | 2                   |
| `driver.driver.status.v1`               | Driver       | `region:driver_id`  | 32         | 7 дней    | 3                  | Avro         | 2                   |
| `driver.driver.assignment.v1`           | Driver       | `region:driver_id`  | 64         | 7 дней    | 3                  | Avro         | 2                   |
| `pricing.quote.lifecycle.v1`            | Pricing      | `region:quote_id`   | 32         | 3 дня     | 3                  | Avro         | 2                   |
| `pricing.surge.updates.v1`              | Pricing      | `region:zone_id`    | 16         | 24 часа   | 3                  | Avro         | 2                   |
| `payments.payment.lifecycle.v1`         | Payments     | `region:payment_id` | 32         | 30 дней   | 3                  | Avro         | 2                   |
| `payments.payout.lifecycle.v1`          | Payments     | `region:payout_id`  | 16         | 30 дней   | 3                  | Avro         | 2                   |
| `notification.notification.requests.v1` | Notification | `region:user_id`    | 32         | 3 дня     | 3                  | Avro         | 2                   |
| `geography.geo.events.v1`               | Geography    | `region:zone_id`    | 16         | 7 дней    | 3                  | Avro         | 2                   |
| `fraud.check.lifecycle.v1`              | Fraud        | `region:check_id`   | 16         | 30 дней   | 3                  | Avro         | 2                   |
| `analytics.events.raw.v1`               | Analytics    | `region:event_id`   | 64         | 14 дней   | 3                  | Avro         | 2                   |

### Обоснование выбора количества partitions

| Топик                          | Partitions | Обоснование                                                             |
|--------------------------------|------------|-------------------------------------------------------------------------|
| `driver.driver.location.v1`    | 128        | Наивысшая частота: ~500K водителей × обновление каждые 1–5 сек          |
| `booking.booking.lifecycle.v1` | 64         | Высокий throughput: до 500K конкурентных бронирований                   |
| `analytics.events.raw.v1`      | 64         | Агрегирует все события — высокий объём                                  |
| `driver.driver.assignment.v1`  | 64         | Параллельный процесс назначения водителей во время пика                 |
| Остальные                      | 16–32      | Средняя/низкая частота — достаточно для горизонтального масштабирования |

## Служебные топики

### Outbox Relay (Debezium CDC)

Debezium считывает изменения из Outbox-таблиц через PostgreSQL WAL и публикует в промежуточные топики:

| Топик                       | Источник          | Описание                                 |
|-----------------------------|-------------------|------------------------------------------|
| `__outbox.booking.events`   | Booking Service   | CDC-реле из outbox-таблицы бронирований  |
| `__outbox.driver.events`    | Driver Service    | CDC-реле из outbox-таблицы водителей     |
| `__outbox.payments.events`  | Payments Service  | CDC-реле из outbox-таблицы платежей      |
| `__outbox.fraud.events`     | Fraud Service     | CDC-реле из outbox-таблицы фрод-проверок |
| `__outbox.geography.events` | Geography Service | CDC-реле из outbox-таблицы геосервиса    |
| `__outbox.pricing.events`   | Pricing Service   | CDC-реле из outbox-таблицы прайсинга     |

Debezium Kafka Connect использует SMT (Single Message Transform) для маршрутизации событий из `__outbox.*` топиков
в соответствующие доменные топики на основании поля `event_type` в outbox-записи.

### Retry-топики (экспоненциальная задержка)

| Уровень | Топик                      | Задержка   | Описание                           |
|---------|----------------------------|------------|------------------------------------|
| Retry 1 | `{domain}.{topic}.retry.1` | 1 минута   | Первая попытка повторной обработки |
| Retry 2 | `{domain}.{topic}.retry.2` | 5 минут    | Вторая попытка                     |
| Retry 3 | `{domain}.{topic}.retry.3` | 30 минут   | Третья попытка                     |
| DLQ     | `{domain}.{topic}.dlq`     | ∞ (ручная) | Dead Letter Queue — ручной разбор  |

**Пример:** `booking.booking.lifecycle.v1.retry.1`, `booking.booking.lifecycle.v1.dlq`

### Формат DLQ-сообщения

| Поле                 | Описание                           |
|----------------------|------------------------------------|
| `original_topic`     | Исходный топик                     |
| `original_partition` | Исходный partition                 |
| `original_offset`    | Исходный offset                    |
| `original_key`       | Ключ сообщения                     |
| `original_value`     | Тело оригинального сообщения       |
| `error_message`      | Описание ошибки                    |
| `retry_count`        | Количество попыток                 |
| `failed_at`          | Время последнего сбоя (UTC)        |
| `consumer_group`     | Consumer group, где произошёл сбой |

## Consumer Groups

### Соглашение об именовании

```
{service}-{purpose}-cg
```

### Реестр consumer groups

| Consumer Group                   | Сервис               | Потребляемые топики                                                                            | Назначение                            |
|----------------------------------|----------------------|------------------------------------------------------------------------------------------------|---------------------------------------|
| `booking-saga-events-cg`         | Booking Service      | `driver.driver.assignment.v1`, `payments.payment.lifecycle.v1`, `fraud.check.lifecycle.v1`     | Обработка Saga-событий                |
| `driver-booking-events-cg`       | Driver Service       | `booking.booking.lifecycle.v1`                                                                 | Получение запросов на назначение      |
| `pricing-booking-events-cg`      | Pricing Service      | `booking.booking.lifecycle.v1`                                                                 | Расчёт стоимости по запросу           |
| `payments-booking-events-cg`     | Payments Service     | `booking.booking.lifecycle.v1`                                                                 | Авторизация платежа при подтверждении |
| `notification-all-events-cg`     | Notification Service | `booking.booking.lifecycle.v1`, `payments.payment.lifecycle.v1`, `driver.driver.assignment.v1` | Уведомления по событиям               |
| `analytics-all-events-cg`        | Analytics Service    | Все доменные топики                                                                            | Сбор аналитических данных             |
| `fraud-scoring-events-cg`        | Fraud Service        | `booking.booking.lifecycle.v1`, `payments.payment.lifecycle.v1`                                | ML-скоринг риска                      |
| `flink-surge-pricing-cg`         | Flink                | `driver.driver.location.v1`, `booking.booking.lifecycle.v1`                                    | Расчёт сурж-коэффициентов             |
| `flink-analytics-aggregation-cg` | Flink                | Все доменные топики                                                                            | Агрегация в ClickHouse                |
| `flink-driver-matching-cg`       | Flink                | `booking.booking.lifecycle.v1`, `driver.driver.location.v1`, `driver.driver.status.v1`         | «Умный» матчинг водителей             |

## Потоковая обработка (Apache Flink)

### Flink-джобы

| Джоба                     | Входные топики                                                                         | Выходной топик / хранилище    | Описание                                                      |
|---------------------------|----------------------------------------------------------------------------------------|-------------------------------|---------------------------------------------------------------|
| `SurgePricingJob`         | `driver.driver.location.v1`, `booking.booking.lifecycle.v1`                            | `pricing.surge.updates.v1`    | Вычисление сурж-коэффициента на основе спроса/предложения     |
| `FraudScoringJob`         | `booking.booking.lifecycle.v1`, `payments.payment.lifecycle.v1`                        | `fraud.check.lifecycle.v1`    | ML-скоринг риска в реальном времени                           |
| `AnalyticsAggregationJob` | Все доменные топики                                                                    | ClickHouse                    | Агрегация событий, запись в аналитическое хранилище           |
| `DriverMatchingJob`       | `booking.booking.lifecycle.v1`, `driver.driver.location.v1`, `driver.driver.status.v1` | `driver.driver.assignment.v1` | «Умное» распределение водителей (не ближайший, а оптимальный) |
| `HotspotDetectionJob`     | `driver.driver.location.v1`                                                            | `geography.geo.events.v1`     | Обнаружение «горячих зон» концентрации водителей              |

### Гарантии обработки

- **Exactly-once семантика**: Flink checkpointing + Kafka транзакции
- **Checkpointing**: каждые 60 секунд, хранение в S3/MinIO
- **Savepoints**: перед каждым деплоем для graceful restart
- **Параллелизм**: масштабируется по количеству partitions входных топиков

## Конфигурация Kafka-кластера

### Рекомендуемая конфигурация (на регион)

| Параметр                                | Значение                | Обоснование                                        |
|-----------------------------------------|-------------------------|----------------------------------------------------|
| Количество брокеров                     | ≥ 5                     | Отказоустойчивость + производительность            |
| `default.replication.factor`            | 3                       | Устойчивость к потере 1 брокера                    |
| `min.insync.replicas`                   | 2                       | Гарантия записи минимум на 2 реплики               |
| `acks`                                  | `all`                   | Запись подтверждена всеми ISR-репликами            |
| `enable.idempotence`                    | `true`                  | Дедупликация на стороне продюсера                  |
| `max.in.flight.requests.per.connection` | 5                       | Максимум с idempotence                             |
| `log.retention.hours`                   | По топику (см. таблицу) | Разная retention для разных доменов                |
| `auto.create.topics.enable`             | `false`                 | Все топики создаются declaratively через Terraform |
| `message.max.bytes`                     | 1 МБ                    | Стандартный лимит                                  |
| Rack awareness                          | Включено                | Реплики на разных rack/AZ                          |

### Мониторинг кластера

- **Kafka Exporter** → Prometheus: метрики брокеров, топиков, ISR
- **Burrow**: мониторинг consumer lag и статусов consumer groups
- Подробнее: [monitoring.md](monitoring.md)
