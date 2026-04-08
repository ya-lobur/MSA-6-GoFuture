# Подход к мониторингу событийной платформы

## Контекст

Событийно-ориентированная архитектура GoFuture требует наблюдаемости на всех уровнях: от публикации событий через
Outbox/Debezium, через Kafka-брокер, до потребления и обработки потребителями. Традиционный мониторинг
request-response недостаточен — необходимы специализированные инструменты для мониторинга event pipeline.

## Требования к мониторингу

| Требование                        | Обоснование                                                   |
|-----------------------------------|---------------------------------------------------------------|
| Мониторинг Kafka-брокеров         | Здоровье кластера, throughput, репликация                     |
| Мониторинг consumer lag           | Обнаружение отставания потребителей от потока событий         |
| Трассировка событий через сервисы | Отслеживание полного пути события (Saga) через correlation_id |
| Мониторинг Outbox pipeline        | Задержка между записью в Outbox и публикацией в Kafka         |
| Мониторинг Flink-джобов           | Throughput, backpressure, здоровье checkpointing              |
| Мониторинг DLQ                    | Алерт при наличии «ядовитых» сообщений                        |
| Бизнес-метрики SLI/SLO            | Время бронирования, процент успешных Saga и т.д.              |

## Подход к мониторингу — обоснование

### Три столпа наблюдаемости

| Столп   | Инструмент | Назначение                                 |
|---------|------------|--------------------------------------------|
| Метрики | Prometheus | Числовые показатели: счётчики, гистограммы |
| Логи    | Loki       | Текстовые записи с контекстом              |
| Трейсы  | Tempo      | Распределённая трассировка через сервисы   |

### Дополнительные инструменты для Event Platform

| Инструмент                  | Назначение                                           | Обоснование выбора                                                                        |
|-----------------------------|------------------------------------------------------|-------------------------------------------------------------------------------------------|
| **Kafka Exporter**          | Экспорт метрик Kafka-брокеров и топиков в Prometheus | Стандарт де-факто, поддерживает все JMX-метрики Kafka                                     |
| **Burrow**                  | Специализированный мониторинг consumer lag           | Оценивает статус consumer groups (OK/WARN/ERR/STOP), а не только числовой lag             |
| **OpenTelemetry Collector** | Централизованный pipeline телеметрии                 | Единая точка сбора от всех сервисов; vendor-neutral; поддерживает Prometheus, Tempo, Loki |

### Почему именно эти инструменты

1. **Open-source**: Отсутствие vendor lock-in, активное сообщество
2. **Kubernetes-native**: Все инструменты разворачиваются как Helm-чарты в K8s
3. **Преемственность стека Task 1**: Prometheus, Grafana, Loki, Tempo уже в архитектуре
4. **Интеграция**: Все инструменты интегрируются через единый Grafana

## Инструменты мониторинга

### Полный реестр инструментов

| Инструмент              | Роль                       | Развёртывание                     | Интеграция на C2-диаграмме                       |
|-------------------------|----------------------------|-----------------------------------|--------------------------------------------------|
| Prometheus              | Сбор и хранение метрик     | K8s (Helm: kube-prometheus-stack) | Все сервисы → Prometheus                         |
| Grafana                 | Визуализация и дашборды    | K8s (Helm)                        | Prometheus, Loki, Tempo, ClickHouse → Grafana    |
| Loki                    | Агрегация логов            | K8s (Helm: loki-stack)            | Все сервисы → Loki                               |
| Tempo                   | Распределённая трассировка | K8s (Helm)                        | OTel Collector → Tempo                           |
| Alertmanager            | Управление алертами        | K8s (Helm)                        | Prometheus → Alertmanager → Slack/PagerDuty      |
| Kafka Exporter          | Метрики Kafka-брокеров     | K8s (sidecar/deployment)          | Kafka → Kafka Exporter → Prometheus              |
| Burrow                  | Consumer lag мониторинг    | K8s (deployment)                  | Kafka → Burrow → Prometheus                      |
| OpenTelemetry Collector | Телеметрический pipeline   | K8s (DaemonSet/Deployment)        | Сервисы → OTel Collector → Prometheus/Tempo/Loki |

### Отражение на диаграмме C2

На диаграмме [C2_event_platform.puml](diagrams/puml/C2_event_platform.puml) все инструменты мониторинга размещены
в границе **"Observability Platform"**:

- **OpenTelemetry Collector** — принимает телеметрию от всех доменных сервисов и Flink через OTLP-протокол
- **Kafka Exporter** — подключён к Kafka для сбора метрик брокеров и топиков
- **Burrow** — подключён к Kafka для мониторинга consumer groups и lag
- **Prometheus** — получает метрики от OTel Collector, Kafka Exporter и Burrow
- **Grafana** — визуализирует данные из Prometheus, Loki, Tempo и ClickHouse
- **Alertmanager** — получает алерты от Prometheus и маршрутизирует SRE-инженерам

## Список метрик

### Метрики Kafka-брокеров (Kafka Exporter → Prometheus)

| Метрика                                  | Тип     | Описание                                | Критический порог                |
|------------------------------------------|---------|-----------------------------------------|----------------------------------|
| `kafka_brokers`                          | Gauge   | Количество активных брокеров            | < 3 → Critical                   |
| `kafka_topic_partitions`                 | Gauge   | Количество partitions по топику         | Информационная                   |
| `kafka_topic_partition_current_offset`   | Gauge   | Текущий offset (последнее сообщение)    | Мониторинг throughput            |
| `kafka_topic_partition_in_sync_replica`  | Gauge   | Количество ISR-реплик                   | < min.insync.replicas → Critical |
| `kafka_topic_partition_under_replicated` | Gauge   | Количество under-replicated partitions  | > 0 → Critical                   |
| `kafka_broker_request_rate`              | Counter | Количество запросов к брокеру в секунду | Мониторинг нагрузки              |
| `kafka_broker_bytes_in_per_sec`          | Gauge   | Входящий трафик на брокер (bytes/sec)   | Мониторинг throughput            |
| `kafka_broker_bytes_out_per_sec`         | Gauge   | Исходящий трафик с брокера (bytes/sec)  | Мониторинг throughput            |

### Метрики Consumer Lag (Burrow + Kafka Exporter → Prometheus)

| Метрика                        | Тип   | Описание                                    | Критический порог          |
|--------------------------------|-------|---------------------------------------------|----------------------------|
| `kafka_consumergroup_lag`      | Gauge | Lag по partition для consumer group         | > 10000 (5 мин) → Critical |
| `kafka_consumergroup_lag_sum`  | Gauge | Суммарный lag по consumer group             | > 50000 → Critical         |
| `burrow_consumer_status`       | Gauge | Статус Burrow: 1=OK, 2=WARN, 3=ERR, 4=STOP  | ≥ 3 → Critical             |
| `burrow_consumer_lag_velocity` | Gauge | Скорость изменения lag (растёт/уменьшается) | Растёт > 5 мин → Warning   |

### Метрики событий на уровне приложений (OpenTelemetry → Prometheus)

| Метрика                              | Тип       | Labels                                | Описание                          |
|--------------------------------------|-----------|---------------------------------------|-----------------------------------|
| `events_published_total`             | Counter   | `service`, `event_type`, `region`     | Количество опубликованных событий |
| `events_consumed_total`              | Counter   | `service`, `event_type`, `region`     | Количество потреблённых событий   |
| `events_processing_duration_seconds` | Histogram | `service`, `event_type`               | Длительность обработки события    |
| `events_processing_errors_total`     | Counter   | `service`, `event_type`, `error_type` | Количество ошибок обработки       |
| `events_retry_total`                 | Counter   | `service`, `event_type`, `attempt`    | Количество retry-попыток          |
| `events_dlq_total`                   | Counter   | `service`, `event_type`               | Количество сообщений в DLQ        |

### Метрики Saga (Booking Service → OpenTelemetry → Prometheus)

| Метрика                             | Тип       | Labels                                | Описание                             |
|-------------------------------------|-----------|---------------------------------------|--------------------------------------|
| `saga_started_total`                | Counter   | `saga_type`, `region`                 | Количество запущенных Saga           |
| `saga_completed_total`              | Counter   | `saga_type`, `region`                 | Количество успешных Saga             |
| `saga_failed_total`                 | Counter   | `saga_type`, `region`, `failure_step` | Количество неуспешных Saga           |
| `saga_duration_seconds`             | Histogram | `saga_type`, `region`                 | Длительность Saga от начала до конца |
| `saga_step_duration_seconds`        | Histogram | `saga_type`, `step`                   | Длительность каждого шага Saga       |
| `saga_compensation_triggered_total` | Counter   | `saga_type`, `step`                   | Количество запущенных компенсаций    |
| `saga_stuck_bookings`               | Gauge     | `region`                              | Бронирования в PENDING > 60 сек      |

### Метрики Outbox Pipeline (Debezium → Prometheus)

| Метрика                           | Тип       | Labels      | Описание                                       |
|-----------------------------------|-----------|-------------|------------------------------------------------|
| `outbox_pending_events`           | Gauge     | `service`   | Количество неопубликованных событий            |
| `outbox_publish_lag_seconds`      | Histogram | `service`   | Задержка: запись в Outbox → публикация в Kafka |
| `debezium_connector_status`       | Gauge     | `connector` | Статус Debezium коннектора (1=running)         |
| `debezium_snapshot_completed`     | Gauge     | `connector` | Снапшот завершён (1=yes)                       |
| `debezium_total_events_processed` | Counter   | `connector` | Общее количество обработанных CDC-событий      |

### Метрики Apache Flink (Flink → OpenTelemetry → Prometheus)

| Метрика                                    | Тип     | Labels                 | Описание                                |
|--------------------------------------------|---------|------------------------|-----------------------------------------|
| `flink_job_uptime`                         | Gauge   | `job_name`             | Время работы джобы (для расчёта uptime) |
| `flink_taskmanager_numRecordsInPerSecond`  | Gauge   | `job_name`, `task`     | Входной throughput (записей/сек)        |
| `flink_taskmanager_numRecordsOutPerSecond` | Gauge   | `job_name`, `task`     | Выходной throughput (записей/сек)       |
| `flink_job_lastCheckpointDuration`         | Gauge   | `job_name`             | Длительность последнего checkpoint      |
| `flink_job_lastCheckpointSize`             | Gauge   | `job_name`             | Размер последнего checkpoint            |
| `flink_operator_backpressure`              | Gauge   | `job_name`, `operator` | Уровень backpressure (0–1)              |
| `flink_job_numRestarts`                    | Counter | `job_name`             | Количество перезапусков джобы           |

### Бизнес-метрики (SLI / SLO)

| SLI                             | Метрика-источник                                                         | SLO (целевое значение)         | Формула                                |
|---------------------------------|--------------------------------------------------------------------------|--------------------------------|----------------------------------------|
| Время бронирования (end-to-end) | `saga_duration_seconds{saga_type="booking"}`                             | p99 < 10 секунд                | histogram_quantile(0.99, ...)          |
| Авторизация платежа             | `saga_step_duration_seconds{step="authorize_payment"}`                   | p99 < 3 секунды                | histogram_quantile(0.99, ...)          |
| Свежесть локации водителя       | `events_processing_duration_seconds{event_type="DriverLocationUpdated"}` | p99 < 2 секунды                | histogram_quantile(0.99, ...)          |
| Доставка события end-to-end     | `outbox_publish_lag_seconds` + consumer lag                              | p99 < 1 секунда                | outbox_lag + consumer_lag_time         |
| Глубина DLQ                     | `events_dlq_total`                                                       | = 0 (нулевые ядовитые события) | rate(events_dlq_total[5m]) == 0        |
| Процент успешных Saga           | `saga_completed_total` / `saga_started_total`                            | ≥ 95%                          | rate(completed) / rate(started)        |
| Доступность Kafka-кластера      | `kafka_brokers`, `under_replicated`                                      | 99.99%                         | uptime без under-replicated partitions |
| Доступность Flink-джобов        | `flink_job_uptime`                                                       | 99.9%                          | uptime / total_time                    |

## Правила алертов

### Critical (немедленная реакция)

| Алерт                            | Условие                                                             | Действие                                       |
|----------------------------------|---------------------------------------------------------------------|------------------------------------------------|
| `KafkaUnderReplicatedPartitions` | `kafka_topic_partition_under_replicated > 0` (5 мин)                | Проверить брокеры, rack awareness              |
| `KafkaBrokersDown`               | `kafka_brokers < 3`                                                 | Перезапуск брокеров, проверка PV               |
| `ConsumerLagCritical`            | `kafka_consumergroup_lag_sum > 10000` (5 мин)                       | Масштабирование consumers, проверка throughput |
| `DLQNotEmpty`                    | `rate(events_dlq_total[5m]) > 0`                                    | Анализ ошибок, replay после фикса              |
| `SagaFailureRateHigh`            | `rate(saga_failed_total[5m]) / rate(saga_started_total[5m]) > 0.05` | Проверка зависимых сервисов                    |
| `SagaStuckBookings`              | `saga_stuck_bookings > 0`                                           | Проверка Saga Watchdog, ручная компенсация     |
| `DebeziumConnectorDown`          | `debezium_connector_status != 1` (2 мин)                            | Перезапуск коннектора, проверка WAL            |
| `OutboxLagHigh`                  | `outbox_publish_lag_seconds > 30`                                   | Проверка Debezium, WAL backlog                 |

### Warning (расследование в рабочее время)

| Алерт                        | Условие                                                                            | Действие                           |
|------------------------------|------------------------------------------------------------------------------------|------------------------------------|
| `ConsumerLagWarning`         | `kafka_consumergroup_lag_sum > 1000` (5 мин)                                       | Мониторинг тренда lag              |
| `EventProcessingLatencyHigh` | `events_processing_duration_seconds p99 > 2с`                                      | Оптимизация обработки              |
| `SagaDurationHigh`           | `saga_duration_seconds p99 > 10с`                                                  | Профилирование шагов Saga          |
| `FraudCheckLatencyHigh`      | `saga_step_duration_seconds{step="check_fraud"} p99 > 5с`                          | Оптимизация ML-скоринга            |
| `FlinkCheckpointSlow`        | `flink_job_lastCheckpointDuration > 60000`                                         | Проверка state size, S3 throughput |
| `FlinkBackpressure`          | `flink_operator_backpressure > 0.5` (5 мин)                                        | Масштабирование Flink parallelism  |
| `SagaCompensationRateHigh`   | `rate(saga_compensation_triggered_total[1h]) / rate(saga_started_total[1h]) > 0.1` | Анализ причин                      |

### Info (информирование)

| Алерт                        | Условие                             | Действие                    |
|------------------------------|-------------------------------------|-----------------------------|
| `SchemaRegistryNewVersion`   | Новая версия схемы зарегистрирована | Информирование команды      |
| `NewConsumerGroupRegistered` | Обнаружена новая consumer group     | Проверка: ожидаемая или нет |
| `FlinkJobRestarted`          | `flink_job_numRestarts` увеличился  | Проверка логов, root cause  |

## Дашборды Grafana

### Рекомендуемый набор дашбордов

| Дашборд            | Источники данных            | Ключевые панели                                                           |
|--------------------|-----------------------------|---------------------------------------------------------------------------|
| **Kafka Overview** | Prometheus (Kafka Exporter) | Брокеры, throughput (bytes in/out), ISR, under-replicated, request rate   |
| **Consumer Lag**   | Prometheus (Burrow)         | Lag по consumer group и partition, статус Burrow, тренд lag               |
| **Event Flow**     | Prometheus (OTel)           | Published vs consumed rates per topic, processing latency, error rates    |
| **Saga Health**    | Prometheus (OTel)           | Completion rate, duration (p50/p95/p99), failures by step, compensations  |
| **Outbox Health**  | Prometheus (Debezium)       | Pending events, publish lag, connector status, CDC throughput             |
| **Flink Jobs**     | Prometheus (Flink)          | Throughput (in/out), backpressure, checkpoint duration/size, restarts     |
| **DLQ Monitor**    | Prometheus (OTel)           | DLQ depth by topic, last DLQ event time, replay status                    |
| **Business SLIs**  | Prometheus + ClickHouse     | Booking time p99, payment latency, Saga success rate, rides/min by region |

### Пример: дашборд «Saga Health»

| Панель                | Тип         | PromQL                                                                             |
|-----------------------|-------------|------------------------------------------------------------------------------------|
| Saga Completion Rate  | Stat (%)    | `rate(saga_completed_total[5m]) / rate(saga_started_total[5m]) * 100`              |
| Saga Duration (p99)   | Time series | `histogram_quantile(0.99, rate(saga_duration_seconds_bucket[5m]))`                 |
| Saga Failures by Step | Bar chart   | `sum by (failure_step) (rate(saga_failed_total[5m]))`                              |
| Compensation Rate     | Gauge       | `rate(saga_compensation_triggered_total[5m]) / rate(saga_started_total[5m]) * 100` |
| Stuck Bookings        | Stat        | `saga_stuck_bookings`                                                              |
| Step Duration Heatmap | Heatmap     | `rate(saga_step_duration_seconds_bucket[5m])`                                      |

## Трассировка событий через Saga (Distributed Tracing)

### Propagation через Kafka

Каждое доменное событие содержит `correlation_id` — уникальный UUID Saga-инстанса. При публикации события
в Kafka, OpenTelemetry context (trace_id, span_id) передаётся через Kafka headers:

| Header           | Описание           |
|------------------|--------------------|
| `traceparent`    | W3C Trace Context  |
| `correlation_id` | UUID Saga-инстанса |

При потреблении события consumer извлекает context из headers и создаёт дочерний span, продолжая трассировку.

### Визуализация в Tempo/Grafana

В Grafana Tempo можно отследить полный путь Saga:

```
[Booking Service] BookingCreated (5ms)
├── [Fraud Service] FraudCheckPassed (120ms)
├── [Pricing Service] PriceQuoteCalculated (45ms)
├── [Geography Service] NearbyDriversFound (200ms)
├── [Driver Service] DriverAssignmentAccepted (2s)
├── [Payments Service] PaymentAuthorized (1.5s)
├── [Booking Service] BookingConfirmed (10ms)
├── [Notification Service] NotifyPassenger (50ms)
└── [Notification Service] NotifyDriver (50ms)
```

Поиск по `correlation_id` в Loki показывает все логи связанного Saga-инстанса.

## Компромиссы

| Решение                            | Преимущество                              | Ограничение                                      |
|------------------------------------|-------------------------------------------|--------------------------------------------------|
| Kafka Exporter вместо JMX напрямую | Prometheus-native, простая интеграция     | Ограниченный набор метрик (но достаточный)       |
| Burrow отдельно от Kafka Exporter  | Специализированная оценка lag status      | Ещё один компонент для поддержки                 |
| OTel Collector как единый pipeline | Vendor-neutral, единая точка конфигурации | Потенциальная точка отказа (решается HA)         |
| Grafana как единый UI              | Одно стекло для метрик, логов, трейсов    | Нагрузка на Grafana при большом кол-ве дашбордов |
| Prometheus Remote Write            | Долгосрочное хранение метрик              | Доп. latency, потребление ресурсов               |
