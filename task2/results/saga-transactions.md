# Реестр Saga-транзакций: Choreography-based Booking Saga

## Контекст

Процесс бронирования поездки в GoFuture охватывает несколько доменных сервисов: Booking, Fraud, Pricing, Geography,
Driver, Payments и Notification. Для обеспечения согласованности данных без распределённых транзакций используется
паттерн **Choreography-based Saga** — каждый сервис реагирует на события других сервисов и публикует свои,
продвигая процесс вперёд или инициируя компенсацию.

### Почему хореография, а не оркестрация

| Критерий              | Хореография (выбрано)                          | Оркестрация                                          |
|-----------------------|------------------------------------------------|------------------------------------------------------|
| Связанность           | Слабая — сервисы не знают друг о друге         | Сильная — оркестратор знает обо всех                 |
| Единая точка отказа   | Отсутствует                                    | Оркестратор — SPOF                                   |
| Масштабируемость      | Каждый сервис масштабируется независимо        | Оркестратор может стать узким горлышком              |
| Сложность отладки     | Выше — поток трассируется через correlation_id | Ниже — логика в одном месте                          |
| Подходит для GoFuture | Да — 8 независимых команд, event-driven стек   | Менее подходит — создаёт зависимость от координатора |

**Компенсация**: при хореографии каждый сервис сам отслеживает компенсирующие события и откатывает свои действия.
Трассировка обеспечивается через `correlation_id`, передаваемый во всех событиях Saga.

## Обзор транзакций

| #  | Транзакция          | Сервис               | Описание                                              | Тип           | Компенсация     |
|----|---------------------|----------------------|-------------------------------------------------------|---------------|-----------------|
| T1 | CREATE_BOOKING      | Booking Service      | Создание записи бронирования со статусом PENDING      | Compensatable | CANCEL_BOOKING  |
| T2 | CHECK_FRAUD         | Fraud Service        | ML-скоринг риска пассажира и поездки                  | Retriable     | —               |
| T3 | CALCULATE_PRICE     | Pricing Service      | Расчёт стоимости с учётом сурж-коэффициента           | Retriable     | —               |
| T4 | FIND_NEARBY_DRIVERS | Geography Service    | Поиск доступных водителей в радиусе от точки подачи   | Retriable     | —               |
| T5 | ASSIGN_DRIVER       | Driver Service       | Резервирование оптимального водителя                  | Compensatable | RELEASE_DRIVER  |
| T6 | AUTHORIZE_PAYMENT   | Payments Service     | Блокировка средств на карте пассажира                 | Compensatable | RELEASE_PAYMENT |
| T7 | CONFIRM_BOOKING     | Booking Service      | Подтверждение бронирования (Pivot — точка невозврата) | Pivot         | —               |
| N1 | NOTIFY_PASSENGER    | Notification Service | Отправка push-уведомления пассажиру                   | Retriable     | —               |
| N2 | NOTIFY_DRIVER       | Notification Service | Отправка push-уведомления водителю                    | Retriable     | —               |
| C1 | CANCEL_BOOKING      | Booking Service      | Отмена бронирования, статус → CANCELLED               | Compensating  | —               |
| C2 | RELEASE_DRIVER      | Driver Service       | Освобождение водителя, статус → AVAILABLE             | Compensating  | —               |
| C3 | RELEASE_PAYMENT     | Payments Service     | Снятие блокировки средств, возврат холда              | Compensating  | —               |
| N3 | NOTIFY_FAILURE      | Notification Service | Уведомление пассажира об отмене бронирования          | Retriable     | —               |

## Детали транзакций

### Прямые транзакции (Forward)

#### T1: CREATE_BOOKING

- **Тип**: Compensatable
- **Сервис**: Booking Service
- **Описание**: Создаёт запись бронирования в БД со статусом `PENDING` и записывает событие `BookingCreated` в
  Outbox-таблицу. Это входная точка Saga — все последующие шаги запускаются реакцией на это событие.
- **Входное событие**: HTTP-запрос от пассажира через API Gateway
- **Выходное событие**: `BookingCreated` (через Outbox → Debezium → Kafka)
- **Компенсация**: `CANCEL_BOOKING`
- **Таймаут**: 5 секунд на запись в БД
- **Обоснование**: Запись в БД и Outbox атомарно в одной PostgreSQL-транзакции. Компенсируется отменой записи.

#### T2: CHECK_FRAUD

- **Тип**: Retriable
- **Сервис**: Fraud Service
- **Описание**: Потребляет `BookingCreated`, выполняет ML-скоринг риска пассажира (история поездок, паттерны
  поведения, геолокация). Публикует `FraudCheckPassed` или `FraudCheckFailed`.
- **Входное событие**: `BookingCreated`
- **Выходное событие (успех)**: `FraudCheckPassed`
- **Выходное событие (отказ)**: `FraudCheckFailed`
- **Компенсация**: Не требуется — операция read-only (скоринг не меняет состояние)
- **Retry-политика**: 3 попытки с экспоненциальным backoff (1с, 5с, 30с)
- **Таймаут**: 10 секунд на скоринг

#### T3: CALCULATE_PRICE

- **Тип**: Retriable
- **Сервис**: Pricing Service
- **Описание**: Потребляет `BookingCreated`, рассчитывает стоимость с учётом маршрута, тарифа,
  сурж-коэффициента и промокодов. Публикует `PriceQuoteCalculated`.
- **Входное событие**: `BookingCreated`
- **Выходное событие**: `PriceQuoteCalculated`
- **Компенсация**: Не требуется — stateless расчёт
- **Retry-политика**: 3 попытки с экспоненциальным backoff
- **Таймаут**: 3 секунды на расчёт

#### T4: FIND_NEARBY_DRIVERS

- **Тип**: Retriable
- **Сервис**: Geography Service
- **Описание**: Потребляет `BookingCreated`, выполняет геопоиск в Elasticsearch по координатам точки подачи.
  Возвращает отсортированный список доступных водителей. Публикует `NearbyDriversFound`.
- **Входное событие**: `BookingCreated`
- **Выходное событие**: `NearbyDriversFound` (со списком driver_id)
- **Компенсация**: Не требуется — read-only геопоиск
- **Retry-политика**: 3 попытки
- **Таймаут**: 5 секунд

> **Примечание**: T2, T3 и T4 выполняются **параллельно** — все три реагируют на `BookingCreated` независимо.
> Booking Service ожидает завершения всех трёх перед переходом к T5.

#### T5: ASSIGN_DRIVER

- **Тип**: Compensatable
- **Сервис**: Driver Service
- **Описание**: Booking Service публикует `DriverAssignmentRequested` после получения результатов T2+T3+T4.
  Driver Service потребляет это событие, выбирает оптимального водителя из списка (не ближайшего, а с учётом
  загрузки зон), резервирует его (статус → `BUSY`) и публикует `DriverAssignmentAccepted`.
- **Входное событие**: `DriverAssignmentRequested`
- **Выходное событие (успех)**: `DriverAssignmentAccepted`
- **Выходное событие (отказ)**: `DriverAssignmentDeclined`
- **Компенсация**: `RELEASE_DRIVER`
- **Таймаут**: 30 секунд (ожидание подтверждения от водителя)
- **Retry**: При `DriverAssignmentDeclined` — повторная попытка со следующим водителем из списка (до 3 попыток)

#### T6: AUTHORIZE_PAYMENT

- **Тип**: Compensatable
- **Сервис**: Payments Service
- **Описание**: Потребляет `DriverAssignmentAccepted`, инициирует холд средств на карте пассажира через
  Яндекс Пэй. Публикует `PaymentAuthorized` или `PaymentFailed`.
- **Входное событие**: `DriverAssignmentAccepted`
- **Выходное событие (успех)**: `PaymentAuthorized`
- **Выходное событие (отказ)**: `PaymentFailed`
- **Компенсация**: `RELEASE_PAYMENT`
- **Retry-политика**: 2 попытки (платёжный шлюз идемпотентен по idempotency_key)
- **Таймаут**: 15 секунд на ответ платёжного шлюза

#### T7: CONFIRM_BOOKING (Pivot)

- **Тип**: Pivot (точка невозврата)
- **Сервис**: Booking Service
- **Описание**: Потребляет `PaymentAuthorized`, переводит бронирование в статус `CONFIRMED`. Это точка
  невозврата Saga — после подтверждения автоматическая компенсация невозможна, требуется бизнес-процесс
  отмены/возврата.
- **Входное событие**: `PaymentAuthorized`
- **Выходное событие**: `BookingConfirmed`
- **Компенсация**: Не предусмотрена — Pivot-транзакция
- **Обоснование**: Все компенсируемые транзакции (T1, T5, T6) завершены до этого шага. После Pivot отката нет.

#### N1: NOTIFY_PASSENGER

- **Тип**: Retriable
- **Сервис**: Notification Service
- **Описание**: Потребляет `BookingConfirmed`, отправляет push-уведомление пассажиру с деталями поездки
  (водитель, автомобиль, ETA).
- **Входное событие**: `BookingConfirmed`
- **Retry-политика**: До 5 попыток (уведомление не влияет на бизнес-процесс)

#### N2: NOTIFY_DRIVER

- **Тип**: Retriable
- **Сервис**: Notification Service
- **Описание**: Потребляет `BookingConfirmed`, отправляет push-уведомление водителю с деталями поездки
  (адрес подачи, имя пассажира).
- **Входное событие**: `BookingConfirmed`
- **Retry-политика**: До 5 попыток

### Компенсирующие транзакции (Compensation)

#### C1: CANCEL_BOOKING

- **Тип**: Compensating
- **Сервис**: Booking Service
- **Описание**: Переводит бронирование в статус `CANCELLED`, фиксирует причину отмены (фрод, нет водителей,
  сбой оплаты, таймаут). Публикует `BookingCancelled`.
- **Компенсирует**: T1 (CREATE_BOOKING)
- **Обоснование**: Очищает запись бронирования при сбое Saga до Pivot-точки.

#### C2: RELEASE_DRIVER

- **Тип**: Compensating
- **Сервис**: Driver Service
- **Описание**: Переводит статус водителя обратно в `AVAILABLE`. Публикует `DriverReleased`.
- **Компенсирует**: T5 (ASSIGN_DRIVER)
- **Обоснование**: Освобождает зарезервированного водителя при сбое оплаты или отмене бронирования.

#### C3: RELEASE_PAYMENT

- **Тип**: Compensating
- **Сервис**: Payments Service
- **Описание**: Отменяет холд средств через Яндекс Пэй, переводит платёж в статус `RELEASED`.
  Публикует `PaymentRefunded`.
- **Компенсирует**: T6 (AUTHORIZE_PAYMENT)
- **Обоснование**: Возвращает заблокированные средства при отмене бронирования.

### Уведомления при отказе

#### N3: NOTIFY_FAILURE

- **Тип**: Retriable
- **Сервис**: Notification Service
- **Описание**: Потребляет `BookingCancelled` или `BookingFailed`, отправляет push-уведомление пассажиру
  с причиной отмены.
- **Retry-политика**: До 5 попыток

## Потоки Saga (Saga Flow Patterns)

### Happy Path (успешное бронирование)

```
Пассажир → [HTTP] → Booking Service
  1. CREATE_BOOKING → BookingCreated
     ┌──────────────┼──────────────┐  (параллельно)
     ↓              ↓              ↓
  2. CHECK_FRAUD  3. CALCULATE_PRICE  4. FIND_NEARBY_DRIVERS
     ↓              ↓              ↓
  FraudCheckPassed  PriceQuoteCalculated  NearbyDriversFound
     └──────────────┼──────────────┘
                    ↓
  Booking Service агрегирует результаты →
  5. ASSIGN_DRIVER → DriverAssignmentRequested
     ↓
  DriverAssignmentAccepted
     ↓
  6. AUTHORIZE_PAYMENT → PaymentAuthorized
     ↓
  7. CONFIRM_BOOKING (Pivot) → BookingConfirmed
     ┌──────────┐
     ↓          ↓
  N1. NOTIFY_PASSENGER  N2. NOTIFY_DRIVER
```

**Общее время (p99)**: < 10 секунд

### Путь: Фрод обнаружен (Fraud Detected)

```
  1. CREATE_BOOKING → BookingCreated
     ↓
  2. CHECK_FRAUD → FraudCheckFailed
     ↓
  Booking Service потребляет FraudCheckFailed:
     C1. CANCEL_BOOKING → BookingCancelled
     ↓
  N3. NOTIFY_FAILURE (причина: подозрение на мошенничество)
```

**Компенсация**: Только C1 — на этом этапе водитель и платёж ещё не затронуты.

### Путь: Нет доступных водителей

```
  1. CREATE_BOOKING → BookingCreated
     ↓ (параллельно T2, T3, T4)
  4. FIND_NEARBY_DRIVERS → NearbyDriversFound (пустой список)
     ↓
  Booking Service:
     C1. CANCEL_BOOKING → BookingCancelled
     ↓
  N3. NOTIFY_FAILURE (причина: нет свободных водителей)
```

### Путь: Водитель отклонил заказ (с повтором)

```
  ...
  5. ASSIGN_DRIVER → DriverAssignmentDeclined
     ↓
  Booking Service: повторная попытка со следующим водителем
  5'. ASSIGN_DRIVER (водитель #2) → DriverAssignmentDeclined
     ↓
  5''. ASSIGN_DRIVER (водитель #3) → DriverAssignmentAccepted
     ↓
  6. AUTHORIZE_PAYMENT → ...
```

**Если все 3 водителя отклонили:**

```
  5'''. DriverAssignmentDeclined (третий раз)
     ↓
  C1. CANCEL_BOOKING → BookingCancelled
     ↓
  N3. NOTIFY_FAILURE (причина: нет доступных водителей)
```

### Путь: Ошибка оплаты (Payment Failed)

```
  ...
  5. ASSIGN_DRIVER → DriverAssignmentAccepted
     ↓
  6. AUTHORIZE_PAYMENT → PaymentFailed
     ↓
  Компенсация (обратный порядок):
     C2. RELEASE_DRIVER → DriverReleased
     C1. CANCEL_BOOKING → BookingCancelled
     ↓
  N3. NOTIFY_FAILURE (причина: ошибка оплаты)
```

### Путь: Таймаут Saga

```
  1. CREATE_BOOKING → BookingCreated (запущен таймер 60 секунд)
     ↓
  ... (какой-то шаг не ответил в течение таймаута)
     ↓
  Saga Watchdog обнаруживает застрявшее бронирование:
     Компенсация всех выполненных шагов (обратный порядок):
     C3. RELEASE_PAYMENT (если T6 выполнена)
     C2. RELEASE_DRIVER (если T5 выполнена)
     C1. CANCEL_BOOKING
     ↓
  N3. NOTIFY_FAILURE (причина: таймаут системы)
```

## Ключевые архитектурные решения

### Паттерны устойчивости

| Паттерн              | Применение                          | Описание                                                                    |
|----------------------|-------------------------------------|-----------------------------------------------------------------------------|
| Retry с backoff      | T2, T3, T4, N1, N2, N3              | Экспоненциальная задержка (1с, 5с, 30с)                                     |
| Circuit Breaker      | T6 (Яндекс Пэй), T4 (Elasticsearch) | Защита от каскадных отказов внешних систем                                  |
| Таймаут Saga         | Весь поток                          | 60 сек — макс. время жизни Saga                                             |
| Saga Watchdog        | Booking Service                     | Периодический cron: поиск зависших PENDING-бронирований, запуск компенсации |
| Transactional Outbox | T1, T5, T6, T7, C1, C2, C3          | Атомарная запись в БД + публикация через Debezium                           |
| Idempotent Consumer  | Все потребители                     | Дедупликация по `event_id` через `processed_events`                         |

### Управление состоянием

- **Booking Service** хранит состояние Saga через поле `booking_status`:
  ```
  PENDING → FRAUD_CHECKED → PRICED → DRIVERS_FOUND →
  DRIVER_ASSIGNED → PAYMENT_AUTHORIZED → CONFIRMED
  ```
- При сбое: `→ COMPENSATING → CANCELLED`
- Каждый переход состояния — отдельная строка в таблице `booking_events` (Event Sourcing-like audit log)
- Redis кеширует текущий статус для быстрого чтения

### Идемпотентность

- Все события содержат `event_id` (UUID v4)
- Каждый consumer ведёт таблицу `processed_events` с unique constraint на `event_id`
- Перед обработкой: `INSERT INTO processed_events (event_id) ... ON CONFLICT DO NOTHING`
- Если конфликт — событие уже обработано, пропускаем
- Для T6 (Payments): дополнительный `idempotency_key` на уровне API платёжного шлюза

### Мониторинг и трассировка

| Метрика                      | Описание                                    | Алерт                |
|------------------------------|---------------------------------------------|----------------------|
| `saga_duration_seconds`      | Время от BookingCreated до BookingConfirmed | p99 > 10с → Warning  |
| `saga_completed_total`       | Количество успешных Saga                    | —                    |
| `saga_failed_total`          | Количество неуспешных Saga                  | rate > 5% → Critical |
| `saga_compensation_total`    | Количество компенсаций по шагам             | rate > 10% → Warning |
| `saga_stuck_bookings`        | Бронирования в PENDING > 60 секунд          | > 0 → Critical       |
| `saga_step_duration_seconds` | Длительность каждого шага                   | p99 > 5с → Warning   |

- **Distributed Tracing**: `correlation_id` передаётся во всех событиях Saga. OpenTelemetry Collector собирает
  span'ы каждого шага и визуализирует весь поток в Grafana Tempo.
- **Логирование**: Каждый шаг логируется с `correlation_id` для поиска в Loki.

## Примечания к реализации

### Параллельные шаги

Шаги T2 (CHECK_FRAUD), T3 (CALCULATE_PRICE) и T4 (FIND_NEARBY_DRIVERS) выполняются **параллельно** —
все три сервиса потребляют `BookingCreated` независимо. Booking Service ожидает получения всех трёх результатов
(паттерн **Scatter-Gather**) перед переходом к T5. Это сокращает общее время Saga с ~15 до ~5 секунд.

### Saga Watchdog

Периодический процесс (Kubernetes CronJob, каждые 30 секунд):

1. Ищет бронирования в статусе `PENDING` с `created_at > 60 секунд назад`
2. Для каждого зависшего бронирования запускает компенсацию по текущему статусу
3. Логирует причину таймаута и `correlation_id` для расследования

### Масштабирование на новые Saga

Этот подход может быть расширен для других бизнес-процессов:

- **Saga завершения поездки**: RideCompleted → CompletePayment → UpdateDriverStats → GenerateReceipt
- **Saga выплаты водителю**: PayoutInitiated → ValidateBalance → BankTransfer → UpdateDriverAccount
