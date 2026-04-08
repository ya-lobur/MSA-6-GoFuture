---
apply: always
---

Take all the following into account

# Who am I

- I'm a python backend developer.
- I'm doing a course about microservice architecture.

# Project Description

- This is the final sprit project.

## The sprint's main topics

- Breaking down a monolithic application into microservices.
- Designing an event-driven architecture
- Ensuring high availability
- Designing a monitoring system

### Working with microservice telemetry

- Monitoring, Logging, Observability
- Collecting and visualizing metrics using Prometheus and Grafana
- ELK. Creating and configuring indexes using Elasticsearch
- Tracing

## Approaches, patterns, and tools

Ниже практичный To-Be стек для всех 4 заданий с базой на asynchronous Python + FastAPI и брокером Kafka

Он выглядит цельно и хорошо защищается на ревью: язык не меняется радикально, но платформа уходит от монолита к
событийной микросервисной архитектуре.

### Базовый стек платформы

- Язык: Python 3.12+
- Backend framework: FastAPI
- Async runtime: asyncio
- ASGI server: Uvicorn
- Межсервисные API: REST для синхронных вызовов, Kafka для асинхронных
- Брокер событий: Apache Kafka
- Schema management: Schema Registry с Avro или Protobuf
- База данных сервисов: PostgreSQL
- Кэш: Redis
- Поиск и геоданные: Elasticsearch или OpenSearch
- Аналитика: ClickHouse
- Контейнеризация: Docker
- Оркестрация: Kubernetes
- API gateway / ingress: NGINX Ingress или Kong
- Observability: Prometheus, Grafana, Loki, Tempo/Jaeger
- CI/CD: GitHub Actions или GitLab CI, Helm, Argo CD
- Secrets/config: Vault или Kubernetes Secrets + External Secrets
- Service mesh при необходимости: Istio или Linkerd

### Task1: Декомпозиция на доменные сервисы

[README.md](../../task1/README.md)

- Сервисы: Booking, Driver, Pricing, Payments, Notification, Geography, Analytics, Fraud
- Каждый сервис: FastAPI + PostgreSQL + Redis
- Интеграция между сервисами: сначала REST + Kafka, затем смещение в сторону event-driven
- Паттерны миграции: Strangler Fig, Anti-Corruption Layer, Outbox Pattern
- Миграции БД: Alembic
- Контракты API: OpenAPI
- Контракты событий: Avro/Protobuf + Schema Registry

#### Почему подходит:

- соответствует постепенной декомпозиции без полного rewrite;
- Python/FastAPI позволяет быстро выделять сервисы из монолита.

### Task2: Event-Driven архитектура

[README.md](../../task2/README.md)

- Брокер: Apache Kafka
- Python Kafka client: aiokafka
- Stream processing:
    - основной вариант: Kafka Streams/ksqlDB как платформенный слой;
    - если хочешь остаться ближе к Python: Faust лучше не брать как основной выбор, он спорный по зрелости;
    - для серьёзной потоковой обработки лучше указать Apache Flink
- Saga: Choreography-based Saga для бронирования/назначения водителя/оплаты
- Надёжная доставка: Transactional Outbox, idempotent consumers, DLQ, retry topics
- Мониторинг Kafka: Kafka Exporter, Burrow опционально
- Трассировка: OpenTelemetry

#### Рекомендация для защиты:

- бизнес-сервисы пишутся на async Python/FastAPI;
- Kafka и Flink используются как инфраструктурный и streaming-слой, не как основной язык разработки доменных сервисов.

### Task3: Высокая нагрузка и multi-region

[README.md](../../task3/README.md)

- Оркестрация: Kubernetes
- Multi-region deployment: несколько Kubernetes-кластеров по регионам
- Global routing: Global DNS + GeoDNS + health checks
- Edge protection: WAF, CDN, Cloud Load Balancer
- Kafka в нескольких регионах:
    - основной вариант: regional Kafka clusters + MirrorMaker 2
    - если хочешь упростить описание: active-passive replication для части доменов, active-active только там, где это
      оправдано
- БД:
    - PostgreSQL с региональными инстансами и репликацией
    - для глобально чувствительных сценариев: разделение данных по регионам
- Кэш: Redis локально в каждом регионе
- Failover: DNS failover, репликация Kafka, read replicas / standby DB, резервный ingress

#### Почему это логично:

- Python здесь не ограничение;
- требования высокой доступности решаются инфраструктурой, репликацией и отказоустойчивым дизайном, а не сменой языка.

### Task4: Мультитенантность, IAM и onboarding

[README.md](../../task4/README.md)

- IAM / SSO: Keycloak
- AuthN/AuthZ: OIDC, OAuth 2.0, JWT
- Tenant management service: FastAPI
- Partner onboarding service: FastAPI
- Хранение tenant metadata: PostgreSQL
- Изоляция данных:
    - рекомендуемый вариант: schema-per-tenant или database-per-tenant для крупных партнёров
    - для менее критичных данных можно shared DB + tenant_id
- Асинхронный onboarding: через Kafka
- Аудит: Kafka + ClickHouse или PostgreSQL audit log
- Мониторинг по арендаторам: Prometheus с tenant labels, Grafana dashboards, централизованные логи в Loki

### Итоговый стек, который можно последовательно использовать во всех заданиях

- Python 3.12+
- FastAPI
- Uvicorn
- PostgreSQL
- Redis
- Apache Kafka
- aiokafka
- Schema Registry + Avro/Protobuf
- Elasticsearch/OpenSearch
- ClickHouse
- Docker
- Kubernetes
- Helm
- Argo CD
- Prometheus + Grafana + Loki + Tempo/Jaeger
- OpenTelemetry
- Keycloak
- Batch Processing
- ETL
- Airflow
- MapReduce
- Spring Batch
- Distributed Scheduling
- Cron Jobs в k8s
- OLK / ELK
- OpenTelemetry
- Prometheus
- Alertmanager
- Grafana

# Task Execution Rules

- All the tasks are in Russian.
- If it's asked to make a C4 / .puml diagram all the components must be named in English but all the rest diagram text
  in Russian
- Reports and ADRs should be in Russian, and they have to comply with .puml diagrams.
- Prefer using technologies that are described in this file and task-files and any related tech stack.
- You are allowed to bring new and/or related technologies.
- After the execution any task always print the phrase "All done!" and list what was done.  