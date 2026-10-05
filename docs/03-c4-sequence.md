# 3. C4-модель и Sequence-диаграммы

Диаграммы в Mermaid (рендерятся в GitLab/GitHub; для отчёта экспортируются в PNG через mermaid-cli).

> Примечание: модель C4 содержит 4 уровня (Context, Container, Component, Code). В задании указано «L1–L7»; уровни C4 сделаны L1–L4, а дополнительно приведена привязка компонентов к уровням сетевой модели OSI L1–L7 (раздел 3.5) — уточнить у преподавателя, что имелось в виду.

## 3.1 L1 — System Context
```mermaid
C4Context
title FlashMarket — System Context
Person(buyer, "Покупатель", "Ищет и покупает товары, в т.ч. на распродаже")
Person(seller, "Продавец", "Загружает товары, остатки, акции; смотрит продажи")
Person(admin, "Администратор/SRE", "Эксплуатирует платформу")
System(fm, "FlashMarket", "Маркетплейс, рассчитанный на пик Чёрной пятницы")
System_Ext(idp, "Keycloak", "OIDC Identity Provider")
System_Ext(pay, "Платёжный провайдер", "Эмулятор эквайринга")
System_Ext(mail, "SMTP / Push", "Доставка уведомлений")
Rel(buyer, fm, "HTTPS/JSON")
Rel(seller, fm, "HTTPS/JSON")
Rel(admin, fm, "Grafana, ArgoCD")
Rel(buyer, idp, "OIDC login")
Rel(fm, idp, "JWKS (проверка JWT)")
Rel(fm, pay, "HTTPS API, webhooks")
Rel(fm, mail, "SMTP/HTTPS")
```

## 3.2 L2 — Containers
```mermaid
C4Container
title FlashMarket — Containers
Person(buyer, "Покупатель")
System_Boundary(fm, "FlashMarket (Kubernetes)") {
  Container(lb, "Keepalived + HAProxy x2", "L4/L7 LB", "VIP, балансировка, health-checks")
  Container(gw, "Envoy Gateway", "Gateway API", "Маршрутизация, JWT, rate limiting, waiting room")
  Container(rls, "Envoy Rate Limit Service", "Go", "Счётчики лимитов")
  Container(cat, "catalog-service", "FastAPI")
  Container(inv, "inventory-service", "Go")
  Container(ord, "order-service", "Go")
  Container(paysvc, "payment-service", "FastAPI")
  Container(notif, "notification-service", "FastAPI")
  Container(an, "analytics-service", "FastAPI")
  ContainerQueue(kafka, "Apache Kafka (Strimzi)", "KRaft, 3 брокера", "Шина событий, буфер пика")
  ContainerDb(pg, "PostgreSQL (CloudNativePG)", "RDBMS", "остатки, заказы, платежи")
  ContainerDb(mongo, "MongoDB", "Document NoSQL", "каталог, журнал уведомлений")
  ContainerDb(ch, "ClickHouse", "Columnar NoSQL", "аналитика, холодные события")
  ContainerDb(valkey, "Valkey", "In-memory KV", "кэш, счётчики остатков, корзины, rate limit, очередь ожидания")
  ContainerDb(s3, "MinIO (S3)", "Object storage", "архив событий, картинки товаров")
}
System_Ext(pay, "Платёжный провайдер")
Rel(buyer, lb, "HTTPS")
Rel(lb, gw, "HTTP/2")
Rel(gw, rls, "gRPC")
Rel(rls, valkey, "RESP")
Rel(gw, cat, "HTTP")
Rel(gw, ord, "HTTP")
Rel(gw, paysvc, "HTTP")
Rel(gw, an, "HTTP")
Rel(ord, inv, "HTTP (резерв, sync)")
Rel(cat, mongo, "")
Rel(cat, valkey, "cache-aside")
Rel(inv, valkey, "Lua DECRBY")
Rel(inv, pg, "SQL")
Rel(ord, pg, "SQL")
Rel(ord, valkey, "корзины")
Rel(paysvc, pg, "SQL")
Rel(ord, kafka, "produce/consume")
Rel(inv, kafka, "produce/consume")
Rel(paysvc, kafka, "produce")
Rel(notif, kafka, "consume")
Rel(an, kafka, "consume")
Rel(an, ch, "insert/select")
Rel(an, s3, "archive")
Rel(paysvc, pay, "HTTPS")
```

## 3.3 L3 — Components (inventory-service)
```mermaid
C4Component
title inventory-service — Components
Container_Boundary(i, "inventory-service") {
  Component(api, "REST API", "HTTP handlers", "/reservations, /stock")
  Component(res, "ReservationEngine", "Lua в Valkey", "атомарный DECRBY всех позиций + лимит на покупателя")
  Component(repo, "StockRepository", "pgx", "остатки и резервы + outbox в одной транзакции")
  Component(sync, "StockSync", "worker", "сверка Valkey ↔ PostgreSQL, прогрев перед акцией")
  Component(relay, "OutboxRelay", "worker", "outbox → Kafka")
  Component(cons, "OrderEventsConsumer", "Kafka consumer group", "order.paid / order.cancelled")
  Component(exp, "ExpirationScheduler", "cron 30s", "возврат просроченных резервов")
}
ContainerDb(pg, "PostgreSQL")
ContainerDb(v, "Valkey")
ContainerQueue(k, "Kafka")
Rel(api, res, "")
Rel(api, repo, "")
Rel(res, v, "EVALSHA")
Rel(repo, pg, "SQL")
Rel(sync, v, "")
Rel(sync, pg, "")
Rel(relay, pg, "SELECT ... FOR UPDATE SKIP LOCKED")
Rel(relay, k, "produce inventory.*")
Rel(cons, k, "consume order.*")
Rel(cons, repo, "commit / release")
Rel(exp, res, "INCRBY (возврат)")
Rel(exp, repo, "")
```

Аналогичные L3 для остальных сервисов — по одному на участника команды (в работе).

## 3.4 L4 — Code (фрагмент inventory-service)
```mermaid
classDiagram
class Reservation {
  +UUID id
  +UUID orderId
  +UUID buyerId
  +List~ReservationItem~ items
  +ReservationStatus status
  +Instant expiresAt
  +commit()
  +release(reason)
}
class ReservationItem {
  +String sku
  +int qty
}
class ReservationStatus {
  <<enum>>
  ACTIVE
  COMMITTED
  RELEASED
  EXPIRED
}
class ReservationEngine {
  +reserve(orderId, buyerId, items, ttl) Result
  +release(reservation)
}
class StockRepository {
  +saveReservation(r, outbox)
  +decrementAvailable(sku, qty) bool
}
class OutboxMessage {
  +UUID id
  +String topic
  +String key
  +bytes payload
}
Reservation --> ReservationItem
Reservation --> ReservationStatus
ReservationEngine --> Reservation
ReservationEngine --> StockRepository
StockRepository --> OutboxMessage
```

## 3.5 Привязка к уровням OSI (L1–L7)
| OSI | Что в системе |
|---|---|
| L7 Application | Envoy Gateway (HTTP-маршруты, JWT, rate limit, waiting room), Istio VirtualService/retries, Kafka protocol |
| L6 Presentation | TLS 1.3 на входе, mTLS в mesh, JSON/Avro-сериализация |
| L5 Session | HTTP/2 keep-alive, gRPC-стримы (RLS), сессии консьюмер-групп Kafka |
| L4 Transport | HAProxy TCP-mode, Cilium eBPF kube-proxy replacement, outlier detection по TCP-ошибкам |
| L3 Network | Cilium (IP-адресация подов, NetworkPolicy), Keepalived VIP |
| L2 Data link | VRRP (Keepalived) / gratuitous ARP для VIP |
| L1 Physical | узлы кластера (VM/Talos), виртуальные NIC |

## 3.6 Sequence — оформление и оплата заказа (happy path + компенсация)
```mermaid
sequenceDiagram
autonumber
actor U as Покупатель
participant LB as HAProxy (VIP)
participant GW as Envoy Gateway
participant RL as RateLimit (Valkey)
participant O as order-service
participant I as inventory-service
participant V as Valkey
participant PG as PostgreSQL
participant K as Kafka
participant P as payment-service
participant N as notification-service
participant A as analytics-service

U->>LB: POST /api/v1/orders {cart}
LB->>GW: forward
GW->>RL: ShouldRateLimit(ip, user)
RL-->>GW: OK
GW->>O: POST /orders (JWT claims)
O->>I: POST /reservations {items, buyer}
I->>V: EVAL reserve.lua (лимит на покупателя + DECRBY всех SKU)
alt остатка хватает
  V-->>I: OK
  I->>PG: BEGIN; INSERT reservation; INSERT outbox; COMMIT
  I-->>O: 201 reservationId
  O->>PG: INSERT order RESERVED + outbox
  O-->>U: 201 {orderId, payUrl, expiresAt}
  I--)K: inventory.reserved
  O--)K: order.created
else нет в наличии
  V-->>I: OUT_OF_STOCK (частичные списания откатаны в скрипте)
  I-->>O: 409
  O-->>U: 409 Нет в наличии
end
U->>GW: POST /payments (Idempotency-Key)
GW->>P: forward
P->>P: вызов провайдера
alt успех
  P--)K: payment.succeeded
  K--)O: payment.succeeded
  O->>PG: UPDATE order PAID + outbox
  O--)K: order.paid
  K--)I: order.paid → резерв COMMITTED
  K--)N: order.paid → письмо покупателю
  K--)A: order.paid → INSERT ClickHouse
else отказ
  P--)K: payment.failed
  K--)O: payment.failed
  O--)K: order.cancelled
  K--)I: order.cancelled → INCRBY (возврат остатка)
end
```

## 3.7 Sequence — старт распродажи: rate limiter, waiting room, circuit breaker
```mermaid
sequenceDiagram
actor Bot
actor U as Покупатель
participant GW as Envoy Gateway
participant RL as RateLimit Service
participant WR as Waiting room (Valkey ZSET)
participant SC as istio-proxy (sidecar)
participant P as payment-service (pod-2, сбоит)
Note over GW: 00:00 — старт Чёрной пятницы, трафик x30
Bot->>GW: 200 req/s POST /orders
GW->>RL: ShouldRateLimit
RL-->>GW: OVER_LIMIT
GW-->>Bot: 429 Too Many Requests
U->>GW: GET /promo/black-friday
GW->>WR: активных сессий > порога?
WR-->>GW: да, позиция 1532
GW-->>U: 200 страница очереди (позиция, ETA)
Note over SC,P: Outlier detection: 3 подряд 5xx → pod извлекается из пула на 30 с
SC->>P: POST /payments
P-->>SC: 503
SC->>SC: pod-2 ejected, трафик идёт на здоровые pod'ы
```
