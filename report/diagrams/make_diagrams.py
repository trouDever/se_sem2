"""Исходники диаграмм отчёта (Mermaid) и их рендер в PNG через mermaid-cli + локальный Edge."""
import json
import os
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).parent
STYLE = """
    classDef person fill:#08427b,stroke:#052e56,color:#fff
    classDef sys fill:#1168bd,stroke:#0b4884,color:#fff
    classDef ext fill:#8a8a8a,stroke:#6b6b6b,color:#fff
    classDef cont fill:#438dd5,stroke:#2e6295,color:#fff
    classDef edge fill:#2f7d6d,stroke:#1f574b,color:#fff
    classDef db fill:#f5f5f5,stroke:#444,color:#111
    classDef plat fill:#5b6f8f,stroke:#3c4a60,color:#fff
"""

D = {}

D["01-l1-context"] = """flowchart LR
    buyer(["<b>Покупатель</b><br/>[Person]<br/>Смотрит акции, покупает<br/>дефицитные товары в 00:00"]):::person
    seller(["<b>Продавец</b><br/>[Person]<br/>Товары, остатки,<br/>акционные цены"]):::person
    fm["<b>FlashMarket</b><br/>[Software System]<br/>Маркетплейс, рассчитанный на пик<br/>Чёрной пятницы: витрина, резерв без<br/>overselling, оплата, уведомления, аналитика"]:::sys
    idp["<b>Keycloak</b><br/>[External System]<br/>OIDC, выдаёт JWT"]:::ext
    psp["<b>Платёжный провайдер</b><br/>[External System]<br/>Эмулируется в payment-service"]:::ext
    mail["<b>Почтовый провайдер</b><br/>[External System]<br/>Эмулируется журналом<br/>notification-service"]:::ext
    buyer -- "HTTPS через VIP" --> fm
    seller -- "HTTPS через VIP" --> fm
    buyer -. "вход (OIDC)" .-> idp
    fm -- "проверка JWT (JWKS)" --> idp
    fm -- "списания [HTTPS]" --> psp
    fm -- "письма [SMTP/API]" --> mail
""" + STYLE

D["02-l2-container"] = """flowchart TB
    buyer(["<b>Покупатель</b>"]):::person
    subgraph FM["FlashMarket [Software System]"]
      direction TB
      lb["<b>Edge load balancer</b><br/>[HAProxy 3.2 ×2 + Keepalived]<br/>VIP по VRRP, L4 leastconn"]:::edge
      gw["<b>API gateway</b><br/>[Istio Gateway, Gateway API]<br/>маршруты /api/v1/*, mTLS"]:::edge
      rl["<b>Rate limit service</b><br/>[envoyproxy/ratelimit ×2]<br/>120 rpm на покупателя,<br/>300 rps на /orders, fail-open"]:::edge
      cat["<b>catalog-service</b><br/>[FastAPI]<br/>витрина, cache-aside"]:::cont
      ord["<b>order-service</b><br/>[FastAPI]<br/>заказы, сага, outbox"]:::cont
      inv["<b>inventory-service</b><br/>[FastAPI]<br/>атомарный резерв,<br/>лимит в руки, sweeper"]:::cont
      pay["<b>payment-service</b><br/>[FastAPI]<br/>платежи, эмуляция PSP"]:::cont
      noti["<b>notification-service</b><br/>[FastAPI]"]:::cont
      ana["<b>analytics-service</b><br/>[FastAPI]<br/>продажи/мин, архив"]:::cont
      kafka[["<b>Kafka</b> [Strimzi, KRaft]<br/>order / payment / inventory /<br/>catalog .events × 6 партиций"]]:::cont
      pg[("<b>PostgreSQL 17 ×3</b><br/>inventory · order · payment")]:::db
      mongo[("<b>MongoDB 7</b><br/>каталог, журнал уведомлений")]:::db
      vk[("<b>Valkey 8.1</b><br/>горячие остатки, кэш,<br/>счётчики лимитов, агрегаты")]:::db
      s3[("<b>MinIO</b><br/>events-archive (JSONL.gz)")]:::db
    end
    buyer -->|HTTP via VIP| lb -->|TCP NodePort 30080| gw
    gw -->|gRPC check| rl -->|RESP| vk
    gw --> cat & ord & pay & ana & noti
    ord -->|"HTTP: резерв (retry + CB)"| inv
    inv -->|Lua DECRBY| vk
    cat -->|кэш + остаток| vk
    cat --> mongo
    noti --> mongo
    inv & ord & pay --> pg
    ord -->|order.*| kafka
    inv -->|inventory.*| kafka
    pay -->|payment.*| kafka
    kafka -->|payment.*, inventory.*| ord
    kafka -->|order.*| inv & cat & noti
    kafka -->|все топики| ana
    ana --> s3 & vk
""" + STYLE

D["03-l3-component"] = """flowchart LR
    ordsvc["<b>order-service</b><br/>[Container]"]:::cont
    subgraph INV["inventory-service [Container: Python 3.12, FastAPI]"]
      direction TB
      api["<b>Reservation API</b><br/>[FastAPI router]<br/>POST /inventory/reservations<br/>идемпотентно по order_id"]:::cont
      eng["<b>ReservationEngine</b><br/>[Lua в Valkey]<br/>остаток + лимит в руки,<br/>всё или ничего"]:::cont
      repo["<b>StockRepository</b><br/>[asyncpg]<br/>UPDATE … WHERE available ≥ qty,<br/>reservation + outbox в 1 tx"]:::cont
      relay["<b>OutboxRelay</b><br/>[asyncio task]<br/>FOR UPDATE SKIP LOCKED"]:::cont
      cons["<b>OrderEventsConsumer</b><br/>[aiokafka, group inventory-service]<br/>order.paid → commit,<br/>order.cancelled → release"]:::cont
      sw["<b>ExpirationSweeper</b><br/>[каждые 10 с]<br/>возврат просроченных резервов"]:::cont
      audit["<b>Audit API</b><br/>GET /inventory/audit<br/>проверка overselling"]:::cont
    end
    vk[("Valkey")]:::db
    pg[("PostgreSQL inventory")]:::db
    k[["Kafka"]]:::cont
    ordsvc -->|HTTP| api --> eng -->|EVALSHA| vk
    api --> repo -->|SQL| pg
    relay -->|SELECT outbox| pg
    relay -->|inventory.*| k
    k -->|order.*| cons --> repo
    cons -->|INCRBY при отмене| vk
    sw --> repo
    sw -->|INCRBY| vk
    audit --> pg
""" + STYLE

D["04-l4-code"] = """classDiagram
    direction LR
    class ReservationAPI {
      +reserve(order_id, buyer_id, items)
      +stock(sku)
      +audit()
    }
    class RESERVE_LUA {
      <<Valkey script>>
      KEYS stock:sku, buyer:id:sku
      ARGV n, qty..., limit
      1 ok / -1 out_of_stock / -2 buyer_limit
    }
    class Stock {
      <<table stock>>
      +sku text PK
      +available int CHECK >= 0
      +reserved int CHECK >= 0
      +initial int
    }
    class Reservation {
      <<table reservations>>
      +order_id uuid PK
      +buyer_id text
      +items jsonb
      +status ReservationStatus
      +expires_at timestamptz
    }
    class ReservationStatus {
      <<enum>>
      ACTIVE
      COMMITTED
      RELEASED
      EXPIRED
    }
    class Outbox {
      <<table outbox>>
      +id bigserial
      +topic text
      +key text
      +payload jsonb
      +published_at timestamptz
    }
    class ProcessedEvent {
      <<table processed_events>>
      +event_id uuid PK
    }
    class OutboxRelay {
      +notify()
      +run()
    }
    class Producer {
      acks=all, idempotence
      +send(topic, event)
    }
    class Consumer {
      <<fm_common.run_consumer>>
      at-least-once, commit after handle
    }
    ReservationAPI ..> RESERVE_LUA : 1. горячий рубеж
    ReservationAPI ..> Stock : 2. источник истины
    ReservationAPI ..> Reservation
    ReservationAPI ..> Outbox : stage inventory.reserved
    Reservation --> ReservationStatus
    OutboxRelay ..> Outbox
    OutboxRelay ..> Producer
    Consumer ..> ProcessedEvent : dedup
"""

D["05-l5-landscape"] = """flowchart LR
    buyer(["<b>Покупатель</b>"]):::person
    seller(["<b>Продавец</b>"]):::person
    eng(["<b>Инженер платформы</b><br/>[Person]<br/>разрабатывает и эксплуатирует"]):::person
    fm["<b>FlashMarket</b><br/>[Software System]"]:::sys
    deliv["<b>Delivery platform</b><br/>[Software System]<br/>Git, Terraform, Ansible, Helm,<br/>ArgoCD (App of Apps), CI + Kaniko"]:::plat
    obs["<b>Observability platform</b><br/>[Software System]<br/>VictoriaMetrics, vmagent, vmalert,<br/>Alertmanager, Grafana, Hubble"]:::plat
    idp["Keycloak"]:::ext
    psp["PSP"]:::ext
    mail["E-mail"]:::ext
    buyer --> fm
    seller --> fm
    eng -->|git push| deliv -->|собирает и выкатывает| fm
    fm -->|метрики| obs
    eng -->|дашборды, алерты| obs
    fm --> idp & psp & mail
""" + STYLE

D["06-l6-dynamic"] = """flowchart LR
    b(["Покупатель"]):::person
    lb["HAProxy (VIP)"]:::edge
    gw["Istio Gateway"]:::edge
    rl["Rate limit"]:::edge
    o["order-service"]:::cont
    i["inventory-service"]:::cont
    p["payment-service"]:::cont
    k[["Kafka"]]:::cont
    n["notification"]:::cont
    a["analytics"]:::cont
    c["catalog"]:::cont
    vk[("Valkey")]:::db
    pg[("PostgreSQL")]:::db
    b -->|"1: POST /orders"| lb -->|2| gw
    gw -->|"3: check limits"| rl
    gw -->|4| o
    o -->|"5: POST /reservations"| i
    i -->|"6: Lua: остаток+лимит"| vk
    i -->|"7: reservation + outbox"| pg
    o -->|"8: order RESERVED + outbox, 201"| pg
    b -->|"9: POST /payments"| gw
    gw -->|10| p
    p -->|"11: payment.succeeded"| k
    k -->|12| o
    o -->|"13: order.paid"| k
    k -->|"14: commit резерва"| i
    k -->|"15: письмо"| n
    k -->|"16: продажи/мин + архив"| a
    k -->|"17: сброс кэша витрины"| c
""" + STYLE

D["07-l7-deployment"] = """flowchart TB
    subgraph HOST["Ноутбук: Windows 10, 16 GB / 16 CPU"]
      direction TB
      subgraph DD["Docker Desktop (WSL2), лимит 8 GB"]
        direction TB
        subgraph NET["docker network fm-edge 172.30.0.0/24"]
          h1["<b>haproxy-1</b> 172.30.0.11<br/>MASTER, priority 110"]:::edge
          h2["<b>haproxy-2</b> 172.30.0.12<br/>BACKUP, priority 100"]:::edge
          vip(("VIP<br/>172.30.0.100")):::edge
          loc["<b>locust</b><br/>нагрузочный тест"]:::plat
        end
        subgraph NODE["kind node flashmarket-control-plane: K8s 1.37, Cilium 1.20 без kube-proxy"]
          direction TB
          subgraph E["ns edge"]
            gw["Istio Gateway<br/>NodePort 30080"]:::edge
            rl["ratelimit ×2"]:::edge
          end
          subgraph F["ns flashmarket: Istio sidecar, mTLS STRICT"]
            svc["catalog ×2 · order ×2 · inventory ×2<br/>payment ×2 · notification · analytics"]:::cont
          end
          subgraph K["ns kafka"]
            kf[["Strimzi operator<br/>Kafka broker+controller, profile local<br/>kafka-exporter"]]:::cont
          end
          subgraph DT["ns data"]
            dbs[("pg-inventory · pg-order · pg-payment<br/>mongodb · valkey · minio")]:::db
          end
          subgraph O["ns observability"]
            ob["vmsingle · vmagent · vmalert<br/>alertmanager · grafana"]:::plat
          end
          ist["ns istio-system: istiod"]:::plat
        end
      end
    end
    loc --> vip
    vip -.-> h1
    vip -.-> h2
    h1 -->|TCP| gw
    h2 -->|TCP| gw
    gw --> rl
    gw --> svc
    svc --> kf
    svc --> dbs
    ob -. scrape .-> svc
""" + STYLE

D["08-seq-order"] = """sequenceDiagram
    autonumber
    actor U as Покупатель
    participant E as Edge (HAProxy, Istio GW)
    participant R as Rate limit (Valkey)
    participant O as order-service
    participant I as inventory-service
    participant V as Valkey
    participant DB as PostgreSQL
    participant K as Kafka
    participant P as payment-service
    U->>E: POST /api/v1/orders (Idempotency-Key)
    E->>R: user_id, route=orders
    R-->>E: OK
    E->>O: POST /orders
    O->>I: POST /inventory/reservations
    I->>V: EVAL reserve.lua (остаток + лимит в руки)
    alt товар есть
        V-->>I: 1
        I->>DB: UPDATE stock WHERE available >= qty, reservation + outbox
        I-->>O: 201 ACTIVE, expires_at
        O->>DB: INSERT order RESERVED + outbox (1 tx)
        O-->>U: 201 order_id, pay_url
        I--)K: inventory.reserved
        O--)K: order.created
    else нет остатка или лимит
        V-->>I: -1 или -2
        I-->>O: 409 или 422
        O-->>U: 409 Нет в наличии / 422 Лимит 2 шт.
    end
    U->>E: POST /api/v1/payments (Idempotency-Key)
    E->>P: POST /payments
    P->>DB: INSERT payment + outbox
    P-->>U: 201 SUCCEEDED
    P--)K: payment.succeeded
    K--)O: payment.succeeded
    O->>DB: order PAID + outbox
    O--)K: order.paid
    K--)I: order.paid, резерв COMMITTED
"""

D["09-seq-compensation"] = """sequenceDiagram
    participant P as payment-service
    participant K as Kafka
    participant O as order-service
    participant I as inventory-service
    participant V as Valkey
    participant N as notification-service
    Note over P,N: 2a. PSP отклонил платёж
    P--)K: payment.failed
    K--)O: payment.failed
    O->>O: RESERVED -> CANCELLED (+ outbox)
    O--)K: order.cancelled
    K--)I: order.cancelled
    I->>I: reservation RELEASED, available += qty
    I->>V: INCRBY stock, DECRBY buyer limit
    I--)K: inventory.released (reason=released)
    K--)N: письмо: оплата не прошла
    Note over P,N: UC-3. Резерв истёк (15 мин без оплаты)
    loop каждые 10 с
        I->>I: SELECT reservations WHERE ACTIVE AND expires_at < now()
    end
    I->>I: ACTIVE -> EXPIRED, available += qty
    I->>V: INCRBY stock
    I--)K: inventory.released (reason=expired)
    K--)O: inventory.released
    O->>O: RESERVED -> EXPIRED
    O--)K: order.expired
    K--)N: письмо: время резерва истекло
"""

D["10-seq-catalog"] = """sequenceDiagram
    actor U as Покупатель
    participant G as Istio Gateway
    participant C as catalog-service
    participant V as Valkey
    participant M as MongoDB
    U->>G: GET /api/v1/catalog/products?promo=true
    G->>C: GET /catalog/products
    C->>V: GET cat:list:...
    alt cache hit
        V-->>C: товары
    else cache miss
        C->>M: find(promo).limit(20)
        C->>V: SET cat:list:... EX 30+jitter
    end
    C->>V: MGET stock:BF-001..BF-020 (горячие остатки inventory)
    C-->>U: 200 товары + in_stock
    Note over C,V: order.paid / order.cancelled / price-changed -> DEL cat:list:*
"""

D["11-routing"] = """flowchart LR
    cl(["Клиент / Locust"]):::person
    vip(("VIP 172.30.0.100<br/>Keepalived VRRP")):::edge
    h1["haproxy-1 MASTER<br/>leastconn, tcp-check 2s"]:::edge
    h2["haproxy-2 BACKUP"]:::edge
    np["NodePort 30080"]:::edge
    gw["Istio Gateway (Envoy)<br/>HTTPRoute /api/v1/*<br/>timeout 5s, без retry"]:::edge
    rl["envoyproxy/ratelimit ×2<br/>user_id 120/min<br/>orders 300/s<br/>fail-open 250ms"]:::edge
    vk[("Valkey<br/>счётчики")]:::db
    subgraph MESH["ns flashmarket: sidecar Envoy, mTLS STRICT"]
      c["catalog"]:::cont
      o["order"]:::cont
      i["inventory"]:::cont
      p["payment"]:::cont
    end
    cl --> vip --> h1
    vip -. failover .-> h2
    h1 --> np
    h2 --> np
    np --> gw
    gw -->|gRPC| rl --> vk
    gw --> c & o & p
    o -->|"retry x2, outlier detection,<br/>least request"| i
""" + STYLE


def main():
    mmdc = pathlib.Path(sys.argv[1])
    out = HERE / "png"
    out.mkdir(exist_ok=True)
    cfg = HERE / "puppeteer.json"
    cfg.write_text(json.dumps({"executablePath": r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                               "args": ["--no-sandbox"]}))
    theme = HERE / "mermaid-config.json"
    theme.write_text(json.dumps({"theme": "default", "flowchart": {"htmlLabels": True, "curve": "basis"},
                                 "themeVariables": {"fontFamily": "Segoe UI, Arial"}}))
    only = sys.argv[2:]
    for name, src in D.items():
        if only and name not in only:
            continue
        f = HERE / f"{name}.mmd"
        f.write_text(src, encoding="utf-8")
        r = subprocess.run(["node", "node_modules/@mermaid-js/mermaid-cli/src/cli.js", "-i", str(f),
                            "-o", str(out / f"{name}.png"), "-p", str(cfg),
                            "-c", str(theme), "-s", "2", "-b", "white"], capture_output=True, text=True, cwd=mmdc)
        print(name, "OK" if r.returncode == 0 else "FAIL " + r.stderr[-400:])


if __name__ == "__main__":
    main()
