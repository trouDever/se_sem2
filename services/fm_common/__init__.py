"""Общий код сервисов FlashMarket: конверт события, Kafka-продюсер/консьюмер,
transactional outbox (PostgreSQL), метрики Prometheus, инъекция отказов, health-пробы."""
import asyncio
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

SERVICE = os.getenv("SERVICE_NAME", "service")
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='{"ts":"%(asctime)s","level":"%(levelname)s","service":"' + SERVICE + '","msg":%(message)s}')
log = logging.getLogger(SERVICE)


def jlog(msg, **kw):
    log.info(json.dumps({"text": msg, **kw}, default=str, ensure_ascii=False))


EVENTS_PRODUCED = Counter("fm_events_produced_total", "Events published to Kafka", ["topic", "type"])
EVENTS_CONSUMED = Counter("fm_events_consumed_total", "Events consumed", ["topic", "type", "group", "result"])
EVENT_LATENCY = Histogram("fm_event_e2e_latency_seconds", "occurred_at -> consumed latency", ["topic", "group"],
                          buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30))
PRODUCE_LATENCY = Histogram("fm_kafka_produce_ack_seconds", "Broker ack latency (acks=all)", ["topic"],
                            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2))
OUTBOX_PENDING = Histogram("fm_outbox_batch_size", "Outbox rows published per batch",
                           buckets=(1, 5, 10, 25, 50, 100, 250))


def make_event(event_type: str, key: str, data: dict) -> dict:
    return {"event_id": str(uuid.uuid4()), "event_type": event_type, "key": key,
            "occurred_at": datetime.now(timezone.utc).isoformat(), "producer": SERVICE, "data": data}


# ---------------------------------------------------------------- Kafka
class Producer:
    """Kafka-клиент создаётся в start(), внутри event loop: так модули сервисов
    можно импортировать без Kafka (например, в модульных тестах)."""

    def __init__(self):
        self._p = None

    @staticmethod
    def _client():
        return AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all", enable_idempotence=True,
                                linger_ms=5, value_serializer=lambda v: json.dumps(v, default=str).encode())

    async def start(self):
        for attempt in range(60):
            self._p = self._client()
            try:
                await self._p.start()
                return
            except Exception as e:  # Kafka ещё не готова при старте пода
                jlog("kafka producer not ready", attempt=attempt, error=str(e))
                try:
                    await self._p.stop()
                except Exception:
                    pass
                await asyncio.sleep(2)
        raise RuntimeError("kafka unavailable")

    async def stop(self):
        if self._p is not None:
            await self._p.stop()

    async def send(self, topic: str, event: dict):
        t0 = time.perf_counter()
        await self._p.send_and_wait(topic, event, key=event["key"].encode(),
                                    headers=[("event_type", event["event_type"].encode())])
        PRODUCE_LATENCY.labels(topic).observe(time.perf_counter() - t0)
        EVENTS_PRODUCED.labels(topic, event["event_type"]).inc()


async def run_consumer(topics, group, handler, is_duplicate=None):
    """At-least-once консьюмер: коммит offset только после успешной обработки.
    is_duplicate(event) -> bool позволяет пропускать уже обработанные event_id."""
    while True:
        consumer = AIOKafkaConsumer(*topics, bootstrap_servers=KAFKA, group_id=group,
                                    enable_auto_commit=False, auto_offset_reset="earliest",
                                    value_deserializer=lambda v: json.loads(v))
        try:
            await consumer.start()
            jlog("consumer started", topics=topics, group=group)
            async for msg in consumer:
                ev = msg.value
                occurred = datetime.fromisoformat(ev["occurred_at"])
                EVENT_LATENCY.labels(msg.topic, group).observe(
                    (datetime.now(timezone.utc) - occurred).total_seconds())
                try:
                    if is_duplicate and await is_duplicate(ev):
                        EVENTS_CONSUMED.labels(msg.topic, ev["event_type"], group, "duplicate").inc()
                    else:
                        await handler(msg.topic, ev)
                        EVENTS_CONSUMED.labels(msg.topic, ev["event_type"], group, "ok").inc()
                    await consumer.commit()
                except Exception as e:
                    EVENTS_CONSUMED.labels(msg.topic, ev["event_type"], group, "error").inc()
                    jlog("handler failed, will retry", error=repr(e), event_id=ev["event_id"])
                    await asyncio.sleep(1)
                    raise  # offset не закоммичен -> после рестарта консьюмера событие придёт снова
        except Exception as e:
            jlog("consumer crashed, restarting", error=repr(e))
            await asyncio.sleep(3)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass


# ---------------------------------------------------------------- Outbox (PostgreSQL)
OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY, topic text NOT NULL, key text NOT NULL, payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS processed_events (event_id uuid PRIMARY KEY, processed_at timestamptz DEFAULT now());
"""


async def stage(conn, topic: str, event: dict):
    """Записать событие в outbox в той же транзакции, что и бизнес-данные."""
    await conn.execute("INSERT INTO outbox(topic, key, payload) VALUES ($1, $2, $3)",
                       topic, event["key"], json.dumps(event, default=str))


async def mark_processed(conn, event_id: str) -> bool:
    """True, если событие новое (вставка прошла); False — дубликат."""
    r = await conn.execute("INSERT INTO processed_events(event_id) VALUES ($1) ON CONFLICT DO NOTHING",
                           uuid.UUID(event_id))
    return r.endswith("1")


class OutboxRelay:
    def __init__(self, pool, producer: Producer):
        self.pool, self.producer, self.wake = pool, producer, asyncio.Event()

    def notify(self):
        self.wake.set()

    async def run(self):
        while True:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        rows = await conn.fetch(
                            "SELECT id, topic, payload FROM outbox WHERE published_at IS NULL "
                            "ORDER BY id LIMIT 200 FOR UPDATE SKIP LOCKED")
                        for r in rows:
                            await self.producer.send(r["topic"], json.loads(r["payload"]))
                        if rows:
                            await conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])",
                                               [r["id"] for r in rows])
                            OUTBOX_PENDING.observe(len(rows))
                if not rows:
                    try:
                        await asyncio.wait_for(self.wake.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass
                    self.wake.clear()
            except Exception as e:
                jlog("outbox relay error", error=repr(e))
                await asyncio.sleep(1)


# ---------------------------------------------------------------- App factory
FAULT = {"error_rate": 0.0, "until": 0.0}


def create_app(lifespan=None) -> FastAPI:
    app = FastAPI(title=SERVICE, lifespan=lifespan)

    @app.middleware("http")
    async def fault_injection(request: Request, call_next):
        p = request.url.path
        if (FAULT["error_rate"] > 0 and time.time() < FAULT["until"]
                and not p.startswith(("/health", "/metrics", "/admin"))
                and random.random() < FAULT["error_rate"]):
            return JSONResponse({"error": "injected fault"}, status_code=503)
        resp = await call_next(request)
        resp.headers["x-served-by"] = os.getenv("HOSTNAME", SERVICE)
        return resp

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready():
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/admin/fault")
    async def set_fault(body: dict):
        """Тестовый хук для chaos-экспериментов: {"error_rate": 1.0, "seconds": 120}."""
        FAULT["error_rate"] = float(body.get("error_rate", 0))
        FAULT["until"] = time.time() + float(body.get("seconds", 60))
        jlog("fault injection set", **FAULT)
        return FAULT

    return app
