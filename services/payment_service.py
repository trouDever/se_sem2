"""payment-service: оплата заказа с эмуляцией платёжного провайдера (PSP) и идемпотентностью."""
import asyncio
import os
import random
import uuid
from contextlib import asynccontextmanager

import asyncpg
from fastapi import Header, HTTPException, Response
from prometheus_client import Counter

from fm_common import OUTBOX_DDL, OutboxRelay, Producer, create_app, make_event, stage

DB_URL = os.environ["DB_URL"]
PSP_DECLINE_RATE = float(os.getenv("PSP_DECLINE_RATE", "0.03"))
TOPIC = "payment.events"
PAYMENTS = Counter("fm_payments_total", "Payments by result", ["result"])

DDL = """
CREATE TABLE IF NOT EXISTS payments (
  id uuid PRIMARY KEY, order_id uuid NOT NULL, amount_cents bigint NOT NULL,
  status text NOT NULL CHECK (status IN ('SUCCEEDED','FAILED')), reason text,
  idempotency_key text UNIQUE NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE UNIQUE INDEX IF NOT EXISTS payments_one_success ON payments (order_id) WHERE status = 'SUCCEEDED';
"""

pool: asyncpg.Pool = None
producer = Producer()
relay: OutboxRelay = None


@asynccontextmanager
async def lifespan(app):
    global pool, relay
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=20)
    async with pool.acquire() as conn:
        await conn.execute(DDL + OUTBOX_DDL)
    await producer.start()
    relay = OutboxRelay(pool, producer)
    task = asyncio.create_task(relay.run())
    yield
    task.cancel()
    await producer.stop()


app = create_app(lifespan)


def view(r):
    return {"payment_id": str(r["id"]), "order_id": str(r["order_id"]), "status": r["status"],
            "amount_cents": r["amount_cents"], "reason": r["reason"]}


@app.post("/payments", status_code=201)
async def pay(body: dict, response: Response, idempotency_key: str = Header(...)):
    async with pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM payments WHERE idempotency_key=$1", idempotency_key)
    if r:  # повторный клик «Оплатить» не списывает деньги второй раз
        response.status_code = 200
        PAYMENTS.labels("duplicate").inc()
        return view(r)

    await asyncio.sleep(random.uniform(0.02, 0.06))  # эмуляция задержки PSP
    declined = body.get("card") == "fail" or random.random() < PSP_DECLINE_RATE
    status, reason = ("FAILED", "card declined") if declined else ("SUCCEEDED", None)
    order_id = str(body["order_id"])
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                r = await conn.fetchrow(
                    "INSERT INTO payments(id, order_id, amount_cents, status, reason, idempotency_key) "
                    "VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
                    uuid.uuid4(), uuid.UUID(order_id), int(body["amount_cents"]), status, reason, idempotency_key)
                await stage(conn, TOPIC, make_event(
                    "payment.succeeded" if status == "SUCCEEDED" else "payment.failed", order_id,
                    {"order_id": order_id, "amount_cents": int(body["amount_cents"]), "reason": reason}))
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "order already paid")
    relay.notify()
    PAYMENTS.labels(status.lower()).inc()
    return view(r)
