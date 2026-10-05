"""order-service: оформление заказа и оркестрация саги.

POST /orders -> синхронный резерв в inventory (через Istio: retries + circuit breaker)
-> заказ RESERVED + order.created в outbox одной транзакцией.
Дальше сага асинхронная: payment.succeeded -> PAID (order.paid),
payment.failed -> CANCELLED (order.cancelled -> inventory возвращает остаток),
inventory.released(expired) -> EXPIRED.
"""
import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import Header, HTTPException, Request, Response

from fm_common import (OUTBOX_DDL, OutboxRelay, Producer, create_app, jlog, make_event, mark_processed,
                       run_consumer, stage)

DB_URL = os.environ["DB_URL"]
INVENTORY_URL = os.getenv("INVENTORY_URL", "http://inventory-service")
TOPIC = "order.events"

DDL = """
CREATE TABLE IF NOT EXISTS orders (
  id uuid PRIMARY KEY, buyer_id text NOT NULL, items jsonb NOT NULL, total_cents bigint NOT NULL,
  status text NOT NULL CHECK (status IN ('RESERVED','PAID','CANCELLED','EXPIRED')),
  idempotency_key text UNIQUE, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz);
CREATE INDEX IF NOT EXISTS orders_buyer ON orders (buyer_id, created_at DESC);
"""

pool: asyncpg.Pool = None
http: httpx.AsyncClient = None
producer = Producer()
relay: OutboxRelay = None


def view(r):
    return {"order_id": str(r["id"]), "buyer_id": r["buyer_id"], "status": r["status"],
            "items": json.loads(r["items"]), "total_cents": r["total_cents"]}


async def transition(conn, order_id, expected, new, event_type):
    r = await conn.fetchrow("UPDATE orders SET status=$3, updated_at=now() WHERE id=$1 AND status=$2 RETURNING *",
                            uuid.UUID(order_id), expected, new)
    if r:
        await stage(conn, TOPIC, make_event(event_type, order_id, {
            "order_id": order_id, "buyer_id": r["buyer_id"], "total_cents": r["total_cents"],
            "items": json.loads(r["items"])}))


async def on_event(topic, ev):
    t, d = ev["event_type"], ev["data"]
    async with pool.acquire() as conn:
        async with conn.transaction():
            if not await mark_processed(conn, ev["event_id"]):
                return
            if t == "payment.succeeded":
                await transition(conn, d["order_id"], "RESERVED", "PAID", "order.paid")
            elif t == "payment.failed":
                await transition(conn, d["order_id"], "RESERVED", "CANCELLED", "order.cancelled")
            elif t == "inventory.released" and d.get("reason") == "expired":
                await transition(conn, d["order_id"], "RESERVED", "EXPIRED", "order.expired")
    relay.notify()


@asynccontextmanager
async def lifespan(app):
    global pool, http, relay
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=20)
    async with pool.acquire() as conn:
        await conn.execute(DDL + OUTBOX_DDL)
    http = httpx.AsyncClient(base_url=INVENTORY_URL, timeout=3.0)
    await producer.start()
    relay = OutboxRelay(pool, producer)
    tasks = [asyncio.create_task(relay.run()),
             asyncio.create_task(run_consumer(["payment.events", "inventory.events"], "order-service", on_event))]
    yield
    for t in tasks:
        t.cancel()
    await producer.stop()
    await http.aclose()


app = create_app(lifespan)


# Заголовки трассировки, которые Envoy ставит на входящий запрос. Их нужно передать дальше,
# иначе вызов inventory окажется отдельным трейсом, а не продолжением запроса покупателя.
TRACE_HEADERS = ("x-request-id", "traceparent", "tracestate", "x-b3-traceid", "x-b3-spanid",
                 "x-b3-parentspanid", "x-b3-sampled", "x-b3-flags")


@app.post("/orders", status_code=201)
async def create_order(body: dict, request: Request, response: Response, x_user_id: str = Header(...),
                       idempotency_key: str | None = Header(None)):
    items = [{"sku": i["sku"], "qty": int(i.get("qty", 1)), "price_cents": int(i["price_cents"])}
             for i in body["items"]]
    if not items or any(i["qty"] <= 0 for i in items):
        raise HTTPException(400, "empty order")
    if idempotency_key:
        async with pool.acquire() as conn:
            r = await conn.fetchrow("SELECT * FROM orders WHERE idempotency_key=$1", idempotency_key)
        if r:
            response.status_code = 200
            return view(r)

    order_id = str(uuid.uuid4())
    try:
        res = await http.post("/inventory/reservations", json={
            "order_id": order_id, "buyer_id": x_user_id,
            "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items]},
            headers={h: request.headers[h] for h in TRACE_HEADERS if h in request.headers})
    except httpx.HTTPError as e:
        jlog("inventory unavailable", error=repr(e))
        raise HTTPException(503, "inventory unavailable, try again")
    if res.status_code in (409, 422):
        raise HTTPException(res.status_code, res.json().get("detail"))
    if res.status_code >= 400:
        raise HTTPException(503, f"inventory error {res.status_code}")

    total = sum(i["qty"] * i["price_cents"] for i in items)
    async with pool.acquire() as conn:
        async with conn.transaction():
            r = await conn.fetchrow(
                "INSERT INTO orders(id, buyer_id, items, total_cents, status, idempotency_key) "
                "VALUES ($1,$2,$3,$4,'RESERVED',$5) RETURNING *",
                uuid.UUID(order_id), x_user_id, json.dumps(items), total, idempotency_key)
            await stage(conn, TOPIC, make_event("order.created", order_id, {
                "order_id": order_id, "buyer_id": x_user_id, "total_cents": total, "items": items}))
    relay.notify()
    return view(r) | {"pay_url": f"/api/v1/payments?order_id={order_id}"}


@app.get("/orders/{order_id}")
async def get_order(order_id: uuid.UUID, x_user_id: str = Header(...)):
    async with pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM orders WHERE id=$1 AND buyer_id=$2", order_id, x_user_id)
    if not r:
        raise HTTPException(404)
    return view(r)


@app.get("/orders")
async def my_orders(x_user_id: str = Header(...)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM orders WHERE buyer_id=$1 ORDER BY created_at DESC LIMIT 20", x_user_id)
    return [view(r) for r in rows]
