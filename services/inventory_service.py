"""inventory-service: остатки по SKU, атомарный резерв без overselling, возврат резервов.

Два рубежа защиты от overselling:
1. Valkey (горячий путь): Lua-скрипт проверяет остаток и лимит на покупателя для всех позиций
   и списывает их атомарно — всё или ничего. Отсекает основную массу конкурентных запросов
   без похода в БД.
2. PostgreSQL (источник истины): UPDATE ... WHERE available >= qty + CHECK (available >= 0).
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg
import valkey.asyncio as valkey
from fastapi import HTTPException
from prometheus_client import Counter

from fm_common import (OUTBOX_DDL, OutboxRelay, Producer, create_app, jlog, make_event, mark_processed,
                       run_consumer, stage)

DB_URL = os.environ["DB_URL"]
VALKEY_URL = os.getenv("VALKEY_URL", "valkey://valkey.data:6379")
RESERVATION_TTL = int(os.getenv("RESERVATION_TTL_SECONDS", "900"))
PROMO_LIMIT = int(os.getenv("PROMO_LIMIT_PER_BUYER", "2"))
TOPIC = "inventory.events"

RESERVE_RESULT = Counter("fm_inventory_reserve_total", "Reservation attempts", ["result"])

DDL = """
CREATE TABLE IF NOT EXISTS stock (
  sku text PRIMARY KEY, available int NOT NULL CHECK (available >= 0),
  reserved int NOT NULL DEFAULT 0 CHECK (reserved >= 0), initial int NOT NULL);
CREATE TABLE IF NOT EXISTS reservations (
  order_id uuid PRIMARY KEY, buyer_id text NOT NULL, items jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('ACTIVE','COMMITTED','RELEASED','EXPIRED')),
  created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL);
CREATE INDEX IF NOT EXISTS reservations_active ON reservations (expires_at) WHERE status = 'ACTIVE';
"""

# KEYS: stock:{sku}..., buyer:{buyer}:{sku}...   ARGV: n, qty..., limit
RESERVE_LUA = """
local n = tonumber(ARGV[1]); local limit = tonumber(ARGV[n + 2])
for i = 1, n do
  local qty = tonumber(ARGV[i + 1])
  local stock = tonumber(redis.call('GET', KEYS[i]) or '0')
  if stock < qty then return -1 end
  local bought = tonumber(redis.call('GET', KEYS[n + i]) or '0')
  if bought + qty > limit then return -2 end
end
for i = 1, n do
  local qty = tonumber(ARGV[i + 1])
  redis.call('DECRBY', KEYS[i], qty)
  redis.call('INCRBY', KEYS[n + i], qty)
  redis.call('EXPIRE', KEYS[n + i], 86400)
end
return 1
"""

pool: asyncpg.Pool = None
vk: valkey.Valkey = None
producer = Producer()
relay: OutboxRelay = None


def _keys(buyer, items):
    return [f"stock:{i['sku']}" for i in items] + [f"buyer:{buyer}:{i['sku']}" for i in items]


async def _valkey_release(buyer, items):
    async with vk.pipeline(transaction=True) as p:
        for i in items:
            p.incrby(f"stock:{i['sku']}", i["qty"])
            p.decrby(f"buyer:{buyer}:{i['sku']}", i["qty"])
        await p.execute()


async def seed():
    async with pool.acquire() as conn:
        await conn.execute(DDL + OUTBOX_DDL)
        if await conn.fetchval("SELECT count(*) FROM stock") == 0:
            # 20 акционных SKU с маленьким остатком (дефицит Чёрной пятницы) + 80 обычных
            rows = [(f"BF-{n:03d}", 300, 300) for n in range(1, 21)] + \
                   [(f"SKU-{n:03d}", 100000, 100000) for n in range(1, 81)]
            await conn.executemany("INSERT INTO stock(sku, available, initial) VALUES ($1,$2,$3)", rows)
        # прогрев Valkey из источника истины
        rows = await conn.fetch("SELECT sku, available FROM stock")
    async with vk.pipeline(transaction=False) as p:
        for r in rows:
            p.set(f"stock:{r['sku']}", r["available"])
        await p.execute()
    jlog("stock warmed into valkey", skus=len(rows))


async def release(conn, order_id, status):
    r = await conn.fetchrow("SELECT buyer_id, items, status FROM reservations WHERE order_id=$1 FOR UPDATE",
                            order_id)
    if not r or r["status"] != "ACTIVE":
        return False
    items = json.loads(r["items"])
    for i in items:
        await conn.execute("UPDATE stock SET available=available+$2, reserved=reserved-$2 WHERE sku=$1",
                           i["sku"], i["qty"])
    await conn.execute("UPDATE reservations SET status=$2 WHERE order_id=$1", order_id, status)
    await stage(conn, TOPIC, make_event("inventory.released", str(order_id),
                                        {"order_id": str(order_id), "reason": status.lower(), "items": items}))
    return r["buyer_id"], items


async def on_event(topic, ev):
    order_id = ev["data"]["order_id"]
    released = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            if not await mark_processed(conn, ev["event_id"]):
                return
            if ev["event_type"] == "order.paid":
                r = await conn.fetchrow("SELECT items FROM reservations WHERE order_id=$1 AND status='ACTIVE' "
                                        "FOR UPDATE", order_id)
                if r:
                    for i in json.loads(r["items"]):
                        await conn.execute("UPDATE stock SET reserved=reserved-$2 WHERE sku=$1", i["sku"], i["qty"])
                    await conn.execute("UPDATE reservations SET status='COMMITTED' WHERE order_id=$1", order_id)
                    await stage(conn, TOPIC, make_event("inventory.committed", order_id, {"order_id": order_id}))
            elif ev["event_type"] == "order.cancelled":
                released = await release(conn, order_id, "RELEASED")
    if released:
        await _valkey_release(*released)
    relay.notify()


async def sweeper():
    """Раз в 10 с возвращает остатки по просроченным неоплаченным резервам."""
    while True:
        await asyncio.sleep(10)
        try:
            async with pool.acquire() as conn:
                ids = await conn.fetch("SELECT order_id FROM reservations WHERE status='ACTIVE' AND expires_at < now() "
                                       "LIMIT 500")
                for row in ids:
                    async with conn.transaction():
                        released = await release(conn, row["order_id"], "EXPIRED")
                    if released:
                        await _valkey_release(*released)
            if ids:
                jlog("expired reservations released", count=len(ids))
                relay.notify()
        except Exception as e:
            jlog("sweeper error", error=repr(e))


@asynccontextmanager
async def lifespan(app):
    global pool, vk, relay
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=20)
    vk = valkey.from_url(VALKEY_URL, password=os.getenv("VALKEY_PASSWORD"), decode_responses=True)
    await seed()
    await producer.start()
    relay = OutboxRelay(pool, producer)
    tasks = [asyncio.create_task(t) for t in (
        relay.run(), sweeper(),
        run_consumer(["order.events"], "inventory-service", on_event))]
    yield
    for t in tasks:
        t.cancel()
    await producer.stop()


app = create_app(lifespan)
reserve_script = None


@app.post("/inventory/reservations", status_code=201)
async def reserve(body: dict):
    global reserve_script
    order_id, buyer, items = body["order_id"], body["buyer_id"], body["items"]
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT status, expires_at FROM reservations WHERE order_id=$1", order_id)
    if existing:  # идемпотентность: повтор того же заказа (например, retry из mesh)
        RESERVE_RESULT.labels("duplicate").inc()
        return {"order_id": order_id, "status": existing["status"], "expires_at": existing["expires_at"]}

    if reserve_script is None:
        reserve_script = vk.register_script(RESERVE_LUA)
    limit = PROMO_LIMIT if any(i["sku"].startswith("BF-") for i in items) else 1000
    res = await reserve_script(keys=_keys(buyer, items), args=[len(items)] + [i["qty"] for i in items] + [limit])
    if res == -1:
        RESERVE_RESULT.labels("out_of_stock").inc()
        raise HTTPException(409, "out of stock")
    if res == -2:
        RESERVE_RESULT.labels("buyer_limit").inc()
        raise HTTPException(422, f"limit {PROMO_LIMIT} items per buyer for promo SKUs")

    expires = datetime.now(timezone.utc) + timedelta(seconds=RESERVATION_TTL)
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in items:
                    ok = await conn.execute("UPDATE stock SET available=available-$2, reserved=reserved+$2 "
                                            "WHERE sku=$1 AND available >= $2", i["sku"], i["qty"])
                    if ok != "UPDATE 1":
                        raise HTTPException(409, "out of stock (db)")
                await conn.execute("INSERT INTO reservations(order_id, buyer_id, items, status, expires_at) "
                                   "VALUES ($1,$2,$3,'ACTIVE',$4)", order_id, buyer, json.dumps(items), expires)
                await stage(conn, TOPIC, make_event("inventory.reserved", order_id,
                                                    {"order_id": order_id, "items": items}))
    except Exception:
        await _valkey_release(buyer, items)  # компенсация горячего счётчика
        RESERVE_RESULT.labels("db_rejected").inc()
        raise
    relay.notify()
    RESERVE_RESULT.labels("ok").inc()
    return {"order_id": order_id, "status": "ACTIVE", "expires_at": expires}


@app.get("/inventory/stock/{sku}")
async def stock(sku: str):
    async with pool.acquire() as conn:
        r = await conn.fetchrow("SELECT sku, available, reserved, initial FROM stock WHERE sku=$1", sku)
    if not r:
        raise HTTPException(404)
    return dict(r) | {"hot_available": int(await vk.get(f"stock:{sku}") or 0)}


@app.get("/inventory/audit")
async def audit():
    """Проверка отсутствия overselling: продано + в резерве + доступно == исходный остаток, available >= 0."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
          SELECT s.sku, s.initial, s.available, s.reserved,
                 coalesce(sum((i->>'qty')::int) FILTER (WHERE r.status='COMMITTED'), 0) AS sold
          FROM stock s
          LEFT JOIN reservations r ON TRUE
          LEFT JOIN LATERAL jsonb_array_elements(r.items) i ON i->>'sku' = s.sku
          WHERE s.sku LIKE 'BF-%' GROUP BY s.sku ORDER BY s.sku""")
    out = [dict(r) | {"balanced": r["sold"] + r["reserved"] + r["available"] == r["initial"]} for r in rows]
    return {"oversold": any(r["available"] < 0 or r["sold"] > r["initial"] for r in out),
            "all_balanced": all(r["balanced"] for r in out), "skus": out}
