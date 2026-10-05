"""catalog-service: товары и акции Чёрной пятницы (MongoDB), cache-aside в Valkey,
инвалидация кэша по событиям, остаток показывается из горячих счётчиков inventory в Valkey."""
import asyncio
import json
import os
import random
from contextlib import asynccontextmanager

import valkey.asyncio as valkey
from fastapi import HTTPException
from prometheus_client import Counter
from pymongo import AsyncMongoClient

from fm_common import Producer, create_app, make_event, run_consumer

MONGO_URL = os.getenv("DB_URL", "mongodb://mongodb.data:27017")
VALKEY_URL = os.getenv("VALKEY_URL", "valkey://valkey.data:6379")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "30"))
CACHE = Counter("fm_catalog_cache_total", "Catalog cache lookups", ["result"])
CATEGORIES = ["electronics", "appliances", "fashion", "home", "toys"]

db = None
vk: valkey.Valkey = None
producer = Producer()


async def seed():
    products = db.products
    await products.create_index("category")
    if await products.count_documents({}) == 0:
        rnd = random.Random(42)
        docs = [{"_id": f"BF-{n:03d}", "title": f"Black Friday deal #{n}", "category": CATEGORIES[n % 5],
                 "price_cents": rnd.randint(5000, 90000), "promo": {"discount_pct": 70, "starts_at": "00:00"},
                 "attributes": {"brand": f"brand-{n % 7}", "rating": round(rnd.uniform(3.5, 5), 1)}}
                for n in range(1, 21)]
        docs += [{"_id": f"SKU-{n:03d}", "title": f"Product #{n}", "category": CATEGORIES[n % 5],
                  "price_cents": rnd.randint(500, 50000), "attributes": {"brand": f"brand-{n % 9}"}}
                 for n in range(1, 81)]
        await products.insert_many(docs)


async def with_stock(items):
    stocks = await vk.mget([f"stock:{p['sku']}" for p in items])
    for p, s in zip(items, stocks):
        p["in_stock"] = int(s or 0)
    return items


async def on_event(topic, ev):
    # Оплаченный/отменённый заказ меняет остаток и топ продаж -> сбрасываем кэш списков
    keys = [k async for k in vk.scan_iter("cat:list:*")]
    if keys:
        await vk.delete(*keys)


@asynccontextmanager
async def lifespan(app):
    global db, vk
    db = AsyncMongoClient(MONGO_URL).catalog
    vk = valkey.from_url(VALKEY_URL, password=os.getenv("VALKEY_PASSWORD"), decode_responses=True)
    await seed()
    await producer.start()
    task = asyncio.create_task(run_consumer(["order.events"], "catalog-service", on_event))
    yield
    task.cancel()
    await producer.stop()


app = create_app(lifespan)


def to_view(d):
    return {"sku": d["_id"], "title": d["title"], "category": d["category"], "price_cents": d["price_cents"],
            "promo": d.get("promo")}


@app.get("/catalog/products")
async def list_products(category: str | None = None, promo: bool = False, page: int = 1):
    key = f"cat:list:{category}:{promo}:{page}"
    cached = await vk.get(key)
    if cached:
        CACHE.labels("hit").inc()
        items = json.loads(cached)
    else:
        CACHE.labels("miss").inc()
        q = {}
        if category:
            q["category"] = category
        if promo:
            q["promo"] = {"$exists": True}
        items = [to_view(d) async for d in db.products.find(q).skip((page - 1) * 20).limit(20)]
        await vk.set(key, json.dumps(items), ex=CACHE_TTL + random.randint(0, 5))  # jitter против stampede
    return {"page": page, "items": await with_stock(items)}


@app.get("/catalog/products/{sku}")
async def product(sku: str):
    key = f"cat:item:{sku}"
    cached = await vk.get(key)
    if cached:
        CACHE.labels("hit").inc()
        item = json.loads(cached)
    else:
        CACHE.labels("miss").inc()
        d = await db.products.find_one({"_id": sku})
        if not d:
            raise HTTPException(404)
        item = to_view(d)
        await vk.set(key, json.dumps(item), ex=CACHE_TTL * 2)
    return (await with_stock([item]))[0]


@app.put("/catalog/products/{sku}/price")
async def set_price(sku: str, body: dict):
    r = await db.products.update_one({"_id": sku}, {"$set": {"price_cents": int(body["price_cents"])}})
    if not r.matched_count:
        raise HTTPException(404)
    await vk.delete(f"cat:item:{sku}")
    await producer.send("catalog.events", make_event("catalog.price-changed", sku,
                                                     {"sku": sku, "price_cents": int(body["price_cents"])}))
    return {"sku": sku, "price_cents": int(body["price_cents"])}
