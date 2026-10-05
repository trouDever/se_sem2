"""analytics-service: продажи в реальном времени (поминутные агрегаты в Valkey)
и холодный архив всех доменных событий в MinIO (S3) в формате JSONL.gz
по пути <topic>/<yyyy>/<mm>/<dd>/<HHMMSS>-<uuid>.jsonl.gz (читается DuckDB/ClickHouse напрямую)."""
import asyncio
import gzip
import io
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import valkey.asyncio as valkey
from minio import Minio
from prometheus_client import Counter

from fm_common import create_app, jlog, run_consumer

VALKEY_URL = os.getenv("VALKEY_URL", "valkey://valkey.data:6379")
S3 = os.getenv("S3_ENDPOINT", "minio.data:9000")
BUCKET = os.getenv("S3_BUCKET", "events-archive")
FLUSH_EVERY = int(os.getenv("ARCHIVE_FLUSH_SECONDS", "30"))
ARCHIVED = Counter("fm_archive_records_total", "Events written to cold archive", ["topic"])
TOPICS = ["order.events", "payment.events", "inventory.events", "catalog.events"]

vk = None
s3: Minio = None
buffer: dict[str, list] = {t: [] for t in TOPICS}


async def on_event(topic, ev):
    buffer[topic].append(ev)
    if ev["event_type"] == "order.paid":
        minute = ev["occurred_at"][:16]  # 2026-11-27T00:01
        async with vk.pipeline(transaction=False) as p:
            p.incrby(f"an:revenue:{minute}", ev["data"]["total_cents"])
            p.incr(f"an:orders:{minute}")
            for i in ev["data"]["items"]:
                p.zincrby("an:top_skus", i["qty"], i["sku"])
            p.expire(f"an:revenue:{minute}", 7 * 86400)
            p.expire(f"an:orders:{minute}", 7 * 86400)
            await p.execute()


def _put(topic, rows):
    now = datetime.now(timezone.utc)
    body = gzip.compress("\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode())
    name = f"{topic}/{now:%Y/%m/%d}/{now:%H%M%S}-{uuid.uuid4().hex[:8]}.jsonl.gz"
    s3.put_object(BUCKET, name, io.BytesIO(body), len(body), content_type="application/gzip")


async def archiver():
    while True:
        await asyncio.sleep(FLUSH_EVERY)
        for topic in TOPICS:
            rows, buffer[topic] = buffer[topic], []
            if not rows:
                continue
            try:
                await asyncio.to_thread(_put, topic, rows)
                ARCHIVED.labels(topic).inc(len(rows))
            except Exception as e:
                buffer[topic] = rows + buffer[topic]
                jlog("archive flush failed", error=repr(e))


@asynccontextmanager
async def lifespan(app):
    global vk, s3
    vk = valkey.from_url(VALKEY_URL, password=os.getenv("VALKEY_PASSWORD"), decode_responses=True)
    s3 = Minio(S3, access_key=os.getenv("S3_ACCESS_KEY", "minio"), secret_key=os.getenv("S3_SECRET_KEY", "minio123"),
               secure=False)
    for _ in range(30):
        try:
            if not s3.bucket_exists(BUCKET):
                s3.make_bucket(BUCKET)
            break
        except Exception as e:
            jlog("minio not ready", error=repr(e))
            await asyncio.sleep(2)
    tasks = [asyncio.create_task(run_consumer(TOPICS, "analytics-service", on_event)),
             asyncio.create_task(archiver())]
    yield
    for t in tasks:
        t.cancel()


app = create_app(lifespan)


@app.get("/analytics/sales")
async def sales(minutes: int = 60):
    keys = sorted([k async for k in vk.scan_iter("an:orders:*")])[-minutes:]
    minutes_ = [k.split(":", 2)[2] for k in keys]
    orders = await vk.mget(keys) if keys else []
    revenue = await vk.mget([f"an:revenue:{m}" for m in minutes_]) if keys else []
    top = await vk.zrevrange("an:top_skus", 0, 9, withscores=True)
    return {"per_minute": [{"minute": m, "orders": int(o or 0), "revenue_cents": int(r or 0)}
                           for m, o, r in zip(minutes_, orders, revenue)],
            "top_skus": [{"sku": s, "sold": int(q)} for s, q in top]}
