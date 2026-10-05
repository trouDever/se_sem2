"""notification-service: уведомления покупателю по событиям заказа (эмуляция e-mail),
журнал в MongoDB с TTL 90 дней; уникальный индекс по event_id даёт идемпотентность."""
import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Header
from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

from fm_common import create_app, jlog, run_consumer

MONGO_URL = os.getenv("DB_URL", "mongodb://mongodb.data:27017")
TEMPLATES = {
    "order.created": "Заказ {order_id} оформлен, товар зарезервирован на 15 минут — оплатите его",
    "order.paid": "Заказ {order_id} оплачен, сумма {total_cents} коп. Спасибо за покупку!",
    "order.cancelled": "Оплата заказа {order_id} не прошла, резерв снят",
    "order.expired": "Время резерва заказа {order_id} истекло",
}
log_coll = None


async def on_event(topic, ev):
    tpl = TEMPLATES.get(ev["event_type"])
    if not tpl:
        return
    d = ev["data"]
    try:
        await log_coll.insert_one({"_id": ev["event_id"], "buyer_id": d.get("buyer_id"), "channel": "email",
                                   "type": ev["event_type"], "text": tpl.format(**d),
                                   "created_at": datetime.now(timezone.utc)})
        jlog("notification sent", type=ev["event_type"], order_id=d["order_id"])
    except DuplicateKeyError:
        pass  # повторная доставка того же события


@asynccontextmanager
async def lifespan(app):
    global log_coll
    log_coll = AsyncMongoClient(MONGO_URL).notifications.log
    await log_coll.create_index("created_at", expireAfterSeconds=90 * 24 * 3600)
    await log_coll.create_index([("buyer_id", 1), ("created_at", -1)])
    task = asyncio.create_task(run_consumer(["order.events"], "notification-service", on_event))
    yield
    task.cancel()


app = create_app(lifespan)


@app.get("/notifications")
async def mine(x_user_id: str = Header(...)):
    return [{k: v for k, v in d.items() if k != "_id"}
            async for d in log_coll.find({"buyer_id": x_user_id}).sort("created_at", -1).limit(20)]
