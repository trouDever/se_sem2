"""Обработчики событий: уведомления, витрина и аналитика."""
import pytest
from pymongo.errors import DuplicateKeyError

import analytics_service
import catalog_service
import fm_common
import notification_service
from conftest import FakeValkey


class FakeCollection:
    def __init__(self):
        self.docs = {}

    async def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate")
        self.docs[doc["_id"]] = doc


def paid_event(order_id="o1", total=5000):
    return fm_common.make_event("order.paid", order_id, {
        "order_id": order_id, "buyer_id": "u1", "total_cents": total,
        "items": [{"sku": "BF-001", "qty": 2, "price_cents": 2500}]})


# ---------- notification-service

@pytest.fixture
def log_coll(monkeypatch):
    coll = FakeCollection()
    monkeypatch.setattr(notification_service, "log_coll", coll)
    return coll


@pytest.mark.parametrize("event_type", list(notification_service.TEMPLATES))
def test_every_template_renders(event_type):
    text = notification_service.TEMPLATES[event_type].format(order_id="o1", total_cents=100)
    assert "o1" in text


async def test_paid_order_sends_one_notification(log_coll):
    ev = paid_event()
    await notification_service.on_event("order.events", ev)
    doc = log_coll.docs[ev["event_id"]]
    assert doc["type"] == "order.paid" and doc["buyer_id"] == "u1" and "5000" in doc["text"]


async def test_redelivered_event_is_not_sent_twice(log_coll):
    ev = paid_event()
    await notification_service.on_event("order.events", ev)
    await notification_service.on_event("order.events", ev)
    assert len(log_coll.docs) == 1


async def test_unknown_event_is_ignored(log_coll):
    await notification_service.on_event("order.events", fm_common.make_event("order.unknown", "o1", {"order_id": "o1"}))
    assert log_coll.docs == {}


# ---------- catalog-service

def test_catalog_view_hides_internal_fields():
    doc = {"_id": "BF-001", "title": "TV", "category": "electronics", "price_cents": 100, "attributes": {"x": 1}}
    assert catalog_service.to_view(doc) == {"sku": "BF-001", "title": "TV", "category": "electronics",
                                            "price_cents": 100, "promo": None}


async def test_stock_is_taken_from_hot_counters(monkeypatch):
    monkeypatch.setattr(catalog_service, "vk", FakeValkey({"stock:BF-001": "7"}))
    items = await catalog_service.with_stock([{"sku": "BF-001"}, {"sku": "BF-404"}])
    assert [i["in_stock"] for i in items] == [7, 0]


async def test_order_event_marks_cache_stale_without_scanning():
    catalog_service.invalidate.clear()
    await catalog_service.on_event("order.events", paid_event())
    assert catalog_service.invalidate.is_set()


# ---------- analytics-service

async def test_paid_order_updates_minute_aggregates(monkeypatch):
    vk = FakeValkey()
    monkeypatch.setattr(analytics_service, "vk", vk)
    monkeypatch.setattr(analytics_service, "buffer", {t: [] for t in analytics_service.TOPICS})
    ev = paid_event(total=5000)
    await analytics_service.on_event("order.events", ev)
    minute = ev["occurred_at"][:16]
    assert ("incrby", (f"an:revenue:{minute}", 5000)) in vk.pipeline_calls
    assert ("incr", (f"an:orders:{minute}",)) in vk.pipeline_calls
    assert ("zincrby", ("an:top_skus", 2, "BF-001")) in vk.pipeline_calls
    assert analytics_service.buffer["order.events"] == [ev]


async def test_other_events_only_go_to_archive(monkeypatch):
    vk = FakeValkey()
    monkeypatch.setattr(analytics_service, "vk", vk)
    monkeypatch.setattr(analytics_service, "buffer", {t: [] for t in analytics_service.TOPICS})
    ev = fm_common.make_event("payment.failed", "o1", {"order_id": "o1"})
    await analytics_service.on_event("payment.events", ev)
    assert vk.pipeline_calls == []
    assert analytics_service.buffer["payment.events"] == [ev]
