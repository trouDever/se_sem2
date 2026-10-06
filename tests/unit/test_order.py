"""order-service: проверка заказа, обращение к inventory и запись в outbox."""
import json
import uuid

import pytest
from fastapi.testclient import TestClient

import order_service
from conftest import FakeConn, FakeHttp, FakePool, FakeRelay, FakeResponse

ITEMS = [{"sku": "BF-001", "qty": 2, "price_cents": 1500}]


def order_row(sql, *args):
    if sql.startswith("SELECT"):
        return None
    oid, buyer, items, total, key = args
    return {"id": oid, "buyer_id": buyer, "items": items, "total_cents": total, "status": "RESERVED"}


@pytest.fixture
def env(monkeypatch):
    conn = FakeConn(order_row)
    http = FakeHttp(FakeResponse(201, {"status": "ACTIVE"}))
    monkeypatch.setattr(order_service, "pool", FakePool(conn))
    monkeypatch.setattr(order_service, "http", http)
    monkeypatch.setattr(order_service, "relay", FakeRelay())
    return conn, http, TestClient(order_service.app)


def test_view_parses_items():
    r = {"id": uuid.uuid4(), "buyer_id": "u", "status": "PAID", "items": json.dumps(ITEMS), "total_cents": 3000}
    v = order_service.view(r)
    assert v["items"] == ITEMS and v["status"] == "PAID" and isinstance(v["order_id"], str)


@pytest.mark.parametrize("items", [[], [{"sku": "BF-001", "qty": 0, "price_cents": 1}]])
def test_empty_or_zero_qty_order_is_rejected(env, items):
    _, http, client = env
    r = client.post("/orders", json={"items": items}, headers={"x-user-id": "u1"})
    assert r.status_code == 400
    assert http.calls == []


def test_order_without_buyer_is_rejected(env):
    _, _, client = env
    assert client.post("/orders", json={"items": ITEMS}).status_code == 422


def test_successful_order_reserves_and_writes_outbox(env, outbox_events):
    conn, http, client = env
    r = client.post("/orders", json={"items": ITEMS}, headers={"x-user-id": "u1"})
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "RESERVED" and body["total_cents"] == 3000
    assert http.calls[0]["json"]["items"] == [{"sku": "BF-001", "qty": 2}]
    events = outbox_events(conn)
    assert [e["event_type"] for e in events] == ["order.created"]
    assert events[0]["data"]["order_id"] == body["order_id"]


def test_trace_headers_are_forwarded_to_inventory(env):
    _, http, client = env
    headers = {"x-user-id": "u1", "traceparent": "00-abc-def-01", "x-request-id": "req-1", "cookie": "secret"}
    client.post("/orders", json={"items": ITEMS}, headers=headers)
    forwarded = http.calls[0]["headers"]
    assert forwarded["traceparent"] == "00-abc-def-01"
    assert forwarded["x-request-id"] == "req-1"
    assert "cookie" not in forwarded


@pytest.mark.parametrize("status", [409, 422])
def test_inventory_refusal_is_passed_to_buyer(env, outbox_events, status):
    conn, http, client = env
    http.response = FakeResponse(status, {"detail": "out of stock"})
    r = client.post("/orders", json={"items": ITEMS}, headers={"x-user-id": "u1"})
    assert r.status_code == status
    assert outbox_events(conn) == []


def test_inventory_failure_becomes_503(env):
    _, http, client = env
    http.response = FakeResponse(500)
    assert client.post("/orders", json={"items": ITEMS}, headers={"x-user-id": "u1"}).status_code == 503


def test_same_idempotency_key_returns_existing_order(env, monkeypatch):
    conn, http, client = env
    existing = {"id": uuid.uuid4(), "buyer_id": "u1", "status": "RESERVED", "items": json.dumps(ITEMS),
                "total_cents": 3000}
    conn.on_fetchrow = lambda sql, *a: existing if sql.startswith("SELECT") else None
    r = client.post("/orders", json={"items": ITEMS}, headers={"x-user-id": "u1", "Idempotency-Key": "k1"})
    assert r.status_code == 200
    assert r.json()["order_id"] == str(existing["id"])
    assert http.calls == []
