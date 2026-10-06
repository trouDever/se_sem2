"""payment-service: идемпотентность оплаты и отказ платёжного провайдера."""
import uuid

import pytest
from fastapi.testclient import TestClient

import payment_service
from conftest import FakeConn, FakePool, FakeRelay


def payment_row(sql, *args):
    if sql.startswith("SELECT"):
        return None
    pid, order_id, amount, status, reason, key = args
    return {"id": pid, "order_id": order_id, "amount_cents": amount, "status": status, "reason": reason}


@pytest.fixture
def env(monkeypatch):
    conn = FakeConn(payment_row)
    monkeypatch.setattr(payment_service, "pool", FakePool(conn))
    monkeypatch.setattr(payment_service, "relay", FakeRelay())
    monkeypatch.setattr(payment_service, "PSP_DECLINE_RATE", 0.0)
    return conn, TestClient(payment_service.app)


def pay(client, card="4242", key="k1"):
    return client.post("/payments", headers={"Idempotency-Key": key},
                       json={"order_id": str(uuid.uuid4()), "amount_cents": 1999, "card": card})


def test_successful_payment_publishes_event(env, outbox_events):
    conn, client = env
    r = pay(client)
    assert r.status_code == 201 and r.json()["status"] == "SUCCEEDED"
    assert [e["event_type"] for e in outbox_events(conn)] == ["payment.succeeded"]


def test_declined_card_publishes_failure(env, outbox_events):
    conn, client = env
    r = pay(client, card="fail")
    assert r.json()["status"] == "FAILED" and r.json()["reason"] == "card declined"
    assert [e["event_type"] for e in outbox_events(conn)] == ["payment.failed"]


def test_second_click_does_not_charge_again(env, outbox_events):
    conn, client = env
    existing = {"id": uuid.uuid4(), "order_id": uuid.uuid4(), "amount_cents": 1999, "status": "SUCCEEDED",
                "reason": None}
    conn.on_fetchrow = lambda sql, *a: existing if sql.startswith("SELECT") else None
    r = pay(client)
    assert r.status_code == 200
    assert r.json()["payment_id"] == str(existing["id"])
    assert outbox_events(conn) == []


def test_idempotency_key_is_required(env):
    _, client = env
    r = client.post("/payments", json={"order_id": str(uuid.uuid4()), "amount_cents": 1})
    assert r.status_code == 422
