"""fm_common: формат событий, служебные эндпоинты и инъекция отказов."""
import time
import uuid
from datetime import datetime, timezone

from fastapi.testclient import TestClient

import fm_common


def test_event_has_envelope_fields():
    ev = fm_common.make_event("order.created", "order-1", {"total_cents": 100})
    assert set(ev) == {"event_id", "event_type", "key", "occurred_at", "producer", "data"}
    assert ev["event_type"] == "order.created"
    assert ev["key"] == "order-1"
    assert ev["data"] == {"total_cents": 100}
    uuid.UUID(ev["event_id"])


def test_event_time_is_utc_and_recent():
    ev = fm_common.make_event("x", "k", {})
    occurred = datetime.fromisoformat(ev["occurred_at"])
    assert occurred.tzinfo is not None
    assert abs((datetime.now(timezone.utc) - occurred).total_seconds()) < 5


def test_event_ids_are_unique():
    ids = {fm_common.make_event("x", "k", {})["event_id"] for _ in range(100)}
    assert len(ids) == 100


def make_client():
    app = fm_common.create_app()

    @app.get("/work")
    async def work():
        return {"ok": True}

    return TestClient(app)


def test_health_and_metrics_endpoints():
    client = make_client()
    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").status_code == 200
    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "fm_events_produced_total" in metrics.text


def test_served_by_header_is_set():
    assert "x-served-by" in make_client().get("/work").headers


def test_fault_injection_breaks_business_routes_only():
    client = make_client()
    try:
        r = client.post("/admin/fault", json={"error_rate": 1.0, "seconds": 30})
        assert r.json()["error_rate"] == 1.0
        assert client.get("/work").status_code == 503
        # пробы и метрики не ломаются, иначе Kubernetes перезапустил бы под вместо проверки circuit breaker
        assert client.get("/health/live").status_code == 200
        assert client.get("/metrics").status_code == 200
    finally:
        client.post("/admin/fault", json={"error_rate": 0, "seconds": 0})
    assert client.get("/work").status_code == 200


def test_fault_injection_expires():
    client = make_client()
    client.post("/admin/fault", json={"error_rate": 1.0, "seconds": 0.2})
    time.sleep(0.3)
    assert client.get("/work").status_code == 200
