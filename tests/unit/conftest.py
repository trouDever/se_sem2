"""Общие заглушки для модульных тестов: сервисы проверяются без PostgreSQL, Kafka и Valkey."""
import os
import pathlib
import sys
from contextlib import asynccontextmanager

import pytest

SERVICES = pathlib.Path(__file__).resolve().parents[2] / "services"
sys.path.insert(0, str(SERVICES))
os.environ.setdefault("DB_URL", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("SERVICE_NAME", "unit-tests")


class FakeConn:
    """Подменяет соединение asyncpg: отвечает через переданную функцию и запоминает запросы."""

    def __init__(self, on_fetchrow=None):
        self.on_fetchrow = on_fetchrow or (lambda sql, *args: None)
        self.executed = []

    async def fetchrow(self, sql, *args):
        return self.on_fetchrow(sql, *args)

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "INSERT 0 1"

    @asynccontextmanager
    async def transaction(self):
        yield


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


class FakeRelay:
    def __init__(self):
        self.notified = 0

    def notify(self):
        self.notified += 1


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeHttp:
    """Подменяет httpx.AsyncClient для вызова inventory из order-service."""

    def __init__(self, response):
        self.response = response
        self.calls = []

    async def post(self, path, json=None, headers=None):
        self.calls.append({"path": path, "json": json, "headers": headers or {}})
        return self.response


class FakePipeline:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self.store.append((name, args))
        return record

    async def execute(self):
        return []


class FakeValkey:
    def __init__(self, values=None):
        self.values = values or {}
        self.pipeline_calls = []

    async def mget(self, keys):
        return [self.values.get(k) for k in keys]

    def pipeline(self, transaction=True):
        return FakePipeline(self.pipeline_calls)


@pytest.fixture
def outbox_events():
    """Достаёт события, которые сервис положил в outbox через stage()."""
    import json

    def extract(conn):
        return [json.loads(args[2]) for sql, args in conn.executed if "INTO outbox" in sql]
    return extract
