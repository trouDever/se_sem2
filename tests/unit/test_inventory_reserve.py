"""Lua-скрипт резерва в Valkey: главный рубеж против overselling.
Скрипт выполняется настоящим Lua-интерпретатором через fakeredis."""
import fakeredis
import pytest

import inventory_service as inv

OK, OUT_OF_STOCK, BUYER_LIMIT = 1, -1, -2


@pytest.fixture
async def valkey():
    r = fakeredis.FakeAsyncRedis(decode_responses=True)
    yield r
    await r.aclose()


async def reserve(r, buyer, items, limit=2):
    script = r.register_script(inv.RESERVE_LUA)
    return await script(keys=inv._keys(buyer, items), args=[len(items)] + [i["qty"] for i in items] + [limit])


def test_keys_layout():
    keys = inv._keys("u1", [{"sku": "BF-001", "qty": 1}, {"sku": "SKU-002", "qty": 3}])
    assert keys == ["stock:BF-001", "stock:SKU-002", "buyer:u1:BF-001", "buyer:u1:SKU-002"]


async def test_reserve_decrements_stock_and_counts_buyer(valkey):
    await valkey.set("stock:BF-001", 5)
    assert await reserve(valkey, "u1", [{"sku": "BF-001", "qty": 2}]) == OK
    assert await valkey.get("stock:BF-001") == "3"
    assert await valkey.get("buyer:u1:BF-001") == "2"


async def test_cannot_sell_more_than_stock(valkey):
    await valkey.set("stock:BF-001", 1)
    assert await reserve(valkey, "u1", [{"sku": "BF-001", "qty": 2}]) == OUT_OF_STOCK
    assert await valkey.get("stock:BF-001") == "1"


async def test_buyer_limit_two_items(valkey):
    await valkey.set("stock:BF-001", 100)
    assert await reserve(valkey, "u1", [{"sku": "BF-001", "qty": 1}]) == OK
    assert await reserve(valkey, "u1", [{"sku": "BF-001", "qty": 1}]) == OK
    assert await reserve(valkey, "u1", [{"sku": "BF-001", "qty": 1}]) == BUYER_LIMIT
    # другой покупатель ограничением первого не затронут
    assert await reserve(valkey, "u2", [{"sku": "BF-001", "qty": 2}]) == OK
    assert await valkey.get("stock:BF-001") == "96"


async def test_all_or_nothing_for_multi_item_order(valkey):
    await valkey.set("stock:BF-001", 5)
    await valkey.set("stock:BF-002", 0)
    items = [{"sku": "BF-001", "qty": 1}, {"sku": "BF-002", "qty": 1}]
    assert await reserve(valkey, "u1", items) == OUT_OF_STOCK
    # первая позиция не списалась, хотя её хватало
    assert await valkey.get("stock:BF-001") == "5"
    assert await valkey.get("buyer:u1:BF-001") is None


async def test_unknown_sku_is_out_of_stock(valkey):
    assert await reserve(valkey, "u1", [{"sku": "NOPE", "qty": 1}]) == OUT_OF_STOCK


async def test_last_item_goes_to_exactly_one_buyer(valkey):
    await valkey.set("stock:BF-007", 1)
    results = [await reserve(valkey, f"buyer-{n}", [{"sku": "BF-007", "qty": 1}]) for n in range(10)]
    assert results.count(OK) == 1
    assert results.count(OUT_OF_STOCK) == 9
    assert await valkey.get("stock:BF-007") == "0"
