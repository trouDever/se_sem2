"""E2E smoke-тест через VIP: витрина -> заказ -> оплата -> сага (PAID) -> уведомление -> аналитика,
плюс проверки 409 (нет в наличии по чужому SKU не проверяем), 422 (лимит в руки) и идемпотентности оплаты."""
import sys
import time
import uuid

import requests

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://172.30.0.100"
uid = f"smoke-{uuid.uuid4().hex[:6]}"
H = {"x-user-id": uid}
ok = 0


def check(name, cond, info=""):
    global ok
    print(("PASS " if cond else "FAIL ") + name, info)
    ok += bool(cond)


r = requests.get(f"{BASE}/api/v1/catalog/products?promo=true", headers=H, timeout=10)
items = r.json().get("items", []) if r.ok else []
check("1 catalog promo list", r.status_code == 200 and len(items) > 0, f"{r.status_code} items={len(items)}")
sku = items[0]["sku"]

key = str(uuid.uuid4())
body = {"items": [{"sku": sku, "qty": 1, "price_cents": items[0]["price_cents"]}]}
r = requests.post(f"{BASE}/api/v1/orders", json=body, headers=H | {"Idempotency-Key": key}, timeout=10)
order = r.json()
check("2 create order 201 RESERVED", r.status_code == 201 and order.get("status") == "RESERVED", r.status_code)
r2 = requests.post(f"{BASE}/api/v1/orders", json=body, headers=H | {"Idempotency-Key": key}, timeout=10)
check("3 same Idempotency-Key -> same order", r2.status_code == 200 and r2.json()["order_id"] == order["order_id"],
      r2.status_code)

pay = {"order_id": order["order_id"], "amount_cents": order["total_cents"], "card": "4242"}
pk = f"pay-{key}"
r = requests.post(f"{BASE}/api/v1/payments", json=pay, headers=H | {"Idempotency-Key": pk}, timeout=10)
r2 = requests.post(f"{BASE}/api/v1/payments", json=pay, headers=H | {"Idempotency-Key": pk}, timeout=10)
check("4 payment + duplicate click -> one payment", r.status_code == 201 and r2.status_code == 200
      and r.json()["payment_id"] == r2.json()["payment_id"], f"{r.status_code}/{r2.status_code}")

status, t0 = None, time.time()
while time.time() - t0 < 20:
    status = requests.get(f"{BASE}/api/v1/orders/{order['order_id']}", headers=H, timeout=10).json().get("status")
    if status == "PAID":
        break
    time.sleep(0.3)
check("5 saga: order PAID via Kafka", status == "PAID", f"{status} in {time.time() - t0:.2f}s")

time.sleep(2)
n = requests.get(f"{BASE}/api/v1/notifications", headers=H, timeout=10).json()
check("6 notifications delivered", any(x["type"] == "order.paid" for x in n), [x["type"] for x in n])

# лимит в руки: 3-я единица того же акционного SKU
codes = []
for _ in range(3):
    r = requests.post(f"{BASE}/api/v1/orders", headers=H | {"Idempotency-Key": str(uuid.uuid4())},
                      json={"items": [{"sku": "BF-020", "qty": 1, "price_cents": 100}]}, timeout=10)
    codes.append(r.status_code)
check("7 promo limit 2 per buyer -> 422", codes[:2] == [201, 201] and codes[2] == 422, codes)

a = requests.get(f"{BASE}/api/v1/analytics/sales?minutes=5", headers=H, timeout=10)
check("8 analytics per-minute sales", a.ok and len(a.json()["per_minute"]) > 0, a.json() if a.ok else a.status_code)
print(f"\n{ok}/8 passed")
