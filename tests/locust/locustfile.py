"""Задание 6.1: «старт Чёрной пятницы» — поток через VIP -> HAProxy -> Istio Gateway -> сервисы -> Kafka.

Shopper: смотрит акции и товары, оформляет заказ (часто на дефицитные BF-SKU) и оплачивает его.
Bot:     три «перекупщика» с одним x-user-id долбят каталог и заказы — должны получать 429.
Ожидаемые бизнес-ответы (409 нет в наличии, 422 лимит на покупателя, 429 rate limit) не считаются ошибками.

Запуск в docker-сети kind:  make load  (см. tests/locust/run.sh)
"""
import random
import uuid

from locust import HttpUser, between, task

EXPECTED = (409, 422, 429)


class Shopper(HttpUser):
    weight = 10
    wait_time = between(0.5, 1.5)

    def on_start(self):
        self.uid = f"buyer-{uuid.uuid4().hex[:10]}"
        self.client.headers["x-user-id"] = self.uid

    @task(6)
    def browse_promo(self):
        self.client.get("/api/v1/catalog/products?promo=true", name="/catalog/products?promo")

    @task(3)
    def product_card(self):
        sku = random.choice([f"BF-{random.randint(1, 20):03d}", f"SKU-{random.randint(1, 80):03d}"])
        self.client.get(f"/api/v1/catalog/products/{sku}", name="/catalog/products/{sku}")

    @task(2)
    def checkout(self):
        sku = f"BF-{random.randint(1, 20):03d}" if random.random() < 0.6 else f"SKU-{random.randint(1, 80):03d}"
        key = str(uuid.uuid4())
        with self.client.post("/api/v1/orders", name="/orders", catch_response=True,
                              headers={"Idempotency-Key": key},
                              json={"items": [{"sku": sku, "qty": 1, "price_cents": 19900}]}) as r:
            if r.status_code in EXPECTED:
                r.success()
                return
            if r.status_code != 201:
                r.failure(f"{r.status_code}")
                return
            order = r.json()
        with self.client.post("/api/v1/payments", name="/payments", catch_response=True,
                              headers={"Idempotency-Key": f"pay-{key}"},
                              json={"order_id": order["order_id"], "amount_cents": order["total_cents"],
                                    "card": "4242"}) as p:
            if p.status_code in EXPECTED:
                p.success()

    @task(1)
    def my_orders(self):
        self.client.get("/api/v1/orders", name="/orders (mine)")


class Bot(HttpUser):
    """Перекупщик: один и тот же x-user-id из нескольких «браузеров», без пауз."""
    weight = 1
    fixed_count = 3
    wait_time = between(0.05, 0.1)

    def on_start(self):
        self.client.headers["x-user-id"] = "reseller-bot"

    @task
    def hammer(self):
        with self.client.get("/api/v1/catalog/products?promo=true", name="BOT /catalog", catch_response=True) as r:
            if r.status_code == 429:
                r.success()
