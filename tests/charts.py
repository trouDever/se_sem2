"""Графики для отчёта из VictoriaMetrics (те же запросы, что на дашборде Grafana) за окно нагрузочного теста."""
import json
import pathlib
import subprocess
import sys
import urllib.parse
from datetime import datetime, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

START, END = int(sys.argv[1]), int(sys.argv[2])
OUT = pathlib.Path(__file__).parent.parent / "report" / "charts"
OUT.mkdir(parents=True, exist_ok=True)
EVENTS = [(90, "отказ пода payment"), (150, "50% отказов inventory"), (201, "падение haproxy-1"), (270, "haproxy-1 вернулся")]
PALETTE = ["#2a78d6", "#e8743b", "#19a979", "#d64545", "#945ecf", "#13a4b4", "#c79f2b", "#5b6f8f"]


def qr(expr, step=5):
    path = ("/api/v1/namespaces/observability/services/vmsingle-vm:8428/proxy/api/v1/query_range?"
            + urllib.parse.urlencode({"query": expr, "start": START - 20, "end": END + 20, "step": step}))
    out = subprocess.run(["kubectl", "get", "--raw", path], capture_output=True, text=True, timeout=60).stdout
    return json.loads(out)["data"]["result"]


def plot(name, title, expr, label, unit="", step=5, scale=1.0, events=True, legend_fmt=None):
    res = qr(expr, step)
    fig, ax = plt.subplots(figsize=(10, 3.4), dpi=130)
    for i, r in enumerate(sorted(res, key=lambda r: label(r["metric"]))):
        xs = [datetime.fromtimestamp(float(t), tz=timezone.utc) for t, _ in r["values"]]
        ys = [float(v) * scale for _, v in r["values"]]
        ax.plot(xs, ys, color=PALETTE[i % len(PALETTE)], lw=1.8, label=label(r["metric"]))
        ax.fill_between(xs, ys, alpha=0.08, color=PALETTE[i % len(PALETTE)])
    if events:
        for sec, txt in EVENTS:
            x = datetime.fromtimestamp(START + sec, tz=timezone.utc)
            ax.axvline(x, color="#888", ls="--", lw=0.8)
            ax.text(x, ax.get_ylim()[1] * 0.97, " " + txt, fontsize=7, color="#555", va="top", rotation=90)
    ax.set_title(title, loc="left", fontsize=11, fontweight="bold")
    ax.set_ylabel(unit)
    ax.grid(alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    if res:
        ax.legend(fontsize=7.5, loc="upper left", bbox_to_anchor=(1.0, 1.0), frameon=False)
    fig.tight_layout()
    fig.savefig(OUT / f"{name}.png")
    plt.close(fig)
    print(name, "series:", len(res))


GW = 'pod=~"public-istio.*",reporter="source"'
plot("01-gateway-codes", "Ответы шлюза по кодам (req/s)",
     f'sum by (response_code) (rate(istio_requests_total{{{GW}}}[30s]))', lambda m: m.get("response_code", "?"), "req/s")
plot("02-ratelimit", "Решения rate limiter'а (req/s)",
     'sum by (key1) (rate(ratelimit_service_rate_limit_over_limit[30s]))', lambda m: "over limit: " + m.get("key1", ""), "req/s")
plot("03-requests-per-service", "Запросы по сервисам (req/s, Istio telemetry)",
     'sum by (destination_service_name) (rate(istio_requests_total{reporter="destination",destination_service_namespace="flashmarket"}[30s]))',
     lambda m: m.get("destination_service_name", "?"), "req/s")
plot("04-latency-p95", "Задержка p95 по сервисам (мс)",
     'histogram_quantile(0.95, sum by (destination_service_name, le) (rate(istio_request_duration_milliseconds_bucket{reporter="destination",destination_service_namespace="flashmarket"}[30s])))',
     lambda m: m.get("destination_service_name", "?"), "мс")
plot("05-kafka-e2e", "Kafka: сквозная задержка событий p95 (produce → consume), с",
     'histogram_quantile(0.95, sum by (topic, le) (rate(fm_event_e2e_latency_seconds_bucket[30s])))', lambda m: m.get("topic", "?"), "с")
plot("06-kafka-ack", "Kafka: подтверждение записи брокером p95 (acks=all), мс",
     'histogram_quantile(0.95, sum by (topic, le) (rate(fm_kafka_produce_ack_seconds_bucket[30s])))', lambda m: m.get("topic", "?"),
     "мс", scale=1000)
plot("07-kafka-lag", "Kafka: consumer lag по группам (заполнение очередей)",
     'sum by (consumergroup) (kafka_consumergroup_lag)', lambda m: m.get("consumergroup", "?"), "сообщений")
plot("08-produced", "Kafka: события по топикам (ops/s)",
     'sum by (topic) (rate(fm_events_produced_total[30s]))', lambda m: m.get("topic", "?"), "ops/s")
plot("09-ejections", "Circuit breaker: исключённые хосты (outlier detection)",
     'sum by (cluster_name) (envoy_cluster_outlier_detection_ejections_active{cluster_name=~"outbound.*flashmarket.*"}) > 0 or sum by (cluster_name) (envoy_cluster_outlier_detection_ejections_active{cluster_name=~"outbound.*payment.*"})',
     lambda m: m.get("cluster_name", "?").replace("outbound|80||", "").replace(".svc.cluster.local", ""), "хостов")
plot("10-retries", "Повторы Envoy по upstream-кластерам (ops/s)",
     'sum by (cluster_name) (rate(envoy_cluster_upstream_rq_retry{cluster_name=~"outbound.*flashmarket.*"}[30s]))',
     lambda m: m.get("cluster_name", "?").replace("outbound|80||", "").replace(".svc.cluster.local", ""), "ops/s")
plot("11-payment-codes", "payment-service: ответы по кодам на входе сервиса (req/s)",
     'sum by (response_code) (rate(istio_requests_total{reporter="destination",destination_service_name="payment-service"}[30s]))',
     lambda m: m.get("response_code", "?"), "req/s")
plot("12-haproxy", "HAProxy: входящий трафик по узлам пары (байт/с)",
     'sum by (instance) (rate(haproxy_frontend_bytes_in_total{proxy="fe_http"}[30s]))', lambda m: m.get("instance", "?"), "B/s")
plot("13-reserve", "Резервы по результату (ops/s)",
     'sum by (result) (rate(fm_inventory_reserve_total[30s]))', lambda m: m.get("result", "?"), "ops/s")
