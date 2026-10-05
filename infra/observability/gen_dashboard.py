"""Генерирует ConfigMap с дашбордом Grafana «FlashMarket — Black Friday war room» (задание 6.3)."""
import json
import pathlib

ROWS = [
    ("Вход: HAProxy, Istio Gateway, Rate Limiter", [
        ("Gateway responses by code", 'sum by (response_code) (rate(istio_requests_total{reporter="source",pod=~"public-istio.*"}[1m]))', "reqps"),
        ("Rate-limited share (429)", 'sum(rate(istio_requests_total{reporter="source",pod=~"public-istio.*",response_code="429"}[1m])) / sum(rate(istio_requests_total{reporter="source",pod=~"public-istio.*"}[1m]))', "percentunit"),
        ("Rate limiter decisions", 'sum by (key1) (rate(ratelimit_service_rate_limit_over_limit[1m])) or label_replace(sum by (key1) (rate(ratelimit_service_rate_limit_within_limit[1m])), "key1", "within: $1", "key1", "(.*)")', "reqps"),
        ("HAProxy traffic in (VIP pair)", 'sum by (instance) (rate(haproxy_frontend_bytes_in_total{proxy="fe_http"}[1m]))', "Bps"),
    ]),
    ("Сервисы: RED (Istio telemetry)", [
        ("Requests per service", 'sum by (destination_service_name) (rate(istio_requests_total{reporter="destination",destination_service_namespace="flashmarket"}[1m]))', "reqps"),
        ("5xx ratio", 'sum by (destination_service_name) (rate(istio_requests_total{reporter="destination",destination_service_namespace="flashmarket",response_code=~"5.."}[1m])) / sum by (destination_service_name) (rate(istio_requests_total{reporter="destination",destination_service_namespace="flashmarket"}[1m]))', "percentunit"),
        ("Latency p95", 'histogram_quantile(0.95, sum by (destination_service_name, le) (rate(istio_request_duration_milliseconds_bucket{reporter="destination",destination_service_namespace="flashmarket"}[1m])))', "ms"),
    ]),
    ("Kafka: throughput, latency, queue fill", [
        ("Events produced per topic", 'sum by (topic) (rate(fm_events_produced_total[1m]))', "ops"),
        ("Broker ack latency p95 (acks=all)", 'histogram_quantile(0.95, sum by (topic, le) (rate(fm_kafka_produce_ack_seconds_bucket[1m])))', "s"),
        ("End-to-end event latency p95 (produce → consume)", 'histogram_quantile(0.95, sum by (topic, group, le) (rate(fm_event_e2e_latency_seconds_bucket[1m])))', "s"),
        ("Consumer lag (queue fill)", 'sum by (consumergroup, topic) (kafka_consumergroup_lag)', "short"),
    ]),
    ("Устойчивость: circuit breaker и повторы", [
        ("Outlier detection: ejected hosts", 'sum by (cluster_name) (envoy_cluster_outlier_detection_ejections_active{cluster_name=~"outbound.*flashmarket.*"})', "short"),
        ("Retries and Envoy response flags", 'sum by (cluster_name) (rate(envoy_cluster_upstream_rq_retry{cluster_name=~"outbound.*flashmarket.*"}[1m])) or sum by (response_flags) (rate(istio_requests_total{response_flags!="-",reporter="source"}[1m]))', "ops"),
    ]),
    ("Бизнес и данные", [
        ("Reservations by result", 'sum by (result) (rate(fm_inventory_reserve_total[1m]))', "ops"),
        ("Payments by result", 'sum by (result) (rate(fm_payments_total[1m]))', "ops"),
        ("Catalog cache hit ratio", 'sum(rate(fm_catalog_cache_total{result="hit"}[1m])) / sum(rate(fm_catalog_cache_total[1m]))', "percentunit"),
        ("Cold archive: records/s", 'sum by (topic) (rate(fm_archive_records_total[1m]))', "ops"),
    ]),
]


def build():
    panels, y, pid = [], 0, 1
    for title, items in ROWS:
        panels.append({"type": "row", "title": title, "id": pid, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}})
        pid += 1
        y += 1
        w = 24 // len(items)
        for i, (pt, expr, unit) in enumerate(items):
            panels.append({
                "type": "timeseries", "title": pt, "id": pid,
                "datasource": {"type": "prometheus", "uid": "${ds}"},
                "gridPos": {"x": i * w, "y": y, "w": w, "h": 8},
                "fieldConfig": {"defaults": {"unit": unit, "custom": {"fillOpacity": 15, "lineWidth": 2}},
                                "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
                "targets": [{"refId": "A", "expr": expr, "legendFormat": "__auto"}]})
            pid += 1
        y += 8
    return {
        "uid": "flashmarket", "title": "FlashMarket — Black Friday war room", "schemaVersion": 39,
        "time": {"from": "now-30m", "to": "now"}, "refresh": "10s", "panels": panels,
        "templating": {"list": [{"name": "ds", "type": "datasource", "query": "prometheus", "hide": 2}]},
    }


if __name__ == "__main__":
    here = pathlib.Path(__file__).parent
    dash = json.dumps(build(), ensure_ascii=False, indent=1)
    cm = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: dashboard-flashmarket\n  namespace: observability\n"
          "  labels: { grafana_dashboard: \"1\" }\ndata:\n  flashmarket.json: |\n"
          + "\n".join("    " + line for line in dash.splitlines()) + "\n")
    (here / "dashboard-configmap.yaml").write_text(cm, encoding="utf-8")
    print("panels:", sum(1 for p in build()["panels"] if p["type"] != "row"))
