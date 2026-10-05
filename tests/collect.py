"""Собирает метрики окна нагрузочного теста из VictoriaMetrics (через API-proxy kubectl) в results/metrics.json."""
import json
import pathlib
import subprocess
import sys
import urllib.parse

OUT = pathlib.Path(__file__).parent / "results"
start, end = int(sys.argv[1]), int(sys.argv[2])
dur = f"{end - start}s"


def q(expr, t=end):
    path = ("/api/v1/namespaces/observability/services/vmsingle-vm:8428/proxy/api/v1/query?"
            + urllib.parse.urlencode({"query": expr, "time": t}))
    out = subprocess.run(["kubectl", "get", "--raw", path], capture_output=True, text=True, timeout=60).stdout
    return [(r["metric"], float(r["value"][1])) for r in json.loads(out)["data"]["result"]]


def qr(expr, step=5):
    path = ("/api/v1/namespaces/observability/services/vmsingle-vm:8428/proxy/api/v1/query_range?"
            + urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": step}))
    out = subprocess.run(["kubectl", "get", "--raw", path], capture_output=True, text=True, timeout=60).stdout
    return [{"metric": r["metric"], "values": [(int(t), float(v)) for t, v in r["values"]]}
            for r in json.loads(out)["data"]["result"]]


M = {
    "requests_by_code_gateway": q(f'sum by (response_code) (increase(istio_requests_total{{reporter="source",app="public-istio"}}[{dur}]))'),
    "requests_by_service_code": q(f'sum by (destination_service_name, response_code) (increase(istio_requests_total{{reporter="destination",destination_service_namespace="flashmarket"}}[{dur}]))'),
    "ratelimit_over": q(f'sum by (key1) (increase(ratelimit_service_rate_limit_over_limit[{dur}]))'),
    "ratelimit_within": q(f'sum by (key1) (increase(ratelimit_service_rate_limit_within_limit[{dur}]))'),
    "events_produced": q(f'sum by (topic) (increase(fm_events_produced_total[{dur}]))'),
    "events_consumed": q(f'sum by (result) (increase(fm_events_consumed_total[{dur}]))'),
    "e2e_latency_p50": q(f'histogram_quantile(0.5, sum by (le) (increase(fm_event_e2e_latency_seconds_bucket[{dur}])))'),
    "e2e_latency_p95": q(f'histogram_quantile(0.95, sum by (le) (increase(fm_event_e2e_latency_seconds_bucket[{dur}])))'),
    "e2e_latency_p99": q(f'histogram_quantile(0.99, sum by (le) (increase(fm_event_e2e_latency_seconds_bucket[{dur}])))'),
    "ack_latency_p95": q(f'histogram_quantile(0.95, sum by (le) (increase(fm_kafka_produce_ack_seconds_bucket[{dur}])))'),
    "ack_latency_p50": q(f'histogram_quantile(0.5, sum by (le) (increase(fm_kafka_produce_ack_seconds_bucket[{dur}])))'),
    "max_consumer_lag": q(f'max_over_time(sum by (consumergroup) (kafka_consumergroup_lag)[{dur}])'),
    "reserve_results": q(f'sum by (result) (increase(fm_inventory_reserve_total[{dur}]))'),
    "payments_results": q(f'sum by (result) (increase(fm_payments_total[{dur}]))'),
    "cache": q(f'sum by (result) (increase(fm_catalog_cache_total[{dur}]))'),
    "ejections_total": q(f'sum by (cluster_name) (increase(envoy_cluster_outlier_detection_ejections_enforced_total{{cluster_name=~"outbound.*"}}[{dur}]))'),
    "retries_total": q(f'sum by (cluster_name) (increase(envoy_cluster_upstream_rq_retry{{cluster_name=~"outbound.*"}}[{dur}]))'),
    "archived": q(f'sum by (topic) (increase(fm_archive_records_total[{dur}]))'),
    "series_ejections_active": qr('sum by (cluster_name) (envoy_cluster_outlier_detection_ejections_active{cluster_name=~"outbound.*"})'),
    "series_payment_5xx_seen_by_gateway": qr('sum by (response_code) (rate(istio_requests_total{reporter="source",app="public-istio",destination_service_name="payment-service"}[30s]))'),
    "series_haproxy_sessions": qr('haproxy_frontend_current_sessions{proxy="fe_http"}'),
}
OUT.mkdir(exist_ok=True)
(OUT / "metrics.json").write_text(json.dumps(M, ensure_ascii=False, indent=1), encoding="utf-8")
for k, v in M.items():
    if not k.startswith("series_"):
        print(k, [(m.get("key1") or m.get("topic") or m.get("result") or m.get("response_code")
                   or m.get("destination_service_name", "") + ":" + m.get("response_code", "")
                   or m.get("consumergroup") or m.get("cluster_name", "")[:60], round(x, 3)) for m, x in v][:14])
