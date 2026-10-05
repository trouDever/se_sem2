#!/usr/bin/env bash
# Задание 6.2: отказы по расписанию во время нагрузочного теста (время — от старта Locust).
#  t=90s  один из двух подов payment-service отвечает 503 на всё в течение 120 с  -> circuit breaker
#  t=150s один под inventory-service отвечает 503 на 50% запросов 60 с         -> retries
#  t=200s жёсткое падение контейнера haproxy-1 (мастер VIP)                      -> failover Keepalived
#  t=260s haproxy-1 возвращается                                                 -> возврат VIP
set -u
log() { echo "$(date -u +%H:%M:%S) +$(( $(date +%s) - T0 ))s $*"; }
fault() { kubectl exec -n flashmarket "$1" -c istio-proxy -- curl -s -XPOST localhost:8080/admin/fault \
          -H 'content-type: application/json' -d "{\"error_rate\": $2, \"seconds\": $3}"; echo; }
T0=$(date +%s)
PAY=$(kubectl get pod -n flashmarket -l app=payment-service -o jsonpath='{.items[0].metadata.name}')
INV=$(kubectl get pod -n flashmarket -l app=inventory-service -o jsonpath='{.items[0].metadata.name}')
log "start; payment victim=$PAY inventory victim=$INV"
sleep 90;  log "FAULT payment $PAY 100% for 120s"; fault "$PAY" 1.0 120
sleep 60;  log "FAULT inventory $INV 50% for 60s"; fault "$INV" 0.5 60
sleep 50;  log "KILL haproxy-1 (master)"; docker kill haproxy-1 >/dev/null
sleep 3;   log "VIP now on: $(docker exec haproxy-2 ip -4 addr show eth0 | grep -c 172.30.0.100) (haproxy-2 has VIP if 1)"
sleep 57;  log "START haproxy-1"; docker start haproxy-1 >/dev/null
sleep 10;  log "VIP on haproxy-1: $(docker exec haproxy-1 ip -4 addr show eth0 | grep -c 172.30.0.100)"
log "chaos done"
