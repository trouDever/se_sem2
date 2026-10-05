#!/usr/bin/env bash
# Задание 1.2: проверка Cluster Autoscaler с kwok. Поднимает нагрузку, ждёт новых нод, снимает нагрузку, ждёт удаления.
set -u
t0=$(date +%s); log() { echo "+$(( $(date +%s) - t0 ))s $*"; }
nodes() { kubectl get nodes -l kwok-nodegroup=kwok-worker --no-headers 2>/dev/null | wc -l; }
running() { kubectl get pods -n autoscale-demo --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l; }

log "start: kwok nodes=$(nodes)"
kubectl scale deploy/inflate -n autoscale-demo --replicas=10 >/dev/null
log "scaled inflate to 10 pods (1 CPU each)"
prev=-1
for i in $(seq 1 120); do
  n=$(nodes); r=$(running)
  [ "$n" != "$prev" ] && log "kwok nodes=$n running pods=$r" && prev=$n
  [ "$r" -ge 10 ] && break
  [ "$n" -ge 5 ] && [ "$r" -ge 10 ] && break
  sleep 2
done
log "after scale-up: nodes=$(nodes) running=$(running) pending=$(kubectl get pods -n autoscale-demo --field-selector=status.phase=Pending --no-headers | wc -l)"
kubectl get nodes -l kwok-nodegroup=kwok-worker --no-headers | awk '{print "  " $1, $2}'

kubectl scale deploy/inflate -n autoscale-demo --replicas=0 >/dev/null
log "scaled inflate to 0"
prev=-1
for i in $(seq 1 200); do
  n=$(nodes)
  [ "$n" != "$prev" ] && log "kwok nodes=$n" && prev=$n
  [ "$n" -eq 0 ] && break
  sleep 3
done
log "done: kwok nodes=$(nodes)"
kubectl get events -n kube-system --field-selector reason=ScaledUpGroup -o custom-columns=T:.lastTimestamp,MSG:.message --no-headers 2>/dev/null | tail -3
kubectl get events -n default --field-selector reason=ScaleDown -o custom-columns=T:.lastTimestamp,MSG:.message --no-headers 2>/dev/null | tail -3
