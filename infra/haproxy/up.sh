#!/usr/bin/env bash
# Поднимает пару HAProxy+Keepalived в отдельной docker-сети fm-edge (172.30.0.0/24):
# мастер .11, резерв .12, VIP .100; нода кластера подключена в эту же сеть как .2
set -euo pipefail
cd "$(dirname "$0")"
docker build -q -t fm-haproxy .
docker network inspect fm-edge >/dev/null 2>&1 || docker network create --subnet 172.30.0.0/24 fm-edge
docker network connect --ip 172.30.0.2 fm-edge flashmarket-control-plane 2>/dev/null || true
for n in 1 2; do docker rm -f haproxy-$n >/dev/null 2>&1 || true; done
common="--network fm-edge --cap-add NET_ADMIN --cap-add NET_BROADCAST --cap-add NET_RAW -e VIP=172.30.0.100"
docker run -d --name haproxy-1 --hostname haproxy-1 --ip 172.30.0.11 $common \
  -e STATE=MASTER -e PRIORITY=110 -e SELF_IP=172.30.0.11 -e PEER_IP=172.30.0.12 fm-haproxy
docker run -d --name haproxy-2 --hostname haproxy-2 --ip 172.30.0.12 $common \
  -e STATE=BACKUP -e PRIORITY=100 -e SELF_IP=172.30.0.12 -e PEER_IP=172.30.0.11 fm-haproxy
