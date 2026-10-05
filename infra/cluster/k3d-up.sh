#!/usr/bin/env bash
# Задание 1.1: локальный кластер k3s (через k3d) без flannel/kube-proxy -> Cilium как CNI
set -euo pipefail
k3d cluster create ticketflow --servers 1 --agents 3 \
  --k3s-arg "--flannel-backend=none@server:*" \
  --k3s-arg "--disable-network-policy@server:*" \
  --k3s-arg "--disable=traefik@server:*" \
  --k3s-arg "--disable-kube-proxy@server:*" \
  --registry-create ticketflow-registry:0.0.0.0:5000

helm repo add cilium https://helm.cilium.io && helm repo update
helm upgrade --install cilium cilium/cilium -n kube-system \
  --set kubeProxyReplacement=true \
  --set k8sServiceHost=k3d-ticketflow-server-0 --set k8sServicePort=6443 \
  --set hubble.enabled=true --set hubble.relay.enabled=true --set hubble.ui.enabled=true \
  --set hubble.metrics.enabled="{dns,drop,tcp,flow,http}" \
  --set prometheus.enabled=true --set operator.prometheus.enabled=true
cilium status --wait
