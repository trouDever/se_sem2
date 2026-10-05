#!/usr/bin/env bash
# Учим containerd на ноде kind тянуть образы registry.ci:5000/... из локального registry по HTTP.
# containerd 2.x читает /etc/containerd/certs.d на каждый pull, перезапуск не нужен.
set -euo pipefail
NODE=${NODE:-flashmarket-control-plane}
docker exec "$NODE" sh -c 'mkdir -p "/etc/containerd/certs.d/registry.ci:5000" && cat > "/etc/containerd/certs.d/registry.ci:5000/hosts.toml" <<EOF
server = "http://10.96.200.200:5000"

[host."http://10.96.200.200:5000"]
  capabilities = ["pull", "resolve"]
  skip_verify = true
EOF'
echo "registry mirror configured on $NODE"
