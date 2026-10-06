# FlashMarket

## Как поднять

Нужны Docker, kind, kubectl, helm и istioctl. Docker лучше дать 12 ГБ памяти: с 8 ГБ стенд работает на пределе.

```bash
# репозитории Helm-чартов
helm repo add cilium https://helm.cilium.io
helm repo add vm https://victoriametrics.github.io/helm-charts/
helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm repo update

# кластер и сеть
kind create cluster --config infra/cluster/kind-config.yaml
helm upgrade --install cilium cilium/cilium -n kube-system -f infra/cluster/cilium-values.yaml

# неймспейсы, сервисные аккаунты и секреты
# Terraform и Ansible запускаются в контейнерах в сети kind (kubeconfig: kind get kubeconfig --internal)
docker run --rm --network kind -v $PWD/infra/terraform:/work -w /work hashicorp/terraform:1.13 apply

# Kafka через Ansible-роль
docker build -t fm-ansible infra/ansible
docker run --rm --network kind -v $PWD/infra/ansible:/work -e K8S_AUTH_KUBECONFIG=/work/kubeconfig fm-ansible \
  ansible-playbook playbook-kafka.yml -e kafka_profile=local

# базы и кэш
helm upgrade --install data-layer infra/data -n data

# Istio, шлюз, rate limiter, mTLS и трейсинг
istioctl install -y -f infra/mesh/istio-operator.yaml
kubectl apply -f infra/mesh/peer-auth.yaml -f infra/mesh/telemetry.yaml -f infra/gateway/gateway.yaml

# метрики, логи, трейсы и алерты
helm upgrade --install vm vm/victoria-metrics-k8s-stack -n observability -f infra/observability/vm-stack-values.yaml
helm upgrade --install vlogs vm/victoria-logs-single -n observability -f infra/observability/vlogs-values.yaml
helm upgrade --install vlogs-collector vm/victoria-logs-collector -n observability -f infra/observability/vlogs-collector-values.yaml
helm upgrade --install vtraces vm/victoria-traces-single -n observability -f infra/observability/vtraces-values.yaml
kubectl apply -f infra/observability/vtraces-otlp-service.yaml -f infra/observability/dashboard-configmap.yaml -f infra/observability/alerts.yaml

# Gitea и ArgoCD (App of Apps)
kubectl apply -f infra/git/gitea.yaml
kubectl apply -n argocd --server-side -f infra/git/argocd-install.yaml
# в Gitea нужно создать пользователя fm и запушить туда этот репозиторий, после чего:
kubectl apply -f gitops/root-app.yaml

# CI: локальный registry и раннер Gitea Actions
kubectl apply -f infra/ci/registry.yaml
bash infra/ci/node-registry.sh
docker build -t fm-act-runner:0.1.0 -f infra/ci/Dockerfile.runner infra/ci
kind load docker-image fm-act-runner:0.1.0 --name flashmarket
kubectl create secret generic runner-registration -n ci \
  --from-literal=token=$(kubectl exec -n git gitea-0 -- gitea actions generate-runner-token)
kubectl apply -f infra/ci/runner.yaml
# дальше каждый push в services/ собирает образ, обновляет тег в чарте, а ArgoCD выкатывает сервисы

# автомасштабирование нод
kubectl apply -f infra/autoscaler/kwok.yaml -f infra/autoscaler/stage-fast.yaml
helm upgrade --install cluster-autoscaler autoscaler/cluster-autoscaler --version 9.59.0 -n kube-system \
  -f infra/autoscaler/ca-values.yaml
kubectl apply --server-side --force-conflicts -f infra/autoscaler/kwok-provider.yaml

# сетевые политики и пара HAProxy с Keepalived
kubectl apply -f infra/cluster/network-policies.yaml
bash infra/haproxy/up.sh
```
