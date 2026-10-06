# FlashMarket

## Как поднять


```bash
kind create cluster --config infra/cluster/kind-config.yaml
helm upgrade --install cilium cilium/cilium -n kube-system -f infra/cluster/cilium-values.yaml
# Terraform и Ansible запускаются в контейнерах в сети kind (kubeconfig: kind get kubeconfig --internal)
docker run --rm --network kind -v $PWD/infra/terraform:/work -w /work hashicorp/terraform:1.13 apply
docker build -t fm-ansible infra/ansible
docker run --rm --network kind -v $PWD/infra/ansible:/work -e K8S_AUTH_KUBECONFIG=/work/kubeconfig fm-ansible \
  ansible-playbook playbook-kafka.yml -e kafka_profile=local
helm upgrade --install data-layer infra/data -n data
istioctl install -y --set profile=minimal
kubectl apply -f infra/gateway/gateway.yaml -f infra/mesh/peer-auth.yaml
docker build -t flashmarket/app:0.1.0 services && kind load docker-image flashmarket/app:0.1.0 --name flashmarket
for s in catalog inventory order payment notification analytics; do
  helm upgrade --install $s-service helm/microservice -n flashmarket -f helm/values/$s-service.yaml
done
bash infra/haproxy/up.sh
```
