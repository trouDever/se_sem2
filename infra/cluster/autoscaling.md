# Задание 1.2: автоскейлинг нод

Karpenter работает только с облачными провайдерами (AWS, Azure), в локальном кластере он неприменим.

План:
- **Cluster Autoscaler** с провайдером `clusterapi`: кластер поднимается через Cluster API (CAPD — Docker-провайдер, для Talos — CAPI-провайдер Talos), CA масштабирует `MachineDeployment` воркеров по pending-подам (аннотации `cluster.x-k8s.io/cluster-api-autoscaler-node-group-min-size/max-size`).
- Для демонстрации без реальных VM — **KWOK** (фейковые ноды) + CA kwok-провайдер.
- Масштабирование подов: HPA (CPU) + **KEDA** по метрике Kafka consumer lag для консьюмеров (notification, analytics).
