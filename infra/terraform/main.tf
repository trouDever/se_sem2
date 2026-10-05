# Задание 2.1: базовая инфраструктура кластера — неймспейсы, ServiceAccount'ы, секреты.
# Пароли генерирует random_password: в Git нет ни одного секрета, Helm-чарты ссылаются на секреты по имени.
terraform {
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.38" }
    random     = { source = "hashicorp/random", version = "~> 3.7" }
  }
}

provider "kubernetes" {
  config_path = var.kubeconfig
}

variable "kubeconfig" {
  default = "/work/kubeconfig"
}

locals {
  app_namespace = "flashmarket"
  namespaces = {
    flashmarket   = { "istio-injection" = "enabled" }
    data          = {}
    kafka         = {}
    edge          = {}
    observability = {}
    argocd        = {}
  }
  services          = ["catalog-service", "inventory-service", "order-service", "payment-service", "notification-service", "analytics-service"]
  postgres_services = ["inventory-service", "order-service", "payment-service"]
}

resource "kubernetes_namespace_v1" "ns" {
  for_each = local.namespaces
  metadata {
    name   = each.key
    labels = merge({ "app.kubernetes.io/part-of" = "flashmarket" }, each.value)
  }
}

resource "kubernetes_service_account_v1" "svc" {
  for_each = toset(local.services)
  metadata {
    name      = each.value
    namespace = kubernetes_namespace_v1.ns[local.app_namespace].metadata[0].name
  }
}

# --- PostgreSQL: отдельный инстанс и учётка на каждый сервис (database per service)
resource "random_password" "postgres" {
  for_each = toset(local.postgres_services)
  length   = 24
  special  = false
}

resource "kubernetes_secret_v1" "postgres_server" {
  for_each = toset(local.postgres_services)
  metadata {
    name      = "${each.value}-postgres"
    namespace = kubernetes_namespace_v1.ns["data"].metadata[0].name
  }
  data = {
    POSTGRES_USER     = replace(each.value, "-service", "")
    POSTGRES_PASSWORD = random_password.postgres[each.value].result
    POSTGRES_DB       = replace(each.value, "-service", "")
  }
}

resource "kubernetes_secret_v1" "postgres_client" {
  for_each = toset(local.postgres_services)
  metadata {
    name      = "${each.value}-db"
    namespace = kubernetes_namespace_v1.ns[local.app_namespace].metadata[0].name
  }
  data = {
    url = format("postgresql://%s:%s@pg-%s.data.svc:5432/%s",
      replace(each.value, "-service", ""), random_password.postgres[each.value].result,
    replace(each.value, "-service", ""), replace(each.value, "-service", ""))
  }
}

# --- Valkey: общий пароль для сервисов, rate limiter'а и экспортера
resource "random_password" "valkey" {
  length  = 32
  special = false
}

resource "kubernetes_secret_v1" "valkey" {
  for_each = toset([local.app_namespace, "data", "edge", "observability"])
  metadata {
    name      = "valkey-auth"
    namespace = kubernetes_namespace_v1.ns[each.value].metadata[0].name
  }
  data = { password = random_password.valkey.result }
}

# --- MinIO (холодный архив)
resource "random_password" "minio" {
  length  = 32
  special = false
}

resource "kubernetes_secret_v1" "minio" {
  for_each = toset([local.app_namespace, "data"])
  metadata {
    name      = "minio-auth"
    namespace = kubernetes_namespace_v1.ns[each.value].metadata[0].name
  }
  data = { access_key = "flashmarket", secret_key = random_password.minio.result }
}

# --- Grafana
resource "random_password" "grafana" {
  length  = 20
  special = false
}

resource "kubernetes_secret_v1" "grafana" {
  metadata {
    name      = "grafana-admin"
    namespace = kubernetes_namespace_v1.ns["observability"].metadata[0].name
  }
  data = { admin-user = "admin", admin-password = random_password.grafana.result }
}

output "grafana_password" {
  value     = random_password.grafana.result
  sensitive = true
}
