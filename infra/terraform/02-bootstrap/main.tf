# Stage 2: bootstrap GitOps on the cluster created in stage 1.
#   - namespaces + Terraform-owned environment bindings (ConfigMap/Secrets with endpoints and creds)
#   - Argo CD (Helm) and a single root "app-of-apps" Application.
# From here on, EVERYTHING inside Kubernetes (apps, monitoring, ingress) is deployed by Argo CD
# from the Git repository - never by kubectl apply.
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 6.30" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.38" }
    helm       = { source = "hashicorp/helm", version = "~> 3.0" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

variable "git_repo_url" {
  description = "HTTPS URL of the Git repository Argo CD tracks (this repo)"
  type        = string
}

variable "git_revision" {
  type    = string
  default = "main"
}

variable "git_username" {
  description = "Only for private repos: GitHub username"
  type        = string
  default     = ""
}

variable "git_token" {
  description = "Only for private repos: GitHub personal access token (read-only contents)"
  type        = string
  default     = ""
  sensitive   = true
}

data "terraform_remote_state" "platform" {
  backend = "local"
  config = {
    path = "${path.module}/../01-platform/terraform.tfstate"
  }
}

locals {
  p = data.terraform_remote_state.platform.outputs
}

provider "aws" {
  region = local.p.region
}

provider "kubernetes" {
  host                   = local.p.cluster_endpoint
  cluster_ca_certificate = base64decode(local.p.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", local.p.cluster_name, "--region", local.p.region]
  }
}

provider "helm" {
  kubernetes = {
    host                   = local.p.cluster_endpoint
    cluster_ca_certificate = base64decode(local.p.cluster_certificate_authority_data)
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", local.p.cluster_name, "--region", local.p.region]
    }
  }
}

# ------------------------------------------------------------------ namespaces
resource "kubernetes_namespace_v1" "ns" {
  for_each = toset(["supplychain", "monitoring"])
  metadata {
    name   = each.key
    labels = { "app.kubernetes.io/managed-by" = "terraform" }
  }
}

# ------------------------------------------------------------------ environment bindings
# Infrastructure endpoints flow from Terraform outputs into the cluster here, so the Git
# repository holds no account-specific endpoints or secrets.
resource "kubernetes_config_map_v1" "infra" {
  metadata {
    name      = "infra-config"
    namespace = kubernetes_namespace_v1.ns["supplychain"].metadata[0].name
  }
  data = {
    AWS_REGION              = local.p.region
    KAFKA_BOOTSTRAP         = local.p.kafka_bootstrap_brokers
    KAFKA_SECURITY_PROTOCOL = "PLAINTEXT"
    DB_HOST                 = local.p.db_host
    DB_NAME                 = local.p.db_name
    DB_SSLMODE              = "require"
    RAW_BUCKET              = local.p.data_bucket
    NOTIFICATION_QUEUE_URL  = local.p.notification_queue_url
    ANALYTICS_TABLE         = local.p.analytics_table
    NOTIFICATIONS_TABLE     = local.p.notifications_table
    NOTIFICATION_URL        = local.p.notification_function_url
  }
}

# Fluent Bit (monitoring namespace) reads the EMR/Flink logs from Kafka: it needs the brokers too.
resource "kubernetes_config_map_v1" "monitoring_kafka" {
  metadata {
    name      = "kafka-endpoints"
    namespace = kubernetes_namespace_v1.ns["monitoring"].metadata[0].name
  }
  data = {
    KAFKA_BOOTSTRAP = local.p.kafka_bootstrap_brokers
  }
}

resource "kubernetes_secret_v1" "db" {
  metadata {
    name      = "db-credentials"
    namespace = kubernetes_namespace_v1.ns["supplychain"].metadata[0].name
  }
  data = {
    DB_USER     = local.p.db_user
    DB_PASSWORD = local.p.db_password
  }
}

resource "random_password" "grafana" {
  length  = 16
  special = false
}

resource "kubernetes_secret_v1" "grafana_admin" {
  metadata {
    name      = "grafana-admin"
    namespace = kubernetes_namespace_v1.ns["monitoring"].metadata[0].name
  }
  data = {
    "admin-user"     = "admin"
    "admin-password" = random_password.grafana.result
  }
}

# ------------------------------------------------------------------ Argo CD
# Argo CD chart packages are downloaded beforehand by scripts/up.sh (curl with retries) into
# .charts/ and installed from there: Helm's own download from GitHub release assets times out on
# some networks. To change a version, edit it here - up.sh reads it from this file.
locals {
  argocd_chart_version      = "10.11.6"
  argocd_apps_chart_version = "2.0.7"
}

resource "helm_release" "argocd" {
  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  chart            = "${path.module}/.charts/argo-cd-${local.argocd_chart_version}.tgz"
  version          = local.argocd_chart_version
  timeout          = 900

  values = [yamlencode({
    configs = {
      params = { "server.insecure" = true }
      cm = {
        "timeout.reconciliation" = "60s"
        # Let Argo CD treat child Applications as healthy only once they are synced+healthy,
        # so sync waves in the app-of-apps are honoured.
        "resource.customizations.health.argoproj.io_Application" = <<-EOT
          hs = {}
          hs.status = "Progressing"
          hs.message = ""
          if obj.status ~= nil then
            if obj.status.health ~= nil then
              hs.status = obj.status.health.status
              if obj.status.health.message ~= nil then
                hs.message = obj.status.health.message
              end
            end
          end
          return hs
        EOT
      }
    }
    dex = { enabled = false }
  })]
}

resource "kubernetes_secret_v1" "repo" {
  count = var.git_token == "" ? 0 : 1
  metadata {
    name      = "gitops-repo"
    namespace = "argocd"
    labels    = { "argocd.argoproj.io/secret-type" = "repository" }
  }
  data = {
    type     = "git"
    url      = var.git_repo_url
    username = var.git_username
    password = var.git_token
  }
  depends_on = [helm_release.argocd]
}

# Root Application (app-of-apps): Argo CD syncs deploy/argocd/apps, which declares every other app.
resource "helm_release" "root_app" {
  name       = "argocd-root"
  namespace  = "argocd"
  chart      = "${path.module}/.charts/argocd-apps-${local.argocd_apps_chart_version}.tgz"
  version    = local.argocd_apps_chart_version

  values = [yamlencode({
    applications = {
      root = {
        namespace  = "argocd"
        project    = "default"
        finalizers = ["resources-finalizer.argocd.argoproj.io"]
        source = {
          repoURL        = var.git_repo_url
          targetRevision = var.git_revision
          path           = "deploy/argocd/apps"
          directory      = { recurse = false }
        }
        destination = { server = "https://kubernetes.default.svc", namespace = "argocd" }
        syncPolicy = {
          automated   = { prune = true, selfHeal = true }
          syncOptions = ["CreateNamespace=true"]
        }
      }
    }
  })]

  depends_on = [helm_release.argocd, kubernetes_secret_v1.repo, kubernetes_config_map_v1.infra,
  kubernetes_secret_v1.db, kubernetes_secret_v1.grafana_admin]
}

output "grafana_admin_password" {
  value     = random_password.grafana.result
  sensitive = true
}

output "argocd_admin_password_cmd" {
  value = "kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
}
