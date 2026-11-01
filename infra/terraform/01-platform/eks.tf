# Managed Kubernetes: Amazon EKS in Auto Mode. AWS runs the control plane AND the data-plane
# add-ons (Karpenter-based node autoscaling, VPC CNI, CoreDNS, kube-proxy, EBS CSI, ALB/NLB
# controller, Pod Identity agent), so Terraform only declares the cluster.
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name                   = "${local.name}-eks"
  kubernetes_version     = var.kubernetes_version
  endpoint_public_access = true

  enable_cluster_creator_admin_permissions = true

  # Safe to destroy and re-create any number of times (start/stop the whole system at will):
  deletion_protection         = false
  upgrade_policy              = { support_type = "STANDARD" } # never pay for extended support
  enabled_log_types           = []                            # control-plane logs would re-create a
  create_cloudwatch_log_group = false                         # log group during deletion -> next apply fails
  create_kms_key              = false                         # a new KMS key per cycle lingers 7-30 days;
  encryption_config           = null                          # EKS already encrypts with AWS-owned keys

  compute_config = {
    enabled    = true
    node_pools = ["general-purpose"]
  }

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  access_entries = {
    for i, arn in var.admin_principal_arns : "team-${i}" => {
      principal_arn = arn
      policy_associations = {
        admin = {
          policy_arn   = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    }
  }
}

# Container registries for the seven containerised microservices
locals {
  services = [
    "supplier-service",
    "booking-service",
    "inventory-service",
    "tracking-ingestion",
    "event-publisher",
    "delivery-status",
    "analytics-api",
  ]
}

resource "aws_ecr_repository" "svc" {
  for_each             = toset(local.services)
  name                 = "${local.name}/${each.key}"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "svc" {
  for_each   = aws_ecr_repository.svc
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 15 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 15 }
      action       = { type = "expire" }
    }]
  })
}
