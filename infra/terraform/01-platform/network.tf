data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  name       = var.project
  account_id = data.aws_caller_identity.current.account_id
  azs        = slice(data.aws_availability_zones.available.names, 0, 2)
  # Tag required by the EMR v2 managed policies on every network resource EMR touches
  emr_tag = { "for-use-with-amazon-emr-managed-policies" = "true" }
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.0"

  name = "${local.name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]      # /20 each - room for pod IPs
  public_subnets  = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 8, 48 + i)] # /24 each

  enable_nat_gateway   = true
  single_nat_gateway   = true # cost: one NAT for the whole demo
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Subnet discovery tags for EKS Auto Mode load balancing
  public_subnet_tags  = { "kubernetes.io/role/elb" = "1" }
  private_subnet_tags = merge({ "kubernetes.io/role/internal-elb" = "1" }, local.emr_tag)
  vpc_tags            = local.emr_tag
}

# Data stores accept traffic only from inside the VPC (EKS pods, EMR, Lambda-less)
resource "aws_security_group" "data_stores" {
  name        = "${local.name}-data-stores"
  description = "RDS + MSK access from inside the VPC"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  ingress {
    description = "Kafka plaintext (in-VPC only)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  ingress {
    description = "Kafka TLS"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
