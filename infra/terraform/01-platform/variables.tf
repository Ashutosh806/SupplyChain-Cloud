variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "project" {
  description = "Name prefix for every resource"
  type        = string
  default     = "supplychain"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version (keep it in standard support)"
  type        = string
  default     = "1.35"
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "admin_principal_arns" {
  description = "Extra IAM user/role ARNs (team members) that get cluster-admin on EKS"
  type        = list(string)
  default     = []
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.small" # ~200 max_connections; HPAs add pods
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "alert_email" {
  description = "Optional e-mail subscribed (HIGH severity only) to the SNS alerts topic"
  type        = string
  default     = ""
}

variable "enable_emr" {
  description = "Create the EMR (Flink) cluster. Turn on after the platform is up (scripts/up.sh emr)."
  type        = bool
  default     = false
}

variable "emr_release" {
  description = "EMR release label (emr-7.14.0 ships Flink 1.20.0; it is the newest EMR 7.x release)"
  type        = string
  default     = "emr-7.14.0"
}

variable "emr_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "loki_push_url" {
  description = "Loki push endpoint reachable from the VPC (internal NLB); EMR ships Flink logs here"
  type        = string
  default     = ""
}

