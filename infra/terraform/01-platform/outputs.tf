output "region" { value = var.region }
output "account_id" { value = local.account_id }
output "cluster_name" { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_certificate_authority_data" { value = module.eks.cluster_certificate_authority_data }
output "vpc_id" { value = module.vpc.vpc_id }

output "kafka_bootstrap_brokers" { value = aws_msk_cluster.this.bootstrap_brokers }

output "db_host" { value = aws_db_instance.postgres.address }
output "db_name" { value = aws_db_instance.postgres.db_name }
output "db_user" { value = aws_db_instance.postgres.username }
output "db_password" {
  value     = random_password.db.result
  sensitive = true
}

output "data_bucket" { value = aws_s3_bucket.data.id }
output "emr_bucket" { value = aws_s3_bucket.emr.id }
output "notification_queue_url" { value = aws_sqs_queue.notifications.url }
output "notifications_table" { value = aws_dynamodb_table.notifications.name }
output "analytics_table" { value = aws_dynamodb_table.analytics.name }
output "notification_function_url" { value = aws_lambda_function_url.notification.function_url }
output "sns_topic_arn" { value = aws_sns_topic.alerts.arn }

output "ecr_registry" { value = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com" }
output "ecr_repositories" { value = { for k, r in aws_ecr_repository.svc : k => r.repository_url } }

output "emr_cluster_id" { value = try(aws_emr_cluster.flink[0].id, "") }
output "notification_function_name" { value = aws_lambda_function.notification.function_name }
output "emr_master_dns" { value = try(aws_emr_cluster.flink[0].master_public_dns, "") }
