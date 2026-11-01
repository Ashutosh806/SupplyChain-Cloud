# Least-privilege AWS access for pods via EKS Pod Identity (no static keys in the cluster).
locals {
  pod_trust = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })

  workload_policies = {
    # namespace/serviceaccount => policy statements
    "supplychain/tracking-ingestion" = [
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = "${aws_s3_bucket.data.arn}/raw/*" },
    ]
    "supplychain/delivery-status" = [
      { Effect = "Allow", Action = ["sqs:SendMessage", "sqs:GetQueueAttributes"], Resource = aws_sqs_queue.notifications.arn },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = "${aws_s3_bucket.data.arn}/pod/*" },
    ]
    "supplychain/analytics-api" = [
      { Effect = "Allow", Action = ["dynamodb:PutItem", "dynamodb:BatchWriteItem", "dynamodb:Query", "dynamodb:GetItem"], Resource = aws_dynamodb_table.analytics.arn },
    ]
    "monitoring/grafana" = [
      { Effect = "Allow", Action = ["logs:DescribeLogGroups", "logs:GetLogGroupFields", "logs:StartQuery", "logs:StopQuery", "logs:GetQueryResults", "logs:GetLogEvents", "logs:FilterLogEvents"], Resource = "*" },
      { Effect = "Allow", Action = ["cloudwatch:GetMetricData", "cloudwatch:ListMetrics", "cloudwatch:DescribeAlarms", "tag:GetResources", "ec2:DescribeRegions"], Resource = "*" },
    ]
  }
}

resource "aws_iam_role" "workload" {
  for_each           = local.workload_policies
  name               = "${local.name}-${replace(each.key, "/", "-")}"
  assume_role_policy = local.pod_trust
}

resource "aws_iam_role_policy" "workload" {
  for_each = local.workload_policies
  name     = "access"
  role     = aws_iam_role.workload[each.key].id
  policy   = jsonencode({ Version = "2012-10-17", Statement = each.value })
}

resource "aws_eks_pod_identity_association" "workload" {
  for_each        = local.workload_policies
  cluster_name    = module.eks.cluster_name
  namespace       = split("/", each.key)[0]
  service_account = split("/", each.key)[1]
  role_arn        = aws_iam_role.workload[each.key].arn
}
