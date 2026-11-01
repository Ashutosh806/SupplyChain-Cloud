############################################
# Amazon EMR cluster running Apache Flink on YARN (Shipment Analytics stream processor).
# Created only when enable_emr = true (it is the most expensive component; scripts/up.sh emr).
############################################
resource "aws_s3_bucket" "emr" {
  bucket        = "${local.name}-emr-${local.account_id}-${var.region}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "emr" {
  bucket                  = aws_s3_bucket.emr.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "fluent_bit_bootstrap" {
  bucket = aws_s3_bucket.emr.id
  key    = "bootstrap/install-fluent-bit.sh"
  source = "${path.module}/../../emr-bootstrap/install-fluent-bit.sh"
  etag   = filemd5("${path.module}/../../emr-bootstrap/install-fluent-bit.sh")
}

# --- IAM -------------------------------------------------------------------
resource "aws_iam_role" "emr_service" {
  name = "${local.name}-emr-service"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "elasticmapreduce.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

resource "aws_iam_role_policy" "emr_service_passrole" {
  name = "pass-instance-role"
  role = aws_iam_role.emr_service.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "iam:PassRole", Resource = aws_iam_role.emr_ec2.arn }]
  })
}

resource "aws_iam_role" "emr_ec2" {
  name = "${local.name}-emr-ec2"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_ssm" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "emr_ec2" {
  name = "emr-node-access"
  role = aws_iam_role.emr_ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject", "s3:GetBucketLocation"], Resource = [aws_s3_bucket.emr.arn, "${aws_s3_bucket.emr.arn}/*"] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"], Resource = ["arn:aws:s3:::*.elasticmapreduce", "arn:aws:s3:::*.elasticmapreduce/*", "arn:aws:s3:::elasticmapreduce", "arn:aws:s3:::elasticmapreduce/*"] },
      { Effect = "Allow", Action = ["ec2:DescribeInstances", "ec2:DescribeTags", "elasticmapreduce:ListBootstrapActions", "elasticmapreduce:DescribeCluster", "elasticmapreduce:ListInstanceGroups"], Resource = "*" },
      { Effect = "Allow", Action = ["kafka:DescribeCluster", "kafka:GetBootstrapBrokers"], Resource = "*" },
    ]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${local.name}-emr-ec2"
  role = aws_iam_role.emr_ec2.name
}

# --- Security groups (EMR in private subnets needs a service-access SG) -------
resource "aws_security_group" "emr_master" {
  name                   = "${local.name}-emr-master"
  description            = "EMR managed - primary node"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true
  tags                   = local.emr_tag
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { ignore_changes = [ingress, egress] } # EMR manages its own rules
}

resource "aws_security_group" "emr_core" {
  name                   = "${local.name}-emr-core"
  description            = "EMR managed - core nodes"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true
  tags                   = local.emr_tag
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { ignore_changes = [ingress, egress] }
}

resource "aws_security_group" "emr_service" {
  name                   = "${local.name}-emr-service-access"
  description            = "EMR managed - service access (private subnet)"
  vpc_id                 = module.vpc.vpc_id
  revoke_rules_on_delete = true
  tags                   = local.emr_tag
  ingress {
    description     = "EMR service from primary node"
    from_port       = 9443
    to_port         = 9443
    protocol        = "tcp"
    security_groups = [aws_security_group.emr_master.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle { ignore_changes = [ingress, egress] }
}

# --- Cluster -----------------------------------------------------------------
resource "aws_emr_cluster" "flink" {
  count         = var.enable_emr ? 1 : 0
  name          = "${local.name}-flink"
  release_label = var.emr_release
  applications  = ["Flink", "Hadoop"]
  service_role  = aws_iam_role.emr_service.arn
  log_uri       = "s3://${aws_s3_bucket.emr.id}/emr-logs/"

  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true

  ec2_attributes {
    subnet_id                         = module.vpc.private_subnets[0]
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    service_access_security_group     = aws_security_group.emr_service.id
  }

  master_instance_group {
    instance_type = var.emr_instance_type
  }

  core_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = 1
    ebs_config {
      size                 = 32
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  bootstrap_action {
    name = "fluent-bit-to-loki"
    path = "s3://${aws_s3_bucket.emr.id}/${aws_s3_object.fluent_bit_bootstrap.key}"
    args = [var.loki_push_url]
  }

  configurations_json = jsonencode([
    {
      Classification = "flink-conf"
      Properties = {
        "taskmanager.numberOfTaskSlots"         = "2"
        "state.backend.type"                    = "hashmap"
        "restart-strategy.type"                 = "fixed-delay"
        "restart-strategy.fixed-delay.attempts" = "10"
        "restart-strategy.fixed-delay.delay"    = "10 s"
      }
    }
  ])

  tags = local.emr_tag

  depends_on = [aws_iam_role_policy_attachment.emr_service, aws_iam_role_policy.emr_service_passrole]
}
