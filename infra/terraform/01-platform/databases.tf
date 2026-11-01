############################################
# Storage product 1: Amazon RDS for PostgreSQL (relational: suppliers, shipments,
# inventory, carrier events, transactional outbox tables - schema per service)
############################################
resource "random_password" "db" {
  length  = 24
  special = false # keeps the libpq connection string trivially safe
}

resource "aws_db_subnet_group" "this" {
  name       = "${local.name}-db"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_db_instance" "postgres" {
  identifier              = "${local.name}-postgres"
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = var.db_instance_class
  allocated_storage       = 20
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = "supplychain"
  username                = "supplychain"
  password                = random_password.db.result
  db_subnet_group_name    = aws_db_subnet_group.this.name
  vpc_security_group_ids  = [aws_security_group.data_stores.id]
  publicly_accessible     = false
  multi_az                = false
  backup_retention_period = 1
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true
}

############################################
# Storage product 2: Amazon DynamoDB (NoSQL, high-throughput semi-structured data)
############################################
# Notifications written by the Notification Lambda (idempotent on notification_id)
resource "aws_dynamodb_table" "notifications" {
  name         = "${local.name}-notifications"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "tracking_id"
  range_key    = "notification_id"

  attribute {
    name = "tracking_id"
    type = "S"
  }
  attribute {
    name = "notification_id"
    type = "S"
  }
  attribute {
    name = "created_at"
    type = "N"
  }
  attribute {
    name = "feed"
    type = "S"
  }

  local_secondary_index {
    name            = "by-time"
    range_key       = "created_at"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "feed-index"
    hash_key        = "feed"
    range_key       = "created_at"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Real-time analytics results produced by Flink (via the analytics-api consumer)
resource "aws_dynamodb_table" "analytics" {
  name         = "${local.name}-shipment-analytics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "N"
  }
}

############################################
# Storage product 3: Amazon S3 (object store: raw carrier-event archive + proof-of-delivery files)
############################################
resource "aws_s3_bucket" "data" {
  bucket        = "${local.name}-data-${local.account_id}-${var.region}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_cors_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  cors_rule {
    allowed_methods = ["PUT", "GET"]
    allowed_origins = ["*"]
    allowed_headers = ["*"]
    max_age_seconds = 3000
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "expire-raw-events"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = 90
    }
  }
}

############################################
# Kafka: Amazon MSK (managed Apache Kafka)
############################################
resource "aws_msk_configuration" "this" {
  name              = "${local.name}-kafka-config"
  kafka_versions    = ["3.9.x"]
  server_properties = <<-EOT
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=6
    log.retention.hours=72
  EOT
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${local.name}-msk"
  kafka_version          = "3.9.x"
  number_of_broker_nodes = 2 # one per AZ

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.data_stores.id]
    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  client_authentication {
    unauthenticated = true # network-isolated (private subnets + SG); see design doc
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }
}
