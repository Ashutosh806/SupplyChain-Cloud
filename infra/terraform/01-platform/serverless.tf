############################################
# Async messaging: SQS queue (+ DLQ) feeding the Notification Lambda, SNS for fan-out
############################################
resource "aws_sqs_queue" "notifications_dlq" {
  name                      = "${local.name}-notifications-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "notifications" {
  name                       = "${local.name}-notifications"
  visibility_timeout_seconds = 60 # >= 6x Lambda timeout
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 10
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.notifications_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-shipment-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count         = var.alert_email == "" ? 0 : 1
  topic_arn     = aws_sns_topic.alerts.arn
  protocol      = "email"
  endpoint      = var.alert_email
  filter_policy = jsonencode({ severity = ["HIGH"] })
}

############################################
# Notification Service: AWS Lambda (serverless, event-driven)
############################################
data "archive_file" "notification" {
  type        = "zip"
  source_dir  = "${path.module}/../../../services/notification-lambda"
  output_path = "${path.module}/.build/notification-lambda.zip"
}

resource "aws_iam_role" "notification" {
  name = "${local.name}-notification-lambda"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "notification_logs" {
  role       = aws_iam_role.notification.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "notification" {
  name = "notification-access"
  role = aws_iam_role.notification.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"], Resource = aws_sqs_queue.notifications.arn },
      { Effect = "Allow", Action = ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:GetItem"], Resource = [aws_dynamodb_table.notifications.arn, "${aws_dynamodb_table.notifications.arn}/index/*"] },
      { Effect = "Allow", Action = "sns:Publish", Resource = aws_sns_topic.alerts.arn },
      { Effect = "Allow", Action = "s3:GetObject", Resource = "${aws_s3_bucket.data.arn}/pod/*" },
    ]
  })
}

resource "aws_cloudwatch_log_group" "notification" {
  name              = "/aws/lambda/${local.name}-notification-service"
  retention_in_days = 7
}

resource "aws_lambda_function" "notification" {
  function_name    = "${local.name}-notification-service"
  role             = aws_iam_role.notification.arn
  runtime          = "python3.13"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.notification.output_path
  source_code_hash = data.archive_file.notification.output_base64sha256
  timeout          = 10
  memory_size      = 256

  environment {
    variables = {
      NOTIFICATIONS_TABLE = aws_dynamodb_table.notifications.name
      SNS_TOPIC_ARN       = aws_sns_topic.alerts.arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.notification]
}

# Trigger 1: SQS (shipment status notifications)
resource "aws_lambda_event_source_mapping" "sqs" {
  event_source_arn                   = aws_sqs_queue.notifications.arn
  function_name                      = aws_lambda_function.notification.arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 1
  function_response_types            = ["ReportBatchItemFailures"]
  depends_on                         = [aws_iam_role_policy.notification] # avoid IAM propagation race
}

# Trigger 2: S3 uploads of proof-of-delivery documents
resource "aws_lambda_permission" "s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notification.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

resource "aws_s3_bucket_notification" "pod_uploads" {
  bucket = aws_s3_bucket.data.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.notification.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "pod/"
  }
  depends_on = [aws_lambda_permission.s3]
}

# Trigger 3: public HTTPS Function URL (read-only notification feed for the UI)
resource "aws_lambda_function_url" "notification" {
  function_name      = aws_lambda_function.notification.function_name
  authorization_type = "NONE"
  cors {
    allow_origins = ["*"]
    allow_methods = ["GET"]
    max_age       = 300
  }
}

resource "aws_lambda_permission" "url_public" {
  statement_id           = "FunctionURLAllowPublicAccess"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.notification.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "url_invoke" {
  statement_id             = "FunctionURLInvokeAllowPublicAccess"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.notification.function_name
  principal                = "*"
  invoked_via_function_url = true
}
