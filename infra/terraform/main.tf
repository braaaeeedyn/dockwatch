# DockWatch AWS base: storage, catalog, query, alerts, budgets.
# Nothing here runs compute that costs money while idle; heavy services run locally in Docker.

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  # Remote state: create the state bucket once by hand (or with -backend=false first), then uncomment.
  # backend "s3" {
  #   bucket       = "dockwatch-tfstate-<account-id>"
  #   key          = "dockwatch/terraform.tfstate"
  #   region       = "us-west-2"
  #   use_lockfile = true
  # }
}

provider "aws" {
  region = var.region
  default_tags { tags = { project = "dockwatch", managed_by = "terraform" } }
}

data "aws_caller_identity" "me" {}

locals {
  bucket = "dockwatch-${data.aws_caller_identity.me.account_id}"
}

# ---------- S3: one bucket, prefixes raw/ warehouse/ checkpoints/ athena-results/ ----------
resource "aws_s3_bucket" "lake" {
  bucket = local.bucket
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "raw-to-ia-then-expire"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration { days = var.raw_retention_days }
  }

  rule {
    id     = "athena-results-expire"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 2 }
  }
}

# ---------- Glue Data Catalog + Athena ----------
resource "aws_glue_catalog_database" "dbs" {
  for_each = toset(["bronze", "silver", "ops", "marts", "metrics"])
  name     = "dockwatch_${each.key}"
}

resource "aws_athena_workgroup" "dockwatch" {
  name          = "dockwatch"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_cutoff
    publish_cloudwatch_metrics_enabled = true
    engine_version { selected_engine_version = "Athena engine version 3" }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}

# ---------- Alerts ----------
resource "aws_sns_topic" "alerts" {
  name = "dockwatch-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---------- Budgets: $1 and $5 monthly, as code ----------
resource "aws_budgets_budget" "monthly" {
  for_each     = toset(["1", "5"])
  name         = "dockwatch-${each.key}-usd"
  budget_type  = "COST"
  limit_amount = each.key
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = var.budget_emails
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.budget_emails
  }
}
