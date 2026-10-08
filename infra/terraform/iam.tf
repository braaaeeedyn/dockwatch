# Least-privilege roles: Spark writer, Athena reader, alert Lambda, and GitHub Actions via OIDC (no stored keys).

locals {
  lake_arn     = aws_s3_bucket.lake.arn
  glue_db_arns = [for db in aws_glue_catalog_database.dbs : "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:database/${db.name}"]
  glue_tbl_arns = [for db in aws_glue_catalog_database.dbs :
  "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:table/${db.name}/*"]
  glue_catalog_arn = "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:catalog"
}

# ---------- Spark writer (assumed by the local machine's admin user, or a CI role) ----------
data "aws_iam_policy_document" "assume_from_account" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.me.account_id}:root"]
    }
  }
}

data "aws_iam_policy_document" "spark_writer" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [local.lake_arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.lake_arn}/raw/*", "${local.lake_arn}/warehouse/*", "${local.lake_arn}/checkpoints/*"]
  }
  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions",
    ]
    resources = concat([local.glue_catalog_arn], local.glue_db_arns, local.glue_tbl_arns)
  }
}

resource "aws_iam_role" "spark_writer" {
  name               = "dockwatch-spark-writer"
  assume_role_policy = data.aws_iam_policy_document.assume_from_account.json
}

resource "aws_iam_role_policy" "spark_writer" {
  role   = aws_iam_role.spark_writer.id
  policy = data.aws_iam_policy_document.spark_writer.json
}

# ---------- Athena reader (dbt, exports, analysts) ----------
data "aws_iam_policy_document" "athena_reader" {
  statement {
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.dockwatch.arn]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.lake_arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${local.lake_arn}/warehouse/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${local.lake_arn}/athena-results/*"]
  }
  statement {
    actions   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = concat([local.glue_catalog_arn], local.glue_db_arns, local.glue_tbl_arns)
  }
}

resource "aws_iam_role" "athena_reader" {
  name               = "dockwatch-athena-reader"
  assume_role_policy = data.aws_iam_policy_document.assume_from_account.json
}

resource "aws_iam_role_policy" "athena_reader" {
  role   = aws_iam_role.athena_reader.id
  policy = data.aws_iam_policy_document.athena_reader.json
}

# ---------- Alert Lambda: publish to SNS and write logs, nothing else ----------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "alert_lambda" {
  name               = "dockwatch-alert-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "alert_lambda_logs" {
  role       = aws_iam_role.alert_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "alert_lambda_sns" {
  role = aws_iam_role.alert_lambda.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sns:Publish", Resource = aws_sns_topic.alerts.arn }]
  })
}

# ---------- GitHub Actions via OIDC ----------
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "github_ci" {
  name               = "dockwatch-github-ci"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

# CI runs `terraform plan` (read-only) and deploys the static site to the web/ prefix.
resource "aws_iam_role_policy_attachment" "github_ci_readonly" {
  role       = aws_iam_role.github_ci.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}
