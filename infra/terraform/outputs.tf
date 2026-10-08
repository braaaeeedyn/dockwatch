output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "athena_workgroup" {
  value = aws_athena_workgroup.dockwatch.name
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "roles" {
  value = {
    spark_writer  = aws_iam_role.spark_writer.arn
    athena_reader = aws_iam_role.athena_reader.arn
    alert_lambda  = aws_iam_role.alert_lambda.arn
    github_ci     = aws_iam_role.github_ci.arn
  }
}
