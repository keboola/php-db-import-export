output "AWS_REGION" {
  value = var.aws_region
}

output "AWS_ACCESS_KEY_ID" {
  value = aws_iam_access_key.s3.id
}

output "AWS_SECRET_ACCESS_KEY" {
  value     = aws_iam_access_key.s3.secret
  sensitive = true
}

output "AWS_S3_BUCKET" {
  value = aws_s3_bucket.files.bucket
}
