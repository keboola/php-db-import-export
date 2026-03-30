resource "aws_s3_bucket" "files" {
  bucket        = "${local.service_name}-files"
  force_destroy = true

  tags = {
    Name = "${local.service_name}-files"
  }
}

resource "aws_s3_bucket_cors_configuration" "files" {
  bucket = aws_s3_bucket.files.bucket

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST", "DELETE"]
    allowed_origins = ["*"]
    max_age_seconds = 3600
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "files" {
  bucket = aws_s3_bucket.files.bucket

  rule {
    id     = "Delete after 2 days"
    status = "Enabled"
    filter {
      prefix = "exp-2"
    }
    expiration {
      days = 2
    }
  }

  rule {
    id     = "Delete incomplete multipart uploads"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_iam_user" "s3" {
  name = "${local.service_name}-s3"
  path = "/"
}

resource "aws_iam_access_key" "s3" {
  user = aws_iam_user.s3.name
}

data "aws_iam_policy_document" "s3_access" {
  statement {
    effect    = "Allow"
    actions   = ["s3:*"]
    resources = ["${aws_s3_bucket.files.arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.files.arn]
  }
}

data "aws_iam_policy_document" "sts_access" {
  statement {
    effect    = "Allow"
    actions   = ["sts:GetFederationToken"]
    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "s3_access" {
  name   = "S3Access"
  user   = aws_iam_user.s3.name
  policy = data.aws_iam_policy_document.s3_access.json
}

resource "aws_iam_user_policy" "sts_access" {
  name   = "STSAccess"
  user   = aws_iam_user.s3.name
  policy = data.aws_iam_policy_document.sts_access.json
}
