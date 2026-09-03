# S3 bucket for the lakehouse: Delta table storage + streaming checkpoints + source seed files.
#
# Layout convention used by the pipeline (see src/retail_lakehouse/config.py):
#   s3://<bucket>/data/tables/...        Delta table data (managed by Unity Catalog external location)
#   s3://<bucket>/data/checkpoints/...   Structured Streaming checkpoints (never delete casually)
#   s3://<bucket>/data/source/...        Batch seed/source files

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: expire old checkpoint object versions after 30 days to control storage cost.
# Never expire current versions of table data files -- Delta's VACUUM handles that deliberately.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-old-checkpoint-versions"
    status = "Enabled"
    filter {
      prefix = "data/checkpoints/"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

data "aws_caller_identity" "current" {}

output "lakehouse_bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}
