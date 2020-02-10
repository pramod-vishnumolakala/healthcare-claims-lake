# ============================================================
# Healthcare Claims Data Lake — Terraform Infrastructure (AWS)
# Author: Pramod Vishnumolakala
# ============================================================

terraform {
  required_version = ">= 1.4"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket = "pramod-terraform-state"
    key    = "healthcare-claims/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" { region = "us-east-1" }

variable "environment"  { default = "production" }
variable "project_name" { default = "healthcare-claims" }


# ── KMS — HIPAA encryption key ────────────────────────────────────────
resource "aws_kms_key" "hipaa" {
  description             = "Healthcare Claims Lake — HIPAA encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
  tags = { Project = var.project_name, Compliance = "HIPAA" }
}

resource "aws_kms_alias" "hipaa" {
  name          = "alias/healthcare-hipaa-key"
  target_key_id = aws_kms_key.hipaa.key_id
}


# ── S3 buckets — raw, curated, redshift temp ──────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${var.environment}"
  tags   = { Compliance = "HIPAA", DataClass = "PHI" }
}

resource "aws_s3_bucket" "curated" {
  bucket = "${var.project_name}-curated-${var.environment}"
  tags   = { Compliance = "HIPAA", DataClass = "PHI-MASKED" }
}

resource "aws_s3_bucket" "redshift_temp" {
  bucket = "${var.project_name}-redshift-temp-${var.environment}"
}

# Block all public access — HIPAA requirement
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# KMS encryption on all buckets
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.hipaa.arn
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.hipaa.arn
    }
  }
}

# S3 lifecycle — move to Glacier after 2 years (22% storage cost reduction)
resource "aws_s3_bucket_lifecycle_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    id     = "archive-to-glacier"
    status = "Enabled"
    transition {
      days          = 730
      storage_class = "GLACIER"
    }
    expiration {
      days = 2555   # 7 year HIPAA retention
    }
  }
}


# ── CloudTrail — HIPAA audit logging ──────────────────────────────────
resource "aws_cloudtrail" "hipaa_audit" {
  name                          = "${var.project_name}-audit-trail"
  s3_bucket_name                = aws_s3_bucket.raw.id
  include_global_service_events = true
  is_multi_region_trail         = true
  enable_log_file_validation    = true
  kms_key_id                    = aws_kms_key.hipaa.arn
  tags = { Compliance = "HIPAA" }
}


# ── Redshift ──────────────────────────────────────────────────────────
resource "aws_redshift_cluster" "healthcare_dw" {
  cluster_identifier = "${var.project_name}-dw"
  database_name      = "healthdb"
  master_username    = "admin"
  master_password    = var.redshift_password
  node_type          = "ra3.4xlarge"
  number_of_nodes    = 2
  encrypted          = true
  kms_key_id         = aws_kms_key.hipaa.arn
  tags               = { Compliance = "HIPAA" }
}

variable "redshift_password" {
  sensitive = true
}


# ── SNS — pipeline alerts ─────────────────────────────────────────────
resource "aws_sns_topic" "pipeline_alerts" {
  name              = "${var.project_name}-alerts"
  kms_master_key_id = aws_kms_key.hipaa.arn
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = "pramodvishnumolakala@gmail.com"
}


# ── Outputs ───────────────────────────────────────────────────────────
output "raw_bucket"        { value = aws_s3_bucket.raw.bucket }
output "curated_bucket"    { value = aws_s3_bucket.curated.bucket }
output "kms_key_alias"     { value = aws_kms_alias.hipaa.name }
output "redshift_endpoint" { value = aws_redshift_cluster.healthcare_dw.endpoint }
output "cloudtrail_name"   { value = aws_cloudtrail.hipaa_audit.name }
