# ---------------------------------------------------------------------------
# Phase 1 — Foundation: the free skeleton everything else sits inside.
#   * a monthly budget alarm  (safety, free)
#   * an S3 bucket            (holds the data slice + trained model files)
#   * a VPC with 3 public subnets across 3 AZs (the private network)
# No compute, no managed data services yet — so this whole phase costs ~$0.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name       = var.project
  account_id = data.aws_caller_identity.current.account_id
  # 3 AZs -> lets MSK run a 3-broker cluster (replication factor 3), mirroring
  # the local 3-broker docker-compose setup exactly.
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

# ---------- Safety: monthly cost alarm ----------
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Track GROSS usage (before credits). Otherwise, while the $100 credit covers
  # everything, the budget reads $0 and never warns you — until credits run out
  # and real charges suddenly appear. Measuring gross means the alerts reflect
  # how fast you are actually consuming services.
  cost_types {
    include_credit = false
    include_refund = false
  }

  # Email at 50% of the budget ($10 of $20)...
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  # ...and again at 90% ($18 of $20) as a last warning.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 90
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}

# ---------- Storage: data slice + versioned model artifacts ----------
resource "aws_s3_bucket" "artifacts" {
  # Bucket names are globally unique; the account id guarantees uniqueness.
  bucket = "${local.name}-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------- Network: one VPC, 3 public subnets ----------
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${local.name}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${local.name}-igw" }
}

# Public subnets: resources here get public IPs and reach the internet directly
# through the internet gateway. This deliberately AVOIDS a NAT gateway (~$32/mo)
# — access is instead locked down by security groups in later phases.
resource "aws_subnet" "public" {
  count                   = length(local.azs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name}-public-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "${local.name}-public-rt" }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}
