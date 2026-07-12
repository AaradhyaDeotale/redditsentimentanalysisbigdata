# Values other phases (and you) will need. After `apply`, see them anytime with
# `terraform output`.

output "s3_bucket" {
  description = "Bucket holding the data slice and model artifacts"
  value       = aws_s3_bucket.artifacts.bucket
}

output "vpc_id" {
  value = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Subnets MSK, ElastiCache and Fargate will launch into"
  value       = aws_subnet.public[*].id
}
