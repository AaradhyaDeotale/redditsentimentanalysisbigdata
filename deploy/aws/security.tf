# ---------------------------------------------------------------------------
# Firewalls (security groups). These are free and always present — only the
# machines they protect are gated by the run_pipeline switch. Access is limited
# to traffic from inside the VPC, so nothing here is reachable from the public
# internet even though the subnets are public.
# ---------------------------------------------------------------------------

# --- Kafka brokers (MSK) ---
resource "aws_security_group" "msk" {
  name        = "${local.name}-msk"
  description = "MSK Kafka brokers"
  vpc_id      = aws_vpc.main.id

  # Clients (producer, Flink, dashboard) reach Kafka on 9092 from within the VPC
  ingress {
    description = "Kafka plaintext from within the VPC"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Brokers talk to each other on every port — allow traffic from this SG itself
  ingress {
    description = "Broker-to-broker"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-msk" }
}

# --- Redis (ElastiCache) ---
resource "aws_security_group" "redis" {
  name        = "${local.name}-redis"
  description = "ElastiCache Redis"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Redis from within the VPC"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-redis" }
}
