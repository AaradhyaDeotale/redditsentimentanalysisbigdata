# ---------------------------------------------------------------------------
# Phase 2 — the managed "big data" services: Kafka (MSK) and Redis (ElastiCache).
# These are the first things that cost money, so the cluster resources are gated
# behind var.run_pipeline. The free scaffolding (config, subnet group) stays put
# so toggling on/off is fast and clean.
# ---------------------------------------------------------------------------

# ---------- Managed Kafka (MSK) ----------

# Cluster settings mirroring the local 3-broker docker-compose: auto-create
# topics, replication factor 3, keep records forever (2019 replay). Free to hold.
resource "aws_msk_configuration" "this" {
  name           = "${local.name}-config"
  kafka_versions = [var.kafka_version]

  server_properties = <<-EOT
    auto.create.topics.enable=true
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=3
    log.retention.ms=-1
  EOT
}

resource "aws_msk_cluster" "this" {
  count = var.run_pipeline ? 1 : 0

  cluster_name           = "${local.name}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 3 # one broker per subnet / AZ

  broker_node_group_info {
    instance_type   = "kafka.t3.small" # cheapest MSK broker
    client_subnets  = aws_subnet.public[*].id
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 10 # GB per broker — plenty for the data slice
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  # Plaintext (no TLS/auth) — matches the local app config and keeps the Kafka
  # clients unchanged. Safe because access is restricted to inside the VPC.
  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = true
    }
  }
}

# ---------- Managed Redis (ElastiCache) ----------

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${local.name}-redis"
  subnet_ids = aws_subnet.public[*].id
}

resource "aws_elasticache_cluster" "redis" {
  count = var.run_pipeline ? 1 : 0

  cluster_id           = "${local.name}-redis"
  engine               = "redis"
  node_type            = "cache.t3.micro" # cheapest cache node
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.redis.name
  security_group_ids   = [aws_security_group.redis.id]
}

# ---------- Connection details the app (and you) will need ----------

output "kafka_bootstrap_brokers" {
  description = "Kafka bootstrap string for KAFKA_BROKER (plaintext)"
  value       = try(aws_msk_cluster.this[0].bootstrap_brokers, "(pipeline off)")
}

output "redis_endpoint" {
  description = "Redis host for REDIS_URL"
  value       = try(aws_elasticache_cluster.redis[0].cache_nodes[0].address, "(pipeline off)")
}
