variable "project" {
  description = "Short name prefix applied to every resource"
  type        = string
  default     = "bd-reddit"
}

variable "region" {
  description = "AWS region everything is deployed into"
  type        = string
  default     = "eu-central-1"
}

variable "alert_email" {
  description = "Email address that receives the budget alerts"
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly spend ceiling; alerts fire at 50% and 90% of this"
  type        = number
  default     = 20
}

variable "vpc_cidr" {
  description = "Private IP range for the whole VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# The on/off switch. true  = expensive machines (MSK, Redis, later Fargate) run.
#                    false = they are torn down; the free foundation stays.
# Turn off at the end of every session:  terraform apply -var="run_pipeline=false"
# Turn on for the demo:                  terraform apply -var="run_pipeline=true"
variable "run_pipeline" {
  description = "Whether the billable pipeline machines are running"
  type        = bool
  default     = true
}

variable "kafka_version" {
  description = "MSK Kafka version"
  type        = string
  default     = "3.6.0"
}

variable "instance_type" {
  description = "EC2 size that runs the app containers"
  type        = string
  default     = "t3.xlarge" # 4 vCPU / 16 GB — comfortable for Flink + dashboard + builds
}

variable "allowed_cidr" {
  description = "IP range allowed to reach SSH (22) and the dashboard (8000). Your IP by default; widen to 0.0.0.0/0 on demo day if others must view it."
  type        = string
  default     = "0.0.0.0/0"
}
