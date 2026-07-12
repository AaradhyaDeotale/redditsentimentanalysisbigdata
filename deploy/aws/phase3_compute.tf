# ---------------------------------------------------------------------------
# Phase 3 — the compute box. One EC2 instance runs the app containers
# (producer, Flink JobManager+TaskManager, dashboard) via docker-compose,
# pointed at the managed MSK / Redis / S3. Gated by run_pipeline so it tears
# down with the rest when you toggle off.
# ---------------------------------------------------------------------------

# Latest Amazon Linux 2023 image, looked up automatically (no hardcoded AMI id).
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# Register the SSH public key we generated locally.
resource "aws_key_pair" "main" {
  key_name   = "${local.name}-key"
  public_key = file("${path.module}/bd-key.pub")
}

# --- Firewall for the box: SSH + dashboard + Flink UI, locked to your IP ---
resource "aws_security_group" "ec2" {
  name        = "${local.name}-ec2"
  description = "App compute box"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "Dashboard"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  ingress {
    description = "Flink web UI"
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-ec2" }
}

# --- IAM: let the box use SSM (keyless shell) and read/write the S3 bucket ---
resource "aws_iam_role" "ec2" {
  name = "${local.name}-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "s3" {
  name = "${local.name}-s3"
  role = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${local.name}-ec2"
  role = aws_iam_role.ec2.name
}

# --- The instance ---
resource "aws_instance" "app" {
  count = var.run_pipeline ? 1 : 0

  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  key_name                    = aws_key_pair.main.key_name
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true

  root_block_device {
    volume_size = 30 # GB — room for Docker images
    volume_type = "gp3"
  }

  # Install Docker + the compose plugin on first boot.
  user_data = <<-EOF
    #!/bin/bash
    set -eux
    dnf install -y docker
    systemctl enable --now docker
    usermod -aG docker ec2-user
    mkdir -p /usr/libexec/docker/cli-plugins
    curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
      -o /usr/libexec/docker/cli-plugins/docker-compose
    chmod +x /usr/libexec/docker/cli-plugins/docker-compose
    BUILDX_VER=$(curl -s https://api.github.com/repos/docker/buildx/releases/latest | grep '"tag_name":' | head -1 | cut -d'"' -f4)
    curl -SL "https://github.com/docker/buildx/releases/download/$${BUILDX_VER}/buildx-$${BUILDX_VER}.linux-amd64" \
      -o /usr/libexec/docker/cli-plugins/docker-buildx
    chmod +x /usr/libexec/docker/cli-plugins/docker-buildx
    echo "READY" > /home/ec2-user/BOOTSTRAP_DONE
  EOF

  tags = { Name = "${local.name}-app" }
}

output "ec2_public_ip" {
  value = try(aws_instance.app[0].public_ip, "(pipeline off)")
}

output "ssh_command" {
  description = "Run from the deploy/aws folder"
  value       = try("ssh -i bd-key ec2-user@${aws_instance.app[0].public_ip}", "(pipeline off)")
}

output "dashboard_url" {
  value = try("http://${aws_instance.app[0].public_ip}:8000", "(pipeline off)")
}
