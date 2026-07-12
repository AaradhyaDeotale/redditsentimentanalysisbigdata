#!/usr/bin/env bash
# Push the project to the EC2 box and start the pipeline against managed
# MSK / Redis. Re-runnable: safe to run again after code changes.
#
#   ./deploy-to-ec2.sh
#
set -euo pipefail
cd "$(dirname "$0")" # deploy/aws
PROJECT_ROOT="$(cd ../.. && pwd)"

# The project path contains spaces, which break unquoted `ssh -i` arguments.
# Copy the key to a space-free path and use that everywhere.
KEY="/tmp/bd-reddit-key"
cp "$PWD/bd-key" "$KEY"
chmod 600 "$KEY"

IP="$(terraform output -raw ec2_public_ip)"
KAFKA="$(terraform output -raw kafka_bootstrap_brokers)"
REDIS_HOST="$(terraform output -raw redis_endpoint)"

SSH_OPTS="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15"
REMOTE="ec2-user@$IP"

echo "==> EC2: $IP"
echo "==> Waiting for Docker to finish installing on the box ..."
until ssh $SSH_OPTS "$REMOTE" 'test -f /home/ec2-user/BOOTSTRAP_DONE && docker ps >/dev/null 2>&1'; do
  echo "    ...not ready yet, retrying in 10s"
  sleep 10
done
echo "==> Docker is ready."

echo "==> Syncing project (excluding the 15GB dump, venvs, node_modules) ..."
rsync -az --delete -e "ssh $SSH_OPTS" \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude 'node_modules' \
  --exclude '.worktrees' \
  --exclude 'RC_2019-04.zst' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude 'deploy/aws' \
  "$PROJECT_ROOT/" "$REMOTE:/home/ec2-user/app/"

echo "==> Uploading cloud compose + writing .env from Terraform outputs ..."
scp $SSH_OPTS docker-compose.cloud.yml "$REMOTE:/home/ec2-user/app/docker-compose.cloud.yml"
ssh $SSH_OPTS "$REMOTE" "cat > /home/ec2-user/app/.env" <<EOF
KAFKA_BROKER=$KAFKA
REDIS_URL=redis://$REDIS_HOST:6379/0
EOF

echo "==> Building images and starting the pipeline (first build takes a few min) ..."
# `down` first so a redeploy restarts the Flink job cleanly (one job, new config)
# instead of submitting a duplicate alongside the old one. No-op on a fresh box.
ssh $SSH_OPTS "$REMOTE" 'cd /home/ec2-user/app && docker compose -f docker-compose.cloud.yml down --remove-orphans; docker compose -f docker-compose.cloud.yml up -d --build'

echo ""
echo "==================================================================="
echo "  Dashboard : http://$IP:8000"
echo "  Flink UI  : http://$IP:8081"
echo "==================================================================="
