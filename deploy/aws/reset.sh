#!/usr/bin/env bash
# Wipe the Kafka topics for a clean slate, then restart the pipeline fresh.
# Useful before a demo rehearsal when the cluster has accumulated data from
# earlier runs. NOT needed on demo day itself — a fresh `terraform apply`
# gives you a brand-new empty MSK.
set -euo pipefail
cd "$(dirname "$0")"

KEY="/tmp/bd-reddit-key"
cp "$PWD/bd-key" "$KEY"
chmod 600 "$KEY"
IP="$(terraform output -raw ec2_public_ip)"
SSH_OPTS="-i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15"
REMOTE="ec2-user@$IP"

echo "==> Stopping pipeline ..."
ssh $SSH_OPTS "$REMOTE" 'cd /home/ec2-user/app && docker compose -f docker-compose.cloud.yml down --remove-orphans'

echo "==> Deleting Kafka topics (clean slate) ..."
ssh $SSH_OPTS "$REMOTE" 'bash -s' <<'REMOTE_SCRIPT'
cd /home/ec2-user/app
set -a; source .env; set +a
docker run --rm --entrypoint python -e KB="$KAFKA_BROKER" reddit-producer:latest -c '
from confluent_kafka.admin import AdminClient
import os
a = AdminClient({"bootstrap.servers": os.environ["KB"]})
tops = ["reddit-comments","reddit-comments-cleaned","reddit-comments-malformed","sentiment-results"]
for t, f in a.delete_topics(tops, operation_timeout=30).items():
    try:
        f.result(); print("deleted", t)
    except Exception as e:
        print("skip", t, ":", e)
'
REMOTE_SCRIPT

echo "==> Letting deletions settle ..."
ssh $SSH_OPTS "$REMOTE" 'sleep 8'

echo "==> Restarting pipeline on fresh, empty topics ..."
ssh $SSH_OPTS "$REMOTE" 'cd /home/ec2-user/app && docker compose -f docker-compose.cloud.yml up -d'

echo ""
echo "==> Done. Fresh pipeline coming up — producer streams live over ~3 min."
echo "    Dashboard: http://$IP:8000"
