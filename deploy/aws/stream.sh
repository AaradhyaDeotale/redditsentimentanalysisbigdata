#!/usr/bin/env bash
# Trigger a live producer replay on demand — your "go live" button for the demo.
# Streams the slice through the pipeline at a watchable pace so the dashboard
# visibly updates while you present.
#
#   ./stream.sh          # default pace (~2 min)
#   ./stream.sh 10       # slower / more dramatic
#   ./stream.sh 200      # faster
#
# Lower number = slower. Ctrl-C to stop early.
set -euo pipefail
cd "$(dirname "$0")"

SPEED="${1:-30}"
KEY="/tmp/bd-reddit-key"
cp "$PWD/bd-key" "$KEY"
chmod 600 "$KEY"
IP="$(terraform output -raw ec2_public_ip)"

echo "==> Streaming into the pipeline at REPLAY_SPEED=${SPEED}"
echo "==> Watch it live at http://${IP}:8000   (Ctrl-C to stop)"
echo ""
ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 \
  ec2-user@"$IP" \
  "cd /home/ec2-user/app && docker compose -f docker-compose.cloud.yml run --rm --no-deps -e REPLAY_SPEED=${SPEED} -e LIVE_TIMESTAMPS=true producer"
