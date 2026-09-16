#!/usr/bin/env bash
# Deploy app.py + unit to the agent host and restart the service.
# Usage: ./deploy.sh
set -euo pipefail

HOST="${AGENT_HOST:-ec2-user@44.251.166.76}"
KEY="${AGENT_KEY:-$HOME/.ssh/abhi-dev02-c7g.pem}"
SSH="ssh -i $KEY -o BatchMode=yes $HOST"
SCP="scp -i $KEY"

echo ">> copying app.py + mcp.json + unit"
$SCP app.py            "$HOST:/tmp/app.py"
$SCP mcp.json          "$HOST:/tmp/mcp.json"
$SCP systemd/agent.service "$HOST:/tmp/agent.service"

echo ">> installing and restarting"
$SSH '
  sudo cp /tmp/app.py /opt/agent/app.py
  sudo chown agent:agent /opt/agent/app.py
  sudo chmod 755 /opt/agent/app.py
  sudo cp /tmp/mcp.json /opt/agent/mcp.json
  sudo cp /tmp/agent.service /etc/systemd/system/agent.service
  sudo systemctl daemon-reload
  sudo systemctl restart agent
  sleep 3
  sudo systemctl is-active agent
'
echo ">> done"
