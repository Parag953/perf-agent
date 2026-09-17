#!/usr/bin/env bash
set -euo pipefail
HOST=${1:-andro-3@10.0.0.192}
cd "$(dirname "$0")"
python3 -m unittest discover -p 'test_*.py' >/dev/null
tar --exclude pool.db --exclude traces --exclude .git --exclude __pycache__ -cf - . | ssh "$HOST" 'mkdir -p ~/poold && tar -xf - -C ~/poold'
ssh "$HOST" 'mkdir -p ~/.config/systemd/user ~/poold/traces && cp ~/poold/poold.service ~/.config/systemd/user/ && systemctl --user daemon-reload && systemctl --user enable --now poold.service && systemctl --user restart poold.service && sleep 1 && systemctl --user is-active poold.service && curl -s localhost:7070/status | head -c 400'
echo
