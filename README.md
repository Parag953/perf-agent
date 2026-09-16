# perf-agent

Autonomous performance-triage agent. A Datadog alert fires → the agent investigates
the affected Voyager service like a developer (pprof profiles from S3, source code,
Datadog metrics), asks a human for DB query results via Slack when needed, and opens
a **draft PR** if it lands on a concrete fix.

## Architecture

```
Datadog monitor (@webhook-perf-agent)
        │  POST /webhook  (:8080)
        ▼
   app.py  (systemd service `agent`, runs as user `agent`)
        │  spawns:  claude -p  (headless, subscription OAuth)
        ▼
   Claude investigates — fetches pprof from S3 only if needed, reads
   /opt/agent/voyager source, queries Datadog via MCP
        │
   ┌────┴─────────────────────────┐
   │ needs DB data                │ has a fix
   ▼                              ▼
 posts SQL/Cypher to Slack     opens draft PR, posts link to Slack
 thread, pauses                 (human reviews)
   │
 human replies in thread → claude --resume → completes
```

## Host layout (EC2, Amazon Linux 2023)

| Path | What |
|---|---|
| `/opt/agent/app.py` | the service (this repo's `app.py`) |
| `/opt/agent/.env` | secrets, root-owned 0600 — **never committed** (see `.env.example`) |
| `/opt/agent/mcp.json` | Datadog MCP config (`${DD_API_KEY}` placeholders) |
| `/opt/agent/venv` | Python venv (`flask`, `slack_bolt`, `boto3`) |
| `/opt/agent/voyager` | shallow clone of `andromedasec/voyager` (source context) |
| `/etc/systemd/system/agent.service` | the unit (this repo's `systemd/agent.service`) |

Runtime deps on the host: Node 22 + `claude` on PATH, `gh`, `aws`, `go`, `git`.

## Deploy

```bash
./deploy.sh          # scp app.py + mcp.json + unit, restart, show status
```

Override host/key with `AGENT_HOST` / `AGENT_KEY` env vars.

## Notes

- Slack uses **Socket Mode** (no inbound port). Requires `message.channels` event
  subscription and the bot invited to the target channel (`C0C1SH00CP9`).
- Only one process may hold the Slack app token's socket at a time.
- AWS creds in `.env` are short-lived STS tokens — refresh when they expire
  (`ExpiredToken` in logs), or attach an instance role.
- `pending_sessions` is in-memory: a restart drops any paused analyses.
