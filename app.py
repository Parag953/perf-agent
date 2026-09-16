#!/opt/agent/venv/bin/python3
"""
Agent service — Datadog alert → pprof analysis → Slack thread loop.

Flow:
  1. Startup: clone/pull andromedasec/voyager to /opt/agent/voyager
  2. Datadog webhook POST /webhook → start analysis thread
  3. claude -p is briefed on the alert + the resources it CAN reach (local source tree,
     pprof profiles in S3 via aws/go, Datadog MCP, DB-via-Slack). It investigates like a
     developer and fetches only what it needs (e.g. pulls a pprof only if warranted).
  4a. Claude needs DB data → outputs NEED_QUERIES block (PostgreSQL + Neo4j Cypher)
      → posted as a single Slack code-block message; session paused
  4b. Claude has full answer → posts analysis to Slack thread; done
  5. Human runs queries, replies in thread with results
  6. claude --resume → completes analysis, posts to Slack
"""

import json
import logging
import os
import re
import subprocess
import threading
from typing import Optional, Tuple

from flask import Flask, request, jsonify
from slack_bolt import App as SlackApp
from slack_bolt.adapter.socket_mode import SocketModeHandler

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SLACK_CHANNEL  = "C0C1SH00CP9"
MCP_CONFIG     = "/opt/agent/mcp.json"
VOYAGER_PATH   = "/opt/agent/voyager"
VOYAGER_REPO   = "andromedasec/voyager"
S3_BUCKET      = "as-live-heap-dump"
WEBHOOK_PORT   = 8080
CLAUDE_TIMEOUT = 900          # seconds per claude invocation (it fetches + explores now)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared state  {thread_ts -> claude session_id}
# ---------------------------------------------------------------------------
_lock            = threading.Lock()
pending_sessions: dict = {}   # thread_ts -> session_id

# ---------------------------------------------------------------------------
# Voyager repo setup
# ---------------------------------------------------------------------------
def setup_voyager_repo() -> None:
    """Clone or pull the Voyager repo at startup."""
    if not os.path.exists(os.path.join(VOYAGER_PATH, ".git")):
        log.info("Cloning %s → %s (shallow)", VOYAGER_REPO, VOYAGER_PATH)
        subprocess.run(
            ["gh", "repo", "clone", VOYAGER_REPO, VOYAGER_PATH, "--", "--depth=1"],
            check=True,
            env=os.environ.copy(),
        )
        log.info("Voyager clone complete")
    else:
        log.info("Pulling latest voyager")
        result = subprocess.run(
            ["git", "-C", VOYAGER_PATH, "pull", "--ff-only"],
            capture_output=True, text=True,
            env=os.environ.copy(),
        )
        log.info("git pull: %s", result.stdout.strip() or result.stderr.strip())


# ---------------------------------------------------------------------------
# Claude helpers
# ---------------------------------------------------------------------------
def run_claude(prompt: str, session_id: Optional[str] = None) -> Tuple[Optional[str], str]:
    """Invoke `claude -p` and return (session_id, result_text)."""
    cmd = [
        "claude", "-p", prompt,
        "--mcp-config", MCP_CONFIG,
        "--output-format", "json",
        "--allowedTools", "Bash,Edit,Write,mcp__datadog__*",
    ]
    if session_id:
        cmd += ["--resume", session_id]

    log.info("Invoking claude (resume=%s)", session_id or "new")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True, text=True,
            env=os.environ.copy(),
            timeout=CLAUDE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        log.error("claude timed out after %ss", CLAUDE_TIMEOUT)
        return None, "Analysis timed out — please try again."

    if result.returncode != 0:
        log.error("claude exit %s: %s", result.returncode, result.stderr[:500])
        return None, f"Analysis failed (exit {result.returncode}):\n{result.stderr[:300]}"

    try:
        data = json.loads(result.stdout)
        sid  = data.get("session_id")
        text = data.get("result", "")
    except (json.JSONDecodeError, ValueError):
        sid  = None
        text = result.stdout

    log.info("claude done (session=%s, chars=%d)", sid, len(text))
    return sid, text


def extract_queries(response: str) -> Optional[str]:
    """Return everything after NEED_QUERIES: if present, else None."""
    marker = "NEED_QUERIES:"
    idx = response.find(marker)
    if idx == -1:
        return None
    return response[idx + len(marker):].strip()


def extract_pr_url(response: str) -> Optional[str]:
    """Find a PR link the agent opened, via the PR_URL: marker or a raw github pull URL."""
    for line in response.splitlines():
        s = line.strip()
        if s.startswith("PR_URL:"):
            return s[len("PR_URL:"):].strip()
    m = re.search(r"https://github\.com/[\w.-]+/[\w.-]+/pull/\d+", response)
    return m.group(0) if m else None


def post_completion(thread_ts: str, response: str) -> None:
    """Post the final analysis to Slack, highlighting a draft PR if one was opened."""
    pr_url = extract_pr_url(response)
    # Strip the PR_URL: marker line from the displayed body (we surface it separately)
    body = "\n".join(
        ln for ln in response.splitlines() if not ln.strip().startswith("PR_URL:")
    ).strip()

    text = f":white_check_mark: *Analysis complete:*\n{body}"
    if pr_url:
        text += f"\n\n:rocket: *Draft PR opened for review:* {pr_url}"

    slack.client.chat_postMessage(
        channel=SLACK_CHANNEL, thread_ts=thread_ts, text=text,
    )


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------
def build_investigation_prompt(alert_title: str, service: str, payload: dict) -> str:
    """Brief Claude on the alert and the resources it can reach. It decides what to fetch."""
    service_src = os.path.join(VOYAGER_PATH, "services", service)
    src_hint = service_src if os.path.exists(service_src) else VOYAGER_PATH

    return f"""You are an on-call Go engineer for the Voyager platform. A Datadog alert just fired.
Investigate and resolve it the way a developer would: form hypotheses, gather ONLY the
evidence you actually need, and drive to a concrete root cause. Do not gather data you don't need.

Alert: {alert_title}
Service: {service}
Alert payload:
{json.dumps(payload, indent=2)}

You have access to the following. Use each ONLY if your investigation calls for it:

1. SOURCE CODE — the full Voyager monorepo is checked out locally at {VOYAGER_PATH}
   (this service: {src_hint}). Use your Bash tool to grep and read files
   (rg, cat, ls, git log, etc.).

2. pprof PROFILES — production heap/goroutine captures live in S3, one folder per capture:
       s3://{S3_BUCKET}/{service}/<TIMESTAMP>/heap.dump       (Go heap profile, protobuf)
       s3://{S3_BUCKET}/{service}/<TIMESTAMP>/memstats.json   (runtime.MemStats)
       s3://{S3_BUCKET}/{service}/<TIMESTAMP>/stacktrace      (full goroutine dump)
   You have AWS credentials in your environment and the `aws` and `go` CLIs. Fetch a profile
   ONLY if the alert warrants it, e.g.:
       aws s3 ls s3://{S3_BUCKET}/{service}/
       aws s3 cp s3://{S3_BUCKET}/{service}/<TIMESTAMP>/heap.dump /tmp/heap.dump
       go tool pprof -top -sample_index=inuse_space /tmp/heap.dump
       go tool pprof -list=<Func> /tmp/heap.dump
   Pick the most recent capture unless the alert points elsewhere.

3. DATADOG — via MCP tools (metrics, logs, traces). Use for runtime evidence.

4. DATABASES — you CANNOT query PostgreSQL or Neo4j directly. When you need data from them,
   output this EXACT block and then STOP (a human runs the queries and replies with results):

NEED_QUERIES:
-- PostgreSQL
<exact SQL>

-- Neo4j (Cypher)
<exact Cypher>

   Omit a section you don't need. Never guess DB contents — ask via NEED_QUERIES.

Deliver a clear root-cause analysis with concrete, code-level fixes. If DB data is required
to be certain, use NEED_QUERIES rather than speculating.

FIX & PR:
If — and only if — you are confident in a concrete, well-scoped code fix, implement it and
open a DRAFT pull request (a human will review before merge; do NOT merge it yourself):
  cd {VOYAGER_PATH}
  git checkout -b agent/{service}-<short-slug>
  # make the minimal change that fixes the root cause
  git add -A && git commit -m "<clear message explaining the fix>"
  git push -u origin HEAD
  gh pr create --draft --repo {VOYAGER_REPO} --base main \\
    --title "<concise title>" \\
    --body "<what the profile showed, the root cause, and the fix>"
Keep the diff minimal and focused. On the LAST line of your reply, output the PR link as:
PR_URL: <the URL gh printed>
If you are not confident enough to write a fix, do not open a PR — just report the analysis."""


# ---------------------------------------------------------------------------
# Analysis logic
# ---------------------------------------------------------------------------
def analyze(alert_title: str, service: str, payload: dict) -> None:
    """Full analysis pipeline. Runs in a background thread."""
    log.info("Starting analysis: alert=%r service=%r", alert_title, service)

    # Post to Slack immediately so the team knows we're on it
    msg = slack.client.chat_postMessage(
        channel=SLACK_CHANNEL,
        text=(
            f":mag: *New alert — investigating*\n"
            f"*Alert:* {alert_title}\n"
            f"*Service:* `{service}`"
        ),
    )
    thread_ts = msg["ts"]

    # Pull latest voyager source before analysis
    try:
        subprocess.run(
            ["git", "-C", VOYAGER_PATH, "pull", "--ff-only"],
            capture_output=True, timeout=30, env=os.environ.copy(),
        )
    except Exception as e:
        log.warning("git pull failed (continuing): %s", e)

    prompt = build_investigation_prompt(alert_title, service, payload)
    session_id, response = run_claude(prompt)
    queries = extract_queries(response)

    if queries:
        # Post any analysis before the NEED_QUERIES marker
        pre = response[:response.find("NEED_QUERIES:")].strip()
        if pre:
            slack.client.chat_postMessage(
                channel=SLACK_CHANNEL,
                thread_ts=thread_ts,
                text=pre,
            )

        # Post all queries in one code-block message
        slack.client.chat_postMessage(
            channel=SLACK_CHANNEL,
            thread_ts=thread_ts,
            text=(
                ":bar_chart: *Need query results to complete the analysis.*\n"
                "Run these and reply with the output:\n"
                f"```\n{queries}\n```"
            ),
        )
        with _lock:
            pending_sessions[thread_ts] = session_id
        log.info("Analysis paused — awaiting query results (thread=%s session=%s)", thread_ts, session_id)
    else:
        post_completion(thread_ts, response)
        log.info("Analysis complete (thread=%s)", thread_ts)


def resume_analysis(thread_ts: str, human_data: str) -> None:
    """Resume a paused Claude session with query results. Runs in a background thread."""
    with _lock:
        session_id = pending_sessions.pop(thread_ts, None)

    if not session_id:
        log.warning("No pending session for thread %s", thread_ts)
        return

    log.info("Resuming analysis (thread=%s session=%s)", thread_ts, session_id)
    slack.client.chat_postMessage(
        channel=SLACK_CHANNEL,
        thread_ts=thread_ts,
        text=":hourglass_flowing_sand: Got the query results — resuming analysis...",
    )

    prompt = (
        "Here are the query results you requested:\n\n"
        f"{human_data}\n\n"
        "Please complete your analysis with this data."
    )
    session_id, response = run_claude(prompt, session_id=session_id)
    queries = extract_queries(response)

    if queries:
        # Claude needs more data
        slack.client.chat_postMessage(
            channel=SLACK_CHANNEL,
            thread_ts=thread_ts,
            text=(
                ":bar_chart: *Need more query results:*\n"
                f"```\n{queries}\n```"
            ),
        )
        with _lock:
            pending_sessions[thread_ts] = session_id
        log.info("Analysis paused again (thread=%s session=%s)", thread_ts, session_id)
    else:
        post_completion(thread_ts, response)
        log.info("Analysis complete after resume (thread=%s)", thread_ts)


# ---------------------------------------------------------------------------
# Flask — Datadog webhook receiver
# ---------------------------------------------------------------------------
flask_app = Flask(__name__)


def extract_service(payload: dict) -> str:
    """
    Resolve the service name from a Datadog webhook payload.
    Handles a custom payload (tags / service field) AND the default payload,
    where the service only appears as `kube_deployment:<name>` in the body/title.
    """
    # 1. Custom payload with an explicit service field
    if isinstance(payload.get("service"), str) and payload["service"]:
        return payload["service"]

    # 2. tags as list ["service:foo", ...] or dict {"service": "foo"}
    tags = payload.get("tags")
    if isinstance(tags, list):
        for t in tags:
            if isinstance(t, str) and t.startswith("service:"):
                return t.split(":", 1)[1]
    elif isinstance(tags, dict) and tags.get("service"):
        return tags["service"]

    # 3. Default payload: parse kube_deployment:<name> (then "deployment <name>")
    blob = f"{payload.get('title', '')}\n{payload.get('body', '')}"
    m = re.search(r"kube_deployment:([A-Za-z0-9_.-]+)", blob)
    if m:
        return m.group(1)
    m = re.search(r"\bdeployment\s+([A-Za-z0-9_.-]+)", blob)
    if m:
        return m.group(1)

    return "unknown"


@flask_app.route("/webhook", methods=["POST"])
def datadog_webhook():
    payload = request.get_json(force=True, silent=True) or {}
    log.info("Webhook received:\n%s", json.dumps(payload, indent=2))

    alert_title = (
        payload.get("title")
        or payload.get("alert_title")
        or payload.get("event_title")
        or "Unknown alert"
    )
    service = extract_service(payload)
    log.info("Resolved service=%r from payload", service)

    threading.Thread(
        target=analyze,
        args=(alert_title, service, payload),
        daemon=True,
    ).start()

    return jsonify({"status": "accepted"}), 200


@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Slack — Socket Mode
# ---------------------------------------------------------------------------
slack = SlackApp(token=os.environ["SLACK_BOT_TOKEN"])


@slack.event("message")
def handle_message(event, say):  # noqa: ARG001
    """Listen for thread replies with query results."""
    if event.get("channel") != SLACK_CHANNEL:
        return
    if event.get("bot_id") or event.get("bot_profile"):
        return
    thread_ts = event.get("thread_ts")
    if not thread_ts:
        return

    with _lock:
        is_pending = thread_ts in pending_sessions
    if not is_pending:
        return

    human_data = event.get("text", "").strip()
    if not human_data:
        return

    threading.Thread(
        target=resume_analysis,
        args=(thread_ts, human_data),
        daemon=True,
    ).start()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Starting agent service (webhook=:%d)", WEBHOOK_PORT)

    # Clone / pull Voyager repo
    try:
        setup_voyager_repo()
    except Exception as e:
        log.error("Voyager repo setup failed: %s — continuing without source", e)

    # Flask in background thread
    flask_thread = threading.Thread(
        target=lambda: flask_app.run(
            host="0.0.0.0", port=WEBHOOK_PORT,
            debug=False, use_reloader=False,
        ),
        daemon=True,
    )
    flask_thread.start()
    log.info("Webhook listener started on port %d", WEBHOOK_PORT)

    # Slack Socket Mode in main thread (blocks)
    handler = SocketModeHandler(slack, os.environ["SLACK_APP_TOKEN"])
    handler.start()
