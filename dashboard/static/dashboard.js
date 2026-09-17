(function () {
  "use strict";

  var connPill = document.getElementById("connPill");
  var lastUpdate = document.getElementById("lastUpdate");
  var POLL_MS = 4000;
  var pollTimer = null;
  var es = null;

  function fmtAgo(iso) {
    if (!iso) return "";
    var s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.round(s / 60) + "m ago";
    return Math.round(s / 3600) + "h ago";
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function setConn(mode) {
    connPill.classList.remove("live", "poll");
    if (mode === "live") { connPill.classList.add("live"); connPill.textContent = "live · SSE"; }
    else if (mode === "poll") { connPill.classList.add("poll"); connPill.textContent = "polling /api/state"; }
    else { connPill.textContent = "connecting…"; }
  }

  function render(state) {
    if (!state) return;
    // The contract promises arrays + a stats object, but stay defensive: a real
    // backend can send null (e.g. before poold status is first fetched), and one
    // null here would otherwise throw and blank the entire dashboard.
    var s = state.stats || {};
    var vms = state.vms || [];
    var allTasks = state.tasks || [];
    var memory = state.memory || [];

    document.getElementById("tvAlloc").textContent = s.vms_allocated || 0;
    document.getElementById("tvReady").textContent = s.vms_ready || 0;
    document.getElementById("tvQueue").textContent = s.queue_depth || 0;
    document.getElementById("tvHit").textContent = Math.round((s.hit_rate || 0) * 100) + "%";
    document.getElementById("tvTotal").textContent = s.tasks_total || 0;
    document.getElementById("tvAvg").textContent = (s.avg_task_secs || 0) + "s";
    document.getElementById("vmTotal").textContent = (s.vms_total || 0) + " total";

    document.getElementById("vmGrid").innerHTML = vms.map(function (vm) {
      var num = vm.vm_id.replace(/^VM/, "");
      return '<div class="vmchip vm-' + vm.state + '" title="' + esc(vm.host) + (vm.task_id ? " · " + esc(vm.task_id) : "") + '">' +
        '<span class="id">' + num + '</span><span class="stt">' + vm.state.slice(0, 5) + '</span></div>';
    }).join("");

    var running = allTasks.filter(function (t) { return t.phase !== "queued" && t.phase !== "done" && t.phase !== "error"; }).length;
    document.getElementById("taskSummary").textContent = (s.queue_depth || 0) + " queued · " + running + " active";

    var order = { queued: 0, matching: 1, leasing: 2, dispatched: 3, running: 4, collecting: 5, pr_check: 6, distilling: 7, done: 8, error: 8 };
    var tasks = allTasks.slice().sort(function (a, b) {
      var pa = order[a.phase] != null ? order[a.phase] : 9;
      var pb = order[b.phase] != null ? order[b.phase] : 9;
      if (pa !== pb) return pa - pb;
      return (a.task_id > b.task_id) ? 1 : -1;
    });
    document.getElementById("taskList").innerHTML = tasks.map(function (t) {
      var right;
      if (t.phase === "done") {
        var oc = t.outcome || "done";
        var badge = '<span class="phase oc-' + esc(oc) + '">' + esc(oc).replace(/_/g, " ") + '</span>';
        var pr = t.pr_url || "";
        right = (t.outcome === "pr_opened" && pr)
          ? '<a class="prlink" href="' + esc(pr) + '" target="_blank" rel="noopener">PR ' + esc(pr.split("/").pop()) + ' &#8599;</a>' + badge
          : badge;
      } else {
        right = '<span class="phase ph-' + esc(t.phase) + '">' + esc(t.phase).replace(/_/g, " ") + '</span>';
      }
      return '<div class="taskrow">' +
        '<span class="svc">' + esc(t.service) + '</span>' +
        '<span class="al">' + esc(t.alert || "") + '</span>' +
        right +
        '</div>';
    }).join("") || '<div class="taskrow"><span class="al">No tasks in flight.</span></div>';

    document.getElementById("memList").innerHTML = '<div class="memlist">' + memory.map(function (m) {
      return '<button class="memrow" data-id="' + m.id + '">' +
        '<span class="mc">' + esc(m.signature) + '</span>' +
        '<span class="mr">' + esc(m.root_cause) + '</span>' +
        '<span class="hits">' + m.hit_count + ' hits</span>' +
        '</button>';
    }).join("") + '</div>';

    lastUpdate.textContent = "updated " + fmtAgo(new Date().toISOString());
  }

  function fetchState() {
    fetch("/api/state").then(function (r) { return r.json(); }).then(render).catch(function () {});
  }

  function startPolling() {
    setConn("poll");
    if (pollTimer) return;
    fetchState();
    pollTimer = setInterval(fetchState, POLL_MS);
  }

  function stopPolling() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function connect() {
    if (!("EventSource" in window)) { startPolling(); return; }
    es = new EventSource("/api/stream");
    es.onopen = function () { stopPolling(); setConn("live"); };
    es.onmessage = function (e) {
      try { render(JSON.parse(e.data)); } catch (err) {}
    };
    es.onerror = function () {
      es.close();
      startPolling();
      setTimeout(connect, 8000);
    };
  }

  // memory drill-down modal
  var backdrop = document.getElementById("modalBackdrop");
  var modalTitle = document.getElementById("modalTitle");
  var modalBody = document.getElementById("modalBody");

  function openMemory(id) {
    fetch("/api/memory/" + id).then(function (r) { return r.json(); }).then(function (m) {
      modalTitle.textContent = m.signature;
      modalBody.innerHTML =
        '<div class="row"><div class="k">Service</div><div class="v mono">' + esc(m.service) + '</div></div>' +
        '<div class="row"><div class="k">Symptom</div><div class="v">' + esc(m.symptom || "—") + '</div></div>' +
        '<div class="row"><div class="k">Root cause</div><div class="v">' + esc(m.root_cause) + '</div></div>' +
        '<div class="row"><div class="k">Fix</div><div class="v">' + esc(m.fix_summary || "not fixed yet") + '</div></div>' +
        '<div class="row"><div class="k">Pull request</div><div class="v">' + (m.pr_url ? '<a href="' + esc(m.pr_url) + '" target="_blank" rel="noopener">' + esc(m.pr_url) + '</a>' : "none") + '</div></div>' +
        '<div class="row"><div class="k">Hits · last seen</div><div class="v mono">' + m.hit_count + ' &middot; ' + fmtAgo(m.last_seen) + '</div></div>' +
        '<div class="row"><div class="k">Full analysis</div><pre>' + esc(m.full_analysis) + '</pre></div>';
      backdrop.classList.add("open");
    }).catch(function () {});
  }

  document.getElementById("memList") && document.body.addEventListener("click", function (e) {
    var row = e.target.closest(".memrow");
    if (row) openMemory(row.getAttribute("data-id"));
  });
  document.getElementById("modalClose").addEventListener("click", function () { backdrop.classList.remove("open"); });
  backdrop.addEventListener("click", function (e) { if (e.target === backdrop) backdrop.classList.remove("open"); });
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") backdrop.classList.remove("open"); });

  // theme toggle
  (function () {
    var btn = document.getElementById("themeBtn"), root = document.documentElement, KEY = "orch-dashboard-theme";
    function cur() {
      var e = root.getAttribute("data-theme");
      if (e) return e;
      return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }
    try { var s = localStorage.getItem(KEY); if (s) root.setAttribute("data-theme", s); } catch (e) {}
    btn.addEventListener("click", function () {
      var n = cur() === "dark" ? "light" : "dark";
      root.setAttribute("data-theme", n);
      try { localStorage.setItem(KEY, n); } catch (e) {}
    });
  })();

  fetchState();
  connect();
  setInterval(function () { lastUpdate.textContent = "updated " + fmtAgo(new Date(Date.now() - 1000).toISOString()); }, 15000);
})();
