// tickr dashboard — no framework, no build step.
(() => {
  const readonly = document.body.dataset.readonly === "true";
  const $ = (sel) => document.querySelector(sel);

  const state = {
    after: "",
    filters: { status: "", event_type: "", q: "" },
    rows: [],
  };

  // --- fetching ------------------------------------------------------------
  async function loadStats() {
    const r = await fetch("api/stats");
    if (!r.ok) return;
    const stats = await r.json();
    renderStats(stats);
  }

  async function loadEventTypes() {
    const r = await fetch("api/event-types");
    if (!r.ok) return;
    const types = await r.json();
    const sel = $("#filter-event-type");
    const cur = sel.value;
    sel.innerHTML = '<option value="">any</option>' +
      types.sort().map((t) => `<option>${escape(t)}</option>`).join("");
    sel.value = cur;
  }

  async function loadMessages(append = false) {
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(state.filters)) if (v) params.set(k, v);
    if (append && state.after) params.set("after_id", state.after);
    params.set("limit", "50");
    const r = await fetch("api/messages?" + params);
    if (!r.ok) {
      const e = await r.json().catch(() => ({ error: r.statusText }));
      alert("List failed: " + e.error);
      return;
    }
    const data = await r.json();
    if (!append) state.rows = [];
    state.rows = state.rows.concat(data.messages || []);
    state.after = data.next_id || "";
    $("#load-more").hidden = !state.after;
    renderRows();
  }

  async function loadDetail(id) {
    const r = await fetch("api/messages/" + encodeURIComponent(id));
    if (!r.ok) {
      alert("Failed to load message");
      return;
    }
    const data = await r.json();
    renderDetail(data);
    $("#detail").showModal();
  }

  async function action(id, verb) {
    const r = await fetch("api/messages/" + encodeURIComponent(id) + "/" + verb, { method: "POST" });
    if (!r.ok) {
      const e = await r.json().catch(() => ({ error: r.statusText }));
      alert(verb + " failed: " + e.error);
      return;
    }
    refresh();
    if ($("#detail").open) loadDetail(id);
  }

  // --- rendering -----------------------------------------------------------
  function renderStats(stats) {
    const totals = stats.by_status || {};
    const order = ["CREATED", "HANDLING", "FAILED", "RETRYING", "SUCCESS", "DEAD"];
    let total = 0;
    const html = order.map((s) => {
      const n = totals[s] || 0;
      total += n;
      return `<div class="stat" data-status="${s}">
        <span class="dot"></span><span class="label">${s}</span>
        <span class="count">${n.toLocaleString()}</span>
      </div>`;
    }).join("");
    $("#stats").innerHTML = html;
    $("#totals").textContent = total.toLocaleString() + " messages";
  }

  function renderRows() {
    const tbody = $("#messages tbody");
    tbody.innerHTML = state.rows.map((m) => `
      <tr data-id="${m.id}">
        <td class="id">${m.id.slice(0, 8)}…</td>
        <td>${escape(m.type)}</td>
        <td>${m.status ? `<span class="badge" data-status="${m.status}">${m.status}</span>` : "–"}</td>
        <td>${m.attempt}/${m.max_attempts}</td>
        <td>${fmtTime(m.enqueued_at)}</td>
        <td>${fmtTime(m.scheduled_at)}</td>
        <td class="err" title="${escape(m.last_error || "")}">${escape(m.last_error || "")}</td>
        <td>${actionButtons(m)}</td>
      </tr>
    `).join("");
    tbody.querySelectorAll("tr").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        if (e.target.closest("button")) return;
        loadDetail(tr.dataset.id);
      });
    });
    tbody.querySelectorAll("button.action").forEach((b) => {
      b.addEventListener("click", (e) => {
        e.stopPropagation();
        action(b.dataset.id, b.dataset.verb);
      });
    });
  }

  function actionButtons(m) {
    if (readonly) return "";
    const isTerminal = m.status === "SUCCESS" || m.status === "DEAD";
    const isDead = m.status === "DEAD";
    const buttons = [];
    if (isDead) buttons.push(`<button class="action" data-id="${m.id}" data-verb="requeue">Requeue</button>`);
    if (!isTerminal) buttons.push(`<button class="action danger" data-id="${m.id}" data-verb="kill">Kill</button>`);
    return buttons.join("");
  }

  function renderDetail(m) {
    $("#detail-title").textContent = m.type + " — " + m.id;
    const kv = (label, val) => val ? `<dt>${label}</dt><dd>${escape(val)}</dd>` : "";
    let actions = "";
    if (!readonly) {
      const isTerminal = m.status === "SUCCESS" || m.status === "DEAD";
      if (m.status === "DEAD") actions += `<button class="action" onclick="window.__tickrAct('${m.id}','requeue')">Requeue</button>`;
      if (!isTerminal) actions += `<button class="action danger" onclick="window.__tickrAct('${m.id}','kill')">Kill</button>`;
    }
    $("#detail-body").innerHTML = `
      <dl class="kv">
        ${kv("Status", m.status)}
        ${kv("Attempt", m.attempt + "/" + m.max_attempts)}
        ${kv("Enqueued", new Date(m.enqueued_at).toLocaleString())}
        ${kv("Scheduled", new Date(m.scheduled_at).toLocaleString())}
        ${kv("Idempotency", m.idempotency_key)}
        ${kv("Last error", m.last_error)}
      </dl>
      <div>${actions}</div>
      <h4>Payload (${m.payload_size} bytes)</h4>
      <pre class="payload">${escape(m.payload_preview || "")}</pre>
      ${m.history ? renderHistory(m.history) : ""}
    `;
  }

  function renderHistory(history) {
    return `<h4>History</h4>
      <table class="history">
        <thead><tr><th>#</th><th>From</th><th>→</th><th>To</th><th>Attempt</th><th>At</th><th>Worker</th><th>Error</th></tr></thead>
        <tbody>${history.map((t) => `
          <tr>
            <td>${t.seq}</td>
            <td>${escape(t.from || "")}</td><td>→</td>
            <td><span class="badge" data-status="${t.to}">${t.to}</span></td>
            <td>${t.attempt}</td>
            <td>${fmtTime(t.at)}</td>
            <td>${escape(t.worker_id || "")}</td>
            <td class="err">${escape(t.error || "")}</td>
          </tr>`).join("")}</tbody>
      </table>`;
  }

  // expose for inline onclick in detail
  window.__tickrAct = action;

  // --- helpers -------------------------------------------------------------
  function escape(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
  function fmtTime(t) {
    if (!t) return "";
    const d = new Date(t);
    const now = Date.now();
    const ms = now - d.getTime();
    if (ms < 60_000) return Math.round(ms / 1000) + "s ago";
    if (ms < 3_600_000) return Math.round(ms / 60_000) + "m ago";
    if (ms < 86_400_000) return Math.round(ms / 3_600_000) + "h ago";
    return d.toLocaleString();
  }

  function refresh() {
    state.after = "";
    return Promise.all([loadStats(), loadMessages(false), loadEventTypes()]);
  }

  // --- wire-up -------------------------------------------------------------
  $("#refresh").addEventListener("click", refresh);
  $("#load-more").addEventListener("click", () => loadMessages(true));
  $("#detail-close").addEventListener("click", () => $("#detail").close());
  ["status", "event_type", "q"].forEach((k, i) => {
    const el = [$("#filter-status"), $("#filter-event-type"), $("#filter-search")][i];
    const ev = el.tagName === "SELECT" ? "change" : "input";
    let debounceT;
    el.addEventListener(ev, () => {
      clearTimeout(debounceT);
      debounceT = setTimeout(() => {
        state.filters[k] = el.value;
        state.after = "";
        loadMessages(false);
      }, el.tagName === "SELECT" ? 0 : 250);
    });
  });

  refresh();
  setInterval(loadStats, 5000);
})();
