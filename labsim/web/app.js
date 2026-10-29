"use strict";
/* Raft Failure Lab front end. No dependencies.
   The simulator returns the full event trace of a run; everything on the page
   (diagram, narration, timeline, logs) is a replay of that trace. */

// ---------------------------------------------------------------- helpers
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const esc = s => String(s ?? "").replace(/[&<>"']/g, c =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const SVGNS = "http://www.w3.org/2000/svg";
function svg(tag, attrs = {}, parent) {
  const e = document.createElementNS(SVGNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const fmt = v => v === null || v === undefined ? "–"
  : (Math.abs(v) >= 10 || Number.isInteger(v) ? String(Math.round(v)) : v.toFixed(1));
const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* storage unavailable */ } },
};
const nodeList = ids => ids.map(i => "n" + i).join(", ");
let COL = {};
function readColors() {
  const cs = getComputedStyle(document.documentElement);
  for (const k of ["bg", "card", "ink", "ink2", "muted", "line", "line2", "accent", "leader", "candidate",
    "follower", "crashed", "lane", "bad", "part", "degraded", "msg"]) COL[k] = cs.getPropertyValue("--" + k).trim();
}
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg; t.hidden = false;
  clearTimeout(toast.h); toast.h = setTimeout(() => { t.hidden = true; }, 2000);
}

const CAT = {
  send: "net", deliver: "net", drop: "net", duplicate: "net",
  became_candidate: "election", became_leader: "election", became_follower: "election", vote: "election",
  append: "log", truncate: "log", commit: "log", client_accept: "client", client_reject: "client",
  crash: "fault", restart: "fault", partition: "fault", heal: "fault", net: "fault", scenario: "fault", final: "end",
};
const CATS = [["election", "Elections"], ["log", "Log"], ["client", "Client"], ["fault", "Failures"], ["net", "Messages"]];
const cat = k => CAT[k] || "other";
const TERM = ["#2f5bd3", "#1f8a5b", "#c98a14", "#b5468a", "#178ca4", "#7a5cc7", "#c8641a", "#5d8a1f"];
const termColor = t => t > 0 ? TERM[(t - 1) % TERM.length] : "#999";
const GROUP = ["#2f5bd3", "#c8641a", "#1f8a5b", "#b5468a"];
const PLAIN_INVARIANT = {
  election_safety: "Two servers were leader in the same term",
  committed_lost: "Committed data was lost",
  log_divergence: "Servers disagree about committed data",
};
const STATS = [
  ["first_leader_ms", "Time to first leader", "ms"], ["leaderless_ms", "Time without a usable leader", "ms"],
  ["max_gap_ms", "Longest gap without a leader", "ms"], ["elections", "Elections started", ""],
  ["max_term", "Highest term reached", ""], ["commits", "Entries committed", ""],
  ["latency_ms", "Average commit delay", "ms"], ["lost", "Client commands lost", ""], ["rejects", "Client requests rejected", ""],
];

// ---------------------------------------------------------------- API
/* Two runtimes, one interface. With `python -m labsim ui` a local Python
   server answers the JSON API. On the static website there is no server, so a
   Web Worker runs the same labsim code in Pyodide and answers the same paths
   through ui.handle(). */
const API = {
  mode: "server", worker: null, pending: new Map(), seq: 0,
  async init() {
    try {
      const r = await fetch("api/meta");
      if (r.ok && (r.headers.get("content-type") || "").includes("json")) return;
    } catch { /* no server: use the in-browser runtime */ }
    this.mode = "browser";
    $("#boot").hidden = false;
    this.worker = new Worker("static/pyworker.js");
    await new Promise((resolve, reject) => {
      this.worker.onmessage = ({ data }) => {
        if (data.type === "ready") resolve();
        else if (data.type === "error") reject(new Error(data.error));
        else { const cb = this.pending.get(data.id); this.pending.delete(data.id); if (cb) cb(data); }
      };
      this.worker.onerror = e => reject(new Error(e.message || "worker failed to start"));
      this.worker.postMessage({ type: "init", base: new URL("py/", location.href).href });
    });
    $("#boot").hidden = true;
  },
  async get(path) {   // path like "/api/run?…"; returns {ok, status, text}
    if (this.mode === "server") {
      const r = await fetch(path.slice(1));
      return { ok: r.ok, status: r.status, text: await r.text() };
    }
    const id = ++this.seq;
    const { status, body } = await new Promise(res => { this.pending.set(id, res); this.worker.postMessage({ id, path }); });
    return { ok: status < 300, status, text: body };
  },
  async json(path) {
    const r = await this.get(path);
    const d = JSON.parse(r.text);
    if (!r.ok) throw new Error(d.error || "HTTP " + r.status);
    return d;
  },
  // simulations run roughly 3x slower in WebAssembly than in native Python
  get slowdown() { return this.mode === "browser" ? 3 : 1; },
};

// ---------------------------------------------------------------- state
let META = null;   // /api/meta
let RUN = null;    // prepared run
let ST = null;     // replay state at P.t
const P = {
  t: 0, playing: false, rate: 100, sel: null, follow: true, dirty: true, shownIdx: -2, drawnT: -1,
  cats: new Set(["election", "log", "client", "fault"]), q: "", lastFrame: 0,
};

// ---------------------------------------------------------------- narration
/* Turns the important trace events into plain sentences. Routine traffic
   (messages, appends, individual commits) is left to the full event log. */
function narrate(e) {
  const n = "n" + e.node, x = e.data;
  switch (e.kind) {
    case "became_candidate": return ["elect", `${n} stopped hearing from a leader and started an election (term ${x.term}).`];
    case "became_leader": return ["elect", `${n} won a majority of votes and is now the leader (term ${x.term}).`];
    case "became_follower":
      if (x.was === "leader") return ["elect", `${n} stepped down as leader: it saw a newer term (${x.term}).`];
      return null;
    case "crash": return ["fault", `${n} crashed.`];
    case "restart": return ["fault", `${n} restarted. It kept its log on disk but must relearn what is committed.`];
    case "partition": return ["fault", `The network split into ${x.groups.map(g => "{" + nodeList(g) + "}").join(" and ")}. Messages can't cross the split.`];
    case "heal": return ["fault", "The network split healed. Every server can reach every other again."];
    case "net":
      if (x.drop || x.dup || x.delay[1] > META.default_delay[1]) {
        const bits = [];
        if (x.drop) bits.push(`${Math.round(x.drop * 100)}% of messages are dropped`);
        if (x.dup) bits.push(`${Math.round(x.dup * 100)}% are delivered twice`);
        if (x.delay[1] > META.default_delay[1]) bits.push(`messages take up to ${x.delay[1]} ms`);
        return ["fault", `The network got worse: ${bits.join(", ")}.`];
      }
      return ["fault", "The network is back to normal."];
    case "truncate": return ["fault", `${n} deleted ${x.removed.length} log entr${x.removed.length === 1 ? "y" : "ies"} from position ${x.from_index} that conflicted with the leader's log.`];
    case "scenario": return x.step === "complete" ? null : ["script", "Scenario step: " + x.step.replace(/^[a-d]: /, "")];
    default: return null;
  }
}

// ---------------------------------------------------------------- run preparation
function prep(d) {
  const n = META.cluster_size, end = META.run_until;
  const ev = d.trace.map(([seq, time, kind, node, data, text], i) => ({ i, seq, time, kind, node, data, text }));
  const sends = new Map(), flights = [];
  const seg = Array.from({ length: n }, () => []);
  const cur = Array.from({ length: n }, () => ({ role: "follower", term: 0, from: 0 }));
  const close = (i, t) => { const c = cur[i]; if (t > c.from) seg[i].push({ from: c.from, to: t, role: c.role, term: c.term }); c.from = t; };
  const parts = [], nets = [], story = [];
  let pcur = null, ncur = null;
  const vByI = new Map();
  const violations = d.violations.map(v => ({ ...v, i: ev.findIndex(e => e.seq === v.seq) }));
  violations.forEach(v => { if (!vByI.has(v.i)) vByI.set(v.i, []); vByI.get(v.i).push(v); });
  for (const e of ev) {
    const i = e.node, x = e.data;
    switch (e.kind) {
      case "send": sends.set(x.id, e); break;
      case "deliver": {
        const s = sends.get(x.id);
        if (s) flights.push({ src: s.node, dst: i, t0: s.time, t1: e.time, ok: true, hb: / n=0 /.test(x.msg) });
        break;
      }
      case "drop": {
        const s = sends.get(x.id);
        if (!s) break;
        if ("dst" in x) flights.push({ src: s.node, dst: x.dst, t0: e.time, t1: e.time + 8, ok: false, early: true });
        else flights.push({ src: s.node, dst: i, t0: s.time, t1: e.time, ok: false });
        break;
      }
      case "became_candidate": close(i, e.time); Object.assign(cur[i], { role: "candidate", term: x.term }); break;
      case "became_leader": close(i, e.time); Object.assign(cur[i], { role: "leader", term: x.term }); break;
      case "became_follower": close(i, e.time); Object.assign(cur[i], { role: "follower", term: x.term }); break;
      case "crash": close(i, e.time); cur[i].role = "crashed"; break;
      case "restart": close(i, e.time); cur[i].role = "follower"; break;
      case "partition": if (pcur) { pcur.to = e.time; parts.push(pcur); } pcur = { from: e.time, groups: x.groups }; break;
      case "heal": if (pcur) { pcur.to = e.time; parts.push(pcur); pcur = null; } break;
      case "net": {
        if (ncur) { ncur.to = e.time; nets.push(ncur); ncur = null; }
        if (x.drop || x.dup || x.delay[1] > META.default_delay[1]) ncur = { from: e.time, drop: x.drop, dup: x.dup, delay: x.delay };
        break;
      }
    }
    const said = narrate(e);
    if (said) story.push({ i: e.i, time: e.time, cls: said[0], text: said[1] });
    for (const v of vByI.get(e.i) || []) {
      story.push({ i: e.i, time: e.time, cls: "bad", text: `${PLAIN_INVARIANT[v.invariant]}. ${v.detail}.` });
    }
  }
  for (let i = 0; i < n; i++) close(i, end);
  if (pcur) { pcur.to = end; parts.push(pcur); }
  if (ncur) { ncur.to = end; nets.push(ncur); }
  flights.sort((a, b) => a.t0 - b.t0);
  return { ...d, ev, n, end, flights, seg, parts, nets, story, violations, vByI };
}

function freshState(n) {
  return {
    i: 0, role: Array(n).fill("follower"), term: Array(n).fill(0), alive: Array(n).fill(true),
    log: Array.from({ length: n }, () => []), commit: Array(n).fill(0), groups: null, net: null,
  };
}
function apply(s, e) {
  const i = e.node, x = e.data;
  switch (e.kind) {
    case "became_candidate": s.role[i] = "candidate"; s.term[i] = x.term; break;
    case "became_leader": s.role[i] = "leader"; s.term[i] = x.term; break;
    case "became_follower": s.role[i] = "follower"; s.term[i] = x.term; break;
    case "vote": s.term[i] = Math.max(s.term[i], x.term); break;
    case "crash": s.alive[i] = false; break;
    case "restart": s.alive[i] = true; s.role[i] = "follower"; s.commit[i] = 0; break;
    case "append": x.entries.forEach((en, k) => { s.log[i][x.from_index - 1 + k] = en; }); break;
    case "truncate": s.log[i].length = x.from_index - 1; break;
    case "commit": s.commit[i] = Math.max(s.commit[i], x.index); break;
    case "partition": s.groups = {}; x.groups.forEach((g, k) => g.forEach(m => { s.groups[m] = k; })); break;
    case "heal": s.groups = null; break;
    case "net": s.net = x; break;
  }
}
function stateAt(t) {
  if (!ST || (ST.i > 0 && RUN.ev[ST.i - 1].time > t)) ST = freshState(RUN.n);
  while (ST.i < RUN.ev.length && RUN.ev[ST.i].time <= t) apply(ST, RUN.ev[ST.i++]);
  return ST;
}

// ---------------------------------------------------------------- setup form
function buildSetup() {
  $("#scenario").innerHTML = META.scenarios.map(s => `<option value="${esc(s.name)}">${esc(s.title)}</option>`).join("");
  $("#scenario").onchange = describeScenario;
  const names = Object.keys(META.strategies);
  const notes = ["Raft paper default", "faster, but riskier"];
  $("#strat").innerHTML = names.map((nm, k) => {
    const [lo, hi] = META.strategies[nm];
    return `<option value="${k}">${lo}–${hi} ms (${notes[k] || nm})</option>`;
  }).join("") + `<option value="custom">Custom…</option>`;
  $("#strat").onchange = () => applyStrategy();
  $("#setup").onsubmit = e => { e.preventDefault(); run(true); };
}
function describeScenario() {
  const s = META.scenarios.find(s => s.name === $("#scenario").value);
  $("#scendesc").textContent = s ? s.description : "";
}
function applyStrategy() {
  const v = $("#strat").value;
  $("#customrow").hidden = v !== "custom";
  if (v !== "custom") {
    const [lo, hi] = Object.values(META.strategies)[+v];
    $("#lo").value = lo; $("#hi").value = hi;
  }
}

// ---------------------------------------------------------------- running
async function run(autoplay) {
  const q = new URLSearchParams({ scenario: $("#scenario").value, seed: $("#seed").value || 0, lo: $("#lo").value, hi: $("#hi").value });
  const btn = $("#runbtn");
  btn.disabled = true; btn.textContent = "Running…"; $("#err").textContent = "";
  try {
    load(await API.json("/api/run?" + q));
    history.replaceState(null, "", "#" + q);
    setPlaying(!!autoplay);
  } catch (err) {
    $("#err").textContent = err.message;
  }
  btn.disabled = false; btn.textContent = "Run";
}

function load(d) {
  RUN = prep(d);
  ST = null; P.t = 0; P.shownIdx = -2; P.follow = true; P.sel = null;
  buildCluster();
  renderVerdict();
  renderStats();
  buildLanesBase();
  P.dirty = true;
}

function renderVerdict() {
  const v = $("#verdict"), m = RUN.metrics, vs = RUN.violations;
  const avail = Math.round(100 * (1 - m.leaderless_ms / RUN.end));
  const summary = `leader available ${avail}% of the time · ${m.commits} entries committed · ${m.elections} election${m.elections === 1 ? "" : "s"}`;
  // when safe, one line is enough; list the broken rules only when something failed
  const broken = META.invariants.map(inv => vs.filter(x => x.invariant === inv.id)).filter(m => m.length)
    .map(m => `<li class="fail">${esc(PLAIN_INVARIANT[m[0].invariant])} <button class="textbtn" type="button" data-seek="${m[0].time}">see where (${m[0].time} ms)</button></li>`).join("");
  v.className = "verdict" + (vs.length ? " bad" : "");
  v.innerHTML = vs.length
    ? `<h2>✕ Unsafe</h2><p>${summary}</p><ul>${broken}</ul>`
    : `<h2>✓ Safe</h2><p>All three of Raft's guarantees held · ${summary}</p>`;
  v.hidden = false;
  $$("[data-seek]", v).forEach(b => b.onclick = () => seek(+b.dataset.seek));
  $("#b-viol").hidden = !vs.length;
  $("#b-classic").href = RUN.timeline_url.slice(1);
  $("#clockof").textContent = `of ${RUN.end}`;
}

function renderStats() {
  $("#statstable").innerHTML = STATS.map(([k, label, unit]) =>
    `<tr><td>${label}</td><td class="num">${fmt(RUN.metrics[k])}${unit && RUN.metrics[k] !== null ? " " + unit : ""}</td><td>${esc(META.metric_help[k])}</td></tr>`).join("");
}

// ---------------------------------------------------------------- cluster diagram
const CX = 220, CY = 172, R = 128, NR = 30;
const nodePos = (i, n) => { const a = -Math.PI / 2 + i * 2 * Math.PI / n; return [CX + R * Math.cos(a), CY + R * Math.sin(a)]; };

function buildCluster() {
  const s = $("#cluster");
  s.innerHTML = "";
  const gl = svg("g", {}, s);
  RUN.linkEls = [];
  for (let i = 0; i < RUN.n; i++) for (let j = i + 1; j < RUN.n; j++) {
    const [x1, y1] = nodePos(i, RUN.n), [x2, y2] = nodePos(j, RUN.n);
    RUN.linkEls.push({ i, j, el: svg("line", { x1, y1, x2, y2, class: "link" }, gl) });
  }
  RUN.msgLayer = svg("g", {}, s);
  RUN.nodeEls = [];
  for (let i = 0; i < RUN.n; i++) {
    const [x, y] = nodePos(i, RUN.n);
    const g = svg("g", { class: "node follower", transform: `translate(${x},${y})` }, s);
    const grp = svg("circle", { class: "grp", r: NR + 7 }, g);
    svg("circle", { class: "sel", r: NR + 12 }, g);
    svg("circle", { class: "body", r: NR }, g);
    const name = svg("text", { class: "name", y: 5 }, g); name.textContent = "n" + i;
    const role = svg("text", { class: "role", y: NR + 17 }, g);
    g.addEventListener("click", () => selectNode(i));
    RUN.nodeEls.push({ g, grp, role });
  }
}

function renderNodes(st) {
  RUN.nodeEls.forEach((ne, i) => {
    const role = st.alive[i] ? st.role[i] : "crashed";
    ne.g.setAttribute("class", `node ${role}${P.sel === i ? " selected" : ""}`);
    ne.role.textContent = st.alive[i] ? `${role} · term ${st.term[i]}` : "crashed";
    if (st.groups) { ne.grp.style.opacity = 1; ne.grp.style.stroke = GROUP[st.groups[i] % GROUP.length]; }
    else ne.grp.style.opacity = 0;
    ne.g.dataset.tip = `n${i}: ${st.alive[i] ? role : "crashed"}, term ${st.term[i]}\n${st.log[i].length} log entries, ${st.commit[i]} known committed\nClick to focus on this server`;
  });
  RUN.linkEls.forEach(l => {
    l.el.setAttribute("class", "link" + (st.groups && st.groups[l.i] !== st.groups[l.j] ? " cut" : ""));
  });
  const bits = [];
  if (st.groups) bits.push(`<span style="color:var(--part)">network split</span>`);
  if (st.net && (st.net.drop || st.net.dup || st.net.delay[1] > META.default_delay[1])) bits.push(`<span style="color:var(--degraded)">unreliable network</span>`);
  const committed = Math.max(0, ...st.commit.map((c, i) => st.alive[i] ? c : 0));
  bits.push(`${committed} committed`);
  $("#netstate").innerHTML = (st.groups || bits.length > 1 ? "" : "network healthy · ") + bits.join(" · ");
}

function renderMsgs(t) {
  const layer = RUN.msgLayer;
  layer.textContent = "";
  const F = RUN.flights;
  let lo = 0, hi = F.length;
  while (lo < hi) { const m = (lo + hi) >> 1; if (F[m].t0 < t - 200) lo = m + 1; else hi = m; }
  for (let k = lo; k < F.length && F[k].t0 <= t; k++) {
    const f = F[k];
    const fade = f.ok ? 0 : 40;
    if (t > f.t1 + fade) continue;
    const [ax, ay] = nodePos(f.src, RUN.n), [bx, by] = nodePos(f.dst, RUN.n);
    const len = Math.hypot(bx - ax, by - ay), a = NR / len;
    let frac = f.t1 > f.t0 ? clamp((t - f.t0) / (f.t1 - f.t0), 0, 1) : 1;
    if (f.early) frac *= 0.3;
    const p = a + frac * (1 - 2 * a);
    const x = ax + (bx - ax) * p, y = ay + (by - ay) * p;
    if (!f.ok && t >= f.t1) {
      const tx = svg("text", { x, y: y + 5, "text-anchor": "middle", fill: COL.bad, "font-size": 16, "font-weight": 700, opacity: 1 - (t - f.t1) / fade }, layer);
      tx.textContent = "×";
      continue;
    }
    svg("circle", { cx: x, cy: y, r: f.hb ? 2.5 : 3.8, fill: f.hb ? COL.follower : COL.msg, opacity: f.hb ? 0.8 : 1 }, layer);
  }
}

// ---------------------------------------------------------------- story, logs, event log
function renderStory() {
  const box = $("#story");
  const shown = RUN.story.filter(s => s.time <= P.t && (P.sel === null || s.text.includes("n" + P.sel) || s.cls === "fault" || s.cls === "bad"));
  if (!shown.length) {
    box.innerHTML = `<li class="empty">${P.t === 0 ? "Press ▶ to start. Nothing has happened yet." : "Nothing notable yet. The cluster is quietly copying log entries."}</li>`;
    return;
  }
  box.innerHTML = shown.slice(-6).reverse().map((s, k) =>
    `<li class="${s.cls}${k === 0 ? " now" : ""}" data-t="${s.time}"><span class="when">${s.time} ms</span><span class="what">${esc(s.text)}</span></li>`).join("");
  $$("li[data-t]", box).forEach(li => li.onclick = () => seek(+li.dataset.t));
}

function renderLogs(st) {
  const box = $("#logmatrix");
  const maxLen = Math.max(1, ...st.log.map(l => l.length));
  const ref = [];
  for (let idx = 1; idx <= maxLen; idx++) {
    for (let i = 0; i < RUN.n; i++) {
      if (st.commit[i] >= idx && st.log[i].length >= idx) { ref[idx] = st.log[i][idx - 1]; break; }
    }
  }
  const avail = Math.max(120, box.clientWidth - 36 - 132);
  const w = clamp(Math.floor(avail / maxLen) - 3, 5, 22);
  const cap = Math.floor(avail / (w + 3));
  const start = maxLen > cap ? maxLen - cap + 3 : 1;
  const rows = [];
  for (let i = 0; i < RUN.n; i++) {
    const role = st.alive[i] ? st.role[i] : "crashed";
    let cells = start > 1 ? `<div class="cell more" data-tip="${start - 1} older entries not shown">+${start - 1}</div>` : "";
    for (let idx = start; idx <= st.log[i].length; idx++) {
      const [term, cmd] = st.log[i][idx - 1];
      const c = termColor(term);
      const committed = idx <= st.commit[i];
      const r = ref[idx];
      const conflict = r && (r[0] !== term || r[1] !== cmd);
      const tip = `n${i}, position ${idx}: command "${cmd}", written in term ${term}\n${committed ? "Committed (safe on a majority)" : "Not committed yet on this server"}` +
        (conflict ? `\nConflict: position ${idx} was committed as "${r[1]}" (term ${r[0]})` : "");
      cells += `<div class="cell${committed ? "" : " unc"}${conflict ? " conflict" : ""}" style="width:${w}px;background:${c};border-color:${c}" data-tip="${esc(tip)}"></div>`;
    }
    rows.push(`<div class="lrow${P.sel === i ? " selected" : ""}" data-node="${i}">
      <div class="lhead"><b>n${i}</b>${role}</div><div class="cells">${cells}</div></div>`);
  }
  rows.push(`<div class="lindex"><span></span><div><span>position ${start}</span><span>${maxLen}</span></div></div>`);
  box.innerHTML = rows.join("");
  $$(".lrow", box).forEach(r => r.onclick = () => selectNode(+r.dataset.node));
}

function buildEventLog() {
  $("#chips").innerHTML = CATS.map(([k, label]) => `<button class="chip ${P.cats.has(k) ? "on" : ""}" data-c="${k}" type="button">${label}</button>`).join("");
  $$("#chips .chip").forEach(c => c.onclick = () => {
    P.cats.has(c.dataset.c) ? P.cats.delete(c.dataset.c) : P.cats.add(c.dataset.c);
    c.classList.toggle("on");
    P.follow = true; renderEvents();
  });
  $("#search").addEventListener("input", e => { P.q = e.target.value.trim().toLowerCase(); P.follow = true; renderEvents(); });
  $("#evdetails").addEventListener("toggle", () => { P.follow = true; renderEvents(); });
  const list = $("#evlist");
  list.addEventListener("scroll", () => { P.follow = list.scrollTop + list.clientHeight >= list.scrollHeight - 30; });
  list.addEventListener("click", e => {
    const row = e.target.closest("[data-i]");
    if (row) { P.follow = false; seek(RUN.ev[+row.dataset.i].time); }
  });
}
function matches(e) {
  if (!P.cats.has(cat(e.kind))) return false;
  if (P.sel !== null && e.node !== null && e.node !== P.sel) return false;
  if (P.q && !(`${e.kind} ${e.text} n${e.node}`.toLowerCase().includes(P.q))) return false;
  return true;
}
function renderEvents() {
  if (!RUN || !$("#evdetails").open) return;
  const list = $("#evlist");
  const last = (ST ? ST.i : 0) - 1;
  const picked = [];
  for (let k = last; k >= 0 && picked.length < 300; k--) {
    const e = RUN.ev[k];
    if (matches(e) || RUN.vByI.has(k)) picked.push(e);
  }
  picked.reverse();
  if (!picked.length) {
    list.innerHTML = `<div class="empty">${last < 0 ? "Nothing has happened yet." : "No events match these filters."}</div>`;
    return;
  }
  const html = [];
  picked.forEach((e, k) => {
    html.push(`<div class="ev${k === picked.length - 1 ? " cur" : ""}" data-i="${e.i}"><span class="t">${e.time}</span><span>${e.node === null ? "" : "n" + e.node}</span><span>${esc(e.kind.replace(/_/g, " "))}</span><span class="d" title="${esc(e.text)}">${esc(e.text)}</span></div>`);
    for (const v of RUN.vByI.get(e.i) || []) html.push(`<div class="ev vrow" data-i="${e.i}">${esc(PLAIN_INVARIANT[v.invariant])}: ${esc(v.detail)}</div>`);
  });
  list.innerHTML = html.join("");
  if (P.follow) list.scrollTop = list.scrollHeight;
}

function selectNode(i) {
  P.sel = P.sel === i ? null : i;
  if (P.sel !== null) toast(`Focusing on n${i}. Click it again to show all servers.`);
  P.shownIdx = -2; P.follow = true;
  buildLanesBase();
  P.dirty = true;
}

// ---------------------------------------------------------------- timeline (canvas)
const LANE_H = 22, TOP = 44, PAD_L = 66, PAD_R = 14;
let laneBase = null, hatch = null, drag = false;
const laneX = (t, w) => PAD_L + (t / RUN.end) * (w - PAD_L - PAD_R);
const laneT = (x, w) => clamp((x - PAD_L) / (w - PAD_L - PAD_R) * RUN.end, 0, RUN.end);

function buildLanesBase() {
  if (!RUN) return;
  const c = $("#lanes");
  const w = c.clientWidth;
  if (!w) return;
  const h = TOP + RUN.n * LANE_H + 6;
  c.style.height = h + "px";
  const dpr = window.devicePixelRatio || 1;
  c.width = w * dpr; c.height = h * dpr;
  laneBase = document.createElement("canvas");
  laneBase.width = w * dpr; laneBase.height = h * dpr;
  const g = laneBase.getContext("2d");
  g.scale(dpr, dpr);
  if (!hatch) {
    const hc = document.createElement("canvas"); hc.width = hc.height = 6;
    const hg = hc.getContext("2d");
    hg.strokeStyle = COL.crashed; hg.lineWidth = 1.5;
    hg.beginPath(); hg.moveTo(0, 6); hg.lineTo(6, 0); hg.stroke();
    hatch = hc;
  }
  const font = getComputedStyle(document.body).fontFamily;
  g.font = `11px ${font}`;
  g.textBaseline = "middle";
  for (let t = 0; t <= RUN.end; t += 500) {
    const x = laneX(t, w);
    g.fillStyle = COL.line; g.fillRect(Math.round(x), 18, 1, h - 18);
    g.fillStyle = COL.muted; g.textAlign = "center"; g.fillText(t === 0 ? "0 ms" : String(t), x, 9);
  }
  g.textAlign = "left";
  g.fillStyle = COL.muted; g.fillText("network", 8, 29);
  g.fillStyle = COL.lane; g.fillRect(PAD_L, 23, w - PAD_L - PAD_R, 12);
  for (const p of RUN.parts) {
    const x0 = laneX(p.from, w), x1 = laneX(p.to, w);
    g.fillStyle = COL.part; g.globalAlpha = 0.55; g.fillRect(x0, 23, Math.max(2, x1 - x0), 12); g.globalAlpha = 1;
  }
  for (const p of RUN.nets) {
    const x0 = laneX(p.from, w), x1 = laneX(p.to, w);
    g.fillStyle = COL.degraded; g.globalAlpha = 0.5; g.fillRect(x0, 23, Math.max(2, x1 - x0), 12); g.globalAlpha = 1;
  }
  for (let i = 0; i < RUN.n; i++) {
    const y = TOP + i * LANE_H;
    g.fillStyle = P.sel === i ? COL.accent : COL.ink2;
    g.font = `${P.sel === i ? "600 " : ""}12px ${font}`;
    g.fillText("n" + i, 8, y + LANE_H / 2 - 1);
    g.fillStyle = COL.lane; g.fillRect(PAD_L, y + 3, w - PAD_L - PAD_R, LANE_H - 8);
    for (const s of RUN.seg[i]) {
      const x0 = laneX(s.from, w), x1 = laneX(s.to, w), sw = Math.max(1, x1 - x0);
      if (s.role === "crashed") {
        g.fillStyle = g.createPattern(hatch, "repeat"); g.fillRect(x0, y + 3, sw, LANE_H - 8);
      } else if (s.role !== "follower") {
        g.fillStyle = s.role === "leader" ? COL.leader : COL.candidate;
        g.fillRect(x0, y + 3, sw, LANE_H - 8);
        if (s.role === "leader" && sw > 54) {
          g.fillStyle = "#ffffff"; g.font = `11px ${font}`; g.fillText("term " + s.term, x0 + 5, y + LANE_H / 2 - 1);
        }
      }
    }
  }
  const seen = new Set();
  for (const v of RUN.violations) {
    const x = Math.round(laneX(v.time, w));
    if (seen.has(x)) continue;
    seen.add(x);
    g.fillStyle = COL.bad; g.fillRect(x - 1, 18, 2, h - 18);
  }
  P.dirty = true;
}

function drawLanes() {
  const c = $("#lanes");
  if (!laneBase || !c.width) return;
  const g = c.getContext("2d");
  const dpr = window.devicePixelRatio || 1, w = c.width / dpr, h = c.height / dpr;
  g.setTransform(1, 0, 0, 1, 0, 0);
  g.clearRect(0, 0, c.width, c.height);
  g.drawImage(laneBase, 0, 0);
  g.setTransform(dpr, 0, 0, dpr, 0, 0);
  const x = laneX(P.t, w);
  g.fillStyle = COL.ink; g.fillRect(x - 1, 16, 2, h - 16);
  g.beginPath(); g.moveTo(x - 5, 14); g.lineTo(x + 5, 14); g.lineTo(x, 20); g.fill();
}

function bindLanes() {
  const c = $("#lanes");
  const tAt = ev => { const r = c.getBoundingClientRect(); return laneT(ev.clientX - r.left, r.width); };
  c.addEventListener("pointerdown", ev => { if (!RUN) return; drag = true; c.setPointerCapture(ev.pointerId); seek(tAt(ev)); });
  c.addEventListener("pointermove", ev => {
    if (!RUN) return;
    const t = tAt(ev);
    if (drag) seek(t);
    const r = c.getBoundingClientRect(), y = ev.clientY - r.top;
    const lane = Math.floor((y - TOP) / LANE_H);
    let txt = `${Math.round(t)} ms`;
    if (lane >= 0 && lane < RUN.n) {
      const s = RUN.seg[lane].find(s => s.from <= t && t < s.to);
      if (s) txt += `\nn${lane} is ${s.role === "crashed" ? "crashed" : `${s.role} (term ${s.term})`}`;
    } else if (y >= 20 && y < 38) {
      const p = RUN.parts.find(p => p.from <= t && t < p.to);
      const n = RUN.nets.find(p => p.from <= t && t < p.to);
      if (p) txt += `\nnetwork split: ${p.groups.map(g => "{" + nodeList(g) + "}").join(" and ")}`;
      if (n) txt += `\nunreliable network`;
      if (!p && !n) txt += "\nnetwork healthy";
    }
    const near = RUN.violations.find(v => Math.abs(laneX(v.time, r.width) - (ev.clientX - r.left)) < 4);
    if (near) txt += `\n${PLAIN_INVARIANT[near.invariant]}`;
    showTip(txt, ev.clientX, ev.clientY);
  });
  c.addEventListener("pointerup", () => { drag = false; });
  c.addEventListener("pointerleave", hideTip);
  new ResizeObserver(() => { buildLanesBase(); P.shownIdx = -2; }).observe(c);
}

// ---------------------------------------------------------------- playback
function setPlaying(on) {
  if (on && RUN && P.t >= RUN.end) P.t = 0;
  P.playing = on && !!RUN;
  const b = $("#b-play");
  b.textContent = P.playing ? "❚❚" : "▶";
  b.setAttribute("aria-label", P.playing ? "Pause" : "Play");
  P.lastFrame = 0;
}
function seek(t) {
  if (!RUN) return;
  P.t = clamp(t, 0, RUN.end);
  setPlaying(false);
  P.dirty = true;
}
function stepEvent(dir) {
  if (!RUN) return;
  // step through the narrated events: that is what a person wants to see next
  const pts = RUN.story.map(s => s.time);
  if (dir > 0) { const t = pts.find(t => t > P.t); seek(t ?? RUN.end); }
  else { const t = [...pts].reverse().find(t => t < P.t); seek(t ?? 0); }
}
function nextViolation() {
  if (!RUN || !RUN.violations.length) return;
  const v = RUN.violations.find(v => v.time > P.t) || RUN.violations[0];
  seek(v.time);
}
function bindPlayer() {
  $("#b-play").onclick = () => setPlaying(!P.playing);
  $("#b-prev").onclick = () => stepEvent(-1);
  $("#b-next").onclick = () => stepEvent(1);
  $("#b-viol").onclick = nextViolation;
  $("#speed").onchange = e => { P.rate = +e.target.value; };
  $("#b-link").onclick = async () => {
    const url = `${location.href.split("#")[0]}${location.hash.split("&t=")[0]}&t=${Math.round(P.t)}`;
    try { await navigator.clipboard.writeText(url); toast("Link copied. It reopens this run at " + Math.round(P.t) + " ms."); }
    catch { toast(url); }
  };
  $("#b-classic").addEventListener("click", async e => {
    if (API.mode === "server" || !RUN) return;
    e.preventDefault();
    const w = window.open("", "_blank");
    const r = await API.get(RUN.timeline_url);
    const url = URL.createObjectURL(new Blob([r.text], { type: "text/html" }));
    if (w) w.location = url; else location.href = url;
  });
  document.addEventListener("keydown", e => {
    if (e.target.matches("input, select, textarea, summary") || $("#view-sim").hidden) return;
    if (e.key === " ") { e.preventDefault(); setPlaying(!P.playing); }
    else if (e.key === "ArrowRight") { e.preventDefault(); stepEvent(1); }
    else if (e.key === "ArrowLeft") { e.preventDefault(); stepEvent(-1); }
  });
}

function frame() {
  const st = stateAt(P.t);
  if (st.i !== P.shownIdx) {
    P.shownIdx = st.i;
    renderNodes(st); renderLogs(st); renderStory(); renderEvents();
  }
  if (P.t !== P.drawnT || P.dirty) {
    renderMsgs(P.t);
    drawLanes();
    $("#clock").textContent = Math.round(P.t);
    P.drawnT = P.t; P.dirty = false;
  }
}
function loop(ts) {
  const dt = P.lastFrame ? Math.min(100, ts - P.lastFrame) : 0;
  P.lastFrame = ts;
  if (RUN && P.playing) {
    P.t += dt / 1000 * P.rate;
    if (P.t >= RUN.end) { P.t = RUN.end; setPlaying(false); }
  }
  if (RUN && !$("#view-sim").hidden) frame();
  requestAnimationFrame(loop);
}

// ---------------------------------------------------------------- tooltip
function showTip(text, x, y) {
  const t = $("#tip");
  t.textContent = text; t.hidden = false;
  const r = t.getBoundingClientRect();
  t.style.left = clamp(x + 14, 4, innerWidth - r.width - 4) + "px";
  t.style.top = clamp(y + 14, 4, innerHeight - r.height - 4) + "px";
}
function hideTip() { $("#tip").hidden = true; }
function bindTip() {
  document.addEventListener("mousemove", e => {
    if (e.target.closest && e.target.closest("#lanes")) return;
    const el = e.target.closest && e.target.closest("[data-tip]");
    if (el) showTip(el.dataset.tip || el.getAttribute("data-tip"), e.clientX, e.clientY); else hideTip();
  });
}

// ---------------------------------------------------------------- views + theme
function showView(v) {
  $$(".tabs button").forEach(b => b.classList.toggle("on", b.dataset.view === v));
  $("#view-sim").hidden = v !== "sim";
  $("#view-cmp").hidden = v !== "cmp";
  $("#view-find").hidden = v !== "find";
  if (v === "find") loadFindings();
  if (v === "sim") { buildLanesBase(); P.shownIdx = -2; }
  hideTip();
  window.scrollTo(0, 0);
}
function initTheme() {
  const saved = store.get("lab-theme");
  const dark = saved ? saved === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  const apply = d => {
    document.documentElement.dataset.theme = d ? "dark" : "light";
    $("#theme").textContent = d ? "Light mode" : "Dark mode";
    readColors(); hatch = null;
  };
  apply(dark);
  $("#theme").onclick = () => {
    const d = document.documentElement.dataset.theme !== "dark";
    store.set("lab-theme", d ? "dark" : "light");
    apply(d); buildLanesBase(); renderCompare(); P.shownIdx = -2;
  };
}

// ---------------------------------------------------------------- compare
const SHORT = { baseline: "None", lossy: "Lossy", slow: "Slow", leader_crash: "Crash", partition_minority_leader: "Minority",
  flapping_partition: "Flapping", crash_restart_storm: "Storm", figure8: "Fig. 8" };
const CHARTS = [
  ["leaderless_ms", "Time without a leader", "Milliseconds per run (out of 4000). Lower is better."],
  ["elections", "Elections started", "Per run. Extra elections disrupt the cluster. Lower is better."],
  ["commits", "Entries committed", "Per run. More means more useful work got done. Higher is better."],
];
const LOWER = new Set(["violations", "first_leader_ms", "leaderless_ms", "max_gap_ms", "elections", "max_term", "latency_ms", "lost", "rejects"]);
const METRIC_LABEL = { violations: "Safety violations", first_leader_ms: "First leader (ms)", leaderless_ms: "No leader (ms)", max_gap_ms: "Longest gap (ms)",
  elections: "Elections", max_term: "Highest term", commits: "Committed", latency_ms: "Commit delay (ms)", lost: "Lost commands", rejects: "Rejected" };
let CMP = null;

function buildCompareForm() {
  $("#cmpscen").innerHTML = META.scenarios.map(s =>
    `<label><input type="checkbox" value="${esc(s.name)}" checked> ${esc(s.title)}</label>`).join("");
  $$("#cmpscen input").forEach(c => c.onchange = cmpCount);
  ["s0", "s1"].forEach(id => $("#" + id).addEventListener("input", cmpCount));
  $("#cmpbtn").onclick = runCompare;
  cmpCount();
}
const cmpSel = () => $$("#cmpscen input:checked").map(c => c.value);
function cmpCount() {
  const runs = 2 * cmpSel().length * Math.max(0, +$("#s1").value - +$("#s0").value + 1);
  $("#cmpcount").textContent = `${runs} simulations, about ${Math.max(1, Math.round(runs * 0.035 * API.slowdown))} seconds`;
  return runs;
}
async function runCompare() {
  const scen = cmpSel();
  if (!scen.length) { $("#cmperr").textContent = "Choose at least one failure."; return; }
  const est = Math.max(0.5, cmpCount() * 0.035 * API.slowdown);
  const q = new URLSearchParams({ seeds: `${$("#s0").value}..${$("#s1").value}`, scenarios: scen.join(",") });
  const btn = $("#cmpbtn"), prog = $("#cmpprog"), bar = $("i", prog);
  btn.disabled = true; btn.textContent = "Running…"; $("#cmperr").textContent = "";
  prog.hidden = false; bar.style.width = "0";
  const t0 = performance.now();
  const tick = setInterval(() => { bar.style.width = Math.min(92, (performance.now() - t0) / 1000 / est * 92) + "%"; }, 200);
  try {
    CMP = await API.json("/api/compare?" + q);
    bar.style.width = "100%";
    renderCompare();
  } catch (err) { $("#cmperr").textContent = err.message; }
  clearInterval(tick);
  setTimeout(() => { prog.hidden = true; }, 400);
  btn.disabled = false; btn.textContent = "Run comparison";
}
function niceMax(v) {
  if (v <= 0) return 1;
  const p = Math.pow(10, Math.floor(Math.log10(v))), f = v / p;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * p;
}
function renderCompare() {
  if (!CMP) return;
  const names = Object.keys(META.strategies);
  const label = k => { const [lo, hi] = META.strategies[names[k]]; return `${lo}–${hi} ms`; };
  const scen = [...new Set(CMP.rows.map(r => r.scenario))];
  const get = (s, k) => CMP.rows.find(r => r.scenario === s && r.strategy === names[k]).metrics;
  const title = s => META.scenarios.find(x => x.name === s).title;
  const runs = CMP.seeds.length * scen.length * 2;
  const totalV = CMP.rows.reduce((a, r) => a + r.metrics.violations, 0);
  const wins = (m, k) => scen.filter(s => {
    const a = get(s, k)[m], b = get(s, 1 - k)[m];
    return a !== null && b !== null && (LOWER.has(m) ? a < b : a > b);
  }).length;
  const worstB = scen.reduce((w, s) => (w === null || get(s, 1).leaderless_ms > get(w, 1).leaderless_ms ? s : w), null);
  const points = [
    totalV ? `<b class="bad-text">${totalV} safety violation${totalV === 1 ? "" : "s"}</b> across ${runs} simulations.`
      : `<b>No safety violations</b> in any of the ${runs} simulations. Both settings keep the data correct; they differ only in how quickly the cluster recovers.`,
    `The shorter timeout (${label(1)}) had less downtime in <b>${wins("leaderless_ms", 1)} of ${scen.length}</b> failure types.`,
    `The longer timeout (${label(0)}) started fewer elections in <b>${wins("elections", 0)} of ${scen.length}</b>. Short timeouts sit close to the 50 ms heartbeat, so followers sometimes give up on a healthy leader.`,
  ];
  if (worstB) points.push(`The shorter timeout's worst case was <b>${esc(title(worstB))}</b>: ${fmt(get(worstB, 1).leaderless_ms)} ms without a leader, versus ${fmt(get(worstB, 0).leaderless_ms)} ms with the longer timeout.`);
  const charts = CHARTS.map(([m, t, sub]) => chartSvg(m, t, sub, scen, get, label)).join("");
  const th = `<tr><th>Failure</th><th>Timeout</th>${CMP.metrics.map(m => `<th data-tip="${esc(CMP.metric_help[m])}">${METRIC_LABEL[m] || m}</th>`).join("")}</tr>`;
  let body = "";
  for (const s of scen) for (let k = 0; k < 2; k++) {
    const mine = get(s, k), other = get(s, 1 - k);
    body += `<tr><td>${k === 0 ? esc(title(s)) : ""}</td><td>${label(k)}</td>` + CMP.metrics.map(m => {
      const a = mine[m], b = other[m];
      let cls = "";
      if (m === "violations") cls = a ? "bad" : "";
      else if (a !== null && b !== null && a !== b && (LOWER.has(m) ? a < b : a > b)) cls = "better";
      return `<td class="${cls}">${fmt(a)}</td>`;
    }).join("") + "</tr>";
  }
  $("#cmpout").innerHTML = `
    <div class="card summary"><h2>What the comparison shows</h2><ul>${points.map(p => `<li>${p}</li>`).join("")}</ul></div>
    <p class="key"><span><i style="background:var(--accent)"></i>${label(0)} (longer)</span><span><i style="background:var(--candidate)"></i>${label(1)} (shorter)</span><span>averaged over seeds ${CMP.seeds[0]}–${CMP.seeds[CMP.seeds.length - 1]}</span></p>
    <div class="charts">${charts}</div>
    <details class="card more"><summary>All numbers <span class="hint">green marks the better of the two settings</span></summary>
      <div class="tablewrap"><table class="cmp"><thead>${th}</thead><tbody>${body}</tbody></table></div></details>`;
}
function chartSvg(m, title, sub, scen, get, label) {
  const W = 440, H = 200, L = 40, B = 26, T = 10, Rr = 6;
  const vals = scen.flatMap(s => [get(s, 0)[m], get(s, 1)[m]]).map(v => v ?? 0);
  const ymax = niceMax(Math.max(...vals));
  const iw = W - L - Rr, ih = H - T - B, band = iw / scen.length, bw = Math.min(16, band / 2 - 4);
  let g = "";
  for (let k = 0; k <= 4; k++) {
    const y = T + ih - ih * k / 4;
    g += `<line class="grid" x1="${L}" x2="${W - Rr}" y1="${y}" y2="${y}"></line><text x="${L - 6}" y="${y + 3}" text-anchor="end">${fmt(ymax * k / 4)}</text>`;
  }
  scen.forEach((s, i) => {
    const cx = L + band * i + band / 2;
    [0, 1].forEach(k => {
      const v = get(s, k)[m];
      const hgt = v === null ? 0 : ih * v / ymax;
      g += `<rect class="${k ? "serB" : "serA"}" x="${k === 0 ? cx - bw - 1 : cx + 1}" y="${T + ih - hgt}" width="${bw}" height="${Math.max(hgt, v ? 1 : 0)}" rx="2"
        data-tip="${esc(SHORT[s] || s)}, ${label(k)}: ${fmt(v)}"></rect>`;
    });
    g += `<text x="${cx}" y="${H - 8}" text-anchor="middle">${esc(SHORT[s] || s)}</text>`;
  });
  return `<div class="card chart"><h3>${esc(title)}</h3><p>${esc(sub)}</p><svg viewBox="0 0 ${W} ${H}">${g}</svg></div>`;
}

// ---------------------------------------------------------------- findings
let findingsLoaded = false;
async function loadFindings() {
  if (findingsLoaded) return;
  try {
    const { html, toc } = renderMarkdown((await API.json("/api/findings")).markdown);
    $("#findings").innerHTML = html;
    $("#toc").innerHTML = `<b>Contents</b>` + toc.map(([id, t]) => `<a href="#" data-to="${id}">${esc(t)}</a>`).join("");
    $$("#toc a").forEach(a => a.onclick = e => { e.preventDefault(); document.getElementById(a.dataset.to).scrollIntoView({ behavior: "smooth" }); });
    findingsLoaded = true;
  } catch (err) { $("#findings").textContent = "Could not load FINDINGS.md: " + err.message; }
}
function inline(s) {
  return esc(s).replace(/`([^`]+)`/g, "<code>$1</code>").replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|[^*])\*([^*\s][^*]*)\*/g, "$1<em>$2</em>");
}
function renderMarkdown(src) {
  const out = [], toc = [];
  let depth = 0, para = [];
  const flush = () => { if (para.length) { out.push(`<p>${inline(para.join(" "))}</p>`); para = []; } };
  const setDepth = d => { while (depth < d) { out.push("<ul>"); depth++; } while (depth > d) { out.push("</ul>"); depth--; } };
  for (const line of src.split(/\r?\n/)) {
    let m;
    if ((m = line.match(/^## (.*)/))) {
      flush(); setDepth(0);
      const parts = m[1].split(" · "), title = parts.pop(), id = "f" + toc.length;
      toc.push([id, title.replace(/[`*]/g, "")]);
      out.push(`<h2 id="${id}">${parts.length ? `<span class="meta">${esc(parts.join(" · "))}</span>` : ""}${inline(title)}</h2>`);
    } else if ((m = line.match(/^# (.*)/))) { flush(); setDepth(0); out.push(`<h1>${inline(m[1])}</h1>`); }
    else if ((m = line.match(/^(\s*)- (.*)/))) { flush(); setDepth(m[1].length >= 2 ? 2 : 1); out.push(`<li>${inline(m[2])}</li>`); }
    else if (!line.trim()) { flush(); setDepth(0); }
    else { setDepth(0); para.push(line.trim()); }
  }
  flush(); setDepth(0);
  return { html: out.join("\n"), toc };
}

// ---------------------------------------------------------------- boot
async function init() {
  initTheme();
  try {
    await API.init();
    META = await API.json("/api/meta");
  } catch (err) {
    $("#bootmsg").textContent = "Could not start the simulator: " + err.message;
    $("#boot").hidden = false;
    return;
  }
  buildSetup(); buildEventLog(); buildCompareForm(); bindPlayer(); bindLanes(); bindTip();
  $$(".tabs button").forEach(b => b.onclick = () => showView(b.dataset.view));
  new ResizeObserver(() => { P.shownIdx = -2; }).observe($("#logmatrix"));
  const h = new URLSearchParams(location.hash.slice(1));
  $("#scenario").value = META.scenarios.some(s => s.name === h.get("scenario")) ? h.get("scenario") : "leader_crash";
  describeScenario();
  $("#seed").value = h.get("seed") || 0;
  const lo = h.get("lo"), hi = h.get("hi");
  const k = Object.values(META.strategies).findIndex(([a, b]) => String(a) === lo && String(b) === hi);
  if (lo && hi && k < 0) { $("#strat").value = "custom"; applyStrategy(); $("#lo").value = lo; $("#hi").value = hi; }
  else { $("#strat").value = String(k < 0 ? 0 : k); applyStrategy(); }
  await run(false);
  const t = h.get("t");
  if (t !== null && RUN) seek(+t);
  requestAnimationFrame(loop);
}
init();
