"use strict";
/* Raft Failure Lab front end. No dependencies.
   The server returns the full event trace of a run; everything here
   (cluster animation, logs, timeline, feed) is a replay of that trace. */

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
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* private mode */ } },
};
let COL = {};
function readColors() {
  const cs = getComputedStyle(document.documentElement);
  for (const k of ["bg", "panel", "panel2", "raise", "line", "line2", "fg", "fg2", "muted", "accent",
    "follower", "candidate", "leader", "crashed", "bad", "part", "degraded"]) {
    COL[k] = cs.getPropertyValue("--" + k).trim();
  }
}
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg; t.hidden = false;
  clearTimeout(toast.h); toast.h = setTimeout(() => { t.hidden = true; }, 1800);
}

const CAT = {
  send: "net", deliver: "net", drop: "net", duplicate: "net",
  became_candidate: "election", became_leader: "election", became_follower: "election", vote: "election",
  append: "log", truncate: "log", commit: "log", client_accept: "client", client_reject: "client",
  crash: "fault", restart: "fault", partition: "fault", heal: "fault", net: "fault", scenario: "fault", final: "end",
};
const CATS = [["election", "Elections", "candidate"], ["log", "Log", "accent"], ["client", "Client", null],
  ["fault", "Faults", "part"], ["net", "Messages", "muted"], ["end", "Final state", "fg2"]];
const cat = k => CAT[k] || "other";
const TERM = ["#7c95ff", "#3ecf8e", "#f2b94b", "#ff7ab6", "#4dd0e1", "#b38cff", "#ff9f43", "#a3e635"];
const termColor = t => t > 0 ? TERM[(t - 1) % TERM.length] : "#888";
const GROUP = ["#7c95ff", "#ff9f43", "#3ecf8e", "#ff7ab6"];
const MSG = {
  vote: { c: "#f2b94b", r: 4, label: "RequestVote" },
  voteok: { c: "#ffe08a", r: 4, label: "vote granted" },
  voteno: { c: "#9b8455", r: 3.5, label: "vote denied" },
  append: { c: "#7c95ff", r: 4.5, label: "AppendEntries" },
  hb: { c: "#6b7590", r: 2.6, label: "heartbeat" },
  reply: { c: "#4dd0e1", r: 3, label: "append reply" },
};
function msgType(m) {
  if (m.startsWith("RequestVote")) return "vote";
  if (m.startsWith("VoteReply")) return m.includes(" yes") ? "voteok" : "voteno";
  if (m.startsWith("AppendEntries")) return / n=0 /.test(m) ? "hb" : "append";
  return "reply";
}
const METRIC_TILES = [["first_leader_ms", "First leader", "ms"], ["leaderless_ms", "Without leader", "ms"],
  ["max_gap_ms", "Longest gap", "ms"], ["elections", "Elections", ""], ["max_term", "Highest term", ""],
  ["commits", "Entries committed", ""], ["latency_ms", "Commit latency", "ms"], ["lost", "Lost writes", ""]];

// ---------------------------------------------------------------- state
let META = null;   // /api/meta
let RUN = null;    // prepared run
let ST = null;     // replay state at P.t
const P = {
  t: 0, playing: false, rate: 100, sel: null, follow: true, dirty: true, shownIdx: -2, drawnT: -1,
  cats: new Set(["election", "log", "client", "fault", "end"]), q: "", scen: null, strat: "0", lastFrame: 0,
};

// ---------------------------------------------------------------- run preparation
function prep(d) {
  const n = META.cluster_size, end = META.run_until;
  const ev = d.trace.map(([seq, time, kind, node, data, text], i) => ({ i, seq, time, kind, node, data, text }));
  const sends = new Map(), flights = [];
  const seg = Array.from({ length: n }, () => []);
  const cur = Array.from({ length: n }, () => ({ role: "follower", term: 0, from: 0 }));
  const close = (i, t) => { const c = cur[i]; if (t > c.from) seg[i].push({ from: c.from, to: t, role: c.role, term: c.term }); c.from = t; };
  const parts = [], nets = [], steps = [];
  let pcur = null, ncur = null;
  for (const e of ev) {
    const i = e.node, x = e.data;
    switch (e.kind) {
      case "send": sends.set(x.id, e); break;
      case "deliver": {
        const s = sends.get(x.id);
        if (s) flights.push({ src: s.node, dst: i, t0: s.time, t1: e.time, type: msgType(x.msg), ok: true });
        break;
      }
      case "drop": {
        const s = sends.get(x.id);
        if (!s) break;
        if ("dst" in x) flights.push({ src: s.node, dst: x.dst, t0: e.time, t1: e.time + 8, type: msgType(s.data.msg), ok: false, early: true });
        else flights.push({ src: s.node, dst: i, t0: s.time, t1: e.time, type: msgType(s.data.msg), ok: false });
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
      case "scenario": steps.push(e); break;
    }
  }
  for (let i = 0; i < n; i++) close(i, end);
  if (pcur) { pcur.to = end; parts.push(pcur); }
  if (ncur) { ncur.to = end; nets.push(ncur); }
  flights.sort((a, b) => a.t0 - b.t0);
  const violations = d.violations.map(v => ({ ...v, i: ev.findIndex(e => e.seq === v.seq) }));
  return { ...d, ev, n, end, flights, seg, parts, nets, steps, violations };
}

function freshState(n) {
  return {
    i: 0, role: Array(n).fill("follower"), term: Array(n).fill(0), alive: Array(n).fill(true),
    log: Array.from({ length: n }, () => []), commit: Array(n).fill(0), voted: Array(n).fill(null), groups: null, net: null,
  };
}
function apply(s, e) {
  const i = e.node, x = e.data;
  switch (e.kind) {
    case "became_candidate": s.role[i] = "candidate"; s.term[i] = x.term; s.voted[i] = i; break;
    case "became_leader": s.role[i] = "leader"; s.term[i] = x.term; break;
    case "became_follower": s.role[i] = "follower"; s.term[i] = x.term; break;
    case "vote": s.voted[i] = x.candidate; s.term[i] = Math.max(s.term[i], x.term); break;
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

// ---------------------------------------------------------------- sidebar
function buildSidebar() {
  $("#scenarios").innerHTML = META.scenarios.map(s => `
    <button class="scen" data-s="${esc(s.name)}">
      <b>${esc(s.title)}</b><p>${esc(s.description)}</p>
      ${s.tags.map(t => `<span class="tag">${esc(t)}</span>`).join("")}
    </button>`).join("");
  $$(".scen").forEach(b => b.onclick = () => selectScenario(b.dataset.s));
  const names = Object.keys(META.strategies);
  $("#strat").innerHTML = names.map((nm, k) => {
    const [lo, hi] = META.strategies[nm];
    return `<button data-k="${k}" title="${esc(nm)}"><b>${esc(nm.split(" ")[0])}</b>${lo}–${hi}</button>`;
  }).join("") + `<button data-k="custom"><b>Custom</b>your own</button>`;
  $$("#strat button").forEach(b => b.onclick = () => selectStrategy(b.dataset.k));
  ["lo", "hi"].forEach(id => $("#" + id).addEventListener("input", () => selectStrategy("custom", true)));
  $("#dice").onclick = () => { $("#seed").value = Math.floor(Math.random() * 10000); };
  $("#runbtn").onclick = () => run(true);
  $("#seed").addEventListener("keydown", e => { if (e.key === "Enter") run(true); });
}
function selectScenario(name) {
  P.scen = name;
  $$(".scen").forEach(b => b.classList.toggle("on", b.dataset.s === name));
}
function selectStrategy(k, keepValues) {
  P.strat = String(k);
  $$("#strat button").forEach(b => b.classList.toggle("on", b.dataset.k === P.strat));
  $("#customrow").classList.toggle("off", P.strat !== "custom");
  if (P.strat !== "custom" && !keepValues) {
    const [lo, hi] = Object.values(META.strategies)[+P.strat];
    $("#lo").value = lo; $("#hi").value = hi;
  }
}

// ---------------------------------------------------------------- running
async function run(autoplay) {
  const q = new URLSearchParams({ scenario: P.scen, seed: $("#seed").value || 0, lo: $("#lo").value, hi: $("#hi").value });
  const btn = $("#runbtn");
  btn.disabled = true; btn.textContent = "Simulating…"; $("#err").textContent = "";
  try {
    const r = await fetch("/api/run?" + q), d = await r.json();
    if (!r.ok) throw new Error(d.error || r.statusText);
    load(d);
    history.replaceState(null, "", "#" + q);
    setPlaying(!!autoplay);
  } catch (err) {
    $("#err").textContent = err.message;
  }
  btn.disabled = false; btn.textContent = "Run simulation";
}

function load(d) {
  RUN = prep(d);
  ST = null; P.t = 0; P.shownIdx = -2; P.follow = true; P.sel = null;
  buildCluster();
  renderStatic();
  buildLanesBase();
  P.dirty = true;
}

function renderStatic() {
  const bad = RUN.violations.length > 0;
  $("#runpill").className = "runpill" + (bad ? " bad" : "");
  $("#runpill").innerHTML = `<i class="dot"></i>${esc(RUN.scenario)} · seed ${RUN.seed} · ${RUN.timeout[0]}–${RUN.timeout[1]} ms`;
  $("#b-classic").href = RUN.timeline_url;
  $("#b-viol").hidden = !bad;
  $("#clockof").textContent = ` / ${RUN.end}`;
  // invariants
  $("#invariants").innerHTML = META.invariants.map(inv => {
    const vs = RUN.violations.filter(v => v.invariant === inv.id);
    const verdict = vs.length ? `${vs.length} violation${vs.length > 1 ? "s" : ""}, first at ${vs[0].time} ms` : "Holds for the whole run";
    return `<div class="inv ${vs.length ? "bad" : ""}" data-inv="${inv.id}">
      <div class="ic">${vs.length ? "✕" : "✓"}</div>
      <div><b>${esc(inv.name)}</b><p>${esc(inv.desc)}</p>
      <div class="verdict" ${vs.length ? `data-seek="${vs[0].time}"` : ""}>${esc(verdict)}</div></div></div>`;
  }).join("");
  $$("#invariants [data-seek]").forEach(el => el.onclick = () => seek(+el.dataset.seek));
  // metrics
  $("#metrics").innerHTML = METRIC_TILES.map(([k, label, unit]) =>
    `<div class="metric" data-tip="${esc(META.metric_help[k])}"><b>${fmt(RUN.metrics[k])}${unit && RUN.metrics[k] !== null ? `<small>${unit}</small>` : ""}</b><span>${label}</span></div>`).join("");
}

// ---------------------------------------------------------------- cluster view
const CX = 220, CY = 178, R = 132, NR = 32;
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
    svg("circle", { class: "halo", r: NR }, g);
    const grp = svg("circle", { class: "grp", r: NR + 6 }, g);
    svg("circle", { class: "sel", r: NR + 11 }, g);
    svg("circle", { class: "body", r: NR }, g);
    const name = svg("text", { class: "name", y: -2 }, g); name.textContent = "n" + i;
    const role = svg("text", { class: "role", y: 13 }, g);
    const x_ = svg("text", { class: "x", y: 11 }, g); x_.textContent = "✕";
    const meta = svg("text", { class: "meta", y: NR + 18 }, g);
    g.addEventListener("click", () => selectNode(i));
    RUN.nodeEls.push({ g, grp, role, x: x_, meta });
  }
}

function renderNodes(st) {
  RUN.nodeEls.forEach((ne, i) => {
    const role = st.alive[i] ? st.role[i] : "crashed";
    ne.g.setAttribute("class", `node ${role}${P.sel === i ? " selected" : ""}`);
    ne.role.textContent = st.alive[i] ? role : "down";
    ne.x.style.display = st.alive[i] ? "none" : "";
    ne.meta.textContent = `T${st.term[i]} · log ${st.log[i].length} · ci ${st.commit[i]}`;
    if (st.groups) { ne.grp.style.opacity = 1; ne.grp.style.stroke = GROUP[st.groups[i] % GROUP.length]; }
    else ne.grp.style.opacity = 0;
    ne.g.dataset.tip = `n${i} · ${st.alive[i] ? role : "crashed"} · term ${st.term[i]}\nlog ${st.log[i].length} entries · commit index ${st.commit[i]}` +
      (st.voted[i] !== null ? ` · voted n${st.voted[i]}` : "");
  });
  RUN.linkEls.forEach(l => {
    const cut = st.groups && st.groups[l.i] !== st.groups[l.j];
    l.el.setAttribute("class", "link" + (cut ? " cut" : ""));
  });
  const ns = $("#netstate");
  const bits = [];
  if (st.groups) {
    const gs = {};
    for (const k in st.groups) (gs[st.groups[k]] ||= []).push("n" + k);
    bits.push(`<span style="color:var(--part)">partition ${Object.values(gs).map(g => "{" + g.join(",") + "}").join(" | ")}</span>`);
  }
  if (st.net && (st.net.drop || st.net.dup || st.net.delay[1] > META.default_delay[1])) {
    bits.push(`<span style="color:var(--degraded)">drop ${Math.round(st.net.drop * 100)}% · dup ${Math.round(st.net.dup * 100)}% · delay ${st.net.delay[0]}–${st.net.delay[1]} ms</span>`);
  }
  ns.innerHTML = bits.length ? bits.join(" · ") : "network healthy";
}

function renderMsgs(t) {
  const layer = RUN.msgLayer;
  layer.textContent = "";
  const F = RUN.flights;
  // flights are sorted by t0 and last at most ~150 ms: binary search the window start
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
    const m = MSG[f.type];
    if (!f.ok && t >= f.t1) {
      const o = 1 - (t - f.t1) / fade;
      const tx = svg("text", { x, y: y + 5, "text-anchor": "middle", fill: COL.bad, "font-size": 15, "font-weight": 700, opacity: o }, layer);
      tx.textContent = "×";
      continue;
    }
    const color = !f.ok && frac > 0.7 * (f.early ? 0.3 : 1) ? COL.bad : m.c;
    svg("circle", { cx: x, cy: y, r: m.r, fill: color, class: "msg" }, layer);
  }
}

function buildLegend() {
  $("#msglegend").innerHTML = Object.values(MSG).map(m => `<span><i style="background:${m.c}"></i>${m.label}</span>`).join("") +
    `<span><b style="color:var(--bad);margin-right:5px">×</b>dropped</span>`;
}

// ---------------------------------------------------------------- logs
function renderLogs(st) {
  const box = $("#logmatrix");
  const maxLen = Math.max(1, ...st.log.map(l => l.length));
  const ref = [];
  for (let idx = 1; idx <= maxLen; idx++) {
    for (let i = 0; i < RUN.n; i++) {
      if (st.commit[i] >= idx && st.log[i].length >= idx) { ref[idx] = st.log[i][idx - 1]; break; }
    }
  }
  const avail = Math.max(120, box.clientWidth - 28 - 160);
  let w = clamp(Math.floor(avail / maxLen) - 2, 5, 26);
  let cap = Math.floor(avail / (w + 2));
  let start = 1;
  if (maxLen > cap) { start = maxLen - cap + 3; }
  const rows = [];
  for (let i = 0; i < RUN.n; i++) {
    const role = st.alive[i] ? st.role[i] : "crashed";
    let cells = start > 1 ? `<div class="cell more" data-tip="${start - 1} earlier entries hidden">+${start - 1}</div>` : "";
    for (let idx = start; idx <= st.log[i].length; idx++) {
      const [term, cmd] = st.log[i][idx - 1];
      const c = termColor(term);
      const committed = idx <= st.commit[i];
      const r = ref[idx];
      const conflict = r && (r[0] !== term || r[1] !== cmd);
      const tip = `n${i} · index ${idx} · term ${term} · ${cmd}\n${committed ? "committed" : "not committed on this node"}` +
        (conflict ? `\nCONFLICT: index ${idx} is committed elsewhere as t${r[0]} ${r[1]}` : "");
      cells += `<div class="cell${committed ? "" : " unc"}${conflict ? " conflict" : ""}" style="width:${w}px;background:${c};border-color:${c}" data-tip="${esc(tip)}"></div>`;
    }
    rows.push(`<div class="lrow${P.sel === i ? " selected" : ""}" data-node="${i}">
      <div class="lhead"><i class="${role}"></i>n${i} ${role} · T${st.term[i]}</div><div class="cells">${cells}</div></div>`);
  }
  rows.push(`<div class="lindex"><span></span><div style="display:flex;justify-content:space-between"><span>index ${start}</span><span>${maxLen}</span></div></div>`);
  box.innerHTML = rows.join("");
  $$(".lrow", box).forEach(r => r.onclick = () => selectNode(+r.dataset.node));
}

function renderSteps() {
  const box = $("#scenario-steps");
  if (!RUN.steps.length) { box.innerHTML = ""; return; }
  box.innerHTML = `<h3 style="margin-top:6px">Scenario script</h3>` + RUN.steps.map(e =>
    `<div class="step ${e.time <= P.t ? "done" : "future"}" data-t="${e.time}"><b>${e.time} ms</b>${esc(e.data.step)}</div>`).join("");
  $$(".step", box).forEach(s => s.onclick = () => seek(+s.dataset.t));
}

// ---------------------------------------------------------------- event feed
function buildChips() {
  $("#chips").innerHTML = CATS.map(([k, label, color]) =>
    `<button class="chip ${P.cats.has(k) ? "on" : ""}" data-c="${k}"><i style="background:${color ? `var(--${color})` : "#4dd0e1"}"></i>${label}</button>`).join("");
  $$("#chips .chip").forEach(c => c.onclick = () => {
    P.cats.has(c.dataset.c) ? P.cats.delete(c.dataset.c) : P.cats.add(c.dataset.c);
    c.classList.toggle("on");
    P.follow = true; renderEvents();
  });
  $("#search").addEventListener("input", e => { P.q = e.target.value.trim().toLowerCase(); P.follow = true; renderEvents(); });
  const list = $("#evlist");
  list.addEventListener("scroll", () => { P.follow = list.scrollTop + list.clientHeight >= list.scrollHeight - 30; });
  list.addEventListener("click", e => {
    const row = e.target.closest("[data-i]");
    if (row) { const ev = RUN.ev[+row.dataset.i]; P.follow = false; seek(ev.time); }
  });
}
function matches(e) {
  if (!P.cats.has(cat(e.kind))) return false;
  if (P.sel !== null && e.node !== null && e.node !== P.sel) return false;
  if (P.q && !(`${e.kind} ${e.text} n${e.node}`.toLowerCase().includes(P.q))) return false;
  return true;
}
function renderEvents() {
  if (!RUN) return;
  const list = $("#evlist");
  const last = (ST ? ST.i : 0) - 1;
  const vByI = new Map();
  RUN.violations.forEach(v => { if (!vByI.has(v.i)) vByI.set(v.i, []); vByI.get(v.i).push(v); });
  const picked = [];
  for (let k = last; k >= 0 && picked.length < 300; k--) {
    const e = RUN.ev[k];
    if (matches(e) || vByI.has(k)) picked.push(e);
  }
  picked.reverse();
  if (!picked.length) {
    list.innerHTML = `<div class="empty">${last < 0 ? "Press play to start the run." : "No events match the current filters."}</div>`;
    return;
  }
  const html = [];
  picked.forEach((e, k) => {
    const c = cat(e.kind);
    html.push(`<div class="ev${k === picked.length - 1 ? " cur" : ""}" data-i="${e.i}">
      <span class="t">${e.time} ms</span><span class="s">#${e.seq}</span><span class="n">${e.node === null ? "—" : "n" + e.node}</span>
      <span class="k-${c}">${esc(e.kind)}</span><span class="d" title="${esc(e.text)}">${esc(e.text)}</span></div>`);
    for (const v of vByI.get(e.i) || []) {
      html.push(`<div class="ev vrow" data-i="${e.i}">!!! ${esc(v.invariant)} [${v.nodes.map(x => "n" + x).join(",")}] ${esc(v.detail)}</div>`);
    }
  });
  list.innerHTML = html.join("");
  if (P.follow) list.scrollTop = list.scrollHeight;
}

function selectNode(i) {
  P.sel = P.sel === i ? null : i;
  $("#nodefilter").innerHTML = P.sel === null ? "" : `showing n${P.sel} only · <a href="#" id="clearsel">clear</a>`;
  const c = $("#clearsel");
  if (c) c.onclick = ev => { ev.preventDefault(); selectNode(P.sel); };
  P.shownIdx = -2; P.follow = true;
  buildLanesBase();
  P.dirty = true;
}

// ---------------------------------------------------------------- timeline lanes (canvas)
const LANE_H = 22, TOP = 40, PAD_L = 40, PAD_R = 14;
let laneBase = null, hatch = null, drag = false;
const laneX = (t, w) => PAD_L + (t / RUN.end) * (w - PAD_L - PAD_R);
const laneT = (x, w) => clamp((x - PAD_L) / (w - PAD_L - PAD_R) * RUN.end, 0, RUN.end);

function buildLanesBase() {
  if (!RUN) return;
  const c = $("#lanes");
  const w = c.clientWidth;
  if (!w) return;
  const h = TOP + RUN.n * LANE_H + 8;
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
    hg.strokeStyle = COL.muted; hg.globalAlpha = 0.6; hg.lineWidth = 1.2;
    hg.beginPath(); hg.moveTo(0, 6); hg.lineTo(6, 0); hg.stroke();
    hatch = hc;
  }
  g.font = `10px ${getComputedStyle(document.body).getPropertyValue("--mono") || "monospace"}`;
  // time grid
  g.textBaseline = "middle";
  for (let t = 0; t <= RUN.end; t += 250) {
    const x = laneX(t, w);
    g.fillStyle = COL.line; g.fillRect(Math.round(x), 18, 1, h - 18);
    if (t % 500 === 0) { g.fillStyle = COL.muted; g.textAlign = "center"; g.fillText(t + "", x, 9); }
  }
  // fault window
  const [f0, f1] = META.fault_window;
  g.fillStyle = COL.muted; g.globalAlpha = 0.06;
  g.fillRect(laneX(f0, w), 18, laneX(f1, w) - laneX(f0, w), h - 18);
  g.globalAlpha = 1;
  // network row: partitions, degraded network, scenario steps
  g.textAlign = "left";
  g.fillStyle = COL.muted; g.fillText("net", 6, 27);
  for (const p of RUN.parts) {
    const x0 = laneX(p.from, w), x1 = laneX(p.to, w);
    g.globalAlpha = 0.5; g.fillStyle = COL.part; g.fillRect(x0, 21, Math.max(2, x1 - x0), 12); g.globalAlpha = 1;
  }
  for (const p of RUN.nets) {
    const x0 = laneX(p.from, w), x1 = laneX(p.to, w);
    g.globalAlpha = 0.45; g.fillStyle = COL.degraded; g.fillRect(x0, 21, Math.max(2, x1 - x0), 12); g.globalAlpha = 1;
  }
  for (const e of RUN.steps) {
    const x = laneX(e.time, w);
    g.fillStyle = COL.fg; g.beginPath(); g.moveTo(x, 22); g.lineTo(x + 4, 27); g.lineTo(x, 32); g.lineTo(x - 4, 27); g.fill();
  }
  // node lanes
  for (let i = 0; i < RUN.n; i++) {
    const y = TOP + i * LANE_H;
    g.fillStyle = P.sel === i ? COL.accent : COL.fg2;
    g.fillText("n" + i, 8, y + LANE_H / 2 - 1);
    g.fillStyle = COL.follower; g.fillRect(PAD_L, y + 3, w - PAD_L - PAD_R, LANE_H - 8);
    for (const s of RUN.seg[i]) {
      const x0 = laneX(s.from, w), x1 = laneX(s.to, w), sw = Math.max(1, x1 - x0);
      if (s.role === "crashed") {
        g.fillStyle = COL.crashed; g.fillRect(x0, y + 3, sw, LANE_H - 8);
        g.fillStyle = g.createPattern(hatch, "repeat"); g.fillRect(x0, y + 3, sw, LANE_H - 8);
      } else if (s.role !== "follower") {
        g.fillStyle = s.role === "leader" ? COL.leader : COL.candidate;
        g.fillRect(x0, y + 3, sw, LANE_H - 8);
        if (s.role === "leader" && sw > 26) {
          g.fillStyle = "#06130d"; g.fillText("T" + s.term, x0 + 4, y + LANE_H / 2 - 1);
        }
      }
    }
    if (P.sel === i) { g.strokeStyle = COL.accent; g.lineWidth = 1.5; g.strokeRect(PAD_L - 0.5, y + 2.5, w - PAD_L - PAD_R + 1, LANE_H - 7); }
  }
  // violations
  const seen = new Set();
  for (const v of RUN.violations) {
    const x = Math.round(laneX(v.time, w));
    if (seen.has(x)) continue;
    seen.add(x);
    g.fillStyle = COL.bad; g.fillRect(x - 1, 18, 2, h - 18);
    g.beginPath(); g.moveTo(x - 5, 14); g.lineTo(x + 5, 14); g.lineTo(x, 21); g.fill();
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
  g.fillStyle = COL.accent; g.fillRect(x - 1, 14, 2, h - 14);
  const label = Math.round(P.t) + " ms";
  g.font = "10px monospace";
  const tw = g.measureText(label).width + 10;
  const lx = clamp(x - tw / 2, 0, w - tw);
  g.fillStyle = COL.accent; g.fillRect(lx, 0, tw, 14);
  g.fillStyle = "#fff"; g.textAlign = "left"; g.textBaseline = "middle"; g.fillText(label, lx + 5, 7);
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
      if (s) txt += ` · n${lane} ${s.role}${s.role === "crashed" ? "" : " · term " + s.term}`;
    } else if (y >= 20 && y < 34) {
      const p = RUN.parts.find(p => p.from <= t && t < p.to);
      const n = RUN.nets.find(p => p.from <= t && t < p.to);
      if (p) txt += ` · partition ${p.groups.map(g => "{" + g.map(x => "n" + x).join(",") + "}").join(" | ")}`;
      if (n) txt += ` · drop ${Math.round(n.drop * 100)}% dup ${Math.round(n.dup * 100)}% delay ${n.delay.join("–")}ms`;
    }
    const near = RUN.violations.find(v => Math.abs(laneX(v.time, r.width) - (ev.clientX - r.left)) < 4);
    if (near) txt += `\n✕ ${near.invariant}: ${near.detail}`;
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
  $("#b-play").textContent = P.playing ? "❚❚" : "▶";
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
  const ev = RUN.ev;
  if (dir > 0) {
    const e = ev.find(e => e.time > P.t && matches(e));
    seek(e ? e.time : RUN.end);
  } else {
    for (let k = ev.length - 1; k >= 0; k--) if (ev[k].time < P.t && matches(ev[k])) return seek(ev[k].time);
    seek(0);
  }
}
function nextViolation() {
  if (!RUN || !RUN.violations.length) return;
  const v = RUN.violations.find(v => v.time > P.t) || RUN.violations[0];
  P.follow = true;
  seek(v.time);
}
function bindTransport() {
  $("#b-play").onclick = () => setPlaying(!P.playing);
  $("#b-start").onclick = () => seek(0);
  $("#b-end").onclick = () => seek(RUN ? RUN.end : 0);
  $("#b-prev").onclick = () => stepEvent(-1);
  $("#b-next").onclick = () => stepEvent(1);
  $("#b-viol").onclick = nextViolation;
  $("#speed").onchange = e => { P.rate = +e.target.value; };
  $("#b-link").onclick = async () => {
    const url = `${location.origin}/${location.hash.split("&t=")[0]}&t=${Math.round(P.t)}`;
    try { await navigator.clipboard.writeText(url); toast("Link copied: it reopens this run at " + Math.round(P.t) + " ms"); }
    catch { toast(url); }
  };
}
function bindKeys() {
  document.addEventListener("keydown", e => {
    if (e.target.matches("input, select, textarea") || $("#view-sim").hidden) return;
    if (e.key === " ") { e.preventDefault(); setPlaying(!P.playing); }
    else if (e.key === "ArrowRight") { e.preventDefault(); stepEvent(1); }
    else if (e.key === "ArrowLeft") { e.preventDefault(); stepEvent(-1); }
    else if (e.key === "Home") seek(0);
    else if (e.key === "End") seek(RUN ? RUN.end : 0);
    else if (e.key === "v" || e.key === "V") nextViolation();
  });
}

function frame() {
  const st = stateAt(P.t);
  if (st.i !== P.shownIdx) {
    P.shownIdx = st.i;
    renderNodes(st); renderLogs(st); renderEvents(); renderSteps();
    $$("#invariants .inv").forEach(card => {
      const first = RUN.violations.find(v => v.invariant === card.dataset.inv);
      card.classList.toggle("pending", !!first && first.time > P.t);
    });
  }
  if (P.t !== P.drawnT || P.dirty) {
    renderMsgs(P.t);
    drawLanes();
    $("#clock").textContent = P.t.toFixed(1);
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
  t.textContent = text; t.style.whiteSpace = "pre-line"; t.hidden = false;
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
  $$(".views button").forEach(b => b.classList.toggle("on", b.dataset.view === v));
  $("#view-sim").hidden = v !== "sim";
  $("#view-cmp").hidden = v !== "cmp";
  $("#view-find").hidden = v !== "find";
  if (v === "find") loadFindings();
  if (v === "sim") { buildLanesBase(); P.shownIdx = -2; }
  hideTip();
}
function bindViews() { $$(".views button").forEach(b => b.onclick = () => showView(b.dataset.view)); }
function initTheme() {
  const saved = store.get("lab-theme");
  const pref = saved || (matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark");
  document.documentElement.dataset.theme = pref;
  readColors();
  $("#theme").onclick = () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    store.set("lab-theme", next);
    readColors(); hatch = null; buildLanesBase(); renderCompare();
  };
}

// ---------------------------------------------------------------- compare
const SHORT = { baseline: "Base", lossy: "Lossy", slow: "Slow", leader_crash: "Crash", partition_minority_leader: "Minority",
  flapping_partition: "Flap", crash_restart_storm: "Storm", figure8: "Fig 8" };
const CHARTS = [["leaderless_ms", "Time without a leader", "ms, lower is better", true],
  ["max_gap_ms", "Longest leaderless gap", "ms, lower is better", true],
  ["elections", "Elections started", "per run, lower is better", true],
  ["commits", "Entries committed", "per run, higher is better", false],
  ["latency_ms", "Commit latency", "ms, lower is better", true],
  ["lost", "Lost client writes", "per run, lower is better", true]];
const LOWER = new Set(["violations", "first_leader_ms", "leaderless_ms", "max_gap_ms", "elections", "max_term", "latency_ms", "lost", "rejects"]);
const METRIC_LABEL = { violations: "Violations", first_leader_ms: "First leader", leaderless_ms: "No leader", max_gap_ms: "Longest gap",
  elections: "Elections", max_term: "Max term", commits: "Committed", latency_ms: "Latency", lost: "Lost", rejects: "Rejected" };
let CMP = null;

function buildCompareForm() {
  $("#cmpscen").innerHTML = META.scenarios.map(s =>
    `<button class="chip on" data-s="${esc(s.name)}">${esc(s.title)}</button>`).join("");
  $$("#cmpscen .chip").forEach(c => c.onclick = () => { c.classList.toggle("on"); cmpCount(); });
  ["s0", "s1"].forEach(id => $("#" + id).addEventListener("input", cmpCount));
  $("#cmpbtn").onclick = runCompare;
  cmpCount();
}
function cmpSel() { return $$("#cmpscen .chip.on").map(c => c.dataset.s); }
function cmpCount() {
  const k = cmpSel().length, s = Math.max(0, +$("#s1").value - +$("#s0").value + 1);
  const runs = 2 * k * s;
  $("#cmpcount").textContent = `${runs} simulations · about ${Math.max(1, Math.round(runs * 0.035))} s`;
  return runs;
}
async function runCompare() {
  const scen = cmpSel();
  if (!scen.length) { $("#cmperr").textContent = "Pick at least one scenario."; return; }
  const runs = cmpCount(), est = Math.max(0.5, runs * 0.035);
  const q = new URLSearchParams({ seeds: `${$("#s0").value}..${$("#s1").value}`, scenarios: scen.join(",") });
  const btn = $("#cmpbtn"), prog = $("#cmpprog"), bar = $("i", prog);
  btn.disabled = true; btn.textContent = "Running…"; $("#cmperr").textContent = "";
  prog.hidden = false; bar.style.width = "0";
  const t0 = performance.now();
  const tick = setInterval(() => { bar.style.width = Math.min(92, (performance.now() - t0) / 1000 / est * 92) + "%"; }, 200);
  try {
    const r = await fetch("/api/compare?" + q), d = await r.json();
    if (!r.ok) throw new Error(d.error || r.statusText);
    CMP = d;
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
  const strategies = Object.keys(META.strategies);
  const scen = [...new Set(CMP.rows.map(r => r.scenario))];
  const get = (s, k) => CMP.rows.find(r => r.scenario === s && r.strategy === strategies[k]).metrics;
  const runs = CMP.seeds.length * scen.length * 2;
  const totalV = CMP.rows.reduce((a, r) => a + r.metrics.violations, 0);
  const avg = (k, m) => { const v = scen.map(s => get(s, k)[m]).filter(x => x !== null); return v.reduce((a, b) => a + b, 0) / (v.length || 1); };
  let wins = [0, 0];
  for (const s of scen) for (const m of CMP.metrics) {
    if (m === "violations") continue;
    const a = get(s, 0)[m], b = get(s, 1)[m];
    if (a === null || b === null || a === b) continue;
    const aBetter = LOWER.has(m) ? a < b : a > b;
    wins[aBetter ? 0 : 1]++;
  }
  const head = `<div class="headline">
    <div class="panel hl ${totalV ? "bad" : "good"}"><b>${totalV}</b><span>safety violations in ${runs} simulations</span></div>
    <div class="panel hl"><b>${fmt(avg(0, "leaderless_ms"))} <small style="font-size:14px;color:var(--muted)">vs</small> ${fmt(avg(1, "leaderless_ms"))}</b><span>avg ms without a leader: ${esc(strategies[0])} vs ${esc(strategies[1])}</span></div>
    <div class="panel hl"><b>${fmt(avg(0, "commits"))} <small style="font-size:14px;color:var(--muted)">vs</small> ${fmt(avg(1, "commits"))}</b><span>avg entries committed per run</span></div>
    <div class="panel hl"><b>${wins[0]} : ${wins[1]}</b><span>measures won, ${esc(strategies[0])} : ${esc(strategies[1])}</span></div>
  </div>`;
  const key = `<div class="keyline"><span><i class="serA" style="background:var(--accent)"></i>${esc(strategies[0])}</span>
    <span><i style="background:var(--degraded)"></i>${esc(strategies[1])}</span><span>Seeds ${CMP.seeds[0]}–${CMP.seeds[CMP.seeds.length - 1]}</span></div>`;
  const charts = CHARTS.map(([m, title, sub]) => chartSvg(m, title, sub, scen, get, strategies)).join("");
  const th = `<tr><th>Scenario</th><th>Strategy</th>${CMP.metrics.map(m => `<th data-tip="${esc(CMP.metric_help[m])}">${METRIC_LABEL[m] || m}</th>`).join("")}</tr>`;
  let body = "";
  for (const s of scen) {
    for (let k = 0; k < 2; k++) {
      const mine = get(s, k), other = get(s, 1 - k);
      body += `<tr class="${k === 0 ? "first" : ""}"><td>${k === 0 ? esc(META.scenarios.find(x => x.name === s).title) : ""}</td><td>${esc(strategies[k])}</td>` +
        CMP.metrics.map(m => {
          const a = mine[m], b = other[m];
          let cls = "";
          if (m === "violations") cls = a ? "bad" : "";
          else if (a !== null && b !== null && a !== b && (LOWER.has(m) ? a < b : a > b)) cls = "better";
          return `<td class="${cls}">${fmt(a)}</td>`;
        }).join("") + "</tr>";
    }
  }
  $("#cmpout").innerHTML = head + key + `<div class="charts">${charts}</div>
    <div class="panel"><div class="panel-h"><span class="ttl">All measures</span><span class="sub">green = better of the two · hover a column for its meaning</span></div>
    <div class="tablewrap"><table class="cmp"><thead>${th}</thead><tbody>${body}</tbody></table></div></div>`;
}
function chartSvg(m, title, sub, scen, get, strategies) {
  const W = 440, H = 190, L = 40, B = 26, T = 10, Rr = 8;
  const vals = scen.flatMap(s => [get(s, 0)[m], get(s, 1)[m]]).map(v => v ?? 0);
  const ymax = niceMax(Math.max(...vals));
  const iw = W - L - Rr, ih = H - T - B, band = iw / scen.length, bw = Math.min(18, band / 2 - 4);
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
      const x = k === 0 ? cx - bw - 1 : cx + 1;
      g += `<rect class="${k ? "serB" : "serA"}" x="${x}" y="${T + ih - hgt}" width="${bw}" height="${Math.max(hgt, v ? 1 : 0)}" rx="2"
        data-tip="${esc(SHORT[s] || s)} · ${esc(strategies[k])}: ${fmt(v)}"></rect>`;
    });
    g += `<text x="${cx}" y="${H - 8}" text-anchor="middle">${esc(SHORT[s] || s)}</text>`;
  });
  return `<div class="panel chart"><h4>${esc(title)}</h4><p>${esc(sub)}</p><svg viewBox="0 0 ${W} ${H}">${g}</svg></div>`;
}

// ---------------------------------------------------------------- findings
let findingsLoaded = false;
async function loadFindings() {
  if (findingsLoaded) return;
  try {
    const d = await (await fetch("/api/findings")).json();
    const { html, toc } = renderMarkdown(d.markdown);
    $("#findings").innerHTML = html;
    $("#toc").innerHTML = `<h3 style="padding:0 10px">Contents</h3>` + toc.map(([id, t]) => `<a href="#" data-to="${id}">${esc(t)}</a>`).join("");
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
    META = await (await fetch("/api/meta")).json();
  } catch (err) {
    $("#err").textContent = "Cannot reach the lab server: " + err.message;
    return;
  }
  buildSidebar(); buildChips(); buildLegend(); buildCompareForm(); bindTransport(); bindKeys(); bindViews(); bindLanes(); bindTip();
  new ResizeObserver(() => { P.shownIdx = -2; }).observe($("#logmatrix"));
  const h = new URLSearchParams(location.hash.slice(1));
  selectScenario(META.scenarios.some(s => s.name === h.get("scenario")) ? h.get("scenario") : "leader_crash");
  $("#seed").value = h.get("seed") || 0;
  const lo = h.get("lo"), hi = h.get("hi");
  const k = Object.values(META.strategies).findIndex(([a, b]) => String(a) === lo && String(b) === hi);
  if (lo && hi && k < 0) { $("#lo").value = lo; $("#hi").value = hi; selectStrategy("custom", true); }
  else selectStrategy(k < 0 ? "0" : String(k));
  const t = h.get("t");
  await run(t === null);
  if (t !== null && RUN) seek(+t);
  requestAnimationFrame(loop);
}
init();
