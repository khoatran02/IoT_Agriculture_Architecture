"use strict";
// Dashboard: follows one sample through every stage of the architecture. All data arrives from
// /ws (live events) plus /api/* (backfill after load/reconnect). Rendering is throttled; the
// pipeline strip and overview cards update in place so animations and scroll positions survive.

const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
const fmtInt = n => n == null ? "–" : Number(n).toLocaleString();
const fmtT = t => { const d = new Date(t * 1000); return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0"); };
const ms = (a, b) => a == null || b == null ? null : (b - a) * 1000;
const fmtMs = v => v == null ? "–" : Math.abs(v) >= 1000 ? (v / 1000).toFixed(2) + " s" : Math.abs(v) < 10 ? v.toFixed(2) + " ms" : v.toFixed(1) + " ms";
const simTime = ts => (ts || "").replace("T", " ");
const compact = p => { const s = typeof p === "string" ? p : JSON.stringify(p); return s.length > 140 ? s.slice(0, 140) + "…" : s; };

const S = {
  snap: null, sensors: [], labels: {}, rules: null, nodeIds: {}, site: "",
  cycles: [], byKey: new Map(), pinned: null, sample: null, hist: [],
  tap: {}, tapFilter: "telemetry", tapFrozen: false, tapScrolled: false, tapOpen: new Set(), tapTimes: [],
  acks: new Map(), cmds: [], relays: {}, acts: {}, nodes: {}, gateway: null,
  stage: "sensors", counts: {}, db: {}, mqtt: {total: 0, by_kind: {}}, wsClients: 0, ingest: {}, model: "",
};
const labelOf = name => S.labels[name] || name;
// One ring per message kind (same policy as the server): telemetry is ~10x busier than everything else.
function pushTap(m) {
  const list = S.tap[m.kind] || (S.tap[m.kind] = []);
  if (list.length && m.id <= list[list.length - 1].id) return false;
  list.push(m);
  if (list.length > (m.kind === "telemetry" ? 300 : 100)) list.shift();
  return true;
}
// MQTT messages per second over the last 3 s, as seen by this page.
function rate() {
  const now = performance.now();
  while (S.tapTimes.length && now - S.tapTimes[0] > 3000) S.tapTimes.shift();
  return S.tapTimes.length / 3;
}

// ---------------------------------------------------------------------------------------------- API
async function api(path, method = "GET", body) {
  const res = await fetch(path, {method, headers: body ? {"Content-Type": "application/json"} : undefined,
                                 body: body ? JSON.stringify(body) : undefined});
  if (!res.ok) {
    let detail = res.statusText;
    try { const d = (await res.json()).detail; detail = typeof d === "string" ? d : JSON.stringify(d); } catch (e) { /* keep statusText */ }
    throw new Error(detail);
  }
  return res.json();
}
const toast = msg => { $("err").textContent = msg || ""; };
const control = body => api("/api/gateway/control", "POST", body).then(() => toast("")).catch(e => toast("Gateway control: " + e.message));

// ------------------------------------------------------------------------------------- cycle store
const keyOf = c => c.run_id + ":" + c.seq;
function addCycle(c) {
  if (S.byKey.has(keyOf(c))) return false;
  S.cycles.push(c); S.byKey.set(keyOf(c), c);
  while (S.cycles.length > 100) S.byKey.delete(keyOf(S.cycles.shift()));
  return true;
}
function cur() {
  if (S.pinned && !S.byKey.has(S.pinned)) S.pinned = null;
  return (S.pinned && S.byKey.get(S.pinned)) || S.cycles[S.cycles.length - 1] || null;
}
const ackFor = id => S.acks.get(id);
const waiting = () => '<p class="muted">Waiting for the first sensor cycle… start the gateway (<code>scripts/run_all.sh</code>) or press Resume.</p>';

// ----------------------------------------------------------------------------------- stage views
const FILTERS = {
  all: ["All", null], telemetry: ["Telemetry", ["telemetry"]], actuation: ["Commands & ACKs", ["command", "status"]],
  nodes: ["Nodes", ["availability", "meta", "gateway-state"]], control: ["Control", ["control"]],
};

function rSensors(c) {
  const faults = S.gateway?.faults || {};
  const head = `<p class="help">Every sensor column of the CSV is replayed as its own simulated sensor. Each cycle the gateway reads dataset
    row <b>${fmtInt(c?.row)}</b> of ${fmtInt(c?.rows)}; each sensor answers with its column's value for that row over its own bus.
    Inject a fault to watch step 4 clean it up.</p>`;
  if (!c) return head + waiting();
  const rows = S.sensors.map(m => {
    const r = c.sensors.find(x => x.name === m.name), f = faults[m.name], v = r?.raw;
    const [lo, hi] = m.valid_range || [-Infinity, Infinity];
    const glitch = v != null && (v < lo || v > hi);
    return `<tr><td><b>${esc(m.name)}</b></td><td class="mono">${esc(m.column)}</td><td>${esc(m.interface)}</td>
      <td class="num ${glitch ? "warn" : ""}">${v == null ? '<span class="bad">no response</span>' : esc(v) + " " + esc(m.unit) + (glitch ? " (glitch)" : "")}</td>
      <td>${f ? `<span class="chip warn">${esc(f.kind)} · ${f.cycles} left</span>` : ""}
        <button class="sm" data-fault="dropout" data-sensor="${esc(m.name)}">Drop ×3</button>
        <button class="sm" data-fault="spike" data-sensor="${esc(m.name)}">Spike ×3</button></td></tr>`;
  }).join("");
  return head + `<div class="scroll"><table><thead><tr><th>Sensor</th><th>CSV column</th><th>Bus</th><th class="num">Value read (row ${fmtInt(c.row)})</th><th>Inject fault</th></tr></thead>
    <tbody>${rows}</tbody></table></div>
    <p class="small muted">Drop = the sensor stops answering for 3 cycles. Spike = it reports an impossible value for 3 cycles.
    <button class="sm" data-clear-faults="1">Clear faults</button></p>`;
}

function rGateway(c) {
  const g = S.gateway || {}, online = S.nodes[S.nodeIds.gateway];
  const lastTel = (S.tap.telemetry || []).slice(-1)[0];
  return `<p class="help">The gateway (an ESP32 / Raspberry Pi in the real farm) polls all sensors once per cycle and publishes one JSON message
    per sensor to the broker, tagged with a <code>run_id</code> + <code>seq</code> so the platform can group them and drop duplicates.</p>
    <dl class="kv">
      <dt>Node</dt><dd>${esc(S.nodeIds.gateway)} <span class="dot ${online === "online" ? "up" : "down"}"></span>${esc(online || "unknown")}</dd>
      <dt>State</dt><dd>${g.paused ? "⏸ paused" : "▶ running"} · ${esc(g.interval_s ?? "–")} s per cycle${g.loop === false ? " · no loop" : ""}</dd>
      <dt>Run id</dt><dd class="mono">${esc(g.run_id || c?.run_id || "–")}</dd>
      <dt>Next dataset row</dt><dd>${fmtInt(g.row)} of ${fmtInt(g.rows)}</dd>
      <dt>Next seq</dt><dd>${fmtInt(g.seq)}</dd>
      <dt>Active faults</dt><dd>${Object.keys(g.faults || {}).length ? Object.entries(g.faults).map(([k, f]) => `<span class="chip warn">${esc(k)}: ${esc(f.kind)} (${f.cycles} left)</span>`).join("") : "none"}</dd>
      <dt>Publishes to</dt><dd class="mono">agri/${esc(S.site)}/sensor/&lt;name&gt; · QoS 1</dd>
    </dl>
    <p class="help" style="margin-top:10px">Last telemetry message on the wire:</p>
    ${lastTel ? `<pre>${esc(JSON.stringify(lastTel.payload, null, 2))}</pre>` : '<p class="muted">none yet</p>'}`;
}

function tapHtml() {
  return `<div class="tapbar">
      ${Object.entries(FILTERS).map(([k, [label]]) => `<button class="sm ${S.tapFilter === k ? "active" : ""}" data-filter="${k}">${label}</button>`).join("")}
      <span class="grow"></span><span class="muted small" id="tap-stats"></span>
      <button class="sm" id="tap-freeze">${S.tapFrozen ? "▶ Live" : "⏸ Freeze"}</button></div>
    <div id="tap-scroll"><table><thead><tr><th>Time</th><th>From</th><th>Topic</th><th>Kind</th><th class="num">Transit</th><th>Payload</th></tr></thead>
    <tbody id="tap-body"></tbody></table></div>`;
}
function tapBodyHtml() {
  const kinds = FILTERS[S.tapFilter][1];
  const rows = (kinds || Object.keys(S.tap)).flatMap(k => S.tap[k] || []).sort((a, b) => a.id - b.id).slice(-150).reverse();
  if (!rows.length) return '<tr><td colspan="6" class="muted">No messages of this kind yet.</td></tr>';
  return rows.map(m => `<tr class="row" data-id="${m.id}"><td class="mono">${fmtT(m.t)}</td><td>${esc(m.from)}</td>
      <td class="mono" title="${esc(m.topic)}">${esc(m.topic.split("/").slice(2).join("/"))}</td>
      <td><span class="kind ${esc(m.kind)}">${esc(m.kind)}</span> QoS ${m.qos}${m.retain ? " · retained" : ""}</td>
      <td class="num">${m.latency_ms == null ? "–" : fmtMs(m.latency_ms)}</td><td class="pl">${esc(compact(m.payload))}</td></tr>` +
    (S.tapOpen.has(m.id) ? `<tr class="detail"><td colspan="6"><pre>${esc(JSON.stringify(m.payload, null, 2))}</pre></td></tr>` : "")).join("");
}
function updateTap(force) {
  const body = $("tap-body");
  if (!body || (!force && (S.tapFrozen || S.tapScrolled))) return;
  body.innerHTML = tapBodyHtml();
  const paused = S.tapFrozen ? " · frozen" : S.tapScrolled ? " · paused while scrolled" : "";
  $("tap-stats").textContent = `${fmtInt(S.mqtt.total)} messages seen · ${rate().toFixed(1)} msg/s${paused}`;
}
function rBroker(which) {
  const help = which === "mqtt-in"
    ? "Telemetry travels gateway → broker → platform. This is a live tap on the broker: a separate subscriber to <code>agri/" + esc(S.site) + "/#</code>, so it shows what any subscriber would receive. “Transit” is publish-to-receive time. Click a row for the full JSON."
    : "Commands travel rule engine → broker → controller; the controller answers on the retained <code>…/status</code> topic, which doubles as the ACK (its <code>cmd_id</code> matches the command). Click a row for the full JSON.";
  return `<p class="help">${help}</p>` + tapHtml();
}

function rIngestion(c) {
  const head = `<p class="help">The platform groups the per-sensor messages of one gateway cycle into a sample, drops duplicate deliveries,
    range-checks every value, and fills missing or impossible readings with the last good one so the model always gets a complete input.</p>`;
  if (!c) return head + waiting();
  const t0 = c.times.gateway ?? c.times.first_recv, st = c.ingestion_stats || {};
  const got = c.sensors.filter(s => s.raw != null).length;
  const rows = c.sensors.map(r => {
    const m = S.sensors.find(x => x.name === r.name) || {}, [lo, hi] = m.valid_range || [null, null];
    const fixed = r.quality !== "ok";
    const check = r.raw == null ? '<span class="bad">no reading</span>' : r.quality === "imputed_range" ? `<span class="bad">✗ outside [${lo}, ${hi}]</span>` : `<span class="ok">✓ within [${lo}, ${hi}]</span>`;
    return `<tr><td><b>${esc(r.name)}</b></td><td class="num">${r.raw == null ? "–" : esc(r.raw)}</td><td class="num">${r.recv_t == null ? "–" : "+" + fmtMs(ms(t0, r.recv_t))}</td>
      <td>${check}</td><td class="num"><b>${esc(r.value)}</b> ${esc(r.unit)}</td>
      <td>${fixed ? `<span class="chip warn">filled with last good value</span>` : '<span class="ok">used as received</span>'}</td></tr>`;
  }).join("");
  return head + `<p>Cycle <b>#${c.seq}</b> (run <span class="mono">${esc(c.run_id)}</span>): <b>${got}/${c.sensors.length}</b> readings arrived within
    ${fmtMs(ms(c.times.first_recv, c.times.last_recv))}, sample complete ${fmtMs(ms(t0, c.times.ingested))} after the gateway published.</p>
    <div class="scroll"><table><thead><tr><th>Sensor</th><th class="num">Received</th><th class="num">Arrived</th><th>Range check</th><th class="num">Value used</th><th>Result</th></tr></thead><tbody>${rows}</tbody></table></div>
    <div class="chips"><span class="muted small">Counters since start: </span>${["samples", "duplicate", "dropped", "malformed", "unknown_sensor"].map(k => `<span class="chip ${k !== "samples" && st[k] ? "warn" : ""}">${k.replace("_", " ")}: ${fmtInt(st[k] || 0)}</span>`).join("")}</div>`;
}

function rInference(c) {
  const head = `<p class="help">The model gets the cleaned sample as a feature vector and returns, for each actuator, the probability that it should be ON.
    It only <i>proposes</i>: the rule engine (step 6) decides what actually happens.</p>`;
  if (!c) return head + waiting();
  const p = c.prediction;
  const inputs = `<div class="chips">${c.sensors.map(s => {
    const ood = p?.details?.out_of_range_features?.includes(s.name);
    return `<span class="chip ${ood ? "warn" : ""}" title="${ood ? "outside the range the model was trained on" : ""}">${esc(s.name)} = ${esc(s.value)}${ood ? " ⚠" : ""}</span>`;
  }).join("")}</div>`;
  if (!p) return head + `<p class="bad"><b>The model failed on this cycle</b> — no prediction. Safety interlocks (step 6) still ran on the sample.</p>
    <p class="small muted">Input</p>${inputs}`;
  const d = p.details || {};
  const bars = Object.entries(p.scores || {}).map(([name, v]) => `<tr><td><b>${esc(labelOf(name))}</b></td>
    <td style="width:45%"><div class="bar ${p.outputs[name] === "ON" ? "on" : ""}"><i style="width:${Math.round(v * 100)}%"></i></div></td>
    <td class="num">${(v * 100).toFixed(1)}%</td><td><span class="state ${p.outputs[name]}">${esc(p.outputs[name])}</span></td></tr>`).join("");
  return head + `<dl class="kv"><dt>Model</dt><dd>${esc(p.model)}${d.variant ? " · " + esc(d.variant) : ""}</dd>
      <dt>Inference time</dt><dd>${d.inference_ms == null ? fmtMs(ms(c.times.infer_start, c.times.infer_end)) : fmtMs(d.inference_ms)}</dd>
      <dt>Input data status</dt><dd class="${d.data_status && d.data_status !== "ok" ? "warn" : ""}">${esc(d.data_status || "–")}</dd></dl>
    <p class="small muted" style="margin-bottom:0">Input (cleaned sample)</p>${inputs}
    <p class="small muted" style="margin-bottom:2px">Output: P(ON) per actuator → proposed state</p>
    <table><tbody>${bars}</tbody></table>
    <p class="small muted" style="margin-bottom:0">Model details</p><pre>${esc(JSON.stringify(d, null, 2))}</pre>`;
}

function actionText(d) {
  switch (d.action) {
    case "sent": return `<span class="ok">command <span class="mono">${esc(d.cmd_id)}</span> sent</span>`;
    case "unchanged": return '<span class="muted">no change</span>';
    case "held": return '<span class="warn">held — minimum dwell time not reached</span>';
    case "interlock": return `<span class="bad">forced by interlock</span>${d.cmd_id ? ` · command <span class="mono">${esc(d.cmd_id)}</span>` : ""}`;
    case "manual": return '<span class="warn">manual override — model ignored</span>';
    default: return '<span class="muted">no model output</span>';
  }
}
function rRules(c) {
  const r = S.rules || {};
  const head = `<p class="help">The rule engine turns the model's proposal into a command. Priority: <b>interlock &gt; manual override &gt; model</b>.
    In auto mode an actuator must hold its state for the minimum dwell time (in dataset time) so it can't chatter.
    Commands are only sent when the decision changes, and re-sent until the controller confirms.</p>
    <div class="chips"><span class="chip">min dwell ${esc(r.min_dwell_minutes)} sim-min</span><span class="chip">ACK timeout ${esc(r.ack_timeout_s)} s</span>
    <span class="chip">override expires after ${esc(r.override_ttl_s)} s</span>
    ${(r.interlocks || []).map(i => `<span class="chip warn">interlock: ${esc(labelOf(i.actuator))} → ${esc(i.force)} unless ${esc(i.sensor)} ≥ ${esc(i.min ?? "")}${i.max != null ? ` and ≤ ${esc(i.max)}` : ""}</span>`).join("")}</div>`;
  if (!c) return head + waiting();
  const rows = c.decisions.map(d => {
    const sc = c.prediction?.scores?.[d.actuator];
    const il = d.interlocks.length ? d.interlocks.map(i => i.tripped
      ? `<div class="bad">✗ ${esc(i.sensor)} = ${esc(i.value)} (needs ${esc(i.rule.replace(i.sensor + " ", ""))}) → force ${esc(i.force)}</div>`
      : `<div class="ok">✓ ${esc(i.sensor)} = ${esc(i.value)} (${esc(i.rule.replace(i.sensor + " ", ""))})</div>`).join("") : '<span class="muted">none for this actuator</span>';
    const dw = d.dwell ? (d.dwell.ok ? `<span class="ok">✓ ${d.dwell.elapsed_min == null ? "first decision" : d.dwell.elapsed_min.toFixed(0) + " min since last change"}</span>`
      : `<span class="warn">✗ ${d.dwell.elapsed_min.toFixed(0)} of ${d.dwell.min} min</span>`) : '<span class="muted">–</span>';
    return `<tr><td><b>${esc(labelOf(d.actuator))}</b></td><td>${esc(d.mode)}</td>
      <td>${d.proposed ? `<span class="state ${d.proposed}">${esc(d.proposed)}</span>` : "–"}${sc == null ? "" : ` <span class="muted small">${(sc * 100).toFixed(0)}%</span>`}</td>
      <td>${il}</td><td>${dw}</td><td><span class="state ${d.desired}">${esc(d.desired ?? "–")}</span></td><td>${actionText(d)}</td></tr>`;
  }).join("");
  return head + `<div class="scroll"><table><thead><tr><th>Actuator</th><th>Mode</th><th>Model says</th><th>Interlock check</th><th>Min dwell</th><th>Decision</th><th>Outcome</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}

function hops(cmd) {
  const a = ackFor(cmd.cmd_id);
  // A replayed ACK is the controller's retained status delivered after the platform restarted: the controller-side
  // hops are real, but "ACK → platform" would just measure how long the platform was down.
  if (a && a.cmd_ts) return {h: [ms(a.cmd_ts, a.recv_ts), ms(a.recv_ts, a.applied_ts), a.replayed ? null : ms(a.applied_ts, a.ack_at)],
    total: a.replayed ? null : ms(a.cmd_ts, a.ack_at), replayed: !!a.replayed, err: a.ok === false ? a.error : null};
  return {h: [null, null, null], total: cmd.acked_at && cmd.sent_at ? (cmd.acked_at - cmd.sent_at) * 1000 : null, err: null};
}
function rController() {
  const online = S.nodes[S.nodeIds.controller];
  const rows = S.cmds.slice(0, 12).map(cmd => {
    const x = hops(cmd);
    return `<tr><td class="mono">${esc(cmd.cmd_id)}</td><td>${esc(labelOf(cmd.actuator))} → <span class="state ${cmd.state}">${esc(cmd.state)}</span></td><td>${esc(cmd.source)}</td>
      <td class="num">${fmtMs(x.h[0])}</td><td class="num">${fmtMs(x.h[1])}</td><td class="num">${x.replayed ? '<span class="warn" title="platform was down when the ACK was sent">replayed</span>' : fmtMs(x.h[2])}</td>
      <td class="num">${x.err ? `<span class="bad">${esc(x.err)}</span>` : x.replayed ? '<span class="warn">ACK replayed</span>' : x.total == null ? '<span class="warn">awaiting ACK</span>' : "<b>" + fmtMs(x.total) + "</b>"}</td></tr>`;
  }).join("");
  return `<p class="help">The controller (<b>${esc(S.nodeIds.controller)}</b> <span class="dot ${online === "online" ? "up" : "down"}"></span>${esc(online || "unknown")})
    subscribes to <code>…/actuator/+/cmd</code>, waits for the relay to switch, then publishes a retained status carrying the command's
    <code>cmd_id</code> — that is the ACK. Each hop is timed with timestamps the controller echoes back.</p>
    <div class="scroll"><table><thead><tr><th>Command</th><th>Action</th><th>Source</th><th class="num">MQTT → controller</th><th class="num">Relay switching</th><th class="num">ACK → platform</th><th class="num">Round trip</th></tr></thead>
    <tbody>${rows || '<tr><td colspan="7" class="muted">No commands yet.</td></tr>'}</tbody></table></div>`;
}

function rRelays() {
  const cards = Object.keys(S.labels).map(name => {
    const r = S.relays[name] || {}, a = S.acts[name] || {}, on = r.state === "ON";
    const pending = a.desired && a.confirmed !== a.desired;
    return `<div class="card"><div class="row"><b>${esc(labelOf(name))}</b><span class="state ${esc(r.state)}">${esc(r.state || "?")}</span></div>
      <p style="margin:6px 0 0"><span class="led ${on ? "on" : ""}"></span>GPIO ${esc(r.gpio ?? "–")} = <b>${esc(r.level || "–")}</b></p>
      <p class="small muted" style="margin:4px 0 0">platform wants ${esc(a.desired ?? "–")} · confirmed ${esc(a.confirmed ?? "–")}${pending ? ' · <span class="warn">waiting for ACK</span>' : ""}<br>mode ${esc(a.mode || "–")}${r.t ? " · last status " + fmtT(r.t) : ""}</p></div>`;
  }).join("");
  return `<p class="help">Each actuator is a relay on a GPIO pin. The controller reports the relay's real level, and the platform only treats a command as done
    once that status comes back (it re-sends if not). The status is retained, so a restarted platform learns the physical state immediately.</p><div class="grid acts">${cards}</div>`;
}

function rDb(c) {
  const d = S.db, i = S.dbInfo || {}, w = c?.db;
  const wrote = w ? `sample @ ${fmtT(w.sample_at)}${w.prediction_at ? `, prediction @ ${fmtT(w.prediction_at)}` : ", no prediction"}${c.commands.length ? `, ${c.commands.length} command(s)` : ""}` : "–";
  return `<p class="help">Every sample, prediction, command and ACK is written to InfluxDB as a time-stamped point, so history survives a restart and feeds the charts.
    Writes are batched in the background, so the control loop never waits for the database; the newest fraction of a second may not be queryable yet.</p>
    <dl class="kv"><dt>Server</dt><dd>${esc(i.engine || "InfluxDB")} · <span class="mono">${esc(i.url)}</span> <span class="dot ${i.connected ? "up" : "down"}"></span>${i.connected ? "connected" : "<span class='bad'>not connected</span>"}</dd>
    <dt>Organisation / bucket</dt><dd class="mono">${esc(i.org)} / ${esc(i.bucket)}</dd>
    <dt>Points not stored</dt><dd class="${i.write_errors ? "bad" : ""}">${fmtInt(i.write_errors || 0)}${i.last_error ? ` — ${esc(i.last_error)}` : ""}</dd>
    <dt>sample</dt><dd>${fmtInt(d.samples)} points</dd><dt>prediction</dt><dd>${fmtInt(d.predictions)} points</dd>
    <dt>command</dt><dd>${fmtInt(d.commands)} points</dd><dt>ack</dt><dd>${fmtInt(d.acks)} points</dd><dt>actuator_status</dt><dd>${fmtInt(d.actuator_status)} points</dd>
    <dt>Selected cycle wrote</dt><dd>${wrote}</dd></dl>`;
}
function rApi() {
  const ev = Object.entries(S.counts).map(([k, n]) => `<span class="chip">${esc(k)}: ${fmtInt(n)}</span>`).join("");
  return `<p class="help">FastAPI exposes REST for state, history and commands, plus a WebSocket that pushes every event below to this page as it happens.</p>
    <dl class="kv"><dt>WebSocket clients</dt><dd>${fmtInt(S.wsClients)}</dd>
    <dt>REST</dt><dd class="mono">GET /api/state · /api/history · /api/commands · /api/cycles · /api/mqtt<br>POST /api/override · DELETE /api/override/&lt;actuator&gt;<br>POST /api/gateway/control</dd></dl>
    <p class="small muted" style="margin-bottom:2px">Events received by this page since load</p><div class="chips">${ev || "none yet"}</div>`;
}

const FLOW = [
  {id: "sensors", t: "Sensors", render: rSensors, sum: c => c ? `${c.sensors.filter(s => s.raw != null).length}/${c.sensors.length} answered · row ${fmtInt(c.row)}` : "–"},
  {id: "gateway", t: "Gateway", node: "gateway", render: rGateway, sum: c => S.gateway?.paused ? "⏸ paused" : c ? `seq ${fmtInt(c.seq)} · ${S.gateway?.interval_s ?? "?"} s/cycle` : "–"},
  {id: "mqtt-in", t: "MQTT · telemetry", broker: true, render: () => rBroker("mqtt-in"), sum: () => `${rate().toFixed(1)} msg/s · QoS 1`},
  {id: "ingestion", t: "Ingestion", node: "backend", render: rIngestion, sum: c => { if (!c) return "–"; const f = c.sensors.filter(s => s.quality !== "ok").length; return f ? `${f} reading(s) filled` : "all readings ok"; }},
  {id: "inference", t: "Inference", node: "backend", render: rInference, sum: c => !c ? "–" : c.prediction ? `${c.prediction.model} · ${fmtMs(c.prediction.details?.inference_ms ?? ms(c.times.infer_start, c.times.infer_end))}` : "model failed"},
  {id: "rules", t: "Rule engine", node: "backend", render: rRules, sum: c => !c ? "–" : c.commands.length ? `${c.commands.length} command(s) sent` : "no change"},
  {id: "mqtt-out", t: "MQTT · command", broker: true, render: () => rBroker("mqtt-out"), sum: () => { const m = S.cmds[0]; return m ? `${labelOf(m.actuator)} → ${m.state}` : "no commands yet"; }},
  {id: "controller", t: "Controller", node: "controller", render: rController, sum: () => { const m = S.cmds.find(x => hops(x).total != null); return m ? `ACK in ${fmtMs(hops(m).total)}` : "–"; }},
  {id: "relays", t: "Relays", node: "controller", render: rRelays, sum: () => Object.keys(S.labels).map(n => `${labelOf(n).split(" ")[0]} ${S.relays[n]?.state || "?"}`).join(" · ")},
];
const SERVICES = [
  {id: "db", t: "InfluxDB", status: () => S.dbInfo ? (S.dbInfo.connected ? "online" : "offline") : undefined, render: rDb,
    sum: () => S.dbInfo?.write_errors ? `⚠ ${fmtInt(S.dbInfo.write_errors)} points not stored` : `${fmtInt(S.db.samples)} samples`},
  {id: "api", t: "API · WebSocket", node: "backend", render: rApi, sum: () => `${fmtInt(S.wsClients)} client(s)`},
];
const ALL = [...FLOW, ...SERVICES];
const stageDef = id => ALL.find(s => s.id === id);

// ------------------------------------------------------------------------------------ rendering
function buildPipeline() {
  const li = (s, n) => `<li class="stage ${s.broker ? "broker" : ""}" data-stage="${s.id}" id="st-${s.id}" tabindex="0" role="button"><span class="dot" id="dot-${s.id}" ${s.node || s.status ? "" : "hidden"}></span>
    ${n ? `<span class="n">${n}</span>` : ""}<span class="t">${esc(s.t)}</span><div class="s" id="sum-${s.id}">–</div></li>`;
  $("pipeline").innerHTML = FLOW.map((s, i) => li(s, i + 1)).join("");
  $("services").innerHTML = '<span class="muted small">Platform services:</span>' + SERVICES.map(s => li(s, 0)).join("");
}
function updateStrip() {
  const c = cur();
  for (const s of ALL) {
    const el = $("sum-" + s.id); if (!el) continue;
    el.textContent = s.sum(c);
    const dot = $("dot-" + s.id);
    if (dot && (s.node || s.status)) {
      const st = s.status ? s.status() : S.nodes[S.nodeIds[s.node]];
      dot.className = "dot " + (st === "online" ? "up" : st ? "down" : ""); dot.title = (s.node ? S.nodeIds[s.node] : s.t) + ": " + (st || "unknown");
    }
    $("st-" + s.id).classList.toggle("sel", s.id === S.stage);
  }
}
const lastPulse = {};
function pulse(id) {
  const el = $("st-" + id), now = performance.now();
  if (!el || now - (lastPulse[id] || 0) < 200) return;
  lastPulse[id] = now;
  el.classList.remove("pulse"); void el.offsetWidth; el.classList.add("pulse");
}
const flow = ids => ids.forEach((id, i) => setTimeout(() => pulse(id), i * 90));

function renderInspector(force) {
  const def = stageDef(S.stage), isTap = S.stage === "mqtt-in" || S.stage === "mqtt-out";
  if (isTap && !force) return;           // the tap updates its own rows so scrolling and expanded rows survive
  const n = FLOW.findIndex(s => s.id === S.stage) + 1, c = cur();
  $("inspector").innerHTML = `<h3>${n ? `<span class="n">${n}</span>` : ""}${esc(def.t)}</h3>` + def.render(c);
  if (isTap) {
    const sc = $("tap-scroll");
    sc.onscroll = () => { const was = S.tapScrolled; S.tapScrolled = sc.scrollTop > 30; if (was && !S.tapScrolled) updateTap(); };
    updateTap(true);
  }
}
function selectStage(id) {
  S.stage = id;
  if (id === "mqtt-in") S.tapFilter = "telemetry";
  if (id === "mqtt-out") S.tapFilter = "actuation";
  S.tapScrolled = false;
  mark("strip"); renderInspector(true);
}

function renderWaterfall() {
  const c = cur(), el = $("waterfall");
  $("btn-live").disabled = !S.pinned;
  if (!c) { $("cycle-title").textContent = "no cycle yet"; el.innerHTML = ""; return; }
  const t = c.times, t0 = t.gateway ?? t.first_recv;
  const segs = [["Gateway → backend (MQTT)", t.gateway, t.first_recv, ""], ["Wait for all readings", t.first_recv, t.last_recv, ""],
    ["Ingestion · clean", t.last_recv, t.ingested, ""], ["DB write · interlocks", t.ingested, t.infer_start, ""],
    ["Inference", t.infer_start, t.infer_end, ""], ["Rule engine", t.infer_end, t.decided, ""]];
  for (const id of c.commands) {
    const d = c.decisions.find(x => x.cmd_id === id), a = ackFor(id), name = labelOf(d?.actuator);
    if (a && a.cmd_ts) segs.push([`${name}: cmd → controller`, a.cmd_ts, a.recv_ts, "cmd"], [`${name}: relay switches`, a.recv_ts, a.applied_ts, "cmd"], [`${name}: ACK → platform`, a.applied_ts, a.ack_at, "ack"]);
    else segs.push([`${name}: command, awaiting ACK`, null, null, "cmd"]);
  }
  const end = Math.max(...segs.map(s => s[2] ?? 0), t.decided), total = Math.max(end - t0, 1e-6);
  $("cycle-title").innerHTML = `<b>#${c.seq}</b> · ${esc(simTime(c.ts))} · row ${fmtInt(c.row)} · ${S.pinned ? "pinned" : "live"} · sensor → decision <b>${fmtMs(ms(t0, t.decided))}</b>`;
  el.innerHTML = segs.map(([label, a, b, cls]) => {
    const ok = a != null && b != null;
    return `<span>${esc(label)}</span><div class="track">${ok ? `<div class="seg ${cls}" style="left:${((a - t0) / total * 100).toFixed(2)}%;width:${Math.max((b - a) / total * 100, 0.8).toFixed(2)}%"></div>` : ""}</div><span class="v">${ok ? fmtMs(ms(a, b)) : "…"}</span>`;
  }).join("");
}
const cycleRow = c => {
  const fixed = c.sensors.some(s => s.quality !== "ok");
  return `<div class="cyc" data-key="${esc(keyOf(c))}"><b>#${c.seq}</b><span>${esc(simTime(c.ts).slice(5, 16))}</span>
    <span class="grow">row ${fmtInt(c.row)}${fixed ? ' · <span class="warn">⚠ cleaned</span>' : ""}${c.prediction ? "" : ' · <span class="bad">no prediction</span>'}</span>
    <span>${c.commands.length ? c.commands.length + " cmd" : ""}</span></div>`;
};
function rebuildCycleList() { $("cycle-list").innerHTML = S.cycles.slice(-60).reverse().map(cycleRow).join(""); markSelected(); }
function prependCycle(c) {
  const list = $("cycle-list"); list.insertAdjacentHTML("afterbegin", cycleRow(c));
  while (list.children.length > 60) list.lastChild.remove();
  markSelected();
}
function markSelected() {
  const k = cur() && keyOf(cur());
  for (const el of $("cycle-list").children) el.classList.toggle("sel", el.dataset.key === k);
}

function buildOverview() {
  $("sensors").innerHTML = S.sensors.map(s => `<div class="card"><div class="label">${esc(s.name)} · ${esc(s.interface)}</div>
    <div class="value" id="v-${s.name}">–<small> ${esc(s.unit)}</small></div><div class="flag" id="f-${s.name}"></div><canvas id="c-${s.name}" width="300" height="68"></canvas></div>`).join("");
  $("actuators").innerHTML = Object.keys(S.labels).map(n => `<div class="card"><div class="row"><span>${esc(labelOf(n))}</span>
    <span class="state" id="as-${n}">?<span class="label" id="ap-${n}"></span></span></div><div class="label" id="am-${n}"></div>
    <div class="btns"><button data-act="${n}" data-do="ON">Force ON</button><button data-act="${n}" data-do="OFF">Force OFF</button>
    <button data-act="${n}" data-do="auto" id="aa-${n}">Auto</button></div><div class="reason" id="ar-${n}"></div></div>`).join("");
}
function drawSpark(name) {
  const cv = $("c-" + name); if (!cv) return;
  const ctx = cv.getContext("2d"), w = cv.width, h = cv.height, ys = S.hist.map(p => p.values[name]);
  ctx.clearRect(0, 0, w, h);
  if (ys.length < 2) return;
  const lo = Math.min(...ys), hi = Math.max(...ys), span = hi - lo || 1;
  ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue("--accent").trim(); ctx.lineWidth = 2; ctx.beginPath();
  ys.forEach((y, i) => { const px = i / (ys.length - 1) * w, py = h - 4 - (y - lo) / span * (h - 8); i ? ctx.lineTo(px, py) : ctx.moveTo(px, py); });
  ctx.stroke();
}
function updateSensors() {
  const s = S.sample; if (!s) return;
  for (const m of S.sensors) {
    const v = $("v-" + m.name); if (!v) continue;
    v.firstChild.textContent = s.values[m.name];
    const q = s.quality[m.name]; $("f-" + m.name).textContent = q && q !== "ok" ? q.replace("_", " ") : "";
    drawSpark(m.name);
  }
}
function updateActuators() {
  for (const n of Object.keys(S.labels)) {
    const a = S.acts[n] || {}, st = a.confirmed ?? "?", el = $("as-" + n); if (!el) continue;
    el.className = "state " + st; el.firstChild.textContent = st;
    $("ap-" + n).textContent = a.desired && a.confirmed !== a.desired ? ` → ${a.desired} pending` : "";
    $("am-" + n).textContent = "mode: " + (a.mode || "?") + (a.mode === "manual" && a.override_until ? " until " + new Date(a.override_until * 1000).toLocaleTimeString() : "");
    $("aa-" + n).classList.toggle("active", a.mode === "auto");
    $("ar-" + n).textContent = a.reason || "";
  }
}
function renderCmds() {
  $("cmds").innerHTML = S.cmds.slice(0, 15).map(c => { const x = hops(c);
    return `<tr><td>${esc(simTime(c.sim_ts))}</td><td>${esc(labelOf(c.actuator))}</td><td>${esc(c.state)}</td><td>${esc(c.source)}</td><td>${esc(c.reason)}</td>
      <td class="num">${x.replayed ? '<span class="warn">replayed ACK</span>' : x.total == null ? "…" : fmtMs(x.total)}</td></tr>`; }).join("");
}
function updateHeader() {
  $("sim-time").textContent = S.sample ? "sim time " + simTime(S.sample.ts) : "no data yet";
  $("model").textContent = S.model ? "model: " + S.model : "";
  $("nodes").innerHTML = Object.entries(S.nodes).map(([n, s]) => `<span><span class="dot ${s === "online" ? "up" : "down"}"></span>${esc(n)}</span>`).join(" ");
}
const SPEEDS = [0.1, 0.25, 0.5, 1, 2, 5];
function updatePlayback() {
  const g = S.gateway, sel = $("speed"), want = g?.interval_s ?? 1;
  $("btn-pause").textContent = g?.paused ? "▶ Resume" : "⏸ Pause";
  $("btn-pause").classList.toggle("primary", !!g?.paused);
  const speeds = SPEEDS.includes(want) ? SPEEDS : [...SPEEDS, want].sort((a, b) => a - b);
  if (sel.options.length !== speeds.length) sel.innerHTML = speeds.map(v => `<option value="${v}">${v} s / cycle${v === 0.1 ? " (fast)" : ""}</option>`).join("");
  sel.value = String(want);
  $("progress").textContent = g && g.rows ? `next row ${fmtInt(g.row)} / ${fmtInt(g.rows)}` : "";
}

// ---------------------------------------------------------------------------------- scheduling
const dirty = new Set();
let timer = null;
function mark(...names) { names.forEach(n => dirty.add(n)); if (!timer) timer = setTimeout(flush, 100); }
function flush() {
  timer = null;
  const d = new Set(dirty); dirty.clear();
  if (d.has("strip")) updateStrip();
  if (d.has("inspector")) renderInspector();
  if (d.has("tap")) updateTap();
  if (d.has("waterfall")) renderWaterfall();
  if (d.has("overview")) { updateSensors(); updateActuators(); }
  if (d.has("cmds")) renderCmds();
  if (d.has("header")) updateHeader();
  if (d.has("playback")) updatePlayback();
}
const markAll = () => mark("strip", "inspector", "waterfall", "overview", "cmds", "header", "playback");

// ------------------------------------------------------------------------------------ data in
const normCmd = c => ({cmd_id: c.cmd_id, sent_at: c.sent_at ?? c.ts, sim_ts: c.sim_ts, actuator: c.actuator, state: c.state, source: c.source, reason: c.reason, acked_at: c.acked_at ?? null});
function applySnapshot(s) {
  Object.assign(S, {snap: s, sensors: s.sensors, labels: s.actuator_labels, rules: s.rules, nodeIds: s.node_ids, site: s.site, model: s.model,
    acts: s.actuators, nodes: s.nodes, gateway: s.gateway, relays: s.relays, db: s.db, dbInfo: s.db_info, mqtt: s.mqtt, wsClients: s.ws_clients, sample: s.sample});
  S.acks = new Map(s.acks.map(a => [a.cmd_id, a]));
  if (!$("st-sensors")) buildPipeline();
  buildOverview();
  markAll();
}

async function backfill() {
  const [hist, cmds, cycles, tap] = await Promise.all(["/api/history?limit=120", "/api/commands?limit=15", "/api/cycles?limit=60", "/api/mqtt?limit=300"].map(u => api(u)));
  S.hist = hist;
  const byCmd = new Map(S.cmds.map(c => [c.cmd_id, c]));
  cmds.forEach(c => byCmd.set(c.cmd_id, {...byCmd.get(c.cmd_id), ...normCmd(c)}));
  S.cmds = [...byCmd.values()].sort((a, b) => b.sent_at - a.sent_at).slice(0, 40);
  cycles.forEach(addCycle);
  S.cycles.sort((a, b) => a.times.decided - b.times.decided);
  const byId = new Map(Object.values(S.tap).flat().map(m => [m.id, m]));   // live events that raced the fetch
  tap.forEach(m => byId.set(m.id, m));
  S.tap = {};
  [...byId.values()].sort((a, b) => a.id - b.id).forEach(pushTap);
  S.tapTimes = [];
  rebuildCycleList(); markAll(); mark("tap");
}

function onEvent({type, data}) {
  S.counts[type] = (S.counts[type] || 0) + 1;
  switch (type) {
    case "snapshot": applySnapshot(data); backfill().catch(e => toast("Backfill failed: " + e.message)); return;
    case "sample": S.sample = data; S.hist.push({ts: data.ts, values: data.values}); S.hist = S.hist.slice(-120); S.db.samples = (S.db.samples || 0) + 1; mark("overview", "header"); break;
    case "prediction": if (data) S.db.predictions = (S.db.predictions || 0) + 1; break;
    case "cycle":
      if (addCycle(data)) { prependCycle(data); flow(["sensors", "gateway", "mqtt-in", "ingestion", "inference", "rules"]); }
      mark("strip", "waterfall"); if (!S.pinned) mark("inspector"); break;
    case "mqtt": {
      if (!pushTap(data)) break;
      S.mqtt.total = data.id; S.tapTimes.push(performance.now());
      if (data.kind === "command") flow(["rules", "mqtt-out", "controller"]);
      else if (data.kind === "status" && data.payload?.cmd_id) flow(["controller", "relays"]);
      mark("tap", "strip"); break;
    }
    case "command": if (!S.cmds.some(c => c.cmd_id === data.cmd_id)) { S.cmds.unshift(normCmd(data)); S.db.commands = (S.db.commands || 0) + 1; } mark("cmds", "strip", "inspector"); break;
    case "ack": {
      S.acks.set(data.cmd_id, data);
      const c = S.cmds.find(x => x.cmd_id === data.cmd_id); if (c) c.acked_at = data.ack_at;
      mark("cmds", "strip", "inspector", "waterfall"); break;
    }
    case "actuator": S.acts[data.name] = data; mark("overview", "strip", "inspector"); break;
    case "relay": S.relays[data.actuator] = data; mark("strip", "inspector"); break;
    case "node": S.nodes[data.node] = data.status; mark("header", "strip", "inspector"); break;
    case "gateway": S.gateway = data; mark("playback", "strip", "inspector"); break;
  }
}

// ------------------------------------------------------------------------------------- inputs
document.addEventListener("click", async e => {
  const t = e.target;
  const stage = t.closest("[data-stage]"); if (stage) return selectStage(stage.dataset.stage);
  const cyc = t.closest(".cyc"); if (cyc) { S.pinned = cyc.dataset.key; markSelected(); return mark("waterfall", "inspector", "strip"); }
  const act = t.closest("button[data-act]");
  if (act) {
    toast("");
    try {
      if (act.dataset.do === "auto") await api("/api/override/" + act.dataset.act, "DELETE");
      else await api("/api/override", "POST", {actuator: act.dataset.act, state: act.dataset.do});
    } catch (err) { toast(err.message); }
    return;
  }
  const fault = t.closest("button[data-fault]"); if (fault) return control({action: "fault", sensor: fault.dataset.sensor, kind: fault.dataset.fault, cycles: 3});
  if (t.closest("[data-clear-faults]")) return control({action: "clear_faults"});
  const filter = t.closest("button[data-filter]");
  if (filter) { S.tapFilter = filter.dataset.filter; S.tapScrolled = false; return renderInspector(true); }
  if (t.closest("#tap-freeze")) { S.tapFrozen = !S.tapFrozen; $("tap-freeze").textContent = S.tapFrozen ? "▶ Live" : "⏸ Freeze"; return updateTap(); }
  const row = t.closest("tr.row");
  if (row) { const id = Number(row.dataset.id); S.tapOpen.has(id) ? S.tapOpen.delete(id) : S.tapOpen.add(id); return updateTap(true); }
});
document.addEventListener("keydown", e => {
  const stage = e.target.closest?.("[data-stage]");
  if (stage && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); selectStage(stage.dataset.stage); }
});
$("btn-pause").onclick = () => control({action: S.gateway?.paused ? "resume" : "pause"});
$("btn-step").onclick = () => control({action: "step"});
$("speed").onchange = e => control({action: "interval", interval_s: Number(e.target.value)});
// --- light / dark theme: explicit choice is saved; with none, the OS setting applies (see style.css)
const osDark = () => !!(window.matchMedia && matchMedia("(prefers-color-scheme: dark)").matches);
const theme = () => document.documentElement.dataset.theme || (osDark() ? "dark" : "light");
function showTheme() {
  const dark = theme() === "dark";
  $("btn-theme").textContent = dark ? "☀ Light" : "🌙 Dark";
  $("btn-theme").title = dark ? "Switch to the light theme" : "Switch to the dark theme";
  mark("overview");                       // sparklines read the accent colour when drawn
}
$("btn-theme").onclick = () => {
  const next = theme() === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem("theme", next); } catch (e) { /* storage blocked: choice lasts until reload */ }
  showTheme();
};
if (window.matchMedia) matchMedia("(prefers-color-scheme: dark)").addEventListener("change", showTheme);
showTheme();
$("btn-live").onclick = () => { S.pinned = null; markSelected(); mark("waterfall", "inspector", "strip"); };

// ------------------------------------------------------------------------------------- connect
function connect() {
  const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
  ws.onopen = () => { $("ws-dot").classList.add("up"); $("ws-text").textContent = "live"; };
  ws.onclose = () => { $("ws-dot").classList.remove("up"); $("ws-text").textContent = "reconnecting"; setTimeout(connect, 2000); };
  ws.onmessage = ev => onEvent(JSON.parse(ev.data));
}
buildPipeline();
renderInspector(true);
connect();
// Resync counters that the event stream only approximates (row counts, WebSocket clients, node states).
setInterval(() => api("/api/state").then(s => { S.db = s.db; S.dbInfo = s.db_info; S.wsClients = s.ws_clients; S.nodes = s.nodes; S.mqtt = s.mqtt; mark("strip", "header"); }).catch(() => {}), 10000);
