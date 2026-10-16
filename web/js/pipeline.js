// Pipeline page (F4b): the overall status banner, four stat tiles, the latency and events/min charts, table freshness,
// the dbt tests and quality gate, the Great Expectations results on silver, and files before / after the latest
// compaction, all from data/health.json (pipeline_health). States as on Insights: skeletons + aria-busy; 404 -> "not published yet"; any other failure or a
// read that has not answered after 10 s -> a message with Retry. Status colours appear only on this page (DESIGN §2).

import { drawOnResize, durationTicks, lineChart } from "./charts.js";
import { initShell } from "./shell.js";
import { readJson } from "./util/read.js";
import { formatDateTime, formatDuration, formatShortClock, parseTime } from "./util/time.js";

const HEALTH_URL = "data/health.json";
const READ_TIMEOUT_MS = 10_000;
const SLA_S = 300;
const number = new Intl.NumberFormat("en-US");
const oneDecimal = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const STATUS = {
  ok: { word: "Healthy", result: "Passed" },
  warn: { word: "Degraded", result: "Warning" },
  fail: { word: "Failing", result: "Failed" },
};
const TEST_STATUS = { pass: "ok", warn: "warn", fail: "fail", error: "fail", skipped: "warn" };
const COMPONENT = { stream: "Stream", quality: "Data quality", maintenance: "Maintenance" };

/** A status badge: the Lucide icon for `status` (cloned from the page's templates) + its word. */
function badge(status, word) {
  const span = document.createElement("span");
  span.className = "status-badge";
  span.dataset.status = status;
  const icon = document.getElementById(`icon-${status}`)?.content.firstElementChild.cloneNode(true);
  if (icon) span.append(icon);
  span.append(word);
  return span;
}

function rowsOf(block) {
  return (block?.rows ?? []).map((r) => Object.fromEntries(block.columns.map((c, i) => [c, r[i]])));
}

function seconds(value) {
  return value === null || value === undefined ? "—" : `${oneDecimal.format(value)} s`;
}

/** What an expectation found: its unexpected rows, or its observed value with a unit (rows, or the newest row's age). */
function foundText(r) {
  if (r.unexpected_count !== null && r.unexpected_count !== undefined) {
    return `${number.format(r.unexpected_count)} unexpected ${r.unexpected_count === 1 ? "row" : "rows"}`;
  }
  if (r.observed === null || r.observed === undefined) return "—";
  return r.column === "age_s" ? `${formatDuration(r.observed * 1000)} old` : `${number.format(r.observed)} rows`;
}

function initPipeline() {
  const root = document.querySelector("[data-pipeline]");
  if (!root) return;
  const liveLink = document.querySelector("[data-live-link]");
  const content = root.querySelector("[data-pipeline-content]");
  const unpublished = root.querySelector("[data-pipeline-unpublished]");
  const failed = root.querySelector("[data-pipeline-error]");
  const failedText = root.querySelector("[data-pipeline-error-text]");
  const busy = [...root.querySelectorAll("[aria-busy]")];
  let attempt = 0;

  function setState(state) {
    for (const el of busy) {
      if (state === "loading") el.setAttribute("aria-busy", "true");
      else el.removeAttribute("aria-busy");
    }
    content.hidden = state === "unpublished" || state === "error";
    unpublished.hidden = state !== "unpublished";
    liveLink.hidden = state === "unpublished";
    failed.hidden = state !== "error";
    failedText.textContent =
      state === "error" ? "Pipeline health couldn’t be loaded. Check your connection, then try again." : "";
  }

  async function load() {
    const mine = ++attempt;
    setState("loading");
    try {
      const doc = await readJson(HEALTH_URL, { timeoutMs: READ_TIMEOUT_MS });
      if (mine !== attempt) return;
      render(root, doc);
      setState("ready");
    } catch (error) {
      if (mine === attempt) setState(error.status === 404 ? "unpublished" : "error");
    }
  }

  root.querySelector("[data-pipeline-retry]").addEventListener("click", load);
  root.addEventListener("click", (event) => {
    const button = event.target.closest("[data-kpi-info]");
    if (!button) return;
    const open = button.getAttribute("aria-expanded") !== "true";
    button.setAttribute("aria-expanded", String(open));
    document.getElementById(button.getAttribute("aria-controls")).hidden = !open;
  });
  load();
}

function render(root, doc) {
  const generated = parseTime(doc.generated_at);
  const asOf = generated ? `Health as of ${formatDateTime(generated)} (Pacific)` : "Health as of an unknown time";

  // ---- status banner ----
  const overall = doc.status?.overall ?? "warn";
  const banner = root.querySelector("[data-status-banner]");
  banner.dataset.status = overall;
  banner.querySelector("[data-status-overall]").replaceChildren(badge(overall, STATUS[overall].word));
  banner.querySelector("[data-status-components]").replaceChildren(
    ...(doc.status?.components ?? []).map((c) => {
      const li = document.createElement("li");
      const name = document.createElement("span");
      name.className = "status-list__name";
      name.textContent = COMPONENT[c.name] ?? c.name;
      const detail = document.createElement("span");
      detail.className = "status-list__detail";
      detail.textContent = c.detail;
      li.append(name, badge(c.status, STATUS[c.status]?.word ?? c.status), detail);
      return li;
    }),
  );
  banner.querySelector("[data-status-caption]").textContent = asOf;

  // ---- tiles ----
  const t = doc.tiles ?? {};
  const tile = (key, value, sub) => {
    const el = root.querySelector(`[data-kpi="${key}"]`);
    el.querySelector("[data-kpi-value]").textContent = value;
    el.querySelector("[data-kpi-sub]").textContent = sub;
  };
  const p95 = t.latency_p95_s === null ? "" : `${formatDuration(t.latency_p95_s * 1000)} · `;
  tile("latency", seconds(t.latency_p95_s), `${p95}fetch to silver, worst minute of the last active hour`);
  tile("events", t.events_per_min === null ? "—" : oneDecimal.format(t.events_per_min), "station updates a minute, last active hour");
  tile("lag", t.lag_records === null ? "—" : number.format(t.lag_records), "records published but not yet in silver");
  tile("freshness", t.freshness_s === null ? "—" : formatDuration(t.freshness_s * 1000), "since the newest row reached silver");
  root.querySelector("[data-kpi-caption]").textContent = `${asOf}. From lake.metrics.pipeline_minutely and lake.silver.station_status.`;

  // ---- latency and events charts over the 1,440 minutes ----
  const minutes = rowsOf(doc.minutely);
  const times = minutes.map((r) => parseTime(r.minute));
  const window = times.length ? `${formatDateTime(times[0])} – ${formatShortClock(times[times.length - 1])}` : "";
  const active = minutes.filter((r) => r.processed > 0);
  const worstP95 = Math.max(...active.map((r) => r.latency_p95_s ?? 0), 0);
  const lat = root.querySelector('[data-chart="latency"]');
  lat.querySelector("[data-takeaway]").textContent = active.length
    ? `The slowest minute's p95 was ${formatDuration(worstP95 * 1000)}, ${worstP95 > SLA_S ? "over" : "within"} the 5 min SLA; ` +
      `${number.format(active.length)} of the last ${number.format(minutes.length)} minutes landed data in silver.`
    : `No data reached silver in the last ${number.format(minutes.length)} minutes: the stream was not running.`;
  lat.querySelector("[data-caption]").textContent = `Source: lake.metrics.pipeline_minutely, ${window} (Pacific).`;
  lat.classList.add("is-ready"); // before drawing: the chart measures its labels, which needs the svg shown
  drawOnResize(lat.querySelector("[data-chart-box]"), (width) =>
    lineChart(lat.querySelector("svg"), times, [
      { values: minutes.map((r) => r.latency_p50_s), className: "chart__line--primary" },
      { values: minutes.map((r) => r.latency_p95_s), className: "chart__line--secondary" },
    ], { width, yLabel: "Latency", yTicks: durationTicks, formatTime: formatShortClock, threshold: { value: SLA_S, label: "5 min SLA" } }),
  );

  const ev = root.querySelector('[data-chart="events"]');
  const totalEvents = minutes.reduce((s, r) => s + r.events, 0);
  ev.querySelector("[data-takeaway]").textContent =
    `${number.format(totalEvents)} station updates were published in the last 24 hours ` +
    `(${oneDecimal.format(totalEvents / Math.max(1, minutes.length))} a minute on average).`;
  ev.querySelector("[data-caption]").textContent = `Source: lake.metrics.pipeline_minutely, ${window} (Pacific).`;
  ev.classList.add("is-ready");
  drawOnResize(ev.querySelector("[data-chart-box]"), (width) =>
    lineChart(ev.querySelector("svg"), times, [{ values: minutes.map((r) => r.events), className: "chart__line--primary" }],
      { width, yLabel: "Events per minute", formatY: (v) => number.format(v), formatTime: formatShortClock }),
  );

  // ---- freshness per table ----
  const tables = rowsOf(doc.tables);
  const fresh = root.querySelector('[data-chart="freshness"]');
  fresh.querySelector("tbody").replaceChildren(
    ...tables.map((r) => {
      const tr = document.createElement("tr");
      const th = document.createElement("th");
      th.scope = "row";
      th.textContent = r.table;
      th.className = "data";
      const commit = parseTime(r.last_commit);
      const cells = [
        commit ? formatDateTime(commit) : "—",
        r.age_s === null ? "—" : formatDuration(r.age_s * 1000),
        number.format(r.files),
      ].map((text, i) => {
        const td = document.createElement("td");
        td.textContent = text;
        if (i === 2) td.className = "num";
        return td;
      });
      const status = document.createElement("td");
      status.append(badge(r.status, STATUS[r.status]?.word ?? r.status));
      tr.append(th, ...cells, status);
      return tr;
    }),
  );
  const behind = tables.filter((r) => r.status !== "ok").length;
  fresh.querySelector("[data-takeaway]").textContent = behind
    ? `${behind} of ${tables.length} tables are behind their freshness target.`
    : `All ${tables.length} tables are within their freshness target.`;
  fresh.querySelector("[data-caption]").textContent =
    `Source: Iceberg snapshots ($snapshots, compaction excluded) and $files. Stream tables: healthy within 5 min; batch tables within 48 h. ${asOf}.`;
  fresh.classList.add("is-ready");

  // ---- data quality: the gate and the dbt tests ----
  const q = doc.quality ?? {};
  const gate = q.gate;
  const quality = root.querySelector('[data-chart="quality"]');
  const gateStatus = gate ? (gate.passed ? "ok" : "fail") : "warn";
  quality.querySelector("[data-gate]").replaceChildren(
    badge(gateStatus, gate ? STATUS[gateStatus].result : "Not run"),
    document.createTextNode(gate ? ` Quality gate: ${number.format(gate.tests_passed ?? 0)} of ${number.format(gate.tests ?? 0)} tests passed, ${number.format(gate.warnings ?? 0)} warnings.` : " No quality gate has run yet."),
  );
  quality.querySelector("tbody").replaceChildren(
    ...(q.tests ?? []).map((test) => {
      const tr = document.createElement("tr");
      const th = document.createElement("th");
      th.scope = "row";
      th.className = "data";
      th.textContent = test.name;
      const td = document.createElement("td");
      const s = TEST_STATUS[test.status] ?? "warn";
      td.append(badge(s, STATUS[s].result));
      tr.append(th, td);
      return tr;
    }),
  );
  const failedTests = (q.tests ?? []).filter((x) => TEST_STATUS[x.status] !== "ok").length;
  quality.querySelector("[data-takeaway]").textContent = failedTests
    ? `${failedTests} of ${(q.tests ?? []).length} dbt tests did not pass.`
    : `All ${(q.tests ?? []).length} dbt tests passed.`;
  quality.querySelector("[data-caption]").textContent = "Source: dbt run_results.json and data/dbt/quality_gate.json (marts_daily).";
  quality.classList.add("is-ready");

  // ---- Great Expectations on silver (the checkpoint inside marts_daily's quality gate) ----
  const gx = q.expectations ?? null;
  const exp = root.querySelector('[data-chart="expectations"]');
  for (const el of exp.querySelectorAll("[data-takeaway], [data-expectations-table], [data-caption]")) el.hidden = !gx;
  exp.querySelector("[data-expectations-none]").hidden = Boolean(gx);
  if (gx) {
    exp.querySelector("tbody").replaceChildren(
      ...(gx.results ?? []).map((r) => {
        const tr = document.createElement("tr");
        const th = document.createElement("th");
        th.scope = "row";
        th.textContent = r.label;
        const column = document.createElement("td");
        column.className = "data";
        column.textContent = r.column ? r.column.replaceAll(",", ", ") : "whole batch";
        const result = document.createElement("td");
        result.append(badge(r.status, STATUS[r.status]?.result ?? r.status));
        const found = document.createElement("td");
        found.className = "num";
        found.textContent = foundText(r);
        tr.append(th, column, result, found);
        return tr;
      }),
    );
    const freshnessWarns = (gx.results ?? []).some((r) => r.column === "age_s" && r.status === "warn");
    const parts = [`${number.format(gx.passed)} of ${number.format(gx.evaluated)} checks passed`];
    if (gx.failed) parts.push(`${number.format(gx.failed)} blocking ${gx.failed === 1 ? "check" : "checks"} failed, so the insights were not refreshed`);
    if (freshnessWarns) parts.push("freshness is a warning: Spark runs on demand");
    else if (gx.warnings) parts.push(`${number.format(gx.warnings)} ${gx.warnings === 1 ? "warning" : "warnings"}`);
    exp.querySelector("[data-takeaway]").textContent = `${parts.join("; ")}.`;
    const checked = parseTime(gx.run_at);
    exp.querySelector("[data-caption]").textContent =
      `Source: Great Expectations ${gx.gx_version}, suite ${gx.suite}, newest 24 h of silver ` +
      `(${number.format(gx.batch?.rows ?? 0)} rows), checked by marts_daily at ${checked ? formatDateTime(checked) : "an unknown time"} (Pacific).`;
  }
  exp.classList.add("is-ready");

  // ---- compaction: files before and after the latest maintenance run ----
  const runs = rowsOf(doc.compaction);
  const comp = root.querySelector('[data-chart="compaction"]');
  const most = Math.max(1, ...runs.map((r) => r.files_before ?? 0));
  comp.querySelector("[data-bars]").replaceChildren(
    ...runs.map((r) => {
      const li = document.createElement("li");
      li.className = "bars__row";
      const name = document.createElement("span");
      name.className = "bars__name data";
      name.textContent = r.table;
      li.append(name);
      if (r.status === "skipped" || r.files_before === null) {
        const note = document.createElement("span");
        note.className = "bars__note";
        note.textContent = r.status === "skipped" ? "skipped (not created yet)" : "no measurement";
        li.append(note);
        return li;
      }
      for (const [key, label] of [["files_before", "before"], ["files_after", "after"]]) {
        const bar = document.createElement("span");
        bar.className = `bars__bar bars__bar--${label}`;
        bar.style.setProperty("--share", String(r[key] / most));
        const text = document.createElement("span");
        text.className = "bars__value";
        text.textContent = `${number.format(r[key])} ${label}`;
        bar.append(text);
        li.append(bar);
      }
      return li;
    }),
  );
  const before = runs.reduce((s, r) => s + (r.files_before ?? 0), 0);
  const after = runs.reduce((s, r) => s + (r.files_after ?? 0), 0);
  comp.querySelector("[data-takeaway]").textContent =
    `The latest maintenance run took ${runs.length} tables from ${number.format(before)} data files to ${number.format(after)}.`;
  comp.querySelector("[data-caption]").textContent = "Source: lake.metrics.maintenance_runs, the newest iceberg_maintenance run.";
  comp.classList.add("is-ready");
}

initShell();
initPipeline();
