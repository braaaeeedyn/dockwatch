// Insights page (F4): KPI tiles, the "% of time empty" heatmap (station x Pacific hour), the rebalancing-need table and
// the trips vs availability scatter, from the three format-2 files in data/insights/ (marts_daily, model.js).
// States: skeletons + aria-busy while reading; 404 -> "not published yet" with a link to Live; any other failure or a
// read that has not answered after 10 s -> a message with Retry. No status colours here (DESIGN §2).

import { drawOnResize, scatter } from "./charts.js";
import {
  bucket,
  decode,
  heatmapRows,
  hourRange,
  kpis,
  quarters,
  rebalancingRows,
  stationProfiles,
} from "./insights/model.js";
import { initShell } from "./shell.js";
import { readJson } from "./util/read.js";
import { formatDuration, formatMonthDay } from "./util/time.js";

const FILES = ["station_availability_daily", "rebalancing_need", "trips_vs_availability"];
const READ_TIMEOUT_MS = 10_000; // then the error message with Retry, never "Loading…" forever
const number = new Intl.NumberFormat("en-US");
const oneDecimal = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const monthYear = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", year: "numeric" });

const percent = (share) => `${oneDecimal.format(share * 100)} %`;
const day = (iso) => formatMonthDay(new Date(`${iso}T12:00:00Z`)); // a Pacific calendar day, "Oct 9"
const month = (iso) => monthYear.format(new Date(`${iso}T12:00:00Z`)); // "Oct 2025"

function initInsights() {
  const root = document.querySelector("[data-insights]");
  if (!root) return;
  const liveLink = document.querySelector("[data-live-link]");
  const content = root.querySelector("[data-insights-content]");
  const unpublished = root.querySelector("[data-insights-unpublished]");
  const failed = root.querySelector("[data-insights-error]");
  const failedText = root.querySelector("[data-insights-error-text]");
  const busy = [...root.querySelectorAll("[aria-busy]")];
  let attempt = 0;

  function setState(state) {
    for (const el of busy) {
      if (state === "loading") el.setAttribute("aria-busy", "true");
      else el.removeAttribute("aria-busy");
    }
    content.hidden = state === "unpublished" || state === "error";
    unpublished.hidden = state !== "unpublished";
    liveLink.hidden = state === "unpublished"; // the card has its own link to Live
    failed.hidden = state !== "error";
    failedText.textContent =
      state === "error" ? "The insights couldn’t be loaded. Check your connection, then try again." : "";
  }

  async function load() {
    const mine = ++attempt;
    setState("loading");
    try {
      const docs = await Promise.all(
        FILES.map((name) => readJson(`data/insights/${name}.json`, { timeoutMs: READ_TIMEOUT_MS })),
      );
      if (mine !== attempt) return;
      render(root, ...docs);
      setState("ready");
    } catch (error) {
      if (mine === attempt) setState(error.status === 404 ? "unpublished" : "error");
    }
  }

  root.querySelector("[data-insights-retry]").addEventListener("click", load);
  // ⓘ buttons reveal each tile's definition (as on Live).
  root.addEventListener("click", (event) => {
    const button = event.target.closest("[data-kpi-info]");
    if (!button) return;
    const open = button.getAttribute("aria-expanded") !== "true";
    button.setAttribute("aria-expanded", String(open));
    document.getElementById(button.getAttribute("aria-controls")).hidden = !open;
  });
  load();
}

function render(root, dailyDoc, needDoc, tripsDoc) {
  const daily = decode(dailyDoc);
  const trips = decode(tripsDoc);
  const need = decode(needDoc);
  const profiles = stationProfiles(trips);
  const dates = dailyDoc.window.service_dates;
  const tripWindow = `${month(tripsDoc.window.trip_first_month)} – ${month(tripsDoc.window.trip_last_month)}`;

  // ---- KPI tiles ----
  const k = kpis(daily, trips);
  const tile = (key, value, sub) => {
    const el = root.querySelector(`[data-kpi="${key}"]`);
    el.querySelector("[data-kpi-value]").textContent = value;
    el.querySelector("[data-kpi-sub]").textContent = sub;
  };
  const days = `${day(dates[0])} – ${day(dates[dates.length - 1])}`;
  tile("empty", percent(k.timeEmpty), `of station time, ${days}`);
  tile("full", percent(k.timeFull), `of station time, ${days}`);
  tile("hour", hourRange(k.worstHour), `${percent(k.worstHourShare)} of the time empty, on average`);
  tile("trips", number.format(Math.round(k.tripsPerDay)), `trips started, average over ${tripWindow}`);
  root.querySelector("[data-kpi-caption]").textContent =
    `All regions. Time empty and full: marts.mart_station_availability_daily, ${days} (Pacific). ` +
    `Worst hour and trips: marts.mart_trips_vs_availability.`;

  // ---- heatmap: the 25 stations empty most often x 24 Pacific hours ----
  const top = heatmapRows(profiles);
  const heat = root.querySelector('[data-chart="heatmap"]');
  const body = heat.querySelector("tbody");
  body.replaceChildren(
    ...top.map((s) => {
      const tr = document.createElement("tr");
      const th = document.createElement("th");
      th.scope = "row";
      th.textContent = s.name;
      tr.append(th);
      for (const share of s.hours) {
        const td = document.createElement("td");
        if (share === null) {
          td.textContent = "–";
        } else {
          td.textContent = String(Math.round(share * 100));
          td.dataset.bucket = String(bucket(share * 100));
        }
        tr.append(td);
      }
      return tr;
    }),
  );
  const worst = top[0];
  if (worst) {
    const peak = worst.hours.indexOf(Math.max(...worst.hours.filter((v) => v !== null)));
    heat.querySelector("[data-takeaway]").textContent =
      `${worst.name} is empty most often: ${percent(worst.shareEmpty)} of the time, ` +
      `worst at ${hourRange(peak)} (${percent(worst.hours[peak])}).`;
  }
  heat.querySelector("[data-caption]").textContent =
    `Source: marts.mart_trips_vs_availability, all live data so far, by hour of day in Pacific time. ` +
    `The ${top.length} stations empty most often; each cell is the % of 5-minute windows the station was empty.`;
  heat.classList.add("is-ready");

  // ---- rebalancing need, last 7 days ----
  const rows = rebalancingRows(need);
  const reb = root.querySelector('[data-chart="rebalancing"]');
  reb.querySelector("tbody").replaceChildren(
    ...rows.map((r) => {
      const tr = document.createElement("tr");
      const th = document.createElement("th");
      th.scope = "row";
      th.textContent = r.name;
      const cells = [
        String(r.need_rank),
        oneDecimal.format(r.hours_empty),
        oneDecimal.format(r.hours_full),
        number.format(r.episodes),
        r.longest_empty_hours > 0 ? formatDuration(r.longest_empty_hours * 3_600_000) : "—",
      ].map((text) => {
        const td = document.createElement("td");
        td.className = "num";
        td.textContent = text;
        return td;
      });
      tr.append(th, ...cells);
      return tr;
    }),
  );
  const first = rows[0];
  const needDays = `${day(needDoc.window.window_first_day)} – ${day(needDoc.window.window_last_day)}`;
  if (first) {
    const full = first.hours_full >= first.hours_empty;
    reb.querySelector("[data-takeaway]").textContent =
      `${first.name} needed the most: ${full ? "full" : "empty"} for ` +
      `${oneDecimal.format(full ? first.hours_full : first.hours_empty)} hours of the last 7 days.`;
  }
  reb.querySelector("[data-caption]").textContent =
    `Source: marts.mart_rebalancing_need, ${needDays} (Pacific). Ranked by hours empty plus hours full; ` +
    `episodes are times a station went empty or full.`;
  reb.classList.add("is-ready");

  // ---- trips vs availability: one point per station ----
  const points = profiles.map((s) => ({ x: s.tripsPerDay, y: s.shareEmpty * 100 }));
  const q = quarters(profiles);
  const takeaway =
    `The busiest quarter of stations is empty ${percent(q.busiest)} of the time, ` +
    `the quietest quarter ${percent(q.quietest)}.`;
  const sc = root.querySelector('[data-chart="scatter"]');
  const svg = sc.querySelector("svg");
  sc.querySelector("[data-takeaway]").textContent = takeaway;
  svg.setAttribute(
    "aria-label",
    `Scatter plot of ${number.format(points.length)} stations: trips started per day against % of time empty. ${takeaway}`,
  );
  sc.querySelector("[data-caption]").textContent =
    `Source: marts.mart_trips_vs_availability. Trips per day: Bay Wheels trip history, ${tripWindow}; ` +
    `% of time empty: live data so far.`;
  drawOnResize(sc.querySelector("[data-chart-box]"), (width) =>
    scatter(svg, points, {
      width,
      xLabel: "Trips started per day",
      yLabel: "% of time empty",
      formatX: (v) => number.format(v),
      formatY: (v) => `${v} %`,
    }),
  );
  sc.classList.add("is-ready");
}

initShell();
initInsights();
