// Live page (F2): KPI tiles, the station map with region / Map | List / "Show only problems" controls, legend,
// station details, stale banner and the polite map summary. Data: live.json (shared 60 s poll in freshness.js,
// delivered as "dockwatch:live" events), alerts.json (re-read with every live.json refresh), geo/<region>.json.

import { latestLive, refreshLive } from "../freshness.js";
import { createStationMap } from "../map/map.js";
import { DELAYED_MAX_MS, formatShortClock, parseTime } from "../util/time.js";
import { createDetails } from "./details.js";
import { createKpis } from "./kpis.js";
import { createList } from "./list.js";
import { DEFAULT_REGION, REGIONS, STATES, isProblem } from "./states.js";

const STORAGE_KEY = "dockwatch-region";
const TICK_MS = 15 * 1000;
const number = new Intl.NumberFormat("en-US");

function storedRegion() {
  try {
    const value = window.localStorage.getItem(STORAGE_KEY);
    return value in REGIONS ? value : null;
  } catch {
    return null;
  }
}

function storeRegion(region) {
  try {
    window.localStorage.setItem(STORAGE_KEY, region);
  } catch {
    // Not remembered, but the switch still works for this page view.
  }
}

/** Initial UI state: URL (shareable) first, then the remembered region, then San Francisco. */
function initialState() {
  const params = new URLSearchParams(window.location.search);
  const region = params.get("region");
  return {
    region: region in REGIONS ? region : (storedRegion() ?? DEFAULT_REGION),
    view: params.get("view") === "list" ? "list" : "map",
    problemsOnly: params.get("problems") === "1",
  };
}

export function initLive() {
  const root = document.getElementById("map");
  if (!root) return;

  const ui = initialState();
  const geoCache = new Map();
  let live = null;
  let alerts = null; // object, or false when it couldn't be read
  let geoError = false;
  let liveError = false;
  let renderedRegion = null; // region whose markers are on the map now
  let lastSummary = "";

  // ---- elements ----
  const regionInputs = [...root.querySelectorAll('input[name="region"]')];
  const viewInputs = [...root.querySelectorAll('input[name="view"]')];
  const problemsButton = root.querySelector("[data-problems-toggle]");
  const stage = root.querySelector("[data-map-stage]");
  const frame = root.querySelector("[data-map-frame]");
  const listView = root.querySelector("[data-list-view]");
  const message = root.querySelector("[data-map-message]");
  const legend = root.querySelector("[data-legend]");
  const summary = root.querySelector("[data-map-summary]");
  const banner = document.querySelector("[data-stale-banner]");
  const kpis = createKpis(document.querySelector("[data-kpis]"));

  const map = createStationMap({
    svg: root.querySelector("[data-map-svg]"),
    frame,
    onSelect: (id, { via }) => select(id, { via }),
    onResize: () => details.place(),
  });

  const details = createDetails({
    tooltip: root.querySelector("[data-tooltip]"),
    sheet: document.querySelector("[data-sheet]"),
    positionOf: (id) => map.positionOf(id),
    onClose: (id, { restoreFocus }) => {
      map.setSelected(null);
      if (restoreFocus && id) map.focusStation(id);
    },
  });

  const list = createList({ table: listView.querySelector("table"), caption: listView.querySelector("caption") });

  // ---- data helpers ----
  function regionStations() {
    return (live?.stations ?? []).filter((s) => s.view === ui.region);
  }

  function visibleStations() {
    const stations = regionStations();
    return ui.problemsOnly ? stations.filter((s) => isProblem(s.state)) : stations;
  }

  function regionCounts() {
    const fromExport = live?.counts?.[ui.region];
    if (fromExport) return fromExport;
    const counts = {};
    for (const s of regionStations()) counts[s.state] = (counts[s.state] ?? 0) + 1;
    return counts;
  }

  function findStation(id) {
    return (live?.stations ?? []).find((s) => s.id === id) ?? null;
  }

  async function loadGeo(region) {
    if (geoCache.has(region)) return geoCache.get(region);
    const response = await fetch(`geo/${region}.json`);
    if (!response.ok) throw new Error(`geo/${region}.json: HTTP ${response.status}`);
    const geo = await response.json();
    geoCache.set(region, geo);
    return geo;
  }

  async function loadAlerts() {
    try {
      const response = await fetch("data/alerts.json", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      alerts = await response.json();
    } catch {
      alerts = alerts || false; // keep the last good copy if there is one
    }
    if (live) kpis.render(live, alerts);
  }

  // ---- URL + controls ----
  function syncUrl() {
    const params = new URLSearchParams(window.location.search);
    params.delete("region");
    params.delete("view");
    params.delete("problems");
    if (ui.region !== DEFAULT_REGION) params.set("region", ui.region);
    if (ui.view === "list") params.set("view", "list");
    if (ui.problemsOnly) params.set("problems", "1");
    const query = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`);
  }

  function syncControls() {
    for (const input of regionInputs) input.checked = input.value === ui.region;
    for (const input of viewInputs) input.checked = input.value === ui.view;
    problemsButton.setAttribute("aria-pressed", String(ui.problemsOnly));
    stage.hidden = ui.view !== "map";
    listView.hidden = ui.view !== "list";
    root.dataset.view = ui.view;
  }

  // ---- rendering ----
  function showMessage(text, action) {
    if (!text) {
      message.hidden = true;
      message.replaceChildren();
      return;
    }
    const p = document.createElement("p");
    p.textContent = text;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "button-secondary";
    button.textContent = action.label;
    button.addEventListener("click", action.run);
    message.replaceChildren(p, button);
    message.hidden = false;
  }

  function renderLegend() {
    const counts = regionCounts();
    for (const state of STATES) {
      const el = legend.querySelector(`[data-legend-count="${state}"]`);
      if (el) el.textContent = number.format(counts[state] ?? 0);
    }
  }

  function renderSummary() {
    const c = regionCounts();
    const text = `${REGIONS[ui.region].short}: ${c.empty ?? 0} empty, ${c.full ?? 0} full, ${c.offline ?? 0} offline`;
    if (text !== lastSummary) {
      summary.textContent = text; // aria-live="polite": only spoken when the counts change
      lastSummary = text;
    }
  }

  function renderStale() {
    const generatedAt = parseTime(live?.generated_at);
    const stale = generatedAt && Date.now() - generatedAt.getTime() > DELAYED_MAX_MS;
    if (stale) banner.querySelector("[data-stale-time]").textContent = formatShortClock(generatedAt);
    banner.hidden = !stale;
  }

  function render({ animate = false } = {}) {
    const geo = geoCache.get(ui.region);
    stage.setAttribute("aria-busy", String(!(geo && live) && !liveError && !geoError));
    listView.setAttribute("aria-busy", String(!live && !liveError));
    root.classList.toggle("is-loading", !(geo && live));

    if (liveError && !live) {
      showMessage("Live station data couldn’t be loaded. Check your connection, then try again.", {
        label: "Retry",
        run: () => refreshLive(),
      });
      return;
    }
    if (geoError && !geo) {
      showMessage("The map shapes couldn’t be loaded. The List view still shows every station.", {
        label: "Retry",
        run: () => setRegion(ui.region),
      });
    }
    if (!live) return;

    const stations = visibleStations();
    if (geo) {
      if (renderedRegion !== ui.region) {
        details.hide();
        map.clear();
        map.setGeo(geo);
        renderedRegion = ui.region;
        animate = false;
      }
      map.update(stations, { animate });
    }
    if (ui.view === "list") {
      list.render(stations, { regionName: REGIONS[ui.region].short, problemsOnly: ui.problemsOnly });
    }
    if (ui.problemsOnly && stations.length === 0) {
      showMessage(`No problem stations in ${REGIONS[ui.region].short} right now: every station has bikes and docks.`, {
        label: "Show all stations",
        run: () => setProblems(false),
      });
    } else if (!(geoError && !geo)) {
      showMessage(null);
    }
    renderLegend();
    renderSummary();
    renderStale();
    details.refresh((id) => (stations.some((s) => s.id === id) ? findStation(id) : null));
  }

  // ---- actions ----
  async function setRegion(region) {
    ui.region = region;
    storeRegion(region);
    syncUrl();
    syncControls();
    details.hide();
    geoError = false;
    render();
    try {
      await loadGeo(region);
    } catch {
      geoError = true;
    }
    if (ui.region === region) render();
  }

  function setView(view) {
    ui.view = view;
    syncUrl();
    syncControls();
    if (view === "list") details.hide();
    render();
  }

  function setProblems(on) {
    ui.problemsOnly = on;
    syncUrl();
    syncControls();
    render();
  }

  function select(id, { via, focus = false } = {}) {
    if (!id) {
      details.hide();
      map.setSelected(null);
      return;
    }
    const station = findStation(id);
    if (!station) return;
    map.setSelected(id);
    details.show(station);
    if (focus || via === "list") map.focusStation(id);
  }

  // ---- events ----
  for (const input of regionInputs) input.addEventListener("change", () => input.checked && setRegion(input.value));
  for (const input of viewInputs) input.addEventListener("change", () => input.checked && setView(input.value));
  problemsButton.addEventListener("click", () => setProblems(!ui.problemsOnly));

  listView.addEventListener("click", (event) => {
    const button = event.target.closest("[data-show-station]");
    if (!button) return;
    const id = button.dataset.showStation;
    setView("map");
    select(id, { via: "list", focus: true });
  });

  // Escape closes the tooltip (the sheet is a modal dialog and closes itself); focus goes back to the marker.
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape" || !details.isOpen()) return;
    const id = details.hide();
    map.setSelected(null);
    if (id) map.focusStation(id);
  });

  // A click outside the map frame closes the tooltip.
  document.addEventListener("click", (event) => {
    const inside = frame.contains(event.target) || event.target.closest("[data-sheet], [data-show-station]");
    if (details.isOpen() && !inside) {
      details.hide();
      map.setSelected(null);
    }
  });

  function onLive(data) {
    const first = live === null;
    live = data;
    liveError = false;
    kpis.render(live, alerts);
    render({ animate: !first });
    loadAlerts();
  }

  document.addEventListener("dockwatch:live", (event) => onLive(event.detail));

  document.addEventListener("dockwatch:live-error", () => {
    liveError = true;
    render();
  });

  setInterval(() => {
    renderStale();
    if (details.isOpen()) details.refresh(findStation);
  }, TICK_MS);

  // ---- start ----
  syncControls();
  setRegion(ui.region);
  if (latestLive()) onLive(latestLive());
}
