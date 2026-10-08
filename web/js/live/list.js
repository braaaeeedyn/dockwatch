// List view: the map's text alternative (DESIGN.md §6, §7 data table). Same region and "only problems" filter as the
// map. Sortable column headers are buttons; the sorted column carries aria-sort. The wrapper scrolls inside its card.

import { formatDuration, parseTime } from "../util/time.js";
import { escapeHtml } from "./details.js";
import { stateInfo } from "./states.js";

const ICONS = {
  // Lucide v1.52.0 (ISC): chevrons-up-down, arrow-up, arrow-down
  none: '<path d="m7 15 5 5 5-5" /><path d="m7 9 5-5 5 5" />',
  ascending: '<path d="m5 12 7-7 7 7" /><path d="M12 19V5" />',
  descending: '<path d="M12 5v14" /><path d="m19 12-7 7-7-7" />',
};

const number = new Intl.NumberFormat("en-US");

function age(value, now) {
  const t = parseTime(value);
  return t ? now - t.getTime() : null;
}

/** Columns: key, header, numeric?, sort value (null sorts last). */
const COLUMNS = [
  { key: "name", label: "Station", value: (s) => s.name ?? "" },
  { key: "code", label: "Code", value: (s) => s.code ?? null },
  { key: "state", label: "State", value: (s) => stateInfo(s.state).severity },
  { key: "bikes", label: "Bikes", numeric: true, value: (s) => s.bikes },
  { key: "docks", label: "Docks", numeric: true, value: (s) => s.docks },
  { key: "since", label: "In state for", numeric: true, value: (s, now) => age(s.state_since, now) },
  { key: "reported", label: "Last reported", numeric: true, value: (s, now) => age(s.last_reported, now) },
];

function compare(a, b) {
  if (a === b) return 0;
  if (a === null || a === undefined) return 1;
  if (b === null || b === undefined) return -1;
  if (typeof a === "string") return a.localeCompare(b, "en", { numeric: true, sensitivity: "base" });
  return a - b;
}

export function createList({ table, caption }) {
  const sort = { key: "state", direction: "ascending" };
  const head = table.querySelector("thead tr");
  const body = table.querySelector("tbody");
  let last = { stations: [], regionName: "", problemsOnly: false };

  head.innerHTML = COLUMNS.map(
    (c) =>
      `<th scope="col" data-key="${c.key}"${c.numeric ? ' class="num"' : ""}>` +
      `<button type="button" class="sort-button" data-sort="${c.key}">${c.label}` +
      `<svg class="icon icon-sort" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"></svg>` +
      `</button></th>`,
  ).join("");

  function paintHeaders() {
    for (const th of head.children) {
      const sorted = th.dataset.key === sort.key;
      if (sorted) th.setAttribute("aria-sort", sort.direction);
      else th.removeAttribute("aria-sort");
      th.querySelector("svg").innerHTML = ICONS[sorted ? sort.direction : "none"];
    }
  }

  function row(s, now) {
    const info = stateInfo(s.state);
    const since = age(s.state_since, now);
    const reported = age(s.last_reported, now);
    return (
      `<tr data-id="${escapeHtml(s.id)}">` +
      `<th scope="row"><button type="button" class="link-button" data-show-station="${escapeHtml(s.id)}">${escapeHtml(s.name)}</button></th>` +
      `<td class="data" translate="no">${s.code ? escapeHtml(s.code) : "—"}</td>` +
      `<td><span class="state-word" data-state="${escapeHtml(s.state)}"><span class="swatch" data-state="${escapeHtml(s.state)}" aria-hidden="true"></span>${escapeHtml(info.word)}</span></td>` +
      `<td class="num">${number.format(s.bikes)}</td>` +
      `<td class="num">${number.format(s.docks)}</td>` +
      `<td class="num">${since === null ? "—" : formatDuration(since)}</td>` +
      `<td class="num">${reported === null ? "—" : reported < 60_000 ? "under 1 min ago" : `${formatDuration(reported)} ago`}</td>` +
      `</tr>`
    );
  }

  function render(stations, { regionName, problemsOnly }) {
    last = { stations, regionName, problemsOnly };
    const now = Date.now();
    const column = COLUMNS.find((c) => c.key === sort.key);
    const sign = sort.direction === "ascending" ? 1 : -1;
    const rows = stations
      .map((s) => ({ s, v: column.value(s, now) }))
      .sort((a, b) => {
        // Missing values stay at the bottom in both directions; ties fall back to the station name.
        if (a.v === null && b.v !== null) return 1;
        if (b.v === null && a.v !== null) return -1;
        return sign * compare(a.v, b.v) || compare(a.s.name, b.s.name);
      });
    body.innerHTML = rows.map(({ s }) => row(s, now)).join("");
    const what = problemsOnly ? "Problem stations" : "Stations";
    caption.textContent = `${what} in ${regionName}: ${number.format(stations.length)}, sorted by ${column.label.toLowerCase()} (${sort.direction})`;
    paintHeaders();
  }

  head.addEventListener("click", (event) => {
    const button = event.target.closest("[data-sort]");
    if (!button) return;
    const key = button.dataset.sort;
    if (sort.key === key) sort.direction = sort.direction === "ascending" ? "descending" : "ascending";
    else {
      sort.key = key;
      sort.direction = "ascending";
    }
    render(last.stations, last);
  });

  paintHeaders();
  return { render };
}
