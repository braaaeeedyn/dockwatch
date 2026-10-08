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

  // Rows are updated in place, keyed by station id, so the 60 s refresh keeps keyboard focus and scroll position.
  const rowsById = new Map(); // id -> { tr, button, cells: HTMLTableCellElement[], values: string[] }
  const CELL_CLASSES = ["data", "", "num", "num", "num", "num"];

  function count(n) {
    return n === null || n === undefined ? "—" : number.format(n);
  }

  /** The cells after the name: code, state (HTML), bikes, docks, in state for, last reported. */
  function cellValues(s, now) {
    const info = stateInfo(s.state);
    const since = age(s.state_since, now);
    const reported = age(s.last_reported, now);
    return [
      s.code ?? "—",
      `<span class="state-word" data-state="${escapeHtml(s.state)}"><span class="swatch" data-state="${escapeHtml(s.state)}" aria-hidden="true"></span>${escapeHtml(info.word)}</span>`,
      count(s.bikes),
      count(s.docks),
      since === null ? "—" : formatDuration(since),
      reported === null ? "—" : reported < 60_000 ? "under 1 min ago" : `${formatDuration(reported)} ago`,
    ];
  }

  const STATE_CELL = 1; // the only cell set as HTML (escaped above); the others use textContent

  function createRow(s) {
    const tr = document.createElement("tr");
    tr.dataset.id = s.id;
    const th = document.createElement("th");
    th.scope = "row";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "link-button";
    button.dataset.showStation = s.id;
    th.append(button);
    tr.append(th);
    const cells = CELL_CLASSES.map((cls, i) => {
      const td = document.createElement("td");
      if (cls) td.className = cls;
      if (i === 0) td.setAttribute("translate", "no");
      tr.append(td);
      return td;
    });
    return { tr, button, cells, values: [] };
  }

  function updateRow(entry, s, now) {
    if (entry.button.textContent !== s.name) entry.button.textContent = s.name;
    cellValues(s, now).forEach((value, i) => {
      if (entry.values[i] === value) return;
      entry.values[i] = value;
      if (i === STATE_CELL) entry.cells[i].innerHTML = value;
      else entry.cells[i].textContent = value;
    });
  }

  function reconcile(stations, now) {
    const wrap = table.closest(".table-wrap");
    const scroll = wrap && { top: wrap.scrollTop, left: wrap.scrollLeft };
    const active = document.activeElement;
    const focusedId = body.contains(active) && active.matches("[data-show-station]") ? active.dataset.showStation : null;
    const oldOrder = [...body.children].map((tr) => tr.dataset.id);

    const wanted = new Set(stations.map((s) => s.id));
    for (const [id, entry] of rowsById) {
      if (wanted.has(id)) continue;
      entry.tr.remove();
      rowsById.delete(id);
    }
    stations.forEach((s, i) => {
      let entry = rowsById.get(s.id);
      if (!entry) {
        entry = createRow(s);
        rowsById.set(s.id, entry);
      }
      updateRow(entry, s, now);
      const at = body.children[i];
      if (at !== entry.tr) body.insertBefore(entry.tr, at ?? null); // only rows out of place move
    });

    // Safety net: moving a focused node can drop focus. Put it back on the same station, or a neighbour if its row
    // left the list (e.g. "only problems" and it became ok), without scrolling.
    if (focusedId !== null) {
      const from = oldOrder.indexOf(focusedId);
      const candidates = [focusedId, ...oldOrder.slice(from + 1), ...oldOrder.slice(0, from).reverse()];
      const target = candidates.map((id) => rowsById.get(id)).find(Boolean);
      if (target && document.activeElement !== target.button) target.button.focus({ preventScroll: true });
    }
    if (scroll) {
      wrap.scrollTop = scroll.top;
      wrap.scrollLeft = scroll.left;
    }
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
    reconcile(rows.map(({ s }) => s), now);
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
