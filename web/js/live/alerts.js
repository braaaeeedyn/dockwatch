// Alerts rail (F3; DESIGN.md §4, §7 Alert row, §8): open alerts longest-running first, then up to 20 recently resolved,
// from data/alerts.json. Desktop: a sticky rail beside the map; tablet: a card under the map (two columns of rows from a
// 40rem container); phone: a peek bar ("61 open alerts") that opens the alerts sheet. There is one list in the DOM: the
// body node moves into the sheet when it opens and back into the rail when it closes.
// Rows are keyed by episode_id and updated in place, so a refresh keeps focus and scroll position. New open alerts are
// announced politely, never on the first read. In replay mode the rail shows a note instead (alerts.json has no
// history), and the peek bar is hidden. Like replay.js, this module does not fetch: live.js owns the data.

import { formatDuration, formatMonthDay, formatShortClock, isTodayPacific, parseTime } from "../util/time.js";
import { isPhone } from "./details.js";
import { REGIONS } from "./states.js";

const MAX_RESOLVED = 20;
const KIND_WORDS = { empty: "Empty", full: "Full" };
const number = new Intl.NumberFormat("en-US");

function kindWord(kind) {
  return KIND_WORDS[kind] ?? String(kind ?? "Unknown");
}

function time(value) {
  return parseTime(value)?.getTime() ?? null;
}

/** Stable sorts (Array.prototype.sort is stable): ties keep the exporter's order. */
function ordered(alerts) {
  const open = [...(alerts.open ?? [])].sort((a, b) => (time(a.started) ?? 0) - (time(b.started) ?? 0));
  const resolved = [...(alerts.resolved ?? [])]
    .sort((a, b) => (time(b.resolved) ?? 0) - (time(a.resolved) ?? 0))
    .slice(0, MAX_RESOLVED);
  return { open, resolved };
}

/** "Empty for 34 min", or "Empty, resolved after 22 min". */
function metaText(alert, isOpen, now) {
  const word = kindWord(alert.kind);
  if (isOpen) {
    const started = time(alert.started);
    return started === null ? word : `${word} for ${formatDuration(now - started)}`;
  }
  return Number.isFinite(alert.duration_s)
    ? `${word}, resolved after ${formatDuration(alert.duration_s * 1000)}`
    : `${word}, resolved`;
}

/** "San Francisco · started 3:32 PM", or "… started Oct 6, 3:32 PM" when that wasn't today (Pacific). */
function startText(alert, now) {
  const region = REGIONS[alert.view]?.short;
  const started = parseTime(alert.started);
  let text = "";
  if (started) {
    const clock = formatShortClock(started);
    text = `started ${isTodayPacific(started, now) ? clock : `${formatMonthDay(started)}, ${clock}`}`;
  }
  return [region, text].filter(Boolean).join(" · ");
}

/**
 * `section`: the rail; `peek`: the phone peek bar; `sheet`: the alerts dialog.
 * `onSelect(alert)`: a row was pressed (after the sheet closed on phones). `onRetry()`: the error's Retry.
 */
export function createAlerts({ section, peek, sheet, onSelect, onRetry }) {
  const body = section.querySelector("[data-alerts-body]");
  const skeleton = body.querySelector("[data-alerts-skeleton]");
  const message = body.querySelector("[data-alerts-message]");
  const note = body.querySelector("[data-alerts-replay-note]");
  const lists = body.querySelector("[data-alerts-lists]");
  const none = body.querySelector("[data-alerts-none]");
  const openList = body.querySelector("[data-alerts-open]");
  const resolvedList = body.querySelector("[data-alerts-resolved]");
  const resolvedHeading = body.querySelector("[data-alerts-resolved-heading]");
  const counts = {
    open: body.querySelector('[data-alerts-count="open"]'),
    resolved: body.querySelector('[data-alerts-count="resolved"]'),
  };
  const announcer = section.querySelector("[data-alerts-announcer]");
  const peekText = peek.querySelector("[data-alerts-peek-text]");
  const sheetBody = sheet.querySelector("[data-alerts-sheet-body]");
  const railSlot = body.parentNode;
  const railNext = body.nextSibling;

  const rows = new Map(); // episode_id -> { li, el, pressable, alert, values }
  let last = { alerts: undefined, stations: null, replay: false };
  let baseline = null; // open episode ids at the last successful read; null until the first one
  let quietClose = false; // closing the sheet for a selection: focus goes to the station, not the peek bar

  // ---- rows ----
  function createRowElement(alert, pressable) {
    const el = document.createElement(pressable ? "button" : "div");
    el.className = pressable ? "alert-row" : "alert-row alert-row--static";
    if (pressable) {
      el.type = "button";
      el.dataset.alertStation = alert.station_id;
    }
    el.dataset.episode = alert.episode_id;
    const marker = document.createElement("span");
    marker.className = "swatch alert-row__marker";
    marker.setAttribute("aria-hidden", "true");
    const text = document.createElement("span");
    text.className = "alert-row__text";
    const name = document.createElement("span");
    name.className = "alert-row__name";
    name.setAttribute("translate", "no");
    const meta = document.createElement("span");
    meta.className = "alert-row__meta";
    const start = document.createElement("span");
    start.className = "alert-row__start data";
    text.append(name, meta, start);
    el.append(marker, text);
    return el;
  }

  function paintRow(entry, alert, isOpen, now) {
    const values = [alert.kind, alert.name ?? "", metaText(alert, isOpen, now), startText(alert, now)];
    const [marker, text] = entry.el.children;
    if (entry.values[0] !== values[0]) marker.dataset.state = values[0];
    for (let i = 1; i < values.length; i += 1) {
      if (entry.values[i] !== values[i]) text.children[i - 1].textContent = values[i];
    }
    entry.values = values;
    entry.alert = alert;
  }

  function entryFor(alert, stations) {
    const pressable = stations !== null && stations.has(alert.station_id);
    let entry = rows.get(alert.episode_id);
    if (!entry) {
      const li = document.createElement("li");
      entry = { li, el: null, pressable, alert, values: [] };
      rows.set(alert.episode_id, entry);
    }
    if (!entry.el || entry.pressable !== pressable) {
      // A station that left (or joined) live.json: no dead buttons, so the row becomes static (or pressable).
      const el = createRowElement(alert, pressable);
      if (entry.el) entry.el.replaceWith(el);
      else entry.li.append(el);
      entry.el = el;
      entry.pressable = pressable;
      entry.values = [];
    }
    return entry;
  }

  /** Put `entries` into `list` in order, moving only rows that are out of place. */
  function place(list, entries) {
    const wanted = new Set(entries.map((e) => e.li));
    for (const li of [...list.children]) if (!wanted.has(li)) li.remove();
    entries.forEach((entry, i) => {
      const at = list.children[i];
      if (at !== entry.li) list.insertBefore(entry.li, at ?? null);
    });
  }

  function scroller() {
    return sheet.open ? sheet : body;
  }

  function reconcile(alerts, stations) {
    const now = Date.now();
    const { open, resolved } = ordered(alerts);
    const active = document.activeElement;
    const focused = body.contains(active) && active.matches(".alert-row") ? active.dataset.episode : null;
    const oldOrder = [...openList.children, ...resolvedList.children].map((li) => li.firstElementChild?.dataset.episode);
    const box = scroller();
    const scrollTop = box.scrollTop;

    const keep = new Set([...open, ...resolved].map((a) => a.episode_id));
    for (const [id, entry] of rows) {
      if (keep.has(id)) continue;
      entry.li.remove();
      rows.delete(id);
    }
    const openEntries = open.map((a) => entryFor(a, stations));
    const resolvedEntries = resolved.map((a) => entryFor(a, stations));
    openEntries.forEach((entry, i) => paintRow(entry, open[i], true, now));
    resolvedEntries.forEach((entry, i) => paintRow(entry, resolved[i], false, now));
    // Rows leaving a list are taken out first, so an alert that resolved moves once, not every row after it.
    place(openList, openEntries);
    place(resolvedList, resolvedEntries);

    counts.open.textContent = number.format(open.length);
    counts.resolved.textContent = number.format(resolved.length);
    none.hidden = open.length > 0;
    openList.hidden = open.length === 0;
    resolvedHeading.hidden = resolved.length === 0;
    resolvedList.hidden = resolved.length === 0;

    // Safety net (as in list.js): moving a focused node can drop focus. Put it back on the same alert, or on its nearest
    // neighbour if it left both lists, without scrolling.
    if (focused !== null) {
      const from = oldOrder.indexOf(focused);
      const candidates = [focused, ...oldOrder.slice(from + 1), ...oldOrder.slice(0, Math.max(from, 0)).reverse()];
      const target = candidates.map((id) => rows.get(id)).find(Boolean);
      if (target && document.activeElement !== target.el) target.el.focus({ preventScroll: true });
    }
    box.scrollTop = scrollTop;
    return open;
  }

  // ---- announcements (DESIGN §8: polite; nothing on the first read; nothing during replay) ----
  function announce(open, replay) {
    const ids = new Set(open.map((a) => a.episode_id));
    const fresh = baseline === null ? [] : open.filter((a) => !baseline.has(a.episode_id));
    baseline = ids;
    if (replay || fresh.length === 0) return;
    announcer.textContent =
      fresh.length === 1
        ? `New alert: ${fresh[0].name} is ${kindWord(fresh[0].kind).toLowerCase()}.`
        : `${number.format(fresh.length)} new alerts.`;
  }

  // ---- peek bar ----
  function paintPeek(alerts, replay) {
    peek.hidden = replay;
    if (alerts === undefined) {
      peekText.textContent = "Loading alerts…";
      peek.setAttribute("aria-disabled", "true");
      return;
    }
    peek.removeAttribute("aria-disabled");
    if (!alerts) {
      peekText.textContent = "Alerts couldn’t be loaded";
      return;
    }
    const n = alerts.open?.length ?? 0;
    peekText.textContent = n === 0 ? "No open alerts" : `${number.format(n)} open ${n === 1 ? "alert" : "alerts"}`;
  }

  /**
   * `alerts`: the parsed alerts.json, `false` when it couldn't be read (and there is no earlier copy), or undefined while
   * loading. `stations`: a Set of station ids in live.json (rows of other stations are not pressable), or null when
   * live.json couldn't be read (every row is static). `waiting`: live.json hasn't answered yet, so rows wait (they
   * would otherwise flip from static to pressable a moment later). `replay`: replay mode is on.
   */
  function render(alerts, { stations = null, replay = false, waiting = false } = {}) {
    last = { alerts, stations, replay, waiting };
    const loading = alerts === undefined || (Boolean(alerts) && waiting);
    body.setAttribute("aria-busy", String(loading && !replay));
    note.hidden = !replay;
    skeleton.hidden = replay || !loading;
    message.hidden = replay || loading || alerts !== false;
    lists.hidden = replay || loading || !alerts;
    for (const badge of Object.values(counts)) badge.hidden = replay;
    if (alerts && !waiting) {
      const open = reconcile(alerts, stations);
      announce(open, replay);
    }
    paintPeek(waiting && alerts ? undefined : alerts, replay);
    if (replay && sheet.open) closeSheet({ quiet: true });
  }

  /** The 15 s tick: durations move on. */
  function tick() {
    if (last.alerts && !last.waiting) {
      const now = Date.now();
      for (const entry of rows.values()) {
        paintRow(entry, entry.alert, entry.li.parentNode === openList, now);
      }
    }
  }

  // ---- sheet (phones) ----
  function openSheet() {
    if (sheet.open || peek.getAttribute("aria-disabled") === "true") return;
    sheetBody.append(body);
    sheet.showModal();
    document.documentElement.classList.add("is-scroll-locked");
  }

  function restoreBody() {
    if (body.parentNode !== railSlot) railSlot.insertBefore(body, railNext);
  }

  function closeSheet({ quiet = false } = {}) {
    if (!sheet.open) return;
    quietClose = quiet;
    sheet.close();
    restoreBody();
  }

  sheet.addEventListener("close", () => {
    restoreBody();
    if (!document.querySelector("dialog.sheet[open]")) document.documentElement.classList.remove("is-scroll-locked");
    if (!quietClose) peek.focus();
    quietClose = false;
  });
  sheet.querySelector("[data-alerts-sheet-close]").addEventListener("click", () => closeSheet());
  // A tap on the backdrop (the dialog element itself, outside its inner box) closes the sheet.
  sheet.addEventListener("click", (event) => {
    if (event.target === sheet) closeSheet();
  });
  peek.addEventListener("click", openSheet);
  // Rotating or resizing a phone past 600 px: the rail is back in the page, so the sheet closes.
  window.matchMedia("(max-width: 599.98px)").addEventListener("change", (event) => {
    if (!event.matches) closeSheet({ quiet: true });
  });

  body.querySelector("[data-alerts-retry]").addEventListener("click", () => onRetry());
  body.addEventListener("click", (event) => {
    const button = event.target.closest("button.alert-row");
    if (!button) return;
    const entry = rows.get(button.dataset.episode);
    if (!entry) return;
    if (sheet.open || isPhone()) closeSheet({ quiet: true });
    onSelect(entry.alert);
  });

  return { render, tick, isInside: (el) => body.contains(el) || sheet.contains(el) };
}
