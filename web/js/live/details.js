// Station details (DESIGN.md §6): a tooltip pinned beside the marker on tablet/desktop, a bottom sheet on phones.
// Contents in order: name, code, state word with swatch, "8 bikes (5 e-bikes) · 11 docks", "Empty for 34 min" when the
// station is in an episode (`state_since`), "Last reported 2 min ago".

import { formatDuration, parseTime } from "../util/time.js";
import { stateInfo } from "./states.js";

const PHONE = window.matchMedia("(max-width: 599.98px)");
const GAP_PX = 10;

export function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

function plural(n, one, many) {
  return `${n} ${n === 1 ? one : many}`;
}

/** The detail lines (everything after the name), as HTML. */
export function detailsBody(s, now = Date.now()) {
  const info = stateInfo(s.state);
  const since = parseTime(s.state_since);
  const reported = parseTime(s.last_reported);
  const ebikes = s.ebikes ? ` (${plural(s.ebikes, "e-bike", "e-bikes")})` : "";
  const lines = [
    `<p class="details__code data" translate="no">${s.code ? escapeHtml(s.code) : "No station code"}</p>`,
    `<p class="details__state"><span class="swatch" data-state="${escapeHtml(s.state)}" aria-hidden="true"></span>` +
      `<span>${escapeHtml(info.word)}${info.hint ? ` · ${escapeHtml(info.hint)}` : ""}</span></p>`,
    `<p class="details__counts">${plural(s.bikes, "bike", "bikes")}${ebikes} · ${plural(s.docks, "dock", "docks")}</p>`,
  ];
  if (since) lines.push(`<p class="details__since">${escapeHtml(info.word)} for ${formatDuration(now - since.getTime())}</p>`);
  const ago = reported ? now - reported.getTime() : null;
  const reportedText =
    ago === null ? "No report time from the station" : ago < 60_000 ? "Last reported under 1 min ago" : `Last reported ${formatDuration(ago)} ago`;
  lines.push(`<p class="details__reported">${reportedText}</p>`);
  return lines.join("");
}

export function isPhone() {
  return PHONE.matches;
}

/**
 * Tooltip + sheet controller. `positionOf(id)` comes from the map; `onClose(id, {restoreFocus})` lets the page clear the
 * selection and return focus to the marker.
 */
export function createDetails({ tooltip, sheet, positionOf, onClose }) {
  const sheetTitle = sheet.querySelector("[data-sheet-title]");
  const sheetBody = sheet.querySelector("[data-sheet-body]");
  let current = null; // station shown
  let mode = null; // "tooltip" | "sheet"

  function renderTooltip() {
    tooltip.innerHTML =
      `<p class="details__name">${escapeHtml(current.name)}</p>` + detailsBody(current);
    place();
  }

  function place() {
    if (mode !== "tooltip" || !current) return;
    const pos = positionOf(current.id);
    if (!pos) return;
    const right = pos.left < pos.width / 2;
    const vertical = pos.top < pos.height / 3 ? "top" : pos.top > (2 * pos.height) / 3 ? "bottom" : "middle";
    tooltip.dataset.side = right ? "right" : "left";
    tooltip.dataset.vertical = vertical;
    const offset = pos.radius + GAP_PX;
    tooltip.style.left = `${right ? pos.left + offset : pos.left - offset}px`;
    tooltip.style.top = `${pos.top}px`;
  }

  function show(station) {
    current = station;
    if (isPhone()) {
      if (mode === "tooltip") tooltip.hidden = true;
      mode = "sheet";
      sheetTitle.textContent = station.name;
      sheetBody.innerHTML = detailsBody(station);
      if (!sheet.open) {
        sheet.showModal();
        document.documentElement.classList.add("is-scroll-locked");
      }
    } else {
      if (sheet.open) closeSheetQuietly();
      mode = "tooltip";
      tooltip.hidden = false;
      renderTooltip();
    }
  }

  function closeSheetQuietly() {
    mode = null;
    sheet.close();
  }

  /** Close whatever is open. Returns the station id that was shown. */
  function hide() {
    const id = current?.id ?? null;
    if (mode === "sheet" && sheet.open) {
      sheet.close(); // the close listener tidies up
    } else {
      tooltip.hidden = true;
      mode = null;
      current = null;
    }
    return id;
  }

  /** New data arrived: refresh the open details (counts, durations) or close if the station is gone. */
  function refresh(lookup) {
    if (!current) return;
    const next = lookup(current.id);
    if (!next) {
      const id = hide();
      onClose(id, { restoreFocus: false });
      return;
    }
    current = next;
    if (mode === "tooltip") renderTooltip();
    else if (mode === "sheet") {
      sheetTitle.textContent = next.name;
      sheetBody.innerHTML = detailsBody(next);
    }
  }

  sheet.addEventListener("close", () => {
    document.documentElement.classList.remove("is-scroll-locked");
    const id = current?.id ?? null;
    if (mode === "sheet") {
      mode = null;
      current = null;
      onClose(id, { restoreFocus: true });
    }
  });
  sheet.querySelector("[data-sheet-close]").addEventListener("click", () => sheet.close());
  // A tap on the backdrop (the dialog element itself, outside its inner box) closes the sheet.
  sheet.addEventListener("click", (event) => {
    if (event.target === sheet) sheet.close();
  });

  return {
    show,
    hide,
    place,
    refresh,
    isOpen: () => current !== null,
    currentId: () => current?.id ?? null,
  };
}
