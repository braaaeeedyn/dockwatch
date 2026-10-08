// Freshness pill + footer "Data through": reads data/live.json `generated_at` (pill) and `data_as_of` (footer).
// Refetches every 60 s and re-evaluates the age every 15 s, so the pill turns Delayed/Paused on its own.
// This is the page's only live.json poll: each successful fetch is broadcast as a "dockwatch:live" event (the Live
// page's map listens), a failed one as "dockwatch:live-error". A read that hasn't answered after 10 s is aborted and
// counts as failed, so a stalled connection shows "No data yet" (and the Live page's Retry), not "Loading…".
// Every render also broadcasts "dockwatch:freshness" ({state, generatedAt}), so the pill, the Live page's paused banner
// and the announcer below flip on the same tick, from the one freshnessState() rule.
// Pages without the paused banner get a visually hidden polite announcer that speaks only on transitions (into
// Paused, and back), never on first load. On the Live page the banner's role="status" is the announcement instead.

import {
  FRESHNESS_WORDS,
  formatClock,
  formatDateTime,
  formatMonthDay,
  formatPausedAt,
  freshnessState,
  isTodayPacific,
  parseTime,
} from "./util/time.js";

export const LIVE_URL = "data/live.json";
const REFRESH_MS = 60 * 1000;
const TICK_MS = 15 * 1000;
const READ_TIMEOUT_MS = 10_000; // setTimeout, not AbortSignal.timeout(): the tests' fake clock drives it

let latest = null;
let announcer = null;
let announcedState = null; // freshness state last seen with data; null until the first successful read

/** The paused sentence shared by the Live page's banner and the announcer (DESIGN §6). */
export function pausedSentence(generatedAt) {
  return `Live data paused — showing the state at ${formatPausedAt(generatedAt)}.`;
}

function announce(state, generatedAt) {
  if (!latest) return; // no data read yet: nothing has changed, so nothing to say
  const previous = announcedState;
  announcedState = state;
  if (previous === null || !announcer || document.querySelector("[data-stale-banner]")) return;
  if (state === "paused" && previous !== "paused") {
    announcer.textContent = generatedAt ? pausedSentence(generatedAt) : "Live data paused.";
  } else if (previous === "paused" && state !== "paused") {
    announcer.textContent = "Live data is back.";
  }
}

/** Fetch live.json; resolves to the parsed export or null if it can't be read. */
export async function fetchLive() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), READ_TIMEOUT_MS);
  try {
    const response = await fetch(LIVE_URL, { cache: "no-store", signal: controller.signal });
    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function render() {
  const generatedAt = parseTime(latest?.generated_at);
  const state = freshnessState(generatedAt);
  for (const pill of document.querySelectorAll("[data-freshness]")) {
    pill.dataset.state = state;
    pill.querySelector("[data-freshness-word]").textContent = FRESHNESS_WORDS[state];
    pill.querySelector("[data-freshness-time]").textContent = generatedAt
      ? ` · Data as of ${isTodayPacific(generatedAt) ? "" : `${formatMonthDay(generatedAt)}, `}${formatClock(generatedAt)}`
      : " · No data yet";
  }
  announce(state, generatedAt);
  document.dispatchEvent(new CustomEvent("dockwatch:freshness", { detail: { state, generatedAt } }));
  const asOf = parseTime(latest?.data_as_of);
  for (const el of document.querySelectorAll("[data-data-through]")) {
    el.textContent = asOf ? `Data through ${formatDateTime(asOf)}` : "Data through: not available yet";
  }
}

/** Fetch live.json now and broadcast the result. Also used by the Live page's "Retry" action. */
export async function refreshLive() {
  const data = await fetchLive();
  if (data) {
    latest = data;
    document.dispatchEvent(new CustomEvent("dockwatch:live", { detail: data }));
  } else {
    document.dispatchEvent(new CustomEvent("dockwatch:live-error"));
  }
  render();
}

/** The last export read successfully, or null. */
export function latestLive() {
  return latest;
}

export function initFreshness() {
  if (!document.querySelector("[data-stale-banner]")) {
    announcer = document.createElement("p");
    announcer.className = "visually-hidden";
    announcer.setAttribute("aria-live", "polite");
    announcer.dataset.freshnessAnnouncer = "";
    (document.querySelector("main") ?? document.body).append(announcer);
  }
  refreshLive();
  setInterval(refreshLive, REFRESH_MS);
  setInterval(render, TICK_MS);
}
