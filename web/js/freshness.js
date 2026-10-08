// Freshness pill + footer "Data through": reads data/live.json `generated_at` (pill) and `data_as_of` (footer).
// Refetches every 60 s and re-evaluates the age every 15 s, so the pill turns Delayed/Paused on its own.
// This is the page's only live.json poll: each successful fetch is broadcast as a "dockwatch:live" event (the Live
// page's map listens), a failed one as "dockwatch:live-error".

import { FRESHNESS_WORDS, formatClock, formatDateTime, freshnessState, parseTime } from "./util/time.js";

export const LIVE_URL = "data/live.json";
const REFRESH_MS = 60 * 1000;
const TICK_MS = 15 * 1000;

let latest = null;

/** Fetch live.json; resolves to the parsed export or null if it can't be read. */
export async function fetchLive() {
  try {
    const response = await fetch(LIVE_URL, { cache: "no-store" });
    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  }
}

function render() {
  const generatedAt = parseTime(latest?.generated_at);
  const state = freshnessState(generatedAt);
  for (const pill of document.querySelectorAll("[data-freshness]")) {
    pill.dataset.state = state;
    pill.querySelector("[data-freshness-word]").textContent = FRESHNESS_WORDS[state];
    pill.querySelector("[data-freshness-time]").textContent = generatedAt
      ? ` · Data as of ${formatClock(generatedAt)}`
      : " · No data yet";
  }
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
  refreshLive();
  setInterval(refreshLive, REFRESH_MS);
  setInterval(render, TICK_MS);
}
