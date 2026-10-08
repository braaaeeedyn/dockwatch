// Time helpers shared by the shell and (from F2) the map. Times are shown in Pacific time.

export const TIME_ZONE = "America/Los_Angeles";
export const LIVE_MAX_MS = 2 * 60 * 1000; // < 2 min: Live
export const DELAYED_MAX_MS = 5 * 60 * 1000; // 2–5 min: Delayed, > 5 min: Paused

const LOCALE = "en-US"; // the copy is English; keep number/date order consistent with it

const clockFormat = new Intl.DateTimeFormat(LOCALE, {
  timeZone: TIME_ZONE,
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
});

const dateTimeFormat = new Intl.DateTimeFormat(LOCALE, {
  timeZone: TIME_ZONE,
  month: "short",
  day: "numeric",
  year: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

const shortClockFormat = new Intl.DateTimeFormat(LOCALE, {
  timeZone: TIME_ZONE,
  hour: "numeric",
  minute: "2-digit",
});

/** "3:42 PM" (Pacific). */
export function formatShortClock(date) {
  return shortClockFormat.format(date);
}

/** A duration in plain words: "under 1 min", "34 min", "2 h 5 min", "3 days". */
export function formatDuration(ms) {
  const minutes = Math.floor(Math.max(0, ms) / 60_000);
  if (minutes < 1) return "under 1 min";
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return minutes % 60 ? `${hours} h ${minutes % 60} min` : `${hours} h`;
  return `${Math.floor(hours / 24)} days`;
}

/** "3:42:10 PM" (Pacific). */
export function formatClock(date) {
  return clockFormat.format(date);
}

/** "Oct 7, 2026, 9:19 PM" (Pacific). */
export function formatDateTime(date) {
  return dateTimeFormat.format(date);
}

/** Parse an ISO timestamp from the exports; returns null when missing or invalid. */
export function parseTime(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

/**
 * Freshness of an export from its `generated_at` and the current time.
 * Returns "live" (< 2 min), "delayed" (2–5 min) or "paused" (> 5 min, or no timestamp).
 */
export function freshnessState(generatedAt, now = Date.now()) {
  if (!generatedAt) return "paused";
  const age = now - generatedAt.getTime();
  if (age < LIVE_MAX_MS) return "live";
  if (age <= DELAYED_MAX_MS) return "delayed";
  return "paused";
}

export const FRESHNESS_WORDS = { live: "Live", delayed: "Delayed", paused: "Paused" };
