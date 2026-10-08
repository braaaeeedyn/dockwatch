// Station state metadata for the Live page. The state itself always comes from live.json (`state`); the browser never
// recomputes it (DESIGN.md §2). This only maps a state to its word, draw tier and sort order.

/** Legend order (DESIGN.md §2 table). */
export const STATES = ["empty", "low", "ok", "high", "full", "offline"];

export const STATE_INFO = {
  empty: { word: "Empty", hint: "no bikes", tier: 3, severity: 0 },
  full: { word: "Full", hint: "no free docks", tier: 3, severity: 1 },
  low: { word: "Low", hint: "few bikes", tier: 2, severity: 2 },
  high: { word: "High", hint: "few free docks", tier: 2, severity: 3 },
  offline: { word: "Offline", hint: "not reporting or not renting", tier: 1, severity: 4 },
  ok: { word: "OK", hint: "bikes and docks available", tier: 0, severity: 5 },
};

/** Info for a state; an unexpected value is shown as-is rather than guessed. */
export function stateInfo(state) {
  return STATE_INFO[state] ?? { word: String(state ?? "Unknown"), hint: "", tier: 1, severity: 6 };
}

/** Every state except `ok` is a problem: drawn 1.35× larger and kept by "Show only problems". */
export function isProblem(state) {
  return state !== "ok";
}

export const REGIONS = {
  sf: { short: "San Francisco", long: "San Francisco" },
  eastbay: { short: "East Bay", long: "East Bay (Oakland, Emeryville, Berkeley)" },
  sj: { short: "San José", long: "San José" },
};

export const DEFAULT_REGION = "sf";
