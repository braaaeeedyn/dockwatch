// Replay a day (DESIGN.md §6, §7): data/replay.json (format v1, built by `python tasks.py replay-export` from the raw
// GBFS archive) decoded into station objects shaped like live.json's, and a player that steps through its frames at
// 10x speed with plain timers (so tests drive it with page.clock). The page renders each frame through its normal
// code paths; this module knows nothing about the DOM.

export const REPLAY_URL = "data/replay.json";
export const SPEED = 10; // DESIGN §7: "10× speed"

/** True if a replay file is published (HEAD request, so the file itself is only fetched on demand). */
export async function replayAvailable() {
  try {
    const response = await fetch(REPLAY_URL, { method: "HEAD", cache: "no-store" });
    return response.ok;
  } catch {
    return false;
  }
}

/** Fetch and check replay.json; throws if it can't be read or isn't format v1. */
export async function loadReplay() {
  const response = await fetch(REPLAY_URL, { cache: "no-store" });
  if (!response.ok) throw new Error(`${REPLAY_URL}: HTTP ${response.status}`);
  const doc = await response.json();
  const ok =
    doc?.version === 1 &&
    doc.kind === "dockwatch-replay" &&
    Array.isArray(doc.stations) &&
    Array.isArray(doc.frames) &&
    doc.frames.length > 0 &&
    Array.isArray(doc.states) &&
    Number.isInteger(doc.step_s) &&
    doc.step_s > 0;
  if (!ok) throw new Error(`${REPLAY_URL}: not a DockWatch replay file (format v1)`);
  return doc;
}

/**
 * Decoder: `seek(k)` applies frame deltas incrementally (forward from the current frame, or from frame 0 when going
 * back) and returns a live.json-like export for frame k: stations, counts per view and in total, bikes available.
 */
export function createDecoder(doc) {
  const base = doc.stations.map((s) => ({
    id: s.id,
    name: s.name,
    code: s.code ?? null,
    lat: s.lat,
    lon: s.lon,
    region: null,
    view: s.view,
    capacity: s.capacity ?? null,
  }));
  let values = []; // index -> [state, bikes, docks]
  let position = -1;

  function apply(k) {
    const d = doc.frames[k].d;
    for (let j = 0; j < d.length; j += 4) values[d[j]] = [doc.states[d[j + 1]], d[j + 2], d[j + 3]];
  }

  function seek(k) {
    if (k < position) {
      values = [];
      position = -1;
    }
    while (position < k) {
      position += 1;
      apply(position);
    }
    const t = doc.frames[k].t;
    const stations = [];
    const counts = { all: {} };
    let bikes = 0;
    base.forEach((s, i) => {
      const v = values[i];
      if (!v) return; // not seen yet in this replay
      const [state, b, docks] = v;
      stations.push({
        ...s,
        bikes: b,
        ebikes: null,
        docks,
        state,
        state_since: null,
        last_reported: null,
        snapshot: t,
        replay_at: t,
      });
      for (const key of ["all", s.view]) {
        counts[key] ??= {};
        counts[key][state] = (counts[key][state] ?? 0) + 1;
      }
      bikes += b;
    });
    return { generated_at: t, data_as_of: t, station_count: stations.length, bikes_available: bikes, counts, stations };
  }

  return { seek, frameCount: doc.frames.length };
}

/** Steps through the frames, one every `step_s * 1000 / SPEED` ms, looping to frame 0 after the last. */
export function createPlayer(doc, onFrame) {
  const decoder = createDecoder(doc);
  const interval = (doc.step_s * 1000) / SPEED;
  let frame = 0;
  let timer = null;

  function show() {
    onFrame(decoder.seek(frame), frame);
  }

  return {
    start() {
      frame = 0;
      show();
      timer = setInterval(() => {
        frame = (frame + 1) % decoder.frameCount;
        show();
      }, interval);
    },
    stop() {
      clearInterval(timer);
      timer = null;
    },
  };
}
