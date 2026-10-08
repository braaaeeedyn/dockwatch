// The live station map (DESIGN.md §6): SVG land per region and one <circle> marker per station.
//
// - Markers are keyed by station id and updated in place on refresh (same DOM nodes); a marker whose state changed
//   cross-fades its fill (CSS) and pulses once (Web Animations, skipped under reduced motion).
// - Draw order = four tier groups: ok, offline, low/high, empty/full on top.
// - Sizes are in screen pixels: a ResizeObserver sets --u (view-box units per CSS pixel) and --r-ok on the <svg>,
//   and map.css turns them into radii, strokes and label sizes. No layout reads while rendering.
// - Roving tabindex: exactly one marker is in the tab order; arrow keys move to the nearest station in that direction.
// - Pointer: hover / click pick the nearest station within 10 px (22 px for touch). No wheel or pinch handling, so
//   the map never captures page scroll.

import { project } from "./project.js";
import { isProblem, stateInfo } from "../live/states.js";

const SVG_NS = "http://www.w3.org/2000/svg";
const VIEWBOX = 1000;
const HIT_PX = { touch: 22, pointer: 10 };
const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

function svgEl(name, attrs = {}) {
  const el = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, value);
  return el;
}

/** Accessible name of a marker: everything the details show, in one sentence. */
export function markerLabel(s) {
  const info = stateInfo(s.state);
  const code = s.code ? ` (${s.code})` : "";
  return `${s.name}${code}: ${info.word.toLowerCase()}, ${s.bikes} ${s.bikes === 1 ? "bike" : "bikes"}, ${s.docks} ${s.docks === 1 ? "dock" : "docks"}`;
}

const DIRECTIONS = {
  ArrowRight: [1, 0],
  ArrowLeft: [-1, 0],
  ArrowDown: [0, 1],
  ArrowUp: [0, -1],
};

export function createStationMap({ svg, frame, onSelect, onResize }) {
  const shore = svg.querySelector("[data-shore]");
  const land = svg.querySelector("[data-land]");
  const landmarks = svg.querySelector("[data-landmarks]");
  const tiers = [...svg.querySelectorAll("[data-tier]")];
  const hiRing = svg.querySelector('[data-ring="highlight"]');
  const selRing = svg.querySelector('[data-ring="selected"]');
  const label = svg.querySelector("[data-marker-label]");

  /** id -> { el, station, x, y } */
  const markers = new Map();
  let projection = null;
  let rovingId = null;
  let hoverId = null;
  let focusedId = null;
  let selectedId = null;
  let size = { w: VIEWBOX, h: VIEWBOX, s: 1, ox: 0, oy: 0, rOk: 4 };

  // ---- size: screen px <-> view-box units ----
  const observer = new ResizeObserver((entries) => {
    const box = entries[entries.length - 1].contentRect;
    if (!box.width || !box.height) return;
    const s = Math.min(box.width, box.height) / VIEWBOX;
    const rOk = Math.min(5, Math.max(3, box.width * 0.0045));
    size = { w: box.width, h: box.height, s, ox: (box.width - VIEWBOX * s) / 2, oy: (box.height - VIEWBOX * s) / 2, rOk };
    svg.style.setProperty("--u", String(1 / s));
    svg.style.setProperty("--r-ok", `${rOk}px`);
    onResize?.();
  });
  observer.observe(frame);

  // ---- base layer ----
  function setGeo(geo) {
    projection = geo.projection;
    const d = geo.land.join("");
    // Shoreline trick: a 2 px stroke under the fill shows 1 px outside the coast and hides county borders.
    shore.setAttribute("d", d);
    land.setAttribute("d", d);
    landmarks.replaceChildren(
      ...geo.landmarks.map((m) => {
        const text = svgEl("text", { x: m.x, y: m.y, "text-anchor": "middle", class: "map__landmark" });
        text.textContent = m.name;
        return text;
      }),
    );
  }

  function clear() {
    for (const tier of tiers) tier.replaceChildren();
    markers.clear();
    rovingId = hoverId = focusedId = selectedId = null;
    painted = new Set();
    paintedSelected = null;
    paintHighlight();
  }

  // ---- markers ----
  function update(stations, { animate = false } = {}) {
    if (!projection) return;
    const seen = new Set();
    const active = document.activeElement;
    for (const s of stations) {
      if (s.lat == null || s.lon == null) continue;
      seen.add(s.id);
      const [x, y] = project(s.lon, s.lat, projection);
      let m = markers.get(s.id);
      if (!m) {
        const el = svgEl("circle", { class: "marker", role: "button", tabindex: "-1" });
        el.dataset.id = s.id;
        m = { el, station: s, x, y };
        markers.set(s.id, m);
      } else if (m.station.state !== s.state && animate && !reducedMotion.matches) {
        m.el.animate([{ transform: "scale(1)" }, { transform: "scale(1.6)" }, { transform: "scale(1)" }], {
          duration: 600,
          easing: "cubic-bezier(0.2, 0, 0, 1)",
        });
      }
      m.station = s;
      m.x = x;
      m.y = y;
      const { el } = m;
      el.setAttribute("cx", x.toFixed(1));
      el.setAttribute("cy", y.toFixed(1));
      el.dataset.state = s.state;
      el.classList.toggle("is-problem", isProblem(s.state));
      el.setAttribute("aria-label", markerLabel(s));
      const tier = tiers[stateInfo(s.state).tier];
      if (el.parentNode !== tier) {
        tier.append(el);
        if (active === el) el.focus({ preventScroll: true }); // moving a node drops its focus
      }
    }
    for (const [id, m] of markers) {
      if (seen.has(id)) continue;
      m.el.remove();
      markers.delete(id);
      if (hoverId === id) hoverId = null;
      if (focusedId === id) focusedId = null;
      if (selectedId === id) selectedId = null;
    }
    if (!markers.has(rovingId)) setRoving(defaultRovingId());
    paintHighlight();
  }

  /** First stop for keyboard users: the top-left-most station of the most severe tier present. */
  function defaultRovingId() {
    for (let t = tiers.length - 1; t >= 0; t -= 1) {
      const first = tiers[t].firstElementChild;
      if (first) {
        let best = null;
        for (const el of tiers[t].children) {
          const m = markers.get(el.dataset.id);
          if (!best || m.y + m.x < best.y + best.x) best = m;
        }
        return best ? best.el.dataset.id : first.dataset.id;
      }
    }
    return null;
  }

  function setRoving(id) {
    if (rovingId && markers.has(rovingId)) markers.get(rovingId).el.setAttribute("tabindex", "-1");
    rovingId = id;
    if (id && markers.has(id)) markers.get(id).el.setAttribute("tabindex", "0");
  }

  // ---- highlight: enlarged marker, ink ring(s) and the name label ----
  function placeRing(ring, m) {
    if (!m) {
      ring.setAttribute("visibility", "hidden");
      return;
    }
    ring.setAttribute("cx", m.x.toFixed(1));
    ring.setAttribute("cy", m.y.toFixed(1));
    ring.style.setProperty("--m", isProblem(m.station.state) ? "1.35" : "1");
    ring.setAttribute("visibility", "visible");
  }

  let painted = new Set();
  let paintedSelected = null;

  function paintHighlight() {
    const hiId = hoverId ?? focusedId;
    const now = new Set([hiId, focusedId, selectedId].filter((id) => id && markers.has(id)));
    for (const id of painted) if (!now.has(id)) markers.get(id)?.el.classList.remove("is-active");
    for (const id of now) markers.get(id).el.classList.add("is-active");
    painted = now;
    if (paintedSelected !== selectedId) {
      markers.get(paintedSelected)?.el.removeAttribute("aria-expanded");
      markers.get(selectedId)?.el.setAttribute("aria-expanded", "true");
      paintedSelected = selectedId;
    }
    const sel = markers.get(selectedId);
    const hi = hiId && hiId !== selectedId ? markers.get(hiId) : null;
    placeRing(selRing, sel);
    placeRing(hiRing, hi);
    // The name label follows hover/focus; a selected station's name is already in its tooltip or sheet.
    const named = hi;
    if (!named) {
      label.setAttribute("visibility", "hidden");
      return;
    }
    const anchor = named.x < 200 ? "start" : named.x > 800 ? "end" : "middle";
    label.setAttribute("x", named.x.toFixed(1));
    label.setAttribute("y", named.y.toFixed(1));
    label.setAttribute("text-anchor", anchor);
    label.style.setProperty("--m", isProblem(named.station.state) ? "1.35" : "1");
    label.textContent = named.station.name;
    label.setAttribute("visibility", "visible");
  }

  function setSelected(id) {
    selectedId = id && markers.has(id) ? id : null;
    paintHighlight();
  }

  function focusStation(id) {
    const m = markers.get(id);
    if (!m) return;
    setRoving(id);
    m.el.focus({ preventScroll: true });
  }

  // ---- geometry helpers ----
  function toViewBox(event) {
    const ctm = svg.getScreenCTM();
    if (!ctm) return null;
    const p = new DOMPoint(event.clientX, event.clientY).matrixTransform(ctm.inverse());
    return [p.x, p.y];
  }

  function nearest(x, y, maxPx) {
    const max = maxPx / size.s;
    let best = null;
    let bestD = max * max;
    for (const m of markers.values()) {
      const d = (m.x - x) ** 2 + (m.y - y) ** 2;
      if (d <= bestD) {
        bestD = d;
        best = m;
      }
    }
    return best;
  }

  /** Nearest station from `fromId` in an arrow direction: prefer the 90° cone, then the half-plane. */
  function neighbour(fromId, [dx, dy]) {
    const from = markers.get(fromId);
    if (!from) return null;
    let best = null;
    let bestScore = Infinity;
    let fallback = null;
    let fallbackScore = Infinity;
    for (const m of markers.values()) {
      if (m === from) continue;
      const along = (m.x - from.x) * dx + (m.y - from.y) * dy;
      if (along <= 0) continue;
      const across = Math.abs((m.x - from.x) * dy - (m.y - from.y) * dx);
      const score = along + 2 * across;
      if (across <= along && score < bestScore) {
        best = m;
        bestScore = score;
      } else if (score < fallbackScore) {
        fallback = m;
        fallbackScore = score;
      }
    }
    return best ?? fallback;
  }

  /** Where a station sits inside the frame, in CSS px (for the tooltip). */
  function positionOf(id) {
    const m = markers.get(id);
    if (!m) return null;
    const mult = isProblem(m.station.state) ? 1.35 : 1;
    return {
      left: size.ox + m.x * size.s,
      top: size.oy + m.y * size.s,
      radius: size.rOk * mult * 1.6,
      width: size.w,
      height: size.h,
    };
  }

  // ---- events ----
  svg.addEventListener("keydown", (event) => {
    const id = event.target.dataset?.id;
    if (!id) return;
    const direction = DIRECTIONS[event.key];
    if (direction) {
      event.preventDefault();
      const next = neighbour(id, direction);
      if (next) focusStation(next.station.id);
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelect(selectedId === id ? null : id, { via: "keyboard" });
    }
  });

  svg.addEventListener("focusin", (event) => {
    const id = event.target.dataset?.id;
    if (!id) return;
    if (id !== rovingId) setRoving(id);
    focusedId = id;
    paintHighlight();
  });

  svg.addEventListener("focusout", () => {
    focusedId = null;
    paintHighlight();
  });

  svg.addEventListener("pointermove", (event) => {
    if (event.pointerType === "touch") return;
    const p = toViewBox(event);
    const m = p && nearest(p[0], p[1], HIT_PX.pointer);
    const id = m ? m.station.id : null;
    svg.classList.toggle("is-pointing", Boolean(m));
    if (id !== hoverId) {
      hoverId = id;
      paintHighlight();
    }
  });

  svg.addEventListener("pointerleave", () => {
    svg.classList.remove("is-pointing");
    if (hoverId !== null) {
      hoverId = null;
      paintHighlight();
    }
  });

  svg.addEventListener("click", (event) => {
    let id = event.target.dataset?.id ?? null;
    if (!id) {
      const p = toViewBox(event);
      const m = p && nearest(p[0], p[1], event.pointerType === "touch" ? HIT_PX.touch : HIT_PX.pointer);
      id = m ? m.station.id : null;
    }
    if (id) setRoving(id);
    onSelect(id, { via: "pointer" });
  });

  return {
    setGeo,
    clear,
    update,
    setSelected,
    focusStation,
    positionOf,
    has: (id) => markers.has(id),
    count: () => markers.size,
  };
}
