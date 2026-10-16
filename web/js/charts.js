// Shared SVG chart helpers for Insights and Pipeline (DESIGN.md §2 "Data: heatmaps and charts", §7 chart card): horizontal gridlines only, no
// chart borders, ticks and labels at `caption` size. Charts are drawn at their container's pixel width (fixed text
// size; fewer ticks below 600 px) and redrawn when the container is resized.

const SVG = "http://www.w3.org/2000/svg";

/** An SVG element with attributes (and optional text). */
export function svgEl(tag, attrs = {}, text = null) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, String(v));
  if (text !== null) el.textContent = text;
  return el;
}

/** About `count` round tick values from 0 to at least `max` (1, 2 or 5 times a power of ten apart). */
export function ticks(max, count = 5) {
  const top = max > 0 ? max : 1;
  const raw = top / count;
  const pow = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= raw);
  const out = [];
  for (let v = 0; v < top + step / 2; v += step) out.push(Number(v.toPrecision(12)));
  if (out[out.length - 1] < top) out.push(Number((out[out.length - 1] + step).toPrecision(12)));
  return out;
}

/**
 * Round duration ticks for an axis in seconds, in one unit picked from the axis maximum: seconds below 2 min, minutes
 * below 2 h, hours above. Returns the tick values (seconds) and their labels' formatter ("30 s", "20 min", "12 h").
 */
export function durationTicks(maxS, count = 5) {
  const [size, unit] = maxS < 120 ? [1, "s"] : maxS < 7200 ? [60, "min"] : [3600, "h"];
  return {
    values: ticks(maxS / size, count).map((v) => v * size),
    format: (v) => `${Number((v / size).toPrecision(12))} ${unit}`,
  };
}

/** Call `draw(width)` now, whenever `box` changes width, and once the web fonts have loaded (labels are measured). */
export function drawOnResize(box, draw) {
  let width = 0;
  const redraw = () => {
    const w = Math.round(box.clientWidth);
    if (w > 0 && w !== width) {
      width = w;
      draw(w);
    }
  };
  redraw();
  if ("ResizeObserver" in window) new ResizeObserver(redraw).observe(box);
  document.fonts?.ready.then(() => {
    width = 0;
    redraw();
  });
}

/**
 * Scatter plot into `svg`: points [{x, y}], axes from 0, labelled `xLabel` / `yLabel`, values formatted by
 * `formatX` / `formatY`. The svg keeps its role="img" and aria-label (set by the caller).
 */
export function scatter(svg, points, { width, xLabel, yLabel, formatX = String, formatY = String }) {
  const narrow = width < 600;
  const height = Math.round(Math.min(420, Math.max(260, width * 0.6)));
  const m = { top: 12, right: 16, bottom: 44, left: 64 };
  const plotW = width - m.left - m.right;
  const plotH = height - m.top - m.bottom;
  const xt = ticks(Math.max(...points.map((p) => p.x), 0), narrow ? 4 : 6);
  const yt = ticks(Math.max(...points.map((p) => p.y), 0), narrow ? 4 : 5);
  const sx = (v) => m.left + (v / xt[xt.length - 1]) * plotW;
  const sy = (v) => m.top + plotH - (v / yt[yt.length - 1]) * plotH;

  svg.replaceChildren();
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("height", String(height));
  const grid = svgEl("g", { class: "chart__grid", "aria-hidden": "true" });
  for (const v of yt) {
    grid.append(svgEl("line", { x1: m.left, x2: width - m.right, y1: sy(v), y2: sy(v) }));
    grid.append(svgEl("text", { x: m.left - 8, y: sy(v), class: "chart__tick", "text-anchor": "end", dy: "0.32em" }, formatY(v)));
  }
  for (const v of xt) {
    grid.append(svgEl("text", { x: sx(v), y: height - m.bottom + 18, class: "chart__tick", "text-anchor": "middle" }, formatX(v)));
  }
  grid.append(svgEl("text", { x: m.left + plotW / 2, y: height - 6, class: "chart__axis-label", "text-anchor": "middle" }, xLabel));
  grid.append(svgEl("text", { x: 14, y: m.top + plotH / 2, class: "chart__axis-label", "text-anchor": "middle", transform: `rotate(-90 14 ${m.top + plotH / 2})` }, yLabel));
  const dots = svgEl("g", { class: "chart__points", "aria-hidden": "true" });
  for (const p of points) dots.append(svgEl("circle", { cx: sx(p.x).toFixed(1), cy: sy(p.y).toFixed(1), r: 3 }));
  svg.append(grid, dots);
}

/**
 * Line chart into `svg` over a time axis: `times` (Date[]), `series` [{values, className}] (null = gap),
 * an optional `threshold` {value, label} drawn dashed with its label at the line end. y from 0, with ticks from
 * `yTicks(max, count)` ({values, format}; default: round numbers labelled by `formatY`). The left margin fits the
 * widest y label, as measured in the page, beside the rotated y title, so the two never overlap.
 */
export function lineChart(svg, times, series, { width, yLabel, formatY = String, formatTime, threshold = null, yTicks = null }) {
  const narrow = width < 600;
  const height = Math.round(Math.min(320, Math.max(220, width * 0.4)));
  const values = series.flatMap((s) => s.values).filter((v) => v !== null);
  const max = Math.max(...values, threshold ? threshold.value : 0, 0);
  const y = yTicks ? yTicks(max, narrow ? 3 : 4) : { values: ticks(max, narrow ? 3 : 4), format: formatY };
  const yt = y.values;

  svg.replaceChildren();
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("height", String(height));
  const grid = svgEl("g", { class: "chart__grid", "aria-hidden": "true" });
  svg.append(grid);
  const labels = yt.map((v) => grid.appendChild(svgEl("text", { class: "chart__tick", "text-anchor": "end", dy: "0.32em" }, y.format(v))));
  const title = grid.appendChild(svgEl("text", { class: "chart__axis-label", "text-anchor": "middle" }, yLabel));
  const titleBox = title.getBBox(); // at y = 0: its top is -ascent; once rotated, its line height is its width
  const titleW = Math.ceil(titleBox.height);
  const labelW = Math.ceil(Math.max(0, ...labels.map((t) => t.getComputedTextLength())));
  const m = { top: 16, right: 16, bottom: 32, left: 2 + titleW + 8 + labelW + 8 };
  const plotW = width - m.left - m.right;
  const plotH = height - m.top - m.bottom;
  const t0 = times[0].getTime();
  const span = Math.max(1, times[times.length - 1].getTime() - t0);
  const sx = (t) => m.left + ((t.getTime() - t0) / span) * plotW;
  const sy = (v) => m.top + plotH - (v / yt[yt.length - 1]) * plotH;

  yt.forEach((v, i) => {
    labels[i].setAttribute("x", String(m.left - 8));
    labels[i].setAttribute("y", String(sy(v)));
    grid.insertBefore(svgEl("line", { x1: m.left, x2: width - m.right, y1: sy(v), y2: sy(v) }), labels[0]);
  });
  const step = narrow ? 8 : 4; // hours between time ticks
  for (let i = 0; i < times.length; i += 60 * step) {
    grid.append(svgEl("text", { x: sx(times[i]), y: height - 10, class: "chart__tick", "text-anchor": "middle" }, formatTime(times[i])));
  }
  const tx = 2 - titleBox.y; // rotated -90° about its baseline: it spans tx - ascent .. tx + descent = 2 .. 2 + titleW
  title.setAttribute("x", String(tx));
  title.setAttribute("y", String(m.top + plotH / 2));
  title.setAttribute("transform", `rotate(-90 ${tx} ${m.top + plotH / 2})`);
  const lines = svgEl("g", { "aria-hidden": "true" });
  for (const s of series) {
    let d = "";
    let pen = false;
    s.values.forEach((v, i) => {
      if (v === null) {
        pen = false;
        return;
      }
      d += `${pen ? "L" : "M"}${sx(times[i]).toFixed(1)} ${sy(v).toFixed(1)}`;
      pen = true;
    });
    lines.append(svgEl("path", { d, class: `chart__line ${s.className}` }));
    s.values.forEach((v, i) => {
      if (v !== null && (s.values[i - 1] ?? null) === null && (s.values[i + 1] ?? null) === null) {
        lines.append(svgEl("circle", { cx: sx(times[i]).toFixed(1), cy: sy(v).toFixed(1), r: 3, class: `chart__dot ${s.className}` }));
      }
    });
  }
  if (threshold) {
    const ty = sy(threshold.value);
    lines.append(svgEl("line", { x1: m.left, x2: width - m.right, y1: ty, y2: ty, class: "chart__threshold" }));
    lines.append(svgEl("text", { x: width - m.right, y: ty - 6, class: "chart__threshold-label", "text-anchor": "end" }, threshold.label));
  }
  svg.append(lines);
}
