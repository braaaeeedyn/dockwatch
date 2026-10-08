---
version: 1.0.0
status: frozen            # locked from the start of F1 until launch (F5 done). See "Change policy" at the end.
name: DockWatch
description: The design system for DockWatch, a real-time Bay Wheels station monitor and lakehouse demo. A calm, neutral operations console in light and dark themes, built on IBM Plex type and flat 10 px cards, where colour means only one thing — a station's availability state — so problem stations are the first thing the eye finds.
source: docs/reference/DESIGN_SOURCE_transitpulse.md (the "colour = data" rule, fluid sizing, accessibility minimums and change policy carry over; the look, palette, type and components are DockWatch's own)

colors:
  # Interface (chrome) — neutrals only. Light theme is the default; dark mirrors it.
  light:
    canvas: "#ffffff"
    surface: "#f4f5f7"        # stat tiles, chips, inputs, map water
    surface-2: "#eaecef"      # pressed/selected, nested surfaces
    hairline: "#dfe2e6"       # card borders, dividers, table rows
    ink: "#111418"            # headings, body, primary button
    on-ink: "#ffffff"
    body: "#4b5563"           # secondary text; 7.6:1 canvas, 6.4:1 surface-2
    mute: "#626a75"           # timestamps, captions; 5.5:1 canvas, 4.6:1 surface-2
    focus: "#111418"
    map-water: "#f4f5f7"
    map-land: "#ffffff"
    map-shoreline: "#c9ced4"
    marker-casing: "#111418"
  dark:
    canvas: "#0e1116"
    surface: "#161b22"
    surface-2: "#1f262f"
    hairline: "#2b333d"
    ink: "#e8ecf1"
    on-ink: "#0e1116"
    body: "#b4bcc6"           # 9.9:1 canvas
    mute: "#8b95a1"           # 6.2:1 canvas, 5.0:1 surface-2
    focus: "#e8ecf1"
    map-water: "#0e1116"
    map-land: "#1b2129"
    map-shoreline: "#3a434e"
    marker-casing: "#0e1116"

  # Data — station availability state. Only inside the map, the list's state cell, charts and their legends.
  # Built on the Okabe–Ito colour-blind-safe set: "too few bikes" is warm, "too few docks" is cool.
  state:
    empty: { light: "#d55e00", dark: "#ff7a33" }   # 0 bikes
    low:   { light: "#e69f00", dark: "#f0b429" }   # ≤ 2 bikes or ≤ 10% of capacity
    ok:    { light: "#8c959f", dark: "#6e7781" }   # neither — deliberately quiet
    high:  { light: "#56b4e9", dark: "#7cc6f0" }   # ≤ 2 docks or ≤ 10% of capacity
    full:  { light: "#0072b2", dark: "#3d9be0" }   # 0 docks
    offline: "transparent fill, 1.5px dashed {mute} ring"   # not installed / not renting / stale > 30 min
  state-text:                     # when a state word is coloured in text (list view), it uses these AA values
    empty: { light: "#b84f00", dark: "#ff7a33" }
    full:  { light: "#0072b2", dark: "#3d9be0" }
  # Sequential ramps for heatmaps (5 steps, light → strong). Empty-frequency is warm, full-frequency is cool.
  seq-empty: { light: ["#fbeee5", "#f4c7a6", "#eb9a62", "#d55e00", "#8f3f00"], dark: ["#2a1d14", "#5c3214", "#9a4a12", "#d55e00", "#ff9a5c"] }
  seq-full:  { light: ["#e6f1f8", "#b3d4ea", "#6aaed6", "#0072b2", "#00466e"], dark: ["#14202a", "#163a54", "#155c8a", "#0072b2", "#5fb0ea"] }
  # Pipeline status — health page only. Always paired with an icon and a word.
  status:
    ok:   { light: "#1a7f37", dark: "#3fb950" }
    warn: { light: "#8a5c00", dark: "#d29922" }
    fail: { light: "#c62828", dark: "#ff6b5e" }
  # Chart series (non-state charts: latency, throughput, lag)
  series-primary: "{ink}"                 # p50, events/min, "after"
  series-secondary: "{mute}, dashed 4 3"  # p95, "before"
  threshold: "{status.fail}, dashed 2 2"  # SLA lines (e.g. freshness 5 min)
  gridline: { light: "#eceef1", dark: "#232a33" }

typography:
  fontFamily:
    sans: "'IBM Plex Sans', system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
    mono: "'IBM Plex Mono', ui-monospace, 'SF Mono', 'Cascadia Mono', Consolas, monospace"
  # Fluid sizes: clamp(min at 320 px, preferred, max at >= 1200 px)
  display-xl:  { size: "clamp(1.75rem, 1.35rem + 1.6vw, 2.5rem)", weight: 600, lineHeight: 1.2, tracking: "-0.015em" }  # 28 → 40
  display-lg:  { size: "clamp(1.375rem, 1.2rem + 0.8vw, 1.75rem)", weight: 600, lineHeight: 1.25, tracking: "-0.01em" } # 22 → 28
  display-md:  { size: "1.25rem",  weight: 600, lineHeight: 1.35 }   # 20 — card titles
  stat:        { size: "clamp(1.75rem, 1.4rem + 1.4vw, 2.25rem)", weight: 600, lineHeight: 1.1, numeric: tabular-nums }  # 28 → 36
  body-lg:     { size: "1.125rem", weight: 400, lineHeight: 1.55 }   # 18
  body-md:     { size: "1rem",     weight: 400, lineHeight: 1.55 }   # 16 — paragraphs never smaller
  body-md-strong: { size: "1rem",  weight: 500, lineHeight: 1.3 }
  body-sm:     { size: "0.875rem", weight: 400, lineHeight: 1.45 }   # 14
  body-sm-strong: { size: "0.875rem", weight: 500, lineHeight: 1.2 }
  caption:     { size: "0.75rem",  weight: 400, lineHeight: 1.35 }   # 12 — axes, fine print
  eyebrow:     { family: mono, size: "0.75rem", weight: 500, lineHeight: 1.35, transform: uppercase, tracking: "0.08em" }
  data:        { family: mono, size: "0.875rem", weight: 400, lineHeight: 1.45, numeric: tabular-nums }  # IDs, codes, timestamps
  code:        { family: mono, size: "0.875rem", weight: 400, lineHeight: 1.6 }
  map-label:   { size: "clamp(0.6875rem, 0.62rem + 0.25vw, 0.8125rem)", weight: 500, lineHeight: 1.2 }  # 11 → 13

rounded:
  none: 0px
  sm: 4px       # chips' inner badges, inline code, legend swatches
  md: 6px       # buttons, inputs, segmented control, tooltips
  lg: 10px      # every card, map card, sheet top corners
  full: 999px   # status dots, the "data as of" pill, count badges

spacing:          # 4 px base
  xxs: 4px
  xs: 6px
  sm: 8px
  md: 12px
  lg: 16px
  xl: 20px
  2xl: 24px
  3xl: 32px
  4xl: 48px
  5xl: 64px
  gutter: "clamp(16px, 4vw, 32px)"
  section: "clamp(40px, 5vw, 80px)"

layout:
  container: 1280px
  breakpoints: { sm: 600px, md: 768px, lg: 1120px }
  touch-target-min: 44px

elevation:
  level-0: "none"                                  # cards use a hairline border
  level-1: "0 2px 8px rgba(0, 0, 0, 0.10)"         # tooltips, open dropdowns, map controls
  level-2: "0 8px 24px rgba(0, 0, 0, 0.16)"        # phone bottom sheet, nav overlay, toast
  dark-note: "in dark theme shadows are replaced by a 1px {hairline} border plus {surface-2} background"

motion:
  duration-fast: 120ms     # hover, press
  duration-base: 200ms     # disclosure, tooltip, tab change, marker state change
  duration-slow: 320ms     # sheet / overlay open
  ease-standard: "cubic-bezier(0.2, 0, 0, 1)"
  ease-exit: "cubic-bezier(0.4, 0, 1, 1)"

map:
  regions: { sf: "San Francisco", eastbay: "East Bay (Oakland, Emeryville, Berkeley)", sj: "San José" }
  viewBox: "0 0 1000 1000 per region (projected per region, so each fills its card)"
  marker-radius: { ok: "clamp(3px, 0.45% of map width, 5px)", problem: "1.35 × ok radius" }   # empty, low, high, full, offline
  marker-casing: "1px {marker-casing}"
  marker-selected: "radius × 1.6, 2px {ink} ring offset 2px"
  hit-radius-touch: 22px
  hit-radius-pointer: 10px
  land-stroke: "1px {map-shoreline}"

components:
  nav-bar:        { background: "{canvas}", text: "{ink}", typography: "{typography.body-md-strong}", height: 56px, sticky: true, borderBottom: "1px {hairline}" }
  freshness-pill: { background: "{surface}", text: "{ink}", typography: "{typography.data}", rounded: "{rounded.full}", padding: "{spacing.xxs} {spacing.md}", dot: "8px status colour" }
  button-primary: { background: "{ink}", text: "{on-ink}", typography: "{typography.body-md-strong}", rounded: "{rounded.md}", padding: "{spacing.sm} {spacing.lg}", minHeight: 44px }
  button-secondary: { background: "{canvas}", text: "{ink}", border: "1px {hairline}", typography: "{typography.body-md-strong}", rounded: "{rounded.md}", padding: "{spacing.sm} {spacing.lg}", minHeight: 44px }
  icon-button:    { background: "transparent → {surface} on hover", text: "{ink}", rounded: "{rounded.md}", size: 44px, icon: 20px }
  segmented:      { track: "{surface}", thumb: "{canvas} (dark: {surface-2})", border: "1px {hairline}", rounded: "{rounded.md}", padding: "{spacing.xxs}", typography: "{typography.body-sm-strong}" }
  chip:           { background: "{surface}", text: "{ink}", typography: "{typography.body-sm-strong}", rounded: "{rounded.md}", padding: "{spacing.xs} {spacing.md}", minHeight: "32px (44px on touch)" }
  text-input:     { background: "{canvas}", border: "1px {hairline} → 1px {ink} on focus", text: "{ink}", placeholder: "{mute}", rounded: "{rounded.md}", padding: "{spacing.md}", minHeight: 44px }
  card:           { background: "{canvas}", border: "1px {hairline}", rounded: "{rounded.lg}", padding: "clamp(16px, 2.2vw, 24px)" }
  card-soft:      { background: "{surface}", rounded: "{rounded.lg}", padding: "clamp(16px, 2.2vw, 24px)" }
  stat-tile:      { background: "{surface}", rounded: "{rounded.lg}", padding: "{spacing.lg}", label: "{typography.eyebrow}", value: "{typography.stat}", sub: "{typography.body-sm} {body}" }
  map-card:       { background: "{map-water}", border: "1px {hairline}", rounded: "{rounded.lg}", overflow: hidden }
  map-controls:   { background: "{canvas}", border: "1px {hairline}", rounded: "{rounded.md}", shadow: "{elevation.level-1}" }
  legend-item:    { swatch: "10px marker of the state", label: "{typography.body-sm}", count: "{typography.data} {body}" }
  alert-row:      { background: "{canvas}", divider: "1px {hairline}", padding: "{spacing.md} {spacing.lg}", marker: "state marker 12px", title: "{typography.body-md-strong}", meta: "{typography.data} {mute}" }
  status-badge:   { icon: "16px", text: "{typography.body-sm-strong}", colour: "{status.*}", background: "none" }
  tooltip:        { background: "{ink}", text: "{on-ink}", typography: "{typography.body-sm}", rounded: "{rounded.md}", padding: "{spacing.sm} {spacing.md}", shadow: "{elevation.level-1}", maxWidth: 280px }
  bottom-sheet:   { background: "{canvas}", rounded: "{rounded.lg} {rounded.lg} 0 0", shadow: "{elevation.level-2}", handle: "36×4px {hairline}", maxHeight: "85dvh" }
  data-table:     { headerBackground: "{surface}", header: "{typography.body-sm-strong}", body: "{typography.body-sm}", cellPadding: "{spacing.sm} {spacing.md}", rowBorder: "1px {hairline}", numeric: "right-aligned, tabular-nums" }
  code-block:     { background: "{surface}", border: "1px {hairline}", text: "{ink}", typography: "{typography.code}", rounded: "{rounded.md}", padding: "{spacing.lg}" }
  banner:         { background: "{surface}", border-left: "3px status colour", typography: "{typography.body-sm}", rounded: "{rounded.md}", padding: "{spacing.md} {spacing.lg}" }
  toast:          { background: "{ink}", text: "{on-ink}", rounded: "{rounded.md}", shadow: "{elevation.level-2}", padding: "{spacing.md} {spacing.lg}" }
  skeleton:       { background: "{surface}", rounded: "matches the element it stands in for", shimmer: "off under reduced motion" }
  footer:         { background: "{surface}", text: "{body}", typography: "{typography.body-sm}", borderTop: "1px {hairline}", padding: "{spacing.4xl} {spacing.gutter}" }
  focus-ring:     { outline: "2px solid {focus}", offset: 2px }
---

# DockWatch design system

> **Status: v1.0, frozen.** Everything the frontend needs is decided here so it doesn't change while it's being built.
> See [Change policy](#change-policy). The previous file (TransitPulse's system) is kept at
> [`reference/DESIGN_SOURCE_transitpulse.md`](reference/DESIGN_SOURCE_transitpulse.md).

## 1. Overview

DockWatch is an **operations console** for a bike-share system: people look at it to find the stations that are
failing riders right now (no bikes, or no docks) and to check that the pipeline behind it can be trusted.
So the design has one job: **make problem stations and pipeline problems the first thing you see**, and keep
everything else quiet.

**Key characteristics**
- **One colour rule:** the interface is neutral grey. Colour means a station's state (warm = too few bikes,
  cool = too few docks), or — on the pipeline page only — a status. Nothing else is coloured.
- **Quiet by default:** healthy stations are grey and small; problem stations are coloured and slightly bigger.
  A healthy system looks calm; a bad morning lights up.
- **Two typefaces from one family:** IBM Plex Sans for reading, IBM Plex Mono for anything machine-ish
  (station codes, timestamps, IDs, SQL, eyebrows). Tabular numerals everywhere numbers update.
- **Flat and square-ish:** 10 px cards, 6 px controls, hairline borders, almost no shadows.
- **Light and dark themes**, both first-class: operations screens are often left on all day.
- **Fluid from 320 px to 2560 px:** type, spacing and the map scale continuously; breakpoints only rearrange layout.
- **Honest about freshness:** every page says how old its data is, and says it louder when the data is stale.

## 2. Colour

### Interface
Light values first, dark in brackets. All text tokens pass WCAG 2.2 AA on every surface listed for them.

| Token | Light (dark) | Use |
|---|---|---|
| `canvas` | `#ffffff` (`#0e1116`) | Page background, cards. |
| `surface` | `#f4f5f7` (`#161b22`) | Stat tiles, chips, segmented track, footer, map water (light). |
| `surface-2` | `#eaecef` (`#1f262f`) | Pressed/selected states, nested surfaces, dark-theme raised layers. |
| `hairline` | `#dfe2e6` (`#2b333d`) | Card borders, dividers, table rows. |
| `ink` | `#111418` (`#e8ecf1`) | Headings, body text, primary button, tooltips. |
| `body` | `#4b5563` (`#b4bcc6`) | Secondary text: descriptions, takeaways. Any surface. |
| `mute` | `#626a75` (`#8b95a1`) | Timestamps, captions, axis labels, placeholders. Any surface; never for something a user must read to act. |

Links are `ink`, underlined (1 px, 2 px offset). No blue links — blue means "full".

### Data: station state
| State | Light (dark) | Rule (thresholds in pipeline config) | Marker |
|---|---|---|---|
| `empty` | `#d55e00` (`#ff7a33`) | 0 bikes available | Filled, problem size |
| `low` | `#e69f00` (`#f0b429`) | ≤ 2 bikes or ≤ 10 % of capacity | Filled, problem size |
| `ok` | `#8c959f` (`#6e7781`) | Neither low nor high | Filled, normal size |
| `high` | `#56b4e9` (`#7cc6f0`) | ≤ 2 docks or ≤ 10 % of capacity | Filled, problem size |
| `full` | `#0072b2` (`#3d9be0`) | 0 docks available | Filled, problem size |
| `offline` | no fill, dashed `mute` ring | Not installed, not renting, or no report for 30 min | Ring, problem size |

- The palette is from the Okabe–Ito set, chosen so empty/low vs high/full stay distinct with the common
  colour-vision deficiencies. **Warm = riders can't get a bike. Cool = riders can't return one.**
- Light-theme `low` and `high` are under 3:1 against white land, so **every marker has a 1 px `marker-casing`**
  (ink in light, canvas in dark). This keeps every marker ≥ 3:1 against its background (WCAG 1.4.11).
- Colour is never the only cue (WCAG 1.4.1): tooltips, the legend, the list view and alerts always name the state in
  words, and problem states are drawn larger than `ok`.
- When a state word is coloured as text (list view), use `state-text` (`empty` `#b84f00`, `full` `#0072b2` in light),
  which pass 4.5:1. `low`/`high` words stay `ink` with a swatch beside them.
- The state rule is defined **once** in the pipeline (`streaming/transforms.py`) and the site uses the `state` field
  from `live.json`; the browser never recomputes it.

### Data: heatmaps and charts
- `% of time empty` heatmaps use the 5-step `seq-empty` ramp; `% of time full` uses `seq-full`. Each heatmap has
  a labelled scale with the bucket edges (0–5 %, 5–15 %, 15–30 %, 30–50 %, > 50 %).
- Line/bar charts that aren't about state use `series-primary` (ink, solid 2 px) and `series-secondary`
  (mute, dashed 4 3). SLA/threshold lines are `status.fail` dashed 2 2 with a text label at the line end.
- Gridlines horizontal only, `gridline` colour. No chart borders, no 3D, no pie or donut charts.

### Status (pipeline page only)
| Status | Light (dark) | Icon (Lucide) | Word |
|---|---|---|---|
| `ok` | `#1a7f37` (`#3fb950`) | `circle-check` | "Healthy" / "Passed" |
| `warn` | `#8a5c00` (`#d29922`) | `triangle-alert` | "Degraded" / "Warning" |
| `fail` | `#c62828` (`#ff6b5e`) | `circle-x` | "Failing" / "Failed" |

Status colours are text/icon colours on `canvas` or `surface`; they are never fills of large areas, and they never
appear on the Live or Insights pages (there, colour already means station state).

## 3. Typography

**IBM Plex Sans** (400/500/600) and **IBM Plex Mono** (400/500), self-hosted `woff2`, Latin subset,
`font-display: swap`, Sans 600 preloaded. Both are under the SIL Open Font Licence.
Use `font-variant-numeric: tabular-nums` on stats, tables, axes, tooltips and the freshness pill.

| Token | Size (320 → 1200 px) | Weight | Use |
|---|---|---|---|
| `display-xl` | 28 → 40 | 600 | Page title (one per page). |
| `display-lg` | 22 → 28 | 600 | Section headings. |
| `display-md` | 20 | 600 | Card titles. |
| `stat` | 28 → 36 | 600 | KPI values. |
| `body-lg` | 18 | 400 | Page intro paragraph. |
| `body-md` | 16 | 400 | Paragraphs. **Never below 16 px.** |
| `body-sm` | 14 | 400 | Tooltips, table body, legend. |
| `caption` | 12 | 400 | Axis ticks, source lines, fine print. |
| `eyebrow` | 12 | Mono 500, uppercase | Section and tile labels ("EMPTY NOW"). The only uppercase text. |
| `data` | 14 | Mono 400 | Station codes (`SJ-J6`), timestamps, IDs, durations. |
| `code` | 14 | Mono 400 | SQL and config snippets. |
| `map-label` | 11 → 13 | Sans 500 | Map labels (selected station and region landmarks only). |

**Rules**
- Sentence case everywhere, including buttons ("Show list", not "Show List").
- 600 is for headings and stats; emphasis and buttons are 500.
- Paragraphs max `68ch`. All sizes in `rem` so the site works at 200 % zoom.

## 4. Layout

### Container and spacing
- Container: `width: min(100% - 2 × gutter, 1280px)`, centred. Gutter `clamp(16px, 4vw, 32px)`.
- Sections: vertical padding `clamp(40px, 5vw, 80px)`. Grid gaps `clamp(12px, 2vw, 20px)`. Spacing in multiples of 4 px.
- Use `auto-fit` grids (`repeat(auto-fit, minmax(min(100%, 200px), 1fr))`) before media queries, and
  **container queries** for component internals (stat tile, chart card, alert row, table).
- `100dvh`, never `100vh`; pad fixed/sticky elements with `env(safe-area-inset-*)`.
- **No horizontal page scroll at any width ≥ 320 px.** Wide tables and code scroll inside their own card.

### Breakpoints (layout only)
| Name | Width | What changes |
|---|---|---|
| Phone | < 600 px | Nav = wordmark + freshness dot + menu button. Map 4:5. KPI tiles 2-up. Alerts become a bottom sheet with a peek bar ("6 open alerts"). |
| Large phone / small tablet | 600–767 px | Map 1:1. KPI tiles 2-up. Alerts in a card under the map. |
| Tablet | 768–1119 px | Map 4:3. KPI tiles 4-up. Alerts card under the map, two columns of rows. |
| Desktop | ≥ 1120 px | Full nav. Live page is two columns: map (≈ 2/3) and alerts rail (≈ 1/3, sticky, scrolls inside). Map 16:10. |

### Pages and order
- **Live** (`index.html`): page title + freshness pill · KPI tiles (Empty now · Full now · Open alerts · Bikes available) ·
  map card with region switch, legend and `Map | List` toggle · alerts rail.
- **Insights** (`insights.html`): KPI tiles for the selected period · "% of time empty" heatmap (station × hour) ·
  rebalancing-need table · trips vs. availability scatter · period segmented control (7 days · 30 days · 12 months).
- **Pipeline** (`pipeline.html`): overall status banner · stat tiles (p95 latency, events/min, consumer lag, freshness) ·
  latency chart (p50/p95 with SLA line) · throughput chart · freshness table per table · data-quality results table ·
  compaction before/after chart · lineage screenshot · architecture diagram.
- **About** (`about.html`): what DockWatch is, how it works, data sources and licences, "not affiliated with Lyft or Bay Wheels".

## 5. Shape, elevation, motion

**Shape:** 6 px for anything you press (buttons, chips, segmented control, inputs) and for tooltips and code;
10 px for anything that holds content (cards, map card, sheets); full round only for status dots, count badges
and the freshness pill. Nothing else.

**Elevation:** flat. Cards use a hairline border. Shadows only for things floating above content: tooltips,
dropdowns and map controls (level 1); bottom sheet, nav overlay and toast (level 2). In dark theme, floating things
use `surface-2` plus a hairline border instead of a shadow.

**Motion:** 120 / 200 / 320 ms with `cubic-bezier(.2, 0, 0, 1)`; only `opacity`, `transform` and `fill` are animated.
- When `live.json` refreshes, a marker whose state changed cross-fades its fill over 200 ms and pulses its ring once
  (scale 1 → 1.6 → 1, 600 ms). Unchanged markers don't move. There is no other continuous motion.
- The freshness dot pulses slowly (2 s) while data is fresh, and stops when stale.
- Under `prefers-reduced-motion: reduce`: no pulses, fills change instantly, skeleton shimmer stops.

## 6. The live station map

The map is the main visual. It is **geographic but plain**: real station positions over a simplified land/water
shape. No streets, no tiles, no place labels except a few landmarks.

### Base layer (SVG)
- One map per region (San Francisco · East Bay · San José), chosen with a segmented control above the map; the choice
  is remembered per visitor. Each region has its own projection into a `0 0 1000 1000` view box so it fills the card.
- Water = `map-water` (the card background); land = `map-land`, outlined 1 px `map-shoreline`. Shapes come from US Census
  TIGER cartographic boundary files (public domain), simplified in the build step.
- Landmark labels (3–5 per region, e.g. "Ferry Building", "Lake Merritt", "Diridon") in `map-label` + `mute`, with a
  3 px canvas halo (`paint-order: stroke`). No other labels.

### Station markers
- Circles coloured by `state` with a 1 px `marker-casing`. Radius `clamp(3px, 0.45 % of map width, 5px)` for `ok`;
  **1.35×** for every problem state, so problems pop even in greyscale.
- Draw order: `ok` first, then `offline`, `low`/`high`, then `empty`/`full` on top.
- Hover / focus / selected: radius 1.6× and a 2 px ink ring with 2 px offset; the station's name appears as a label.
- Every marker is focusable via the list view and arrow keys move between nearest stations on the map
  (roving tabindex); the hit area is 22 px radius on touch, 10 px on pointer.

### Map chrome
- Top left, inside the card: region segmented control. Top right: `Map | List` segmented control and a
  "Show only problems" toggle chip.
- Bottom, under the map: the legend — one item per state with its marker swatch, its name and its live count
  ("Empty · 80"), wrapping on phones — and the caption
  "Station status from the Bay Wheels GBFS feed, processed by DockWatch. Times are Pacific time."
- **Stale data:** if `generated_at` is more than 5 min old, a `banner` (warn) sits above the map:
  "Live data paused — showing the state at 3:42 PM. [Replay a day]". Markers keep their last state.
- **Station details:** desktop/tablet = tooltip pinned beside the marker; phone = bottom sheet. Contents in order:
  name (`body-md-strong`), code (`data`), state word with swatch, "8 bikes (5 e-bikes) · 11 docks",
  "Empty for 34 min" when in an episode, "Last reported 2 min ago" (`mute`).
- **List view** (text alternative): `data-table` with columns Station · Code · State · Bikes · Docks · In state for ·
  Last reported; sortable; filtered by the same region and "only problems" controls.

## 7. Components

Tokens for each are in the front matter. Behaviour:

- **Nav bar:** sticky, 56 px, hairline bottom border. Wordmark "DockWatch" (Plex Sans 600) left; links Live ·
  Insights · Pipeline · About; right: freshness pill and theme toggle icon button. Below 1120 px, links move into a
  full-screen overlay (level 2, focus trapped, `Esc` closes, body scroll locked).
- **Freshness pill:** status dot + "Data as of 3:42:10 PM" in `data`. Dot = `status.ok` (< 2 min), `status.warn`
  (2–5 min), `status.fail` (> 5 min). The word changes too ("Live" / "Delayed" / "Paused") for screen readers and
  colour-blind users.
- **Buttons:** primary (ink) — at most one per section; secondary (canvas + hairline); icon buttons for toolbar actions.
  Hover = one step darker/lighter (`surface-2`); disabled = 40 % opacity.
- **Segmented control:** region, `Map | List`, period. Thumb slides with `duration-base`.
- **Stat tile:** eyebrow label, `stat` value, one `body-sm` line of context ("of 641 stations"), ⓘ button revealing
  the definition. On the Live page, the "Empty now" and "Full now" tiles show a 10 px state swatch beside the label.
- **Alert row:** state marker, station name (`body-md-strong`), "Empty for 34 min" or "Resolved after 22 min",
  start time in `data`/`mute`. Open alerts first (longest-running first), then up to 20 resolved. Selecting a row
  switches region if needed, focuses the station and opens its details. New alerts are announced politely.
- **Chart card:** `card` with `display-md` title, a one-line takeaway in `body` ("The worst hour is 8–9 AM, when 14 %
  of SF stations are empty"), the chart, and a `caption` source line ("Source: silver.availability_5m, last 30 days").
  Below 600 px card width: fewer ticks, legend under the chart.
- **Data table:** wrapper scrolls horizontally inside its card; first column sticky; numbers right-aligned, tabular;
  sortable headers are buttons with `aria-sort`.
- **Status badge:** icon + word in the status colour; used in the pipeline status banner, freshness table and
  data-quality table.
- **Banner:** for stale data, pipeline incidents and "Replay mode — showing Tuesday 8 Oct, 10× speed [Exit]".
- **Skeletons:** every async region shows a skeleton of its final size (no layout shift), never a spinner alone.
- **Empty and error states:** a `card-soft` with one sentence and one action ("Retry", "Show all stations").
  Example: no problems in the region → "No empty or full stations in San José right now."
- **Toast:** bottom-centre, auto-hides after 4 s; only for confirmations ("Link copied").
- **Footer:** `surface` band: data attribution and licence ("Bay Wheels data provided according to the Bay Wheels
  License Agreement"), "Not affiliated with Lyft or Bay Wheels", GitHub link, "Data through <date>".

### Icons
[Lucide](https://lucide.dev) (ISC licence), inline SVG, 20 px (16 px in badges), 1.75 px stroke, `currentColor`.
Every icon has a text label or `aria-label`. No illustrations, photos or stock images: the map and charts are the imagery.

## 8. Accessibility (minimums)
- WCAG 2.2 AA: text tokens are checked on the surfaces they are allowed on (values in §2); markers ≥ 3:1 via casing.
- Focus always visible: `focus-ring` (2 px `focus`, 2 px offset) via `:focus-visible`.
- Every pressable thing is ≥ 44 × 44 px on touch (hit area may extend past the visible control).
- The map has text alternatives: the List view, plus an `aria-live="polite"` summary updated with each refresh
  ("San Francisco: 41 empty, 3 full, 2 offline") — announced only when the counts change.
- Async regions use `aria-busy` while loading; new alerts are announced politely, never assertively.
- Respects `prefers-reduced-motion` and `prefers-color-scheme`; works at 200 % zoom and with text-spacing overrides.
- Skip link "Skip to map" (Live) / "Skip to content" (other pages) as the first focusable element.
- Map does not hijack page scroll: no wheel/pinch zoom on the map; regions replace zooming.

## 9. Launch checklist (used in F5)
- [ ] Playwright screenshots at 320 / 390 / 768 / 1024 / 1440 / 2560 px, light and dark, no horizontal scroll.
- [ ] axe: zero violations on every page, both themes.
- [ ] Keyboard-only: every action reachable; focus order matches reading order; overlays trap and return focus.
- [ ] Reduced motion: no pulses, no shimmer.
- [ ] Greyscale check: problem stations still findable (size + legend + list).
- [ ] Lighthouse on Live: performance ≥ 95, accessibility 100.
- [ ] Stale-data and replay banners shown correctly with the pipeline stopped.

## 10. Writing style
- Plain English, short sentences, numbers with units: "8 bikes", "empty for 34 min", "p95 4.2 s".
- Times are Pacific time and say so once per page.
- Every chart and tile group has a `caption` saying which table it came from.
- Errors say what happened and what to do next, without blaming the user.
- Never say "real-time" without the freshness next to it.

## 11. Do / don't

**Do**
- Keep colour for station state (and status on the pipeline page). Everything else is neutral.
- Make problems bigger as well as coloured.
- Reserve space for anything that loads with `aspect-ratio` or a skeleton.
- Let components respond to their container, not the viewport.

**Don't**
- Don't add an accent colour, gradient, glow or illustration.
- Don't use green/red for station states — green-vs-red is unreadable for many users and "good/bad" is the
  wrong framing for full vs empty.
- Don't use status colours on the Live or Insights pages.
- Don't shrink paragraphs below 16 px to fit a phone; reflow instead.
- Don't use the Lyft or Bay Wheels logo, brand colours or wordmark.

## Change policy
This document is **frozen at v1.0 from the start of F1** until public launch (F5 done). During frontend work:
- Implementation follows this file. If something is missing, use the closest existing token/component and write it
  down in `docs/DESIGN_BACKLOG.md`. Don't change this file.
- The only allowed edits are fixes for an accessibility failure found in testing, recorded in the changelog below.
- Everything else waits for v1.1 after launch.

### Changelog
- **v1.0.0 (2026-10-07):** first DockWatch version. Replaces the TransitPulse system that had been copied into this
  repo (kept at `reference/DESIGN_SOURCE_transitpulse.md`). Kept: "colour = data" rule, fluid type/spacing, container
  queries, accessibility minimums, freeze policy. Removed: BART line palette, train map and train glyph, pill shape
  language, Ask card / answer panel / SQL agent components, forecast components, ink Findings band. Added: Okabe–Ito
  station-state palette with size redundancy, offline state, heatmap ramps, pipeline status colours, dark theme,
  IBM Plex Sans/Mono, freshness pill, alert row, bottom sheet, banner, region-based station map, launch checklist.
