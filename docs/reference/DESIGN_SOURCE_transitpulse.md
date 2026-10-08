---
version: 1.0.1
status: frozen            # locked before frontend work starts (F1). See "Change policy" at the end.
name: TransitPulse
description: The design system for TransitPulse, a Bay Area transit analytics site. A black-and-white interface built on one pill-shaped control, Inter type and flat 16 px cards, where the only colour on the page is the data itself — BART line colours on the live train map and charts.
source: docs/reference/DESIGN_SOURCE_uber-analysis.md (the black/white + pill language is adapted from it; no Uber names, fonts or assets are used)

colors:
  # Interface (chrome) — black, white and greys only
  ink: "#000000"
  on-ink: "#ffffff"
  canvas: "#ffffff"
  canvas-soft: "#efefef"
  canvas-softer: "#f6f6f6"
  hairline: "#e2e2e2"
  pressed: "#e2e2e2"
  ink-elevated: "#282828"
  body: "#5e5e5e"          # 6.4:1 on canvas, 5.6:1 on canvas-soft
  mute: "#767676"          # 4.5:1 on canvas (AA). Only on white surfaces.
  mute-on-ink: "#a3a3a3"   # 8.0:1 on ink
  error: "#c62828"         # 5.6:1 on canvas. Error text + icon only.
  focus: "#000000"

  # Data — only inside maps, trains, charts and their legends
  line-yellow: "#ffd200"   # Antioch–SFO
  line-orange: "#f7941d"   # Richmond–Berryessa
  line-red: "#ed1c24"      # Richmond–Millbrae/SFO
  line-green: "#4db848"    # Berryessa–Daly City
  line-blue: "#00a6e9"     # Dublin/Pleasanton–Daly City
  line-beige: "#b5a97b"    # Oakland Airport connector
  line-casing: "rgba(0, 0, 0, 0.25)"
  map-water: "#efefef"
  map-land: "#ffffff"
  map-shoreline: "#d4d4d4"
  seq-0: "#f3f3f3"
  seq-1: "#d4d4d4"
  seq-2: "#a3a3a3"
  seq-3: "#6b6b6b"
  seq-4: "#2e2e2e"
  seq-5: "#000000"
  series-primary: "#000000"   # BART in comparisons, actuals in forecasts
  series-secondary: "#8a8a8a" # Bay Wheels in comparisons (dashed)
  interval-fill: "rgba(0, 0, 0, 0.08)"
  gridline: "#ececec"

typography:
  fontFamily:
    sans: "Inter, system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"
    mono: "ui-monospace, 'SF Mono', 'Cascadia Mono', Consolas, 'Liberation Mono', monospace"
  # Fluid sizes: clamp(min at 320 px, preferred, max at >=1200 px)
  display-xxl: { size: "clamp(2rem, 1.2rem + 3.2vw, 3.25rem)",    weight: 700, lineHeight: 1.2,  tracking: "-0.02em" }  # 32 → 52
  display-xl:  { size: "clamp(1.75rem, 1.4rem + 1.4vw, 2.25rem)", weight: 700, lineHeight: 1.22, tracking: "-0.02em" }  # 28 → 36
  display-lg:  { size: "clamp(1.5rem, 1.2rem + 1.2vw, 2rem)",     weight: 700, lineHeight: 1.25, tracking: "-0.01em" }  # 24 → 32
  display-md:  { size: "clamp(1.25rem, 1.1rem + 0.6vw, 1.5rem)",  weight: 700, lineHeight: 1.33, tracking: "-0.01em" }  # 20 → 24
  display-sm:  { size: "1.25rem",  weight: 700, lineHeight: 1.4 }   # 20
  stat:        { size: "clamp(1.75rem, 1.3rem + 1.8vw, 2.5rem)", weight: 700, lineHeight: 1.1, numeric: tabular-nums }  # 28 → 40
  body-lg:     { size: "1.125rem", weight: 400, lineHeight: 1.5 }   # 18
  body-md:     { size: "1rem",     weight: 400, lineHeight: 1.5 }   # 16 — never smaller for paragraphs
  body-md-strong: { size: "1rem",  weight: 500, lineHeight: 1.25 }
  body-sm:     { size: "0.875rem", weight: 400, lineHeight: 1.43 }  # 14
  body-sm-strong: { size: "0.875rem", weight: 500, lineHeight: 1.15 }
  caption:     { size: "0.75rem",  weight: 400, lineHeight: 1.33 }  # 12 — chart axes, fine print
  eyebrow:     { size: "0.75rem",  weight: 500, lineHeight: 1.33, transform: uppercase, tracking: "0.06em" }
  button:      { size: "1rem",     weight: 500, lineHeight: 1.25 }
  code:        { family: mono, size: "0.875rem", weight: 400, lineHeight: 1.6 }
  map-label:   { size: "clamp(0.6875rem, 0.6rem + 0.3vw, 0.8125rem)", weight: 500, lineHeight: 1.2 }  # 11 → 13

rounded:
  none: 0px
  sm: 4px      # tooltips' inner chips, code inline
  md: 8px      # inputs, tooltips, table wrappers
  xl: 16px     # every card
  pill-tab: 36px  # segmented control
  pill: 999px  # every button, chip, toggle

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
  section: "clamp(48px, 6vw, 96px)"   # vertical padding of page bands

layout:
  container: 1200px
  breakpoints: { sm: 600px, md: 768px, lg: 1120px }
  touch-target-min: 44px

elevation:
  level-0: "none"
  level-1: "0 4px 16px rgba(0, 0, 0, 0.12)"   # ask card, open combobox list
  level-2: "0 4px 16px rgba(0, 0, 0, 0.16)"   # modal sheet, toast
  level-3: "0 2px 8px rgba(0, 0, 0, 0.16)"    # floating map controls, tooltips

motion:
  duration-fast: 120ms     # hover, press
  duration-base: 200ms     # disclosure, tooltip, tab change
  duration-slow: 320ms     # sheet / overlay open
  ease-standard: "cubic-bezier(0.2, 0, 0, 1)"
  ease-exit: "cubic-bezier(0.4, 0, 1, 1)"

map:
  viewBox: "0 0 1000 1000"
  line-width: { mobile: 3px, desktop: 4px }   # non-scaling stroke
  lane-gap: 1px                                # gap between parallel lines on shared track
  station-radius: { mobile: 3px, desktop: 4px }
  station-stroke: 2px
  train-length: "clamp(6px, 1.2% of map width, 14px)"
  train-width: "0.42 × train-length"
  train-stroke: "1px {colors.ink}"
  train-selected-scale: 1.5
  hit-radius-touch: 22px                       # 44 px target
  hit-radius-pointer: 12px

components:
  nav-bar:          { background: "{colors.canvas}", text: "{colors.ink}", typography: "{typography.body-md-strong}", padding: "{spacing.lg} {spacing.gutter}", height: 64px, sticky: true, borderBottom: "1px {colors.hairline} when scrolled" }
  button-primary:   { background: "{colors.ink}", text: "{colors.on-ink}", typography: "{typography.button}", rounded: "{rounded.pill}", padding: "{spacing.md} {spacing.xl}", minHeight: 44px }
  button-secondary: { background: "{colors.canvas}", text: "{colors.ink}", border: "1px {colors.ink}", typography: "{typography.button}", rounded: "{rounded.pill}", padding: "{spacing.md} {spacing.xl}", minHeight: 44px }
  button-subtle:    { background: "{colors.canvas-soft}", text: "{colors.ink}", typography: "{typography.button}", rounded: "{rounded.pill}", padding: "{spacing.md} {spacing.lg}", minHeight: 44px }
  button-on-ink:    { background: "{colors.canvas}", text: "{colors.ink}", typography: "{typography.button}", rounded: "{rounded.pill}", padding: "{spacing.md} {spacing.xl}", minHeight: 44px }
  icon-button:      { background: "{colors.canvas-soft}", text: "{colors.ink}", rounded: "{rounded.pill}", size: 44px, icon: 20px }
  chip:             { background: "{colors.canvas-soft}", text: "{colors.ink}", typography: "{typography.body-sm-strong}", rounded: "{rounded.pill}", padding: "{spacing.sm} {spacing.lg}", minHeight: "36px (44px on touch)" }
  segmented:        { track: "{colors.canvas-soft}", thumb: "{colors.canvas}", thumbShadow: "{elevation.level-3}", rounded: "{rounded.pill-tab}", padding: "{spacing.xxs}", typography: "{typography.body-sm-strong}" }
  text-input:       { background: "{colors.canvas-soft}", text: "{colors.ink}", placeholder: "{colors.mute on canvas-softer}", typography: "{typography.body-md}", rounded: "{rounded.md}", padding: "{spacing.lg}", minHeight: 48px }
  card:             { background: "{colors.canvas}", border: "1px {colors.hairline}", rounded: "{rounded.xl}", padding: "clamp(16px, 2.5vw, 24px)" }
  card-soft:        { background: "{colors.canvas-soft}", rounded: "{rounded.xl}", padding: "clamp(16px, 2.5vw, 24px)" }
  card-ink:         { background: "{colors.ink}", text: "{colors.on-ink}", rounded: "{rounded.xl}", padding: "clamp(24px, 4vw, 48px)" }
  ask-card:         { background: "{colors.canvas}", rounded: "{rounded.xl}", shadow: "{elevation.level-1}", padding: "{spacing.lg}", maxWidth: 520px }
  stat-tile:        { background: "{colors.canvas-soft}", rounded: "{rounded.xl}", padding: "{spacing.xl}", value: "{typography.stat}", label: "{typography.body-sm}", delta: "{typography.body-sm-strong}" }
  map-card:         { background: "{colors.map-water}", rounded: "{rounded.xl}", overflow: hidden }
  map-controls:     { background: "{colors.canvas}", rounded: "{rounded.pill}", shadow: "{elevation.level-3}" }
  tooltip:          { background: "{colors.ink}", text: "{colors.on-ink}", typography: "{typography.body-sm}", rounded: "{rounded.md}", padding: "{spacing.sm} {spacing.md}", shadow: "{elevation.level-3}", maxWidth: 260px }
  sql-block:        { background: "{colors.ink}", text: "{colors.on-ink}", typography: "{typography.code}", rounded: "{rounded.md}", padding: "{spacing.lg}" }
  data-table:       { headerBackground: "{colors.canvas-soft}", header: "{typography.body-sm-strong}", body: "{typography.body-sm}", cellPadding: "{spacing.md} {spacing.lg}", rowBorder: "1px {colors.hairline}", numeric: "right-aligned, tabular-nums" }
  toast:            { background: "{colors.ink}", text: "{colors.on-ink}", rounded: "{rounded.md}", shadow: "{elevation.level-2}", padding: "{spacing.md} {spacing.lg}" }
  skeleton:         { background: "{colors.canvas-soft}", rounded: "matches the element it stands in for", shimmer: "off under reduced motion" }
  faq-row:          { question: "{typography.body-md-strong}", padding: "{spacing.lg} 0", divider: "1px {colors.hairline}" }
  footer:           { background: "{colors.ink}", text: "{colors.on-ink}", secondary: "{colors.mute-on-ink}", typography: "{typography.body-sm}", padding: "{spacing.section} {spacing.gutter}" }
  focus-ring:       { outline: "2px solid {colors.focus}", offset: 2px, onInk: "2px solid {colors.on-ink}" }
---

# TransitPulse design system

> **Status: v1.0, frozen.** Everything the frontend needs is decided here so the frontend
> doesn't need design changes while it's being built. See [Change policy](#change-policy).

## 1. Overview

TransitPulse is a data site, so **the data is the decoration**. The interface is a quiet black-and-white
frame: white pages, black text, grey surfaces, one black pill for the main action, and flat 16 px cards.
The only colour anywhere is the BART line colours, which appear only on the train map, in charts and in their
legends. Because the frame has no colour, the moving trains are the first thing a visitor's eye goes to.

**Key characteristics**
- **One colour rule:** interface = black/white/grey. Colour = data. Never the other way round.
- **One shape rule:** everything you can press is a pill (999 px). Everything that holds content is a 16 px card.
- **One typeface:** Inter, at 400 / 500 / 700. Tabular numerals for every number.
- **Band rhythm:** white bands with one black band in the middle (Findings) and a black footer.
- **Fluid at every width:** type, spacing, map and trains scale continuously from 320 px to 2560 px;
  breakpoints only rearrange layout.
- **Light theme only** in v1 (`color-scheme: light`). The map and charts are tuned for a white canvas.

## 2. Colour

### Interface
| Token | Hex | Use |
|---|---|---|
| `ink` | `#000000` | Headings, body on white, primary pill, Findings band, footer, tooltips, SQL block. |
| `on-ink` | `#ffffff` | All text on ink. |
| `canvas` | `#ffffff` | Page background, cards. |
| `canvas-soft` | `#efefef` | Subtle pills, chips, inputs, stat tiles, map water. |
| `canvas-softer` | `#f6f6f6` | Inputs nested on a soft surface. |
| `hairline` / `pressed` | `#e2e2e2` | Card borders, dividers, table rows, pressed state of white/soft pills. |
| `ink-elevated` | `#282828` | Hover/pressed state of the primary pill. |
| `body` | `#5e5e5e` | Secondary text: captions, sub-headings, card descriptions. |
| `mute` | `#767676` | Placeholders, timestamps, "data through" lines. **White surfaces only.** |
| `mute-on-ink` | `#a3a3a3` | Secondary text on ink. |
| `error` | `#c62828` | Error messages and their icon. Never a fill, never decoration. |

Links are `ink`, underlined (1 px, 2 px offset), with no blue. On ink, links are `on-ink`, underlined.

### Data
| Token | Hex | Line |
|---|---|---|
| `line-yellow` | `#ffd200` | Antioch – SFO / Millbrae |
| `line-orange` | `#f7941d` | Richmond – Berryessa |
| `line-red` | `#ed1c24` | Richmond – Millbrae / SFO |
| `line-green` | `#4db848` | Berryessa – Daly City |
| `line-blue` | `#00a6e9` | Dublin/Pleasanton – Daly City |
| `line-beige` | `#b5a97b` | Oakland Airport connector |

- These values are the design's fixed palette. The F2 data build maps each GTFS `route_id` to a token, and
  **ignores** GTFS `route_color`, so a feed change can't recolour the site.
- Yellow and orange are light on white, so **every line stroke gets a 1 px `line-casing`** (25% black), drawn under the colour.
- Colour is never the only cue (WCAG 1.4.1): legends, tooltips and lists always name the line in text.
- **Sequential ramp** `seq-0…seq-5` (light grey → black) for the hour × weekday heatmap and any intensity scale.
- **Comparisons:** BART = `series-primary` solid, Bay Wheels = `series-secondary` dashed (4 3).
- **Forecasts:** actuals = ink solid 2 px; forecast = ink dashed (4 3) 2 px; interval = `interval-fill` band.
- **Deltas** (e.g. "+4.2 pts vs 2019") use ▲/▼ arrows in ink, not green/red. Whether a change is good is up to the reader.

## 3. Typography

**Inter** is self-hosted (`woff2`, 400/500/700, Latin subset, `font-display: swap`, 700 preloaded).
Turn on `font-feature-settings: "ss01", "cv11"` on headings, and use `font-variant-numeric: tabular-nums` for
numbers in stat tiles, tables, clocks, axes and tooltips so digits don't jump while values update. Code and SQL use the system monospace stack (no web font).

| Token | Size (320 px → 1200 px) | Weight | Use |
|---|---|---|---|
| `display-xxl` | 32 → 52 | 700 | Hero headline only. |
| `display-xl` | 28 → 36 | 700 | Section headlines. |
| `display-lg` | 24 → 32 | 700 | Findings headline, answer panel heading. |
| `display-md` | 20 → 24 | 700 | Card titles. |
| `display-sm` | 20 | 700 | Sub-card headings. |
| `stat` | 28 → 40 | 700 | KPI values. |
| `body-lg` | 18 | 400 | Hero sub-text, lead paragraphs. |
| `body-md` | 16 | 400 | Paragraphs. **Paragraphs never go below 16 px.** |
| `body-sm` | 14 | 400 | Captions, tooltips, table body. |
| `caption` | 12 | 400 | Chart axes, fine print. |
| `eyebrow` | 12 | 500, uppercase | Section eyebrows ("LIVE MAP"). The only uppercase text. |
| `map-label` | 11 → 13 | 500 | Station names on the map. |

**Rules**
- Sentence case everywhere, including buttons ("Ask a question", not "Ask A Question").
- 700 is for headings and stat values only; buttons and emphasis are 500.
- Display sizes get slight negative tracking (-0.01 to -0.02 em), because Inter looks loose at large sizes. Body text uses default tracking.
- Line length: paragraphs max `68ch`.
- Text resizing: everything is in `rem`, so the site works at 200% browser zoom.

## 4. Layout

### Container and spacing
- Container: `width: min(100% - 2 × gutter, 1200px)`, centred. Gutter: `clamp(16px, 4vw, 32px)`.
- Page bands: vertical padding `clamp(48px, 6vw, 96px)`. Band backgrounds run full width; their content stays in the container.
- Inside a card: heading → text → action stack uses `8 px` / `12 px` gaps; cards in a grid use `clamp(12px, 2vw, 24px)` gaps.
- Spacing is in multiples of 4 px.

### Breakpoints
Fluid sizing does most of the work. Breakpoints only change **layout**:

| Name | Width | What changes |
|---|---|---|
| Phone | < 600 px | Nav = logo + "Ask" icon + menu button. Hero stacks (headline, then Ask card full width). Map 1:1. 1-column grids. Answer panel opens as a full-screen sheet. |
| Large phone / small tablet | 600–767 px | Chips scroll horizontally in one row. KPI grid 2-up. |
| Tablet | 768–1119 px | Map 4:3, all station labels. Hero still stacked. Forecast chart beside its picker. |
| Desktop | ≥ 1120 px | Full nav row. Hero is 2 columns (headline left, Ask card right). Map 16:9. KPI grid 4-up. |

- Use **container queries** for component internals (stat tile, chart card, map labels, tables) so they respond to their own width.
- Use `auto-fit` grids such as `repeat(auto-fit, minmax(min(100%, 220px), 1fr))` before reaching for a media query.
- Use `100dvh`, never `100vh`. Pad fixed/sticky elements with `env(safe-area-inset-*)`.
- **No horizontal page scroll at any width ≥ 320 px.** Wide content (tables, SQL) scrolls inside its own card.

### Page order
Nav · Hero + Ask · Live map · KPI tiles · Trends · Forecast explorer · **Findings (ink band)** · How it's built (FAQ) · Footer (ink).

## 5. Shape, elevation, motion

**Shape:** pills (999 px) for buttons, chips, toggles, icon buttons and map control groups; 36 px for the
segmented-control track; 16 px for every card (including map, chart, stat tile, Findings band); 8 px for inputs,
tooltips, SQL block, table wrapper and toast. Nothing else.

**Elevation:** flat by default. Shadows only where something sits *above* other content:

| Level | Shadow | Used on |
|---|---|---|
| 0 | none | Cards (they use a 1 px hairline border instead). |
| 1 | `0 4px 16px rgb(0 0 0 / .12)` | Ask card in the hero, open combobox list. |
| 2 | `0 4px 16px rgb(0 0 0 / .16)` | Phone answer sheet, nav overlay, toast. |
| 3 | `0 2px 8px rgb(0 0 0 / .16)` | Floating map controls, tooltips, segmented thumb. |

**Motion:** UI transitions use 120 / 200 / 320 ms with `cubic-bezier(.2, 0, 0, 1)`. Only `opacity` and `transform` are animated.
The train animation is the only continuous motion on the page. Under `prefers-reduced-motion: reduce`, transitions
become instant fades or are removed, skeleton shimmer stops, and trains update as still positions every 30 s.

## 6. The live map

The map is the hero visual. It is **geographic but simplified**: real station positions, smoothed lines, and the
Bay shoreline. There are no streets, labels for places other than stations, or tiles.

### Base layer (SVG, `viewBox="0 0 1000 1000"`)
- **Water** = `map-water` (the card background); **land** = `map-land` white shape from a simplified, public-domain
  coastline (Natural Earth), outlined in `map-shoreline` 1 px.
- **Lines:** 3 px (phone) / 4 px (desktop), `vector-effect: non-scaling-stroke`, round joins and caps,
  1 px `line-casing` under each line.
- **Shared track** (e.g. the Transbay Tube carries four lines) is drawn as **parallel lanes**, 1 px apart, in a fixed order
  (yellow, red, green, blue, orange; west/south side first), like the official BART map. Lane offsets are precomputed in F2.
- **Stations:** white circle, 2 px ink stroke, radius 3 px (phone) / 4 px (desktop). **Transfer stations** are a white
  rounded capsule spanning all lanes — **only when the map is ≥ 600 px wide**; narrower maps draw every station as a
  plain circle (v1.0.1). Hover/selected: ink fill.
- **Labels:** `map-label`, ink, with a 3 px white halo (`paint-order: stroke`). Each station's label side is fixed in the data
  so labels never cross a line. Below 600 px map width only hub labels show: Embarcadero, 12th St Oakland, MacArthur,
  Balboa Park, SFO, Richmond, Antioch, Berryessa, Dublin/Pleasanton.

### Trains (canvas over the SVG)
- **Glyph:** a small **pill** (the system's signature shape), filled with its line colour, 1 px ink outline, pointing along
  the track. Length `clamp(6px, 1.2% of map width, 14px)`; width 42% of length. Sharp at any `devicePixelRatio`.
- A train sits in its own line's lane, so trains on shared track never overlap trains of other lines.
- **At a station** (dwell), the train stays centred on the station marker.
- **Selected** train: scales to 1.5×, ink halo ring 2 px, tooltip pinned. Other trains stay unchanged.
- No trails, glow, shadows or easing beyond linear interpolation: trains move at schedule speed.

### Map controls
- Floating at the bottom of the map card (`map-controls`, level 3), one pill group:
  `● Now` (live dot pulses unless reduced motion) · time scrubber · segmented `1× | 10× | 60×` · play/pause icon button.
- Top right: `+` / `−` zoom icon buttons and a segmented `Map | List` toggle.
- Under the map: the legend (line colour swatch + name, wraps on phones) and the caption
  "Scheduled positions from BART's published timetable. Times in Pacific time."
- Out of service hours: a centred `card` over the map: "No trains running right now. Service resumes at 5:00 AM." + `Jump to 8:00 AM` subtle pill.

### Tooltips
`tooltip` component. Train: colour swatch + "Yellow line to SFO" / "Next: 16th St Mission · 2 min". Station: name, line swatches,
"Entries yesterday: 18,240 (82% of 2019)". On touch, tap opens and tap elsewhere closes; tooltips flip to stay inside the map.

## 7. Components

Tokens for each are in the front matter. Behaviour:

- **Nav bar:** sticky, white; gains a hairline bottom border once scrolled. Links: Map · Trends · Forecast · Findings · About.
  Primary pill "Ask a question" scrolls to and focuses the Ask input. Below 1120 px, links move into a full-screen overlay
  (level 2, focus trapped, `Esc` closes, body scroll locked).
- **Buttons:** primary (ink pill) — at most **one** per viewport section; secondary (white with 1 px ink border); subtle (grey);
  on-ink (white pill inside ink bands). Hover darkens one step (`ink-elevated` / `pressed`); disabled = 40% opacity, no pointer.
- **Chips:** suggestion questions and popular stations. Pressing one fills the field and submits.
- **Segmented control:** speed, Map/List, chart ranges (1M / 1Y / All). The thumb slides with `duration-base`.
- **Ask card:** eyebrow "ASK TRANSITPULSE"; one `text-input` row with a search icon and the placeholder
  "Ask about BART or Bay Wheels ridership…"; 4 chips; primary pill "Ask". Enter submits; Shift+Enter is not needed (single line).
- **Answer panel:** below the hero on tablet/desktop; a full-screen sheet on phones. Contents in order:
  question echo (body-md-strong) → progress steps (each a row with spinner → check: "Writing SQL", "Running query",
  "Drawing chart") → answer (display-lg heading + body) → chart card → `data-table` → "Show SQL" disclosure with `sql-block`
  and a Copy icon button. Refusals and errors replace the answer with an **inline message**: error icon + `error` text
  + one plain-English next step + chips with example questions.
- **Stat tile:** eyebrow label, `stat` value, delta line ("▲ 4.2 pts vs 2019"), ⓘ button revealing the metric definition.
- **Chart card:** `card` with display-md title, one-line takeaway in `body` ("Weekday ridership is 61% of 2019"), chart,
  `caption` source line. Axes in `caption` + `mute`; gridlines `gridline`, horizontal only; no chart borders, no 3D, no pie charts.
  Below 600 px card width: fewer ticks, legend moves under the chart.
- **Combobox (station picker):** `text-input` + listbox (level 1, max 6 visible rows). Matches by name or code ("mac" → MacArthur).
  Full keyboard support (↑/↓, Enter, Esc) per the ARIA combobox pattern.
- **Data table:** in a wrapper that scrolls horizontally inside its card; first column sticky; numbers right-aligned, tabular.
- **Skeletons:** every async region shows a skeleton of its final shape (same size, so no layout shift), never a spinner alone.
- **Empty and error states:** a `card-soft` with one sentence and one action ("Retry", "Try another station"). Never a blank box.
- **Toast:** bottom-centre, ink, auto-hides after 4 s; used only for "Copied" and similar confirmations.
- **FAQ row:** native `<details>/<summary>`; chevron rotates; hairline dividers between rows.
- **Footer:** ink band. Columns: Data sources (with licences), Project (GitHub, write-ups), About. "Data through <date>" in `mute-on-ink`.

### Icons
[Lucide](https://lucide.dev) (ISC licence), inline SVG, 20 px, 1.75 px stroke, `currentColor`. Icons always come with a text
label or an `aria-label`. No illustrations, photos or stock images anywhere: the map and charts are the imagery.

## 8. Accessibility (minimums)
- WCAG 2.2 AA contrast: every text token above is checked on the surfaces it's allowed on.
- Focus is always visible: `focus-ring` (2 px ink, 2 px offset; white on ink bands) via `:focus-visible`.
- Every pressable thing is ≥ 44 × 44 px on touch (hit area may extend past the visible pill).
- The map has a text alternative: an `aria-live="polite"` summary updated each minute ("42 trains running") and the `List` view
  (a table: line, destination, next stop, ETA).
- Async results are announced (`aria-live`) and focus moves to the answer heading when it arrives.
- Respects `prefers-reduced-motion`; works at 200% zoom and with text spacing overrides.
- Skip link "Skip to map" as the first focusable element.

## 9. Writing style
- Plain English, short sentences, numbers with units: "18,240 entries", "61% of 2019", "8:15 AM".
- Times are Pacific time and say so once per section.
- Say where numbers come from (a `caption` source line on every chart and tile group).
- Errors say what happened and what to do next, without blaming the user.

## 10. Do / don't

**Do**
- Keep colour inside the data. If something isn't a line, train or chart mark, it's black, white or grey.
- Use one black pill per section for its main action.
- Reserve space for anything that loads (map, charts, embeds) with `aspect-ratio` or a skeleton.
- Let components respond to their container, not the viewport.

**Don't**
- Don't add an accent colour, gradient, glow or illustration.
- Don't use green/red to judge a number; use ▲/▼ in ink.
- Don't use `mute` text on grey surfaces or for anything a user must read to act.
- Don't shrink paragraphs below 16 px to fit a phone; reflow instead.
- Don't let the map hijack page scrolling on touch (no wheel/pinch zoom; use the `+`/`−` buttons).
- Don't use Uber's (or any company's) name, logo, fonts or imagery.

## Change policy
This document is **frozen at v1.0 from the start of F1** (frontend shell) until public launch (F7 done).
During frontend work:
- Implementation follows this file; if something is missing, choose the closest existing token/component and write it
  down in `docs/DESIGN_BACKLOG.md`. Don't change this file.
- The only allowed edits are fixes for an accessibility failure (e.g. a contrast miss found in testing), recorded in the changelog below.
- Everything else waits for v1.1 after launch.

### Changelog
- **v1.0.1 (2026-10-07), accessibility fix:** on maps narrower than 600 px, transfer-station capsules (up to 23 px long)
  overlapped each other in downtown SF/Oakland and hid the 6 px trains, so trains and stations could not be told apart.
  Below 600 px all stations are now plain circles (§6). Found in F3 phone screenshots.
- **v1.0 (2026-10-07):** first TransitPulse version, adapted from `reference/DESIGN_SOURCE_uber-analysis.md`.
  Swapped proprietary fonts for Inter; removed Uber-specific components (ride-request form, app-download pills, showcase card,
  editorial illustrations); darkened `mute` to pass AA; replaced blue links with underlined ink; added the data palette,
  fluid type/spacing, motion, map/train spec, data components (ask card, answer panel, stat tile, chart card, table, combobox,
  skeleton, toast), accessibility minimums and this change policy.
