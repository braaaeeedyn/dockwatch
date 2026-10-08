// Touch targets (DESIGN.md §8, v1.0.1): on a coarse pointer, the List view's sort buttons and station-name buttons
// are hit areas of at least 44 x 44 px. Checked by hit-testing points around each button, not only by its box.
import { expect, test } from "@playwright/test";
import { LIVE, loadFixture, prepare } from "./helpers.js";

test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

const MIN = 44;

/**
 * Scroll `button` into the middle of the table wrapper, then hit-test its centre, the four points 21 px away and the
 * four corners of the 44 x 44 square around the centre (inset 1 px). Horizontal points are clamped inside the
 * button's own column. Returns the points that resolve to something else.
 */
function hitTest(button, min) {
  button.scrollIntoView({ block: "center", inline: "center" });
  const cell = button.closest("th, td");
  if (cell.cellIndex > 0) {
    // Centre the column in the part of the wrapper not covered by the sticky first column.
    const wrap = button.closest(".table-wrap");
    const area = wrap.getBoundingClientRect();
    const sticky = cell.closest("tr").cells[0].getBoundingClientRect();
    const own = cell.getBoundingClientRect();
    wrap.scrollLeft += own.left + own.width / 2 - (sticky.right + area.right) / 2;
  }
  const box = button.getBoundingClientRect();
  const column = cell.getBoundingClientRect();
  const cx = box.left + box.width / 2;
  const cy = box.top + box.height / 2;
  const r = min / 2 - 1;
  const clampX = (x) => Math.min(Math.max(x, column.left + 1), column.right - 1);
  const points = [
    [0, 0],
    [-r, 0],
    [r, 0],
    [0, -r],
    [0, r],
    [-r, -r],
    [r, -r],
    [-r, r],
    [r, r],
  ];
  const misses = [];
  for (const [dx, dy] of points) {
    const x = clampX(cx + dx);
    const y = cy + dy;
    const hit = document.elementFromPoint(x, y)?.closest("button");
    if (hit !== button) misses.push(`(${dx}, ${dy}) -> ${hit ? hit.textContent.trim() : "no button"}`);
  }
  return misses;
}

test("list sort and station buttons are at least 44 px on touch", async ({ page }) => {
  await prepare(page);
  await page.goto("/index.html?view=list");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);

  // The emulation must really report a coarse pointer, or the checks below would pass vacuously.
  expect(await page.evaluate(() => matchMedia("(pointer: coarse)").matches)).toBe(true);

  const table = page.locator("[data-list-view] table");
  await expect(table).toBeVisible();
  const sortButtons = table.locator("thead .sort-button");
  await expect(sortButtons).toHaveCount(7);
  const nameButtons = table.locator("tbody [data-show-station]");
  const names = Math.min(10, await nameButtons.count());
  expect(names).toBeGreaterThan(0);

  for (let i = 0; i < 7; i += 1) {
    const button = sortButtons.nth(i);
    const label = (await button.textContent()).trim();
    expect(await button.evaluate(hitTest, MIN), `sort button "${label}"`).toEqual([]);
    expect(await button.evaluate((el) => el.getBoundingClientRect().height), `sort button "${label}"`).toBeGreaterThanOrEqual(MIN);
  }

  for (let i = 0; i < names; i += 1) {
    const button = nameButtons.nth(i);
    const label = (await button.textContent()).trim();
    expect(await button.evaluate(hitTest, MIN), `station button "${label}"`).toEqual([]);
    const rowHeight = await button.evaluate((el) => el.closest("tr").getBoundingClientRect().height);
    expect(rowHeight, `row of "${label}"`).toBeGreaterThanOrEqual(MIN);
  }
});

// ---- Every button and link on all four pages (DESIGN §8; DESIGN_BACKLOG #35) ----

const PRESSABLE =
  'a[href], button, summary, [role="button"], label:has(> input[type=radio]), label:has(> input[type=checkbox])';

// Exemptions, and nothing else:
// (a) links inside sentences of running text in `.prose` on About (WCAG 2.2 SC 2.5.8's inline exception). Listed
//     here by name; the scan also checks that each one really sits inside a sentence with other words.
// (b) map markers: nearest-station picking with DESIGN §6's 22 px hit radius on touch, covered by the keyboard and
//     details tests in live-map.spec.js.
const INLINE_PROSE_LINKS = ["Bay Wheels License Agreement", "SIL Open Font License 1.1", "Lucide", "ISC licence"];

/**
 * Runs in the page. For every rendered pressable inside `scope`: scroll it to the middle, then hit-test its centre,
 * the four points 21 px away and the four corners of the 44 x 44 square around its centre. Each point must resolve
 * (elementFromPoint().closest(PRESSABLE)) to the element itself. Table cells keep the sticky-column centring and clamp
 * horizontal points to their own column, as in the test above.
 */
function scanPage({ selector, scope, min, inlineNames, only }) {
  const root = document.querySelector(scope);
  const label = (el) =>
    (el.getAttribute("aria-label") || el.textContent || el.getAttribute("href") || el.tagName).trim().replace(/\s+/g, " ");
  const rendered = (el) => {
    const box = el.getBoundingClientRect();
    if (box.width === 0 || box.height === 0) return false;
    if (el.closest("[hidden]") || el.closest("dialog:not([open])")) return false;
    return getComputedStyle(el).visibility === "visible";
  };
  const out = { scanned: [], misses: [], inline: [], markers: 0 };
  for (const el of root.querySelectorAll(selector)) {
    if (only && !el.matches(only)) continue;
    if (!rendered(el)) continue;
    if (el.matches(".marker")) {
      out.markers += 1; // exemption (b)
      continue;
    }
    if (el.matches(".skip-link") && document.activeElement !== el) continue; // off-screen until focused; checked focused
    const text = label(el);
    if (el.closest(".prose") && el.matches("a") && inlineNames.includes(text)) {
      // exemption (a): must really be inside a sentence, with other words around it
      const sentence = el.parentElement.textContent.replace(/\s+/g, " ").trim();
      const rest = sentence.replace(el.textContent.trim(), "").replace(/[^A-Za-z]+/g, " ").trim();
      out.inline.push({ text, inSentence: rest.split(" ").length >= 3 && getComputedStyle(el).display === "inline" });
      continue;
    }
    el.scrollIntoView({ block: "center", inline: "center" });
    const cell = el.closest("th, td");
    if (cell && cell.cellIndex > 0) {
      const wrap = el.closest(".table-wrap");
      const area = wrap.getBoundingClientRect();
      const sticky = cell.closest("tr").cells[0].getBoundingClientRect();
      const own = cell.getBoundingClientRect();
      wrap.scrollLeft += own.left + own.width / 2 - (sticky.right + area.right) / 2;
    }
    const box = el.getBoundingClientRect();
    const column = cell ? cell.getBoundingClientRect() : null;
    const cx = box.left + box.width / 2;
    const cy = box.top + box.height / 2;
    const r = min / 2 - 1;
    const clampX = (x) => (column ? Math.min(Math.max(x, column.left + 1), column.right - 1) : x);
    const points = [
      [0, 0],
      [-r, 0],
      [r, 0],
      [0, -r],
      [0, r],
      [-r, -r],
      [r, -r],
      [-r, r],
      [r, r],
    ];
    const missed = [];
    for (const [dx, dy] of points) {
      const hit = document.elementFromPoint(clampX(cx + dx), cy + dy)?.closest(selector);
      if (hit !== el) missed.push(`(${dx}, ${dy}) -> ${hit ? label(hit).slice(0, 40) : "nothing pressable"}`);
    }
    const kind = typeof el.className === "string" ? el.className : (el.className?.baseVal ?? "");
    out.scanned.push({ text, kind, footer: Boolean(el.closest(".site-footer")) });
    if (missed.length) {
      out.misses.push(`"${text.slice(0, 60)}" (${Math.round(box.width)} x ${Math.round(box.height)}): ${missed.join("; ")}`);
    }
  }
  return out;
}

async function scan(page, { scope = "body", only = null } = {}) {
  return page.evaluate(scanPage, { selector: PRESSABLE, scope, min: MIN, inlineNames: INLINE_PROSE_LINKS, only });
}

function expectNoMisses(result, state) {
  expect(result.misses, `${state}: pressables below 44 x 44 px on touch`).toEqual([]);
}

const count = (result, test) => result.scanned.filter(test).length;
const footerLinks = (result) => count(result, (s) => s.footer);
const NAV = ["Live", "Insights", "Pipeline", "About"];
const ALERTS = loadFixture("alerts.json");
const ALERT_ROWS = ALERTS.open.length + Math.min(ALERTS.resolved.length, 20); // the rail shows up to 20 resolved

async function checkAllPages(page, width) {
  const sfStations = LIVE.stations.filter((s) => s.view === "sf").length;
  await prepare(page, { ageMs: 10 * 60_000, replay: true });
  await page.goto("/index.html");
  // The emulation must really report a coarse pointer, or every check below would pass vacuously.
  expect(await page.evaluate(() => matchMedia("(pointer: coarse)").matches)).toBe(true);
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);

  // Live, map view, data paused: the banner with Replay a day.
  await expect(page.getByRole("button", { name: "Replay a day" })).toBeVisible();
  await expect(page.locator("[data-alerts-peek]")).not.toHaveAttribute("aria-disabled", "true"); // alerts are in
  let result = await scan(page);
  expectNoMisses(result, `Live, paused, ${width} px`);
  expect(footerLinks(result)).toBeGreaterThanOrEqual(3);
  expect(count(result, (s) => /stat-tile__info/.test(s.kind))).toBeGreaterThanOrEqual(4);
  expect(count(result, (s) => s.text === "Replay a day")).toBe(1);
  expect(count(result, (s) => /segmented__option/.test(s.kind))).toBe(5);
  expect(count(result, (s) => s.text === "Show only problems")).toBe(1);
  expect(result.markers).toBeGreaterThan(0);
  if (width >= 1120) expect(count(result, (s) => NAV.includes(s.text))).toBe(4); // inline nav links

  // The alerts rail (F3): every row beside or under the map; on a phone the peek bar, then every row in the sheet.
  if (width >= 600) {
    expect(count(result, (s) => /alert-row/.test(s.kind))).toBe(ALERT_ROWS);
    expect(count(result, (s) => /alerts-peek/.test(s.kind))).toBe(0);
  } else {
    expect(count(result, (s) => /alerts-peek/.test(s.kind))).toBe(1);
    expect(count(result, (s) => /alert-row/.test(s.kind))).toBe(0); // the rail's rows live in the sheet on phones
    await page.locator("[data-alerts-peek]").click();
    await expect(page.locator("[data-alerts-sheet]")).toBeVisible();
    result = await scan(page, { scope: "[data-alerts-sheet]" });
    expectNoMisses(result, `Live, alerts sheet open, ${width} px`);
    expect(count(result, (s) => /alert-row/.test(s.kind))).toBe(ALERT_ROWS);
    expect(count(result, (s) => s.text === "Close alerts")).toBe(1);
    await page.getByRole("button", { name: "Close alerts" }).click();
    await expect(page.locator("[data-alerts-sheet]")).toBeHidden();
  }

  // Live in replay mode: Exit.
  await page.getByRole("button", { name: "Replay a day" }).click();
  await expect(page.getByRole("button", { name: "Exit" })).toBeFocused();
  result = await scan(page);
  expectNoMisses(result, `Live, replay mode, ${width} px`);
  expect(count(result, (s) => s.text === "Exit")).toBe(1);
  expect(count(result, (s) => /alerts-peek/.test(s.kind))).toBe(0); // hidden in replay mode: nothing to open
  expect(count(result, (s) => /alert-row/.test(s.kind))).toBe(0); // the rail shows its replay note instead
  await page.getByRole("button", { name: "Exit" }).click();

  // Live, list view: every sort button and every station button of the default region.
  await page.getByRole("radio", { name: "List" }).check();
  await expect(page.locator("[data-list-view] tbody tr")).toHaveCount(sfStations);
  result = await scan(page);
  expectNoMisses(result, `Live, list view, ${width} px`);
  expect(count(result, (s) => /sort-button/.test(s.kind))).toBeGreaterThanOrEqual(7);
  expect(count(result, (s) => /link-button/.test(s.kind))).toBe(sfStations);
  await page.getByRole("radio", { name: "Map" }).check();

  if (width < 600) {
    // Phone sheet open: its close button (the rest of the page is inert behind the modal).
    const station = LIVE.stations.find((s) => s.view === "sf" && s.state === "empty");
    await page.locator(`[data-map-svg] .marker[data-id="${station.id}"]`).dispatchEvent("click");
    await expect(page.locator("[data-sheet]")).toBeVisible();
    result = await scan(page, { scope: "[data-sheet]" });
    expectNoMisses(result, `Live, sheet open, ${width} px`);
    expect(count(result, (s) => s.text === "Close station details")).toBe(1);
    await page.getByRole("button", { name: "Close station details" }).click();
    await expect(page.locator("[data-sheet]")).toBeHidden();
  }

  if (width < 1120) {
    // Nav overlay open: its links, close button and theme button (the page behind is inert).
    await page.getByRole("button", { name: "Open menu" }).click();
    await expect(page.locator("#nav-menu")).toBeVisible();
    result = await scan(page, { scope: "#nav-menu" });
    expectNoMisses(result, `nav overlay, ${width} px`);
    expect(count(result, (s) => NAV.includes(s.text))).toBe(4);
    expect(count(result, (s) => s.text === "Close menu")).toBe(1);
    expect(count(result, (s) => /theme-toggle/.test(s.kind))).toBe(1);
    await page.keyboard.press("Escape");
    await expect(page.locator("#nav-menu")).toBeHidden();
  }

  for (const path of ["/insights.html", "/pipeline.html", "/about.html"]) {
    await page.goto(path);
    await expect(page.locator(".nav [data-freshness]")).not.toHaveAttribute("data-state", "loading");
    result = await scan(page);
    expectNoMisses(result, `${path}, ${width} px`);
    expect(footerLinks(result), path).toBeGreaterThanOrEqual(3);
    if (path === "/about.html") {
      // Exemption (a): exactly the listed in-sentence links, each really inside running text.
      expect(result.inline.map((l) => l.text).sort()).toEqual([...INLINE_PROSE_LINKS].sort());
      for (const link of result.inline) expect(link.inSentence, `"${link.text}" is inside a sentence`).toBe(true);
    } else {
      expect(count(result, (s) => s.text === "See live station status"), path).toBe(1);
    }
  }

  // The skip link, focused, on every page.
  for (const path of ["/index.html", "/insights.html", "/pipeline.html", "/about.html"]) {
    await page.goto(path);
    await page.keyboard.press("Tab");
    const skip = page.locator(".skip-link");
    await expect(skip).toBeFocused();
    expect(await skip.evaluate((el) => el.getBoundingClientRect().height)).toBeGreaterThanOrEqual(MIN);
    result = await scan(page, { only: ".skip-link" });
    expect(result.scanned).toHaveLength(1);
    expectNoMisses(result, `${path} skip link, ${width} px`);
  }
}

test.describe("phone", () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

  test("every button and link on all four pages is at least 44 px on touch at 390 px", async ({ page }) => {
    test.setTimeout(120_000);
    await checkAllPages(page, 390);
  });
});

test.describe("large touch screen", () => {
  test.use({ viewport: { width: 1280, height: 800 }, hasTouch: true, isMobile: true });

  test("every button and link on all four pages is at least 44 px on touch at 1280 px", async ({ page }) => {
    test.setTimeout(120_000);
    await checkAllPages(page, 1280);
  });
});
