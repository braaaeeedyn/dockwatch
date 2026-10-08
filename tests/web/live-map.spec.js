// F2 live station map: markers, regions, legend, list, problems filter, details, keyboard, stale banner, refresh, KPIs.
// Every expected number is computed from the fixture (tests/web/fixtures/*.json), never hard-coded.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { LIVE, loadFixture, prepare } from "./helpers.js";

const ALERTS = loadFixture("alerts.json");
const STATES = ["empty", "low", "ok", "high", "full", "offline"];
const WORDS = { empty: "Empty", low: "Low", ok: "OK", high: "High", full: "Full", offline: "Offline" };
const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const REGION_NAMES = { sf: "San Francisco", eastbay: "East Bay", sj: "San José" };

const number = new Intl.NumberFormat("en-US");
const shortClock = new Intl.DateTimeFormat("en-US", { timeZone: "America/Los_Angeles", hour: "numeric", minute: "2-digit" });

const inView = (view, live = LIVE) => live.stations.filter((s) => s.view === view);
const problems = (stations) => stations.filter((s) => s.state !== "ok");
const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;

function countStates(stations) {
  const counts = Object.fromEntries(STATES.map((s) => [s, 0]));
  for (const s of stations) counts[s.state] += 1;
  return counts;
}

const markers = (page) => page.locator("[data-map-svg] .marker");
const marker = (page, id) => page.locator(`[data-map-svg] .marker[data-id="${id}"]`);
const rows = (page) => page.locator("[data-list-view] tbody tr");

async function openLive(page, options) {
  await prepare(page, options);
  await page.goto("/index.html");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
}

async function chooseRegion(page, view) {
  await page.getByRole("radio", { name: REGION_NAMES[view] }).check();
  await expect(markers(page)).toHaveCount(inView(view).length);
}

test("draws one marker per station in the selected region", async ({ page }) => {
  await openLive(page);
  // San Francisco is the default region.
  await expect(page.getByRole("radio", { name: "San Francisco" })).toBeChecked();
  for (const view of ["sf", "sj", "eastbay"]) {
    if (view !== "sf") await chooseRegion(page, view);
    const stations = inView(view);
    await expect(markers(page)).toHaveCount(stations.length);
    const drawn = await markers(page).evaluateAll((els) => els.map((el) => [el.dataset.id, el.dataset.state]));
    expect(new Map(drawn)).toEqual(new Map(stations.map((s) => [s.id, s.state])));
    // Draw order: ok first, then offline, low/high, empty/full on top.
    const tiers = { ok: 0, offline: 1, low: 2, high: 2, empty: 3, full: 3 };
    const order = drawn.map(([, state]) => tiers[state]);
    expect(order).toEqual([...order].sort((a, b) => a - b));
  }
  // Problem markers are drawn 1.35x the ok radius.
  const radius = (state) =>
    page.locator(`[data-map-svg] .marker[data-state="${state}"]`).first().evaluate((el) => parseFloat(getComputedStyle(el).r));
  expect((await radius("empty")) / (await radius("ok"))).toBeCloseTo(1.35, 2);
});

test("region switch is remembered", async ({ page }) => {
  await openLive(page);
  await chooseRegion(page, "eastbay");
  await expect(page).toHaveURL(/region=eastbay/);

  // A fresh visit without the query string still opens the East Bay.
  await page.goto("/index.html");
  await expect(page.getByRole("radio", { name: "East Bay" })).toBeChecked();
  await expect(markers(page)).toHaveCount(inView("eastbay").length);

  await page.reload();
  await expect(page.getByRole("radio", { name: "East Bay" })).toBeChecked();
  await chooseRegion(page, "sj");
  await page.goto("/index.html");
  await expect(page.getByRole("radio", { name: "San José" })).toBeChecked();
  await expect(markers(page)).toHaveCount(inView("sj").length);
});

test("legend counts match live.json", async ({ page }) => {
  await openLive(page);
  for (const view of ["sf", "eastbay", "sj"]) {
    if (view !== "sf") await chooseRegion(page, view);
    const counts = countStates(inView(view));
    for (const state of STATES) {
      // The export's own counts and a count of its stations agree, and the legend shows them.
      expect(LIVE.counts[view][state] ?? 0, `${view} ${state}`).toBe(counts[state]);
      const item = page.locator(".legend__item").filter({ has: page.locator(`[data-legend-count="${state}"]`) });
      await expect(item).toHaveText(`${WORDS[state]} · ${number.format(counts[state])}`);
    }
  }
  await expect(page.locator(".map-footer .caption")).toContainText("Times are Pacific time.");
});

test("list view is a sortable table for the region", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await openLive(page);
  await page.getByRole("radio", { name: "List" }).check();
  await expect(page.locator("[data-map-stage]")).toBeHidden();
  const table = page.getByRole("table");
  await expect(table).toBeVisible();
  await expect(rows(page)).toHaveCount(inView("sf").length);
  await expect(page).toHaveURL(/view=list/);

  const headers = await table.locator("thead th").allTextContents();
  expect(headers.map((h) => h.trim())).toEqual(["Station", "Code", "State", "Bikes", "Docks", "In state for", "Last reported"]);
  await expect(table.locator('thead th[aria-sort="ascending"]')).toHaveText("State");

  const bikesHeader = table.locator('thead th[data-key="bikes"]');
  const bikes = () => rows(page).locator("td:nth-of-type(3)").allTextContents();
  const asNumbers = (texts) => texts.map((t) => Number(t.replace(/,/g, "")));

  await bikesHeader.getByRole("button").click();
  await expect(bikesHeader).toHaveAttribute("aria-sort", "ascending");
  await expect(table.locator("thead th[aria-sort]")).toHaveCount(1);
  let values = asNumbers(await bikes());
  expect(values).toEqual([...values].sort((a, b) => a - b));

  await bikesHeader.getByRole("button").click();
  await expect(bikesHeader).toHaveAttribute("aria-sort", "descending");
  values = asNumbers(await bikes());
  expect(values).toEqual([...values].sort((a, b) => b - a));
  expect(values[0]).toBe(Math.max(...inView("sf").map((s) => s.bikes)));

  // Station names sort alphabetically.
  await table.locator('thead th[data-key="name"] button').click();
  const names = await rows(page).locator("th").allTextContents();
  expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b, "en", { numeric: true, sensitivity: "base" })));

  // First column is sticky; the table scrolls inside its own wrapper.
  expect(await rows(page).first().locator("th").evaluate((el) => getComputedStyle(el).position)).toBe("sticky");
  expect(await page.locator(".table-wrap").evaluate((el) => getComputedStyle(el).overflowX)).toBe("auto");

  // Same region filter as the map.
  await page.getByRole("radio", { name: "San José" }).check();
  await expect(rows(page)).toHaveCount(inView("sj").length);

  // No horizontal page scroll with the table open on a small phone.
  await page.setViewportSize({ width: 320, height: 800 });
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
});

test("show only problems filters map and list", async ({ page }) => {
  await openLive(page);
  const chip = page.getByRole("button", { name: "Show only problems" });
  await expect(chip).toHaveAttribute("aria-pressed", "false");
  await chip.click();
  await expect(chip).toHaveAttribute("aria-pressed", "true");
  await expect(page).toHaveURL(/problems=1/);

  const sfProblems = problems(inView("sf"));
  await expect(markers(page)).toHaveCount(sfProblems.length);
  await expect(page.locator('[data-map-svg] .marker[data-state="ok"]')).toHaveCount(0);

  await page.getByRole("radio", { name: "List" }).check();
  await expect(rows(page)).toHaveCount(sfProblems.length);
  const listed = await rows(page).evaluateAll((trs) => trs.map((tr) => tr.dataset.id).sort());
  expect(listed).toEqual(sfProblems.map((s) => s.id).sort());

  await page.getByRole("radio", { name: "East Bay" }).check();
  await expect(rows(page)).toHaveCount(problems(inView("eastbay")).length);

  await chip.click();
  await expect(rows(page)).toHaveCount(inView("eastbay").length);
});

test("show only problems explains an empty result and offers to show all stations", async ({ page }) => {
  // Same fixture, but every San José station is ok.
  const live = structuredClone(LIVE);
  for (const s of live.stations) if (s.view === "sj") s.state = "ok";
  await openLive(page, { live });
  await page.getByRole("radio", { name: "San José" }).check();
  await page.getByRole("button", { name: "Show only problems" }).click();
  await expect(markers(page)).toHaveCount(0);
  const message = page.locator("[data-map-message]");
  await expect(message).toContainText("No problem stations in San José right now");
  await message.getByRole("button", { name: "Show all stations" }).click();
  await expect(markers(page)).toHaveCount(inView("sj", live).length);
  await expect(message).toBeHidden();
});

function detailsExpectations(s) {
  const ebikes = s.ebikes ? ` (${plural(s.ebikes, "e-bike", "e-bikes")})` : "";
  return [s.name, s.code, WORDS[s.state], `${plural(s.bikes, "bike", "bikes")}${ebikes} · ${plural(s.docks, "dock", "docks")}`];
}

test("station details show name, code, state, bikes and docks", async ({ page }) => {
  const station = inView("sf").find((s) => s.state === "full" && s.code && s.state_since);
  const withEbikes = inView("sf").find((s) => s.state === "ok" && s.code && s.ebikes > 0);

  // Desktop: a tooltip pinned beside the marker.
  await page.setViewportSize({ width: 1440, height: 900 });
  await openLive(page);
  const tooltip = page.locator("[data-tooltip]");
  await expect(tooltip).toBeHidden();
  await marker(page, station.id).dispatchEvent("click");
  await expect(tooltip).toBeVisible();
  for (const text of detailsExpectations(station)) await expect(tooltip).toContainText(text);
  await expect(tooltip).toContainText(/Full for (\d+ min|\d+ h( \d+ min)?)/);
  await expect(tooltip).toContainText(/Last reported/);
  await expect(marker(page, station.id)).toHaveAttribute("aria-expanded", "true");
  await expect(marker(page, station.id)).toHaveClass(/is-active/);

  await marker(page, withEbikes.id).dispatchEvent("click");
  for (const text of detailsExpectations(withEbikes)) await expect(tooltip).toContainText(text);
  await expect(tooltip).not.toContainText(" for "); // ok stations are not in an episode

  await page.keyboard.press("Escape");
  await expect(tooltip).toBeHidden();

  // Phone: a bottom sheet (modal dialog) with the same contents.
  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
  await marker(page, station.id).dispatchEvent("click");
  const sheet = page.getByRole("dialog", { name: station.name });
  await expect(sheet).toBeVisible();
  for (const text of detailsExpectations(station)) await expect(sheet).toContainText(text);
  await expect(tooltip).toBeHidden();
  await sheet.getByRole("button", { name: "Close station details" }).click();
  await expect(sheet).toBeHidden();
  await expect(marker(page, station.id)).toBeFocused();
});

test("markers are reachable with the keyboard", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await openLive(page);

  // Tab from the last map control lands on exactly one marker (roving tabindex).
  await page.getByRole("button", { name: "Show only problems" }).focus();
  await page.keyboard.press("Tab");
  const focused = page.locator(":focus");
  await expect(focused).toHaveClass(/marker/);
  await expect(page.locator('[data-map-svg] .marker[tabindex="0"]')).toHaveCount(1);
  const first = await focused.evaluate((el) => ({ id: el.dataset.id, x: Number(el.getAttribute("cx")) }));
  await expect(focused).toHaveAttribute("aria-label", /: (empty|low|ok|high|full|offline), \d+ bikes?, \d+ docks?$/);

  // Arrow keys move to the nearest station in that direction.
  await page.keyboard.press("ArrowRight");
  const second = await page.locator(":focus").evaluate((el) => ({ id: el.dataset.id, x: Number(el.getAttribute("cx")) }));
  expect(second.id).not.toBe(first.id);
  expect(second.x).toBeGreaterThan(first.x);
  await expect(page.locator('[data-map-svg] .marker[tabindex="0"]')).toHaveCount(1);
  await expect(marker(page, second.id)).toHaveAttribute("tabindex", "0");
  await page.keyboard.press("ArrowLeft");
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("ArrowUp");
  await expect(page.locator(":focus")).toHaveClass(/marker/);

  // Enter opens the details, Escape closes them and keeps focus on the marker.
  const current = await page.locator(":focus").getAttribute("data-id");
  const station = LIVE.stations.find((s) => s.id === current);
  await expect(page.locator("[data-marker-label]")).toHaveText(station.name); // focus shows the name on the map
  await expect(marker(page, current)).toHaveClass(/is-active/);
  await page.keyboard.press("Enter");
  await expect(page.locator("[data-tooltip]")).toContainText(station.name);
  await page.keyboard.press("Escape");
  await expect(page.locator("[data-tooltip]")).toBeHidden();
  await expect(marker(page, current)).toBeFocused();

  // Tab leaves the map in one step (only one marker is in the tab order).
  await page.keyboard.press("Tab");
  await expect(page.locator(":focus")).not.toHaveClass(/marker/);

  // From the list view, a station name takes you to its marker with details open.
  const target = inView("sf").find((s) => s.state === "empty");
  await page.getByRole("radio", { name: "List" }).check();
  await page.getByRole("button", { name: target.name, exact: true }).click();
  await expect(page.locator("[data-map-stage]")).toBeVisible();
  await expect(marker(page, target.id)).toBeFocused();
  await expect(page.locator("[data-tooltip]")).toContainText(target.name);
});

test("stale banner appears when data is older than 5 minutes", async ({ page }) => {
  const banner = page.locator("[data-stale-banner]");
  // DESIGN §6's sentence with the time, then why the data stopped and that the page recovers on its own.
  const expected =
    `Live data paused — showing the state at ${shortClock.format(new Date(LIVE.generated_at))}. ` +
    "The DockWatch pipeline isn’t running right now, so no new station data is coming in; " +
    "the map updates on its own when it restarts.";

  await openLive(page, { ageMs: 30_000 });
  await expect(banner).toBeHidden();
  // No new export arrives: after 5 minutes the banner appears without a reload, and the markers stay.
  await page.clock.fastForward(6 * 60_000);
  await expect(banner).toBeVisible();
  await expect(banner).toHaveText(expected);
  await expect(markers(page)).toHaveCount(inView("sf").length);

  await page.goto("/index.html");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
  await expect(banner).toBeVisible();
  await expect(banner).toHaveText(expected);
});

test("refresh updates markers in place", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  let served = LIVE;
  await prepare(page);
  await page.route("**/data/live.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(served) }),
  );
  await page.goto("/index.html");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);

  const station = inView("sf").find((s) => s.state === "empty");
  const before = countStates(inView("sf"));
  const summary = page.locator("[data-map-summary]");
  await expect(summary).toHaveText(`San Francisco: ${before.empty} empty, ${before.full} full, ${before.offline} offline`);
  await marker(page, station.id).evaluate((el) => {
    el.dataset.probe = "same-node";
  });

  // The next export: one station went from empty to full.
  const next = structuredClone(LIVE);
  next.generated_at = new Date(Date.parse(LIVE.generated_at) + 60_000).toISOString();
  const changed = next.stations.find((s) => s.id === station.id);
  Object.assign(changed, { state: "full", bikes: changed.capacity, ebikes: 0, docks: 0 });
  for (const scope of ["sf", "all"]) {
    next.counts[scope].empty -= 1;
    next.counts[scope].full += 1;
  }
  served = next;
  await page.clock.fastForward(60_000);

  await expect(marker(page, station.id)).toHaveAttribute("data-state", "full");
  await expect(page.locator('[data-probe="same-node"]')).toHaveAttribute("data-id", station.id);
  await expect(markers(page)).toHaveCount(inView("sf").length);
  await expect(page.locator('[data-legend-count="full"]')).toHaveText(number.format(before.full + 1));
  await expect(page.locator('[data-legend-count="empty"]')).toHaveText(number.format(before.empty - 1));
  await expect(summary).toHaveText(`San Francisco: ${before.empty - 1} empty, ${before.full + 1} full, ${before.offline} offline`);
  await expect(page.locator('[data-kpi="full"] [data-kpi-value]')).toHaveText(number.format(LIVE.counts.all.full + 1));
});

test("KPI tiles show empty, full, open alerts and bikes available", async ({ page }) => {
  await openLive(page);
  const tile = (key) => page.locator(`[data-kpi="${key}"]`);
  await expect(tile("empty")).toContainText("Empty now");
  await expect(tile("empty").locator("[data-kpi-value]")).toHaveText(number.format(LIVE.counts.all.empty));
  await expect(tile("empty").locator("[data-kpi-sub]")).toHaveText(`of ${number.format(LIVE.station_count)} stations`);
  await expect(tile("full")).toContainText("Full now");
  await expect(tile("full").locator("[data-kpi-value]")).toHaveText(number.format(LIVE.counts.all.full));
  await expect(tile("alerts")).toContainText("Open alerts");
  await expect(tile("alerts").locator("[data-kpi-value]")).toHaveText(number.format(ALERTS.open.length));
  await expect(tile("bikes")).toContainText("Bikes available");
  await expect(tile("bikes").locator("[data-kpi-value]")).toHaveText(number.format(LIVE.bikes_available));
  const ebikes = LIVE.stations.reduce((sum, s) => sum + (s.ebikes ?? 0), 0);
  await expect(tile("bikes").locator("[data-kpi-sub]")).toHaveText(`including ${number.format(ebikes)} e-bikes`);
  await expect(page.locator("[data-kpis]")).not.toHaveAttribute("aria-busy", /.+/);

  // The info button reveals the definition.
  const info = page.getByRole("button", { name: "What “Open alerts” means" });
  await expect(info).toHaveAttribute("aria-expanded", "false");
  await info.click();
  await expect(info).toHaveAttribute("aria-expanded", "true");
  await expect(page.locator("#kpi-def-alerts")).toBeVisible();
});

test("shows an error with a retry action when live.json can't be read", async ({ page }) => {
  let ok = false;
  await prepare(page);
  await page.route("**/data/live.json", (route) =>
    ok
      ? route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(LIVE) })
      : route.fulfill({ status: 503, body: "unavailable" }),
  );
  await page.goto("/index.html");
  const message = page.locator("[data-map-message]");
  await expect(message).toContainText("couldn’t be loaded");
  ok = true;
  await message.getByRole("button", { name: "Retry" }).click();
  await expect(markers(page)).toHaveCount(inView("sf").length);
  await expect(message).toBeHidden();
});

test("axe finds no violations on the live page with the list, details and sheet open", async ({ page }) => {
  test.setTimeout(120_000);
  const station = inView("sf").find((s) => s.state === "empty" && s.code);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    await page.setViewportSize({ width: 1440, height: 900 });
    await openLive(page);
    const check = async (label) => {
      const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
      const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
      expect(summary, `${label}, ${colorScheme}`).toEqual([]);
    };
    await marker(page, station.id).dispatchEvent("click");
    await expect(page.locator("[data-tooltip]")).toBeVisible();
    await check("map with tooltip");
    await page.getByRole("radio", { name: "List" }).check();
    await page.getByRole("button", { name: "Show only problems" }).click();
    await check("list, problems only");

    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/index.html");
    await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
    await marker(page, station.id).dispatchEvent("click");
    await expect(page.locator("[data-sheet]")).toBeVisible();
    await check("phone sheet");
  }
});

test("list keeps focus on the same station across a refresh", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  let served = LIVE;
  await prepare(page);
  await page.route("**/data/live.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(served) }),
  );
  await page.goto("/index.html?view=list");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
  await expect(rows(page)).toHaveCount(inView("sf").length);

  // A station in the middle of the list, so the wrapper can scroll above and below it.
  const ids = await rows(page).evaluateAll((els) => els.map((el) => el.dataset.id));
  const id = ids[Math.floor(ids.length / 2)];
  const station = LIVE.stations.find((s) => s.id === id);
  const row = page.locator(`[data-list-view] tbody tr[data-id="${id}"]`);
  const button = row.locator("[data-show-station]");
  await row.evaluate((el) => {
    el.dataset.probe = "same-row";
  });
  await button.focus();
  await expect(button).toBeFocused();
  const wrap = page.locator(".table-wrap");
  await wrap.evaluate((el) => {
    el.scrollTop = Math.max(0, el.scrollTop - 40);
    el.scrollLeft = 0;
  });
  const before = await wrap.evaluate((el) => ({ top: el.scrollTop, left: el.scrollLeft }));
  expect(before.top).toBeGreaterThan(0);

  // The next export: the focused station's bikes and docks change, and another station goes from empty to full
  // (so rows above it move).
  const next = structuredClone(LIVE);
  next.generated_at = new Date(Date.parse(LIVE.generated_at) + 60_000).toISOString();
  const mine = next.stations.find((s) => s.id === id);
  const bikes = station.bikes > 0 ? station.bikes - 1 : station.bikes + 1;
  const docks = station.docks > 0 ? station.docks - 1 : station.docks + 1;
  Object.assign(mine, { bikes, docks });
  const other = next.stations.find((s) => s.view === "sf" && s.state === "empty" && s.id !== id);
  Object.assign(other, { state: "full", bikes: other.capacity, ebikes: 0, docks: 0 });
  served = next;
  await page.clock.fastForward(60_000);

  await expect(row.locator("td:nth-of-type(3)")).toHaveText(number.format(bikes));
  await expect(row.locator("td:nth-of-type(4)")).toHaveText(number.format(docks));
  await expect(page.locator(`[data-list-view] tbody tr[data-id="${other.id}"] .state-word`)).toHaveText("Full");
  await expect(button).toBeFocused();
  await expect(page.locator('[data-probe="same-row"]')).toHaveAttribute("data-id", id);
  await expect(rows(page)).toHaveCount(inView("sf").length);
  const after = await wrap.evaluate((el) => ({ top: el.scrollTop, left: el.scrollLeft }));
  expect(Math.abs(after.top - before.top)).toBeLessThanOrEqual(1);
  expect(Math.abs(after.left - before.left)).toBeLessThanOrEqual(1);
});
