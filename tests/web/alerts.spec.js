// F3 alerts rail (DESIGN.md §4, §7 Alert row, §8): order, copy, layout per breakpoint, selection, in-place refresh,
// polite announcements, replay mode, errors and axe. Every expected value is computed from the fixtures
// (alerts.json, live.json); variants are structuredClone()d in the test, never written to the fixtures.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { LIVE, loadFixture, prepare } from "./helpers.js";

const ALERTS = loadFixture("alerts.json");
const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const PAUSED_MS = 10 * 60_000;
const MIN = 60_000;

const PACIFIC = "America/Los_Angeles";
const shortClock = new Intl.DateTimeFormat("en-US", { timeZone: PACIFIC, hour: "numeric", minute: "2-digit" });
const monthDay = new Intl.DateTimeFormat("en-US", { timeZone: PACIFIC, month: "short", day: "numeric" });
const pacificDate = new Intl.DateTimeFormat("en-CA", { timeZone: PACIFIC, year: "numeric", month: "2-digit", day: "2-digit" });
const number = new Intl.NumberFormat("en-US");
const REGION_NAMES = { sf: "San Francisco", eastbay: "East Bay", sj: "San José" };
const WORDS = { empty: "Empty", full: "Full" };

/** Open by `started` ascending, resolved by `resolved` descending (at most 20); ties keep the exporter's order. */
function ordered(doc) {
  const open = [...doc.open].sort((a, b) => Date.parse(a.started) - Date.parse(b.started));
  const resolved = [...doc.resolved].sort((a, b) => Date.parse(b.resolved) - Date.parse(a.resolved)).slice(0, 20);
  return { open, resolved };
}

const ORDER = ordered(ALERTS);

/** "San Francisco · started 3:32 PM", or "… started Oct 6, 3:32 PM" when that wasn't today in Pacific time. */
function startLine(alert, now) {
  const started = new Date(alert.started);
  const today = pacificDate.format(started) === pacificDate.format(new Date(now));
  const at = today ? shortClock.format(started) : `${monthDay.format(started)}, ${shortClock.format(started)}`;
  return `${REGION_NAMES[alert.view]} · started ${at}`;
}

/** A new open alert for a fixture station that has none, `startedMs` as its start. */
function newAlert(startedMs, skip = []) {
  const taken = new Set([...ALERTS.open, ...ALERTS.resolved].map((a) => a.station_id).concat(skip));
  const station = LIVE.stations.find((s) => !taken.has(s.id) && s.state === "empty");
  const started = new Date(startedMs).toISOString();
  return {
    episode_id: `${station.id}:empty:test-${startedMs}`,
    station_id: station.id,
    kind: "empty",
    started,
    raised: started,
    name: station.name,
    code: station.code,
    view: station.view,
  };
}

/** Move the open alert `episodeId` of `doc` to its resolved list, resolved at `atMs`. */
function resolve(doc, episodeId, atMs) {
  const i = doc.open.findIndex((a) => a.episode_id === episodeId);
  const [alert] = doc.open.splice(i, 1);
  const resolved = new Date(atMs).toISOString();
  doc.resolved.unshift({ ...alert, resolved, duration_s: Math.round((atMs - Date.parse(alert.started)) / 1000) });
}

/** Serve data/alerts.json from `state.doc` (or HTTP `state.status`), registered after prepare() so it wins. */
async function serveAlerts(page, doc = ALERTS) {
  const state = { doc, status: 200 };
  await page.route("**/data/alerts.json", (route) =>
    state.status === 200
      ? route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(state.doc) })
      : route.fulfill({ status: state.status, body: "" }),
  );
  return state;
}

const rail = (page) => page.locator("[data-alerts-rail]");
const railBody = (page) => page.locator("[data-alerts-body]");
const openRows = (page) => page.locator("[data-alerts-open] .alert-row");
const resolvedRows = (page) => page.locator("[data-alerts-resolved] .alert-row");
const row = (page, episode) => page.locator(`.alert-row[data-episode="${episode}"]`);
const peek = (page) => page.locator("[data-alerts-peek]");
const alertsSheet = (page) => page.locator("[data-alerts-sheet]");
const episodes = (locator) => locator.evaluateAll((els) => els.map((el) => el.dataset.episode));

async function openLive(page, path = "/index.html") {
  await page.goto(path);
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
}

async function nowInPage(page) {
  return page.evaluate(() => Date.now());
}

test("alerts rail lists open alerts longest-running first, then up to 20 resolved", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  await openLive(page);
  await expect(rail(page).getByRole("heading", { level: 2, name: "Alerts" })).toBeVisible();
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);
  expect(await episodes(openRows(page))).toEqual(ORDER.open.map((a) => a.episode_id));
  await expect(resolvedRows(page)).toHaveCount(ORDER.resolved.length);
  expect(await episodes(resolvedRows(page))).toEqual(ORDER.resolved.map((a) => a.episode_id));

  const headings = rail(page).getByRole("heading", { level: 3 });
  await expect(headings).toHaveCount(2);
  await expect(headings.nth(0)).toHaveText(`Open now ${number.format(ALERTS.open.length)}`);
  await expect(headings.nth(1)).toHaveText(`Recently resolved ${number.format(ORDER.resolved.length)}`);
  await expect(page.locator('[data-alerts-count="open"]')).toHaveText(number.format(ALERTS.open.length));
  await expect(page.locator('[data-alerts-count="resolved"]')).toHaveText(number.format(ORDER.resolved.length));
  // The rows are buttons for stations in live.json (every fixture station is), and the body is no longer busy.
  await expect(rail(page).locator("button.alert-row")).toHaveCount(ALERTS.open.length + ORDER.resolved.length);
  await expect(railBody(page)).toHaveAttribute("aria-busy", "false");
});

test("alert rows say how long the station has been empty or full, or when it resolved", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.setViewportSize({ width: 1440, height: 900 });
  const generated = Date.parse(ALERTS.generated_at);
  const doc = structuredClone(ALERTS);
  const open = doc.open[0];
  open.started = new Date(generated - 34 * MIN).toISOString();
  const closed = doc.resolved[0];
  closed.duration_s = 1320;
  await prepare(page);
  await serveAlerts(page, doc);
  await openLive(page);

  const now = await nowInPage(page);
  const openRow = row(page, open.episode_id);
  await expect(openRow.locator(".alert-row__name")).toHaveText(open.name);
  await expect(openRow.locator(".alert-row__name")).toHaveAttribute("translate", "no");
  await expect(openRow.locator(".alert-row__meta")).toHaveText(`${WORDS[open.kind]} for 34 min`);
  await expect(openRow.locator(".alert-row__start")).toHaveText(startLine(open, now));
  expect(startLine(open, now)).toMatch(/^(San Francisco|East Bay|San José) · started \d{1,2}:\d{2} [AP]M$/);
  await expect(openRow.locator(".alert-row__marker")).toHaveAttribute("data-state", open.kind);

  const closedRow = row(page, closed.episode_id);
  await expect(closedRow.locator(".alert-row__name")).toHaveText(closed.name);
  await expect(closedRow.locator(".alert-row__meta")).toHaveText(`${WORDS[closed.kind]}, resolved after 22 min`);
  await expect(closedRow.locator(".alert-row__start")).toHaveText(startLine(closed, now));

  // Durations move on with the page clock (the 15 s tick and the 60 s re-read).
  await page.clock.fastForward(60_000);
  await expect(openRow.locator(".alert-row__meta")).toHaveText(`${WORDS[open.kind]} for 35 min`);
  await expect(closedRow.locator(".alert-row__meta")).toHaveText(`${WORDS[closed.kind]}, resolved after 22 min`);
});

test("alerts rail sits beside the map on desktop, sticky, and scrolls inside", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  await openLive(page);
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);

  const map = await page.locator("#map").boundingBox();
  const box = await rail(page).boundingBox();
  const layout = await page.locator(".live-layout").boundingBox();
  expect(box.x).toBeGreaterThanOrEqual(map.x + map.width);
  expect(Math.abs(box.y - map.y)).toBeLessThanOrEqual(1);
  const ratio = box.width / layout.width;
  expect(ratio).toBeGreaterThanOrEqual(0.28);
  expect(ratio).toBeLessThanOrEqual(0.38);

  await page.evaluate(() => window.scrollBy(0, 600));
  const after = await page.evaluate(() => {
    const r = document.querySelector("[data-alerts-rail]").getBoundingClientRect();
    const nav = document.querySelector(".site-header").getBoundingClientRect();
    return { top: r.top, bottom: r.bottom, navBottom: nav.bottom, height: window.innerHeight, scrolled: window.scrollY };
  });
  expect(after.scrolled).toBeGreaterThan(0);
  expect(after.top).toBeGreaterThanOrEqual(after.navBottom);
  expect(after.bottom).toBeLessThanOrEqual(after.height);

  const scroller = await railBody(page).evaluate((el) => ({
    scrollHeight: el.scrollHeight,
    clientHeight: el.clientHeight,
    overflowY: getComputedStyle(el).overflowY,
  }));
  expect(scroller.scrollHeight).toBeGreaterThan(scroller.clientHeight);
  expect(scroller.overflowY).toBe("auto");
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
});

test("alerts card sits under the map on tablet, with two columns of rows from 768 px", async ({ page }) => {
  await prepare(page);
  for (const [width, columns] of [
    [900, 2],
    [650, 1],
  ]) {
    await page.setViewportSize({ width, height: 900 });
    await openLive(page);
    await expect(openRows(page)).toHaveCount(ALERTS.open.length);
    const map = await page.locator("#map").boundingBox();
    const card = await rail(page).boundingBox();
    expect(card.y, `${width} px`).toBeGreaterThan(map.y + map.height);
    expect(Math.abs(card.width - map.width), `${width} px`).toBeLessThanOrEqual(1);
    const first = await openRows(page).nth(0).boundingBox();
    const second = await openRows(page).nth(1).boundingBox();
    if (columns === 2) {
      expect(second.x, `${width} px`).not.toBe(first.x);
      expect(Math.abs(second.y - first.y), `${width} px`).toBeLessThanOrEqual(1);
    } else {
      expect(second.x, `${width} px`).toBe(first.x);
      expect(second.y, `${width} px`).toBeGreaterThan(first.y);
    }
    await expect(peek(page)).toBeHidden();
  }
});

test("phone shows a peek bar with the open-alert count that opens the alerts sheet", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await prepare(page);
  await openLive(page);
  await expect(rail(page)).toBeHidden();
  await expect(peek(page)).toBeVisible();
  await expect(peek(page)).toHaveText(`${number.format(ALERTS.open.length)} open alerts`);
  const bar = await peek(page).boundingBox();
  expect(bar.height).toBeGreaterThanOrEqual(44);
  expect(Math.abs(bar.y + bar.height - 844)).toBeLessThanOrEqual(1);

  // At the end of the page the bar doesn't cover the footer.
  await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
  const uncovered = await page.evaluate(() => {
    const links = document.querySelectorAll(".site-footer a");
    const link = links[links.length - 1];
    const r = link.getBoundingClientRect();
    return document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2)?.closest("a") === link;
  });
  expect(uncovered).toBe(true);

  await peek(page).click();
  await expect(alertsSheet(page)).toBeVisible();
  await expect(alertsSheet(page).getByRole("heading", { name: "Alerts" })).toBeVisible();
  await expect(alertsSheet(page).locator(".alert-row")).toHaveCount(ALERTS.open.length + ORDER.resolved.length);
  await page.keyboard.press("Escape");
  await expect(alertsSheet(page)).toBeHidden();
  await expect(peek(page)).toBeFocused();
  await expect(page.locator("html")).not.toHaveClass(/is-scroll-locked/);
});

test("selecting an alert switches region, focuses the station and opens its details", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  await openLive(page, "/index.html?view=list&problems=1");
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);

  const sj = ORDER.open.find((a) => a.view === "sj");
  await row(page, sj.episode_id).click();
  await expect(page.getByRole("radio", { name: "San José" })).toBeChecked();
  await expect(page).toHaveURL(/region=sj/);
  await expect(page.getByRole("radio", { name: "Map" })).toBeChecked();
  await expect(page.locator("#map")).toHaveAttribute("data-view", "map");
  await expect(page.locator(`[data-map-svg] .marker[data-id="${sj.station_id}"]`)).toBeFocused();
  const tooltip = page.locator("[data-tooltip]");
  await expect(tooltip).toBeVisible();
  await expect(tooltip).toContainText(LIVE.stations.find((s) => s.id === sj.station_id).name);

  const sf = ORDER.open.find((a) => a.view === "sf");
  await row(page, sf.episode_id).click();
  await expect(page.getByRole("radio", { name: "San Francisco" })).toBeChecked();
  await expect(page).not.toHaveURL(/region=/);
  await expect(page.locator(`[data-map-svg] .marker[data-id="${sf.station_id}"]`)).toBeFocused();
  await expect(tooltip).toBeVisible();
  await expect(tooltip).toContainText(LIVE.stations.find((s) => s.id === sf.station_id).name);
});

test("selecting an alert on a phone closes the alerts sheet and opens the station sheet", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await prepare(page);
  await openLive(page);
  await peek(page).click();
  await expect(alertsSheet(page)).toBeVisible();

  const east = ORDER.open.find((a) => a.view === "eastbay");
  const station = LIVE.stations.find((s) => s.id === east.station_id);
  await alertsSheet(page).locator(`.alert-row[data-episode="${east.episode_id}"]`).click();
  await expect(alertsSheet(page)).toBeHidden();
  const sheet = page.locator("[data-sheet]");
  await expect(sheet).toBeVisible();
  await expect(sheet.locator("[data-sheet-title]")).toHaveText(station.name);
  await expect(page.getByRole("radio", { name: "East Bay" })).toBeChecked();
  await expect(page.locator("html")).toHaveClass(/is-scroll-locked/);

  await sheet.getByRole("button", { name: "Close station details" }).click();
  await expect(sheet).toBeHidden();
  await expect(page.locator(`[data-map-svg] .marker[data-id="${station.id}"]`)).toBeFocused();
  await expect(page.locator("html")).not.toHaveClass(/is-scroll-locked/);
});

test("alerts rail keeps focus on the same alert across a refresh", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  const served = await serveAlerts(page);
  await openLive(page);
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);

  const focused = ORDER.open[30];
  const button = row(page, focused.episode_id);
  await button.focus();
  await button.evaluate((el) => {
    el.parentElement.dataset.probe = "same-node";
  });
  await railBody(page).evaluate((el) => {
    el.scrollTop += 40;
  });
  const scrollTop = await railBody(page).evaluate((el) => el.scrollTop);
  expect(scrollTop).toBeGreaterThan(0);

  // +60 s: a new open alert earlier in the order, and another alert resolved.
  const generated = Date.parse(ALERTS.generated_at);
  const next = structuredClone(ALERTS);
  next.generated_at = new Date(generated + 60_000).toISOString();
  const added = newAlert(Date.parse(ORDER.open[0].started) - MIN);
  next.open.push(added);
  const other = ORDER.open[50];
  resolve(next, other.episode_id, generated + 30_000);
  served.doc = next;
  await page.clock.fastForward(60_000);

  await expect(row(page, added.episode_id)).toHaveCount(1);
  await expect(openRows(page).first()).toHaveAttribute("data-episode", added.episode_id);
  await expect(page.locator(`[data-alerts-resolved] .alert-row[data-episode="${other.episode_id}"]`)).toHaveCount(1);
  await expect(button).toBeFocused();
  await expect(page.locator('[data-probe="same-node"] .alert-row')).toHaveAttribute("data-episode", focused.episode_id);
  const after = await railBody(page).evaluate((el) => el.scrollTop);
  expect(Math.abs(after - scrollTop)).toBeLessThanOrEqual(1);

  // Then the focused alert itself resolves: focus stays on it, now in the resolved list.
  const third = structuredClone(next);
  third.generated_at = new Date(generated + 120_000).toISOString();
  resolve(third, focused.episode_id, generated + 90_000);
  served.doc = third;
  await page.clock.fastForward(60_000);
  const moved = page.locator(`[data-alerts-resolved] .alert-row[data-episode="${focused.episode_id}"]`);
  await expect(moved).toHaveCount(1);
  await expect(moved).toBeFocused();
  await expect(page.locator('[data-probe="same-node"]')).toHaveCount(1);
});

test("new alerts are announced politely, and nothing is announced on first load", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  const served = await serveAlerts(page);
  await openLive(page);
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);
  const announcer = page.locator("[data-alerts-announcer]");
  await expect(announcer).toHaveAttribute("aria-live", "polite");
  await expect(announcer).toHaveText("");

  const generated = Date.parse(ALERTS.generated_at);
  const one = structuredClone(ALERTS);
  one.generated_at = new Date(generated + 60_000).toISOString();
  const first = newAlert(generated + 30_000);
  one.open.push(first);
  served.doc = one;
  await page.clock.fastForward(60_000);
  await expect(row(page, first.episode_id)).toHaveCount(1);
  await expect(announcer).toHaveText(`New alert: ${first.name} is ${first.kind}.`);

  const two = structuredClone(one);
  two.generated_at = new Date(generated + 120_000).toISOString();
  const a = newAlert(generated + 90_000, [first.station_id]);
  const b = newAlert(generated + 95_000, [first.station_id, a.station_id]);
  two.open.push(a, b);
  served.doc = two;
  await page.clock.fastForward(60_000);
  await expect(openRows(page)).toHaveCount(ALERTS.open.length + 3);
  await expect(announcer).toHaveText("2 new alerts.");
});

test("alerts rail shows a note instead of alerts during replay", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page, { ageMs: PAUSED_MS, replay: true });
  await openLive(page);
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);
  await page.getByRole("button", { name: "Replay a day" }).click();
  await expect(page.locator("[data-replay-banner]")).toBeVisible();

  const note = page.locator("[data-alerts-replay-note]");
  await expect(note).toBeVisible();
  await expect(note).toHaveText("Alerts aren’t part of the replay. Exit the replay to see live alerts.");
  const visibleRows = () =>
    page.locator(".alert-row").evaluateAll((els) => els.filter((el) => el.getClientRects().length > 0).length);
  expect(await visibleRows()).toBe(0);
  await expect(page.locator('[data-alerts-count="open"]')).toBeHidden();
  await expect(railBody(page)).not.toHaveAttribute("aria-busy", "true");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(peek(page)).toBeHidden();

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.getByRole("button", { name: "Exit" }).click();
  await expect(page.locator("[data-replay-banner]")).toBeHidden();
  await expect(note).toBeHidden();
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);
  await expect(openRows(page).first()).toBeVisible();
  expect(await episodes(openRows(page))).toEqual(ORDER.open.map((a) => a.episode_id));

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(peek(page)).toBeVisible();
  await expect(peek(page)).toHaveText(`${number.format(ALERTS.open.length)} open alerts`);
});

test("alerts rail explains when alerts.json can't be read and offers Retry", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  const served = await serveAlerts(page);
  served.status = 503;
  await openLive(page);

  const message = page.locator("[data-alerts-message]");
  await expect(message).toBeVisible();
  await expect(message).toContainText("Alerts couldn’t be loaded. Check your connection, then try again.");
  const retry = rail(page).getByRole("button", { name: "Retry" });
  await expect(retry).toBeVisible();
  await expect(page.locator('[data-kpi="alerts"] [data-kpi-sub]')).toHaveText("Alerts couldn’t be loaded");
  await expect(railBody(page)).toHaveAttribute("aria-busy", "false");

  served.status = 200;
  await retry.click();
  await expect(openRows(page)).toHaveCount(ALERTS.open.length);
  await expect(message).toBeHidden();
  await expect(page.locator('[data-kpi="alerts"] [data-kpi-value]')).toHaveText(number.format(ALERTS.open.length));
});

test("alerts rail does not stay loading when alerts.json never answers", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await prepare(page);
  // alerts.json is requested but never answered: the request stays pending.
  await page.route("**/data/alerts.json", () => undefined);
  const asked = page.waitForRequest("**/data/alerts.json");
  await openLive(page);
  await asked;
  await expect(railBody(page)).toHaveAttribute("aria-busy", "true");
  await expect(page.locator("[data-alerts-message]")).toBeHidden();

  await page.clock.fastForward(10_000);
  await expect(page.locator("[data-alerts-message]")).toBeVisible();
  await expect(page.locator("[data-alerts-message]")).toContainText("Alerts couldn’t be loaded");
  await expect(rail(page).getByRole("button", { name: "Retry" })).toBeVisible();
  await expect(railBody(page)).not.toHaveAttribute("aria-busy", "true");
});

test("axe finds no violations in the alerts rail, the card and the alerts sheet", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    const check = async (label) => {
      const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
      const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
      expect(summary, `${label}, ${colorScheme}`).toEqual([]);
    };
    await page.setViewportSize({ width: 1440, height: 900 });
    await openLive(page);
    await expect(openRows(page)).toHaveCount(ALERTS.open.length);
    await check("rail at 1440 px");

    await page.setViewportSize({ width: 900, height: 900 });
    await rail(page).scrollIntoViewIfNeeded();
    await check("card at 900 px");

    await page.setViewportSize({ width: 390, height: 844 });
    await expect(peek(page)).toBeVisible();
    await check("peek bar at 390 px");
    await peek(page).click();
    await expect(alertsSheet(page)).toBeVisible();
    await check("alerts sheet at 390 px");
    await page.keyboard.press("Escape");
    await expect(alertsSheet(page)).toBeHidden();
  }
});
