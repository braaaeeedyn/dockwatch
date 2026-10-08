// "Pipeline off" as a first-class state: the paused banner (why, since when, what next) and Replay a day.
// Every expected number is computed from the fixtures (live.json, and replay.json decoded here), never hard-coded.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { LIVE, loadFixture, prepare } from "./helpers.js";

const REPLAY = loadFixture("replay.json");
const STATES = ["empty", "low", "ok", "high", "full", "offline"];
const WORDS = { empty: "Empty", low: "Low", ok: "OK", high: "High", full: "Full", offline: "Offline" };
const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const PAUSED_MS = 10 * 60_000;
const FRAME_MS = REPLAY.step_s * 100; // step_s seconds at 10x speed

const number = new Intl.NumberFormat("en-US");
const PACIFIC = "America/Los_Angeles";
const shortClock = new Intl.DateTimeFormat("en-US", { timeZone: PACIFIC, hour: "numeric", minute: "2-digit" });
const clock = new Intl.DateTimeFormat("en-US", { timeZone: PACIFIC, hour: "numeric", minute: "2-digit", second: "2-digit" });
const monthDay = new Intl.DateTimeFormat("en-US", { timeZone: PACIFIC, month: "short", day: "numeric" });

/** DESIGN §7's replay label, e.g. "Wednesday 7 Oct", for the fixture's Pacific day. */
function dayLabel(day) {
  const date = new Date(`${day}T12:00:00Z`);
  const part = (options) => new Intl.DateTimeFormat("en-US", { timeZone: "UTC", ...options }).format(date);
  return `${part({ weekday: "long" })} ${part({ day: "numeric" })} ${part({ month: "short" })}`;
}

/** Decode the fixture up to frame k: station id -> {state, bikes, docks} for stations that can be drawn. */
function decodeFrame(k) {
  const values = new Map();
  for (let f = 0; f <= k; f += 1) {
    const d = REPLAY.frames[f].d;
    for (let j = 0; j < d.length; j += 4) {
      values.set(REPLAY.stations[d[j]].id, { state: REPLAY.states[d[j + 1]], bikes: d[j + 2], docks: d[j + 3] });
    }
  }
  return values;
}

function frameStations(k, view) {
  const values = decodeFrame(k);
  return REPLAY.stations
    .filter((s) => s.view === view && values.has(s.id))
    .map((s) => ({ ...s, ...values.get(s.id) }));
}

function countStates(stations) {
  const counts = Object.fromEntries(STATES.map((s) => [s, 0]));
  for (const s of stations) counts[s.state] += 1;
  return counts;
}

const PAUSED_TEXT = `Live data paused — showing the state at ${shortClock.format(new Date(LIVE.generated_at))}.`;
const REPLAY_TEXT = `Replay mode — showing ${dayLabel(REPLAY.day)}, 10× speed`;

const banner = (page) => page.locator("[data-stale-banner]");
const replayBanner = (page) => page.locator("[data-replay-banner]");
const replayButton = (page) => page.getByRole("button", { name: "Replay a day" });
const exitButton = (page) => page.getByRole("button", { name: "Exit" });
const markers = (page) => page.locator("[data-map-svg] .marker");

async function openPaused(page, options = {}) {
  await prepare(page, { ageMs: PAUSED_MS, replay: true, ...options });
  await page.goto("/index.html");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
  await expect(banner(page)).toBeVisible();
}

async function startReplay(page) {
  await replayButton(page).click();
  await expect(replayBanner(page)).toBeVisible();
}

async function drawnMarkers(page) {
  const drawn = await markers(page).evaluateAll((els) => els.map((el) => [el.dataset.id, el.dataset.state]));
  return new Map(drawn);
}

async function expectFrame(page, k) {
  const stations = frameStations(k, "sf").filter((s) => s.lat != null && s.lon != null);
  await expect(markers(page)).toHaveCount(stations.length);
  await expect.poll(() => drawnMarkers(page)).toEqual(new Map(stations.map((s) => [s.id, s.state])));
  const counts = countStates(frameStations(k, "sf"));
  for (const state of STATES) {
    const item = page.locator(".legend__item").filter({ has: page.locator(`[data-legend-count="${state}"]`) });
    await expect(item).toHaveText(`${WORDS[state]} · ${number.format(counts[state])}`);
  }
  await expect(page.locator("[data-replay-clock]")).toHaveText(shortClock.format(new Date(REPLAY.frames[k].t)));
}

test("stale banner says why data stopped and offers Replay a day", async ({ page }) => {
  await openPaused(page);
  const b = banner(page);
  await expect(b).toHaveAttribute("role", "status");
  await expect(b).toHaveClass(/banner--warn/);
  // DESIGN §6's sentence first, then why (the pipeline isn't running) and what happens next (it recovers by itself).
  await expect(b.locator("p").first()).toHaveText(
    new RegExp(`^${PAUSED_TEXT.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} The DockWatch pipeline isn’t running right now`),
  );
  await expect(b).toContainText("no new station data is coming in");
  await expect(b).toContainText("updates on its own when it restarts");
  await expect(b).not.toContainText(/your|you /i); // never blames the visitor
  await expect(b.getByRole("button", { name: "Replay a day" })).toBeVisible();
  await expect(b.getByRole("button", { name: "Replay a day" })).toHaveClass(/button-secondary/);
  // The pill agrees with the banner.
  await expect(page.locator(".nav [data-freshness]")).toHaveAttribute("data-state", "paused");
  await expect(replayBanner(page)).toBeHidden();
});

test("stale banner names the day when the data is from an earlier day", async ({ page }) => {
  const generated = new Date(LIVE.generated_at);
  await openPaused(page, { ageMs: 30 * 60 * 60_000 }); // 30 h later: the next Pacific day at least
  const at = `${shortClock.format(generated)} on ${monthDay.format(generated)}`;
  await expect(banner(page)).toContainText(`Live data paused — showing the state at ${at}.`);
  await expect(page.locator(".nav [data-freshness]")).toContainText(
    `Paused · Data as of ${monthDay.format(generated)}, ${clock.format(generated)}`,
  );
});

test("no Replay a day button when no replay file is published", async ({ page }) => {
  const requests = [];
  page.on("request", (request) => {
    if (request.url().includes("data/replay.json")) requests.push(request.method());
  });
  await openPaused(page, { replay: 404 });
  await expect.poll(() => requests.length).toBeGreaterThan(0); // the probe ran...
  await page.waitForLoadState("networkidle");
  await expect(replayButton(page)).toHaveCount(0); // ...and found nothing, so there's no dead button
  expect(requests).toEqual(["HEAD"]); // probed once, never downloaded
  await expect(banner(page)).toContainText(PAUSED_TEXT);
  await expect(banner(page)).toContainText("the map updates on its own when it restarts.");
});

test("Replay a day plays the archived day at 10x speed", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await openPaused(page);
  await startReplay(page);
  await expect(replayBanner(page).locator("[data-replay-status]")).toHaveText(REPLAY_TEXT);
  await expect(replayBanner(page)).not.toHaveClass(/banner--warn/); // neutral banner
  await expect(exitButton(page)).toBeFocused();
  await expect(banner(page)).toBeHidden();

  await expectFrame(page, 0);
  const all0 = countStates([...decodeFrame(0).values()]);
  await expect(page.locator('[data-kpi="empty"] [data-kpi-value]')).toHaveText(number.format(all0.empty));
  await expect(page.locator('[data-kpi="full"] [data-kpi-value]')).toHaveText(number.format(all0.full));
  const bikes0 = [...decodeFrame(0).values()].reduce((sum, v) => sum + v.bikes, 0);
  await expect(page.locator('[data-kpi="bikes"] [data-kpi-value]')).toHaveText(number.format(bikes0));
  await expect(page.locator('[data-kpi="alerts"] [data-kpi-value]')).toHaveText("—");
  await expect(page.locator('[data-kpi="alerts"] [data-kpi-sub]')).toContainText("replay");
  await expect(page.locator("[data-kpi-caption]")).toContainText(`replay of ${dayLabel(REPLAY.day)}`);

  // The fixture's frame 1 really changes something in San Francisco, so the next check is not vacuous.
  const changed = frameStations(1, "sf").filter((s) => decodeFrame(0).get(s.id)?.state !== s.state);
  expect(changed.length).toBeGreaterThan(0);

  await page.clock.fastForward(FRAME_MS);
  await expectFrame(page, 1);
  const all1 = countStates([...decodeFrame(1).values()]);
  await expect(page.locator('[data-kpi="empty"] [data-kpi-value]')).toHaveText(number.format(all1.empty));

  // The pill stays honest about live data during the replay.
  await expect(page.locator(".nav [data-freshness]")).toHaveAttribute("data-state", "paused");

  // Region switching works in replay mode too.
  await page.getByRole("radio", { name: "East Bay" }).check();
  const east = frameStations(1, "eastbay").filter((s) => s.lat != null && s.lon != null);
  await expect(markers(page)).toHaveCount(east.length);
});

test("Exit leaves replay mode and returns focus to Replay a day", async ({ page }) => {
  await openPaused(page);
  await startReplay(page);
  await page.clock.fastForward(FRAME_MS);
  await expectFrame(page, 1);

  await exitButton(page).click();
  await expect(replayBanner(page)).toBeHidden();
  await expect(banner(page)).toBeVisible();
  await expect(banner(page)).toContainText(PAUSED_TEXT);
  await expect(replayButton(page)).toBeFocused();

  const live = LIVE.stations.filter((s) => s.view === "sf");
  await expect(markers(page)).toHaveCount(live.length);
  await expect.poll(() => drawnMarkers(page)).toEqual(new Map(live.map((s) => [s.id, s.state])));
  for (const state of STATES) {
    await expect(page.locator(`[data-legend-count="${state}"]`)).toHaveText(number.format(LIVE.counts.sf[state] ?? 0));
  }
  await expect(page.locator('[data-kpi="empty"] [data-kpi-value]')).toHaveText(number.format(LIVE.counts.all.empty));
  await expect(page.locator("#map")).not.toHaveClass(/is-replay/);

  // The player stopped: time passing no longer changes the map.
  await page.clock.fastForward(FRAME_MS * 2);
  await expect.poll(() => drawnMarkers(page)).toEqual(new Map(live.map((s) => [s.id, s.state])));
});

test("replay mode does not announce every frame", async ({ page }) => {
  await openPaused(page);
  const summary = page.locator("[data-map-summary]");
  const summaryBefore = await summary.textContent();
  expect(summaryBefore).toMatch(/^San Francisco: \d+ empty/);
  await startReplay(page);
  await expectFrame(page, 0);

  const live = () =>
    page.locator('[role="status"], [aria-live]').evaluateAll((els) => els.map((el) => el.textContent));
  const before = await live();
  expect(before).toContain(REPLAY_TEXT);
  // The replay clock changes every frame, so it sits outside every live region.
  expect(
    await page.locator("[data-replay-clock]").evaluate((el) => el.closest('[role="status"], [aria-live]') === null),
  ).toBe(true);

  for (let k = 1; k <= 3; k += 1) {
    await page.clock.fastForward(FRAME_MS);
    await expectFrame(page, k); // the frame really advanced...
    expect(await live(), `frame ${k}`).toEqual(before); // ...and no live region text changed
    await expect(summary).toHaveText(summaryBefore);
  }
  // No pulses during replay: no running animations on markers.
  const running = await page.evaluate(() =>
    document.getAnimations().filter((a) => a.effect?.target?.classList?.contains("marker")).length,
  );
  expect(running).toBe(0);
});

test("axe finds no violations in the paused and replay banners", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page, { ageMs: PAUSED_MS, replay: true });
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await page.goto("/index.html");
      await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
      await expect(replayButton(page)).toBeVisible();
      const check = async (label) => {
        const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
        const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
        expect(summary, `${label} at ${width} px, ${colorScheme}`).toEqual([]);
      };
      await check("paused banner");
      await startReplay(page);
      await expectFrame(page, 0);
      await check("replay banner");
      await exitButton(page).click();
      await expect(replayBanner(page)).toBeHidden();
    }
  }
});

test("Replay a day shows a message when replay.json never answers", async ({ page }) => {
  await prepare(page, { ageMs: PAUSED_MS, replay: true });
  // The file is published (HEAD answers 200), but the GET is never answered: the request stays pending.
  await page.route("**/data/replay.json", (route) =>
    route.request().method() === "HEAD" ? route.fulfill({ status: 200, body: "" }) : undefined,
  );
  await page.goto("/index.html");
  await expect(page.locator("#map")).not.toHaveClass(/is-loading/);
  await expect(banner(page)).toBeVisible();
  // Record whether the replay banner ever shows.
  await page.evaluate(() => {
    const el = document.querySelector("[data-replay-banner]");
    window.replayBannerShown = !el.hidden;
    new MutationObserver(() => {
      if (!el.hidden) window.replayBannerShown = true;
    }).observe(el, { attributes: true });
  });

  const asked = page.waitForRequest((request) => request.url().includes("data/replay.json") && request.method() === "GET");
  await replayButton(page).click();
  await asked;
  await expect(replayButton(page)).toHaveAttribute("aria-disabled", "true");

  // The read gives up after 30 s: the message shows and the button can be pressed again.
  await page.clock.fastForward(30_000);
  await expect(banner(page).locator("[data-replay-error]")).toHaveText("The replay couldn’t be loaded. Try again later.");
  await expect(banner(page).getByText("The replay couldn’t be loaded. Try again later.")).toBeVisible();
  await expect(replayButton(page)).not.toHaveAttribute("aria-disabled");
  await expect(replayBanner(page)).toBeHidden();
  expect(await page.evaluate(() => window.replayBannerShown)).toBe(false);
});
