// Loop P (D7): the Live page's title row keeps one height when the freshness pill's text changes from "Loading…" to
// Live or Paused, at every width. Before the fix the row wrapped on phones when the text grew (Lighthouse CLS 0.23).
// Each width loads the page with live.json held, reads the row under "Loading…", releases live.json and reads it again.
import { expect, test } from "@playwright/test";
import { LIVE, prepare } from "./helpers.js";

const WIDTHS = [320, 360, 390, 412, 480, 540, 600, 640, 660, 700, 740, 768, 820, 900, 1024, 1119, 1120, 1280, 1440, 1920, 2560];
// The longest pill texts: a two-digit day and hour on the Pacific day before the page's clock, so the date shows in
// both states ("Live · Data as of Oct 17, 11:59:45 PM", "Paused · Data as of Oct 17, 11:59:45 PM").
const LONG = { ...LIVE, generated_at: "2026-10-18T06:59:45Z", data_as_of: "2026-10-18T06:54:28Z" };
const SHOWN = "Oct 17, 11:59:45 PM";

async function sweep(page, ageMs, state) {
  test.setTimeout(150_000);
  await prepare(page, { live: LONG, ageMs });
  // Date.now() stays put across the 21 loads (timers still run), so the state cannot drift from Live to Delayed.
  await page.clock.setFixedTime(Date.parse(LONG.generated_at) + ageMs);
  const row = page.locator("main .page-header__title-row");
  const pill = row.locator("[data-freshness]");
  for (const width of WIDTHS) {
    await page.setViewportSize({ width, height: 900 });
    let release;
    const held = new Promise((resolve) => (release = resolve));
    await page.unroute("**/data/live.json");
    await page.route("**/data/live.json", async (route) => {
      await held;
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(LONG) });
    });
    await page.goto("/index.html");
    await page.evaluate(() => document.fonts.ready.then(() => true));
    await expect(pill, `${width} px: the pill still says Loading…`).toHaveAttribute("data-state", "loading");
    const loading = (await row.boundingBox()).height;
    release();
    await expect(pill).toHaveAttribute("data-state", state);
    await expect(pill).toContainText(SHOWN);
    await page.evaluate(() => document.fonts.ready.then(() => true));
    const loaded = (await row.boundingBox()).height;
    expect(Math.abs(loaded - loading), `${width} px: title row ${loading} px under Loading…, ${loaded} px ${state}`).toBeLessThan(0.5);
  }
}

test("Live page title row keeps its height when live data arrives, 320 to 2560 px", async ({ page }) => {
  await sweep(page, 30_000, "live");
});

test("Live page title row keeps its height when paused data arrives, 320 to 2560 px", async ({ page }) => {
  await sweep(page, 6 * 60 * 60_000, "paused");
});
