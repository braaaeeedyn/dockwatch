// F5 launch checks (DESIGN.md §9) that need no deployed site: full-page screenshots of the four pages at six widths in
// both themes (saved under the test's output dir, no pixel comparison: pixels differ by OS), keyboard-only reading
// order, and reduced motion. Data: the fixtures through prepare() and a fake clock.
import { expect, test } from "@playwright/test";
import { prepare } from "./helpers.js";

const PAGES = [
  { name: "live", path: "/index.html", ready: "#map:not(.is-loading)" },
  { name: "insights", path: "/insights.html", ready: '[data-chart="scatter"].is-ready' },
  { name: "pipeline", path: "/pipeline.html", ready: '[data-chart="compaction"].is-ready' },
  { name: "about", path: "/about.html", ready: '.nav [data-freshness]:not([data-state="loading"])' },
];
const WIDTHS = [320, 390, 768, 1024, 1440, 2560];

async function open(page, { path, ready }) {
  await page.goto(path);
  await expect(page.locator(ready)).toHaveCount(1);
  await expect(page.locator(".nav [data-freshness]")).not.toHaveAttribute("data-state", "loading");
}

test("screenshots of every page at 320, 390, 768, 1024, 1440 and 2560 px in light and dark", async ({ page }, testInfo) => {
  test.setTimeout(180_000);
  await prepare(page);
  for (const theme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme: theme, reducedMotion: "reduce" });
    for (const width of WIDTHS) {
      await page.setViewportSize({ width, height: 900 });
      for (const p of PAGES) {
        await open(page, p);
        const scroll = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
        expect(scroll, `${p.name} at ${width} px, ${theme}: horizontal scroll`).toBeLessThanOrEqual(0);
        await page.screenshot({ path: testInfo.outputPath(`${p.name}-${width}-${theme}.png`), fullPage: true });
      }
    }
  }
});

/**
 * Runs in the page: the elements Tab should reach, in DOM (reading) order. Visible, not disabled, tabindex >= 0, not
 * inside a closed dialog; of a radio group only its checked radio (or the first, when none is checked).
 */
function tabStops() {
  const selector = 'a[href], button, input, select, textarea, summary, [tabindex]';
  const seen = new Set();
  const stops = [];
  for (const el of document.querySelectorAll(selector)) {
    if (el.tabIndex < 0 || el.disabled || el.closest("[inert], dialog:not([open]), [hidden]")) continue;
    if (!el.checkVisibility({ checkVisibilityCSS: true }) || el.getClientRects().length === 0) continue;
    if (el.matches('input[type="radio"]')) {
      if (seen.has(el.name)) continue;
      const group = [...document.querySelectorAll(`input[type="radio"][name="${el.name}"]`)];
      const pick = group.find((r) => r.checked) ?? group[0];
      seen.add(el.name);
      stops.push(pick);
      continue;
    }
    stops.push(el);
  }
  return stops.map((el, i) => {
    el.dataset.tabStop = String(i);
    return i;
  }).length;
}

test("keyboard only: Tab reaches every control on every page in reading order", async ({ page }) => {
  test.setTimeout(180_000);
  await prepare(page);
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.setViewportSize({ width: 1440, height: 900 });
  for (const p of PAGES) {
    await open(page, p);
    const count = await page.evaluate(tabStops);
    expect(count, `${p.name}: tab stops`).toBeGreaterThan(5);
    const order = [];
    for (let i = 0; i < count; i += 1) {
      await page.keyboard.press("Tab");
      order.push(await page.evaluate(() => document.activeElement?.dataset.tabStop ?? `?${document.activeElement?.tagName}`));
    }
    expect(order, `${p.name}: Tab order`).toEqual([...Array(count).keys()].map(String));
  }

  // Overlays trap focus and give it back: the nav menu at 390 px.
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page, PAGES[1]);
  const menuButton = page.getByRole("button", { name: "Open menu" });
  await menuButton.focus();
  await page.keyboard.press("Enter");
  await expect(page.locator("#nav-menu")).toBeVisible();
  for (let i = 0; i < 8; i += 1) {
    await page.keyboard.press("Tab");
    expect(await page.evaluate(() => document.getElementById("nav-menu").contains(document.activeElement))).toBe(true);
  }
  await page.keyboard.press("Escape");
  await expect(menuButton).toBeFocused();
});

test("reduced motion: nothing animates on any page", async ({ page }) => {
  await prepare(page);
  await page.emulateMedia({ reducedMotion: "reduce" });
  // Reduced motion shortens every transition to 0.01 ms (base.css); one can still be caught mid-frame, so only
  // animations that last longer than 1 ms count as motion.
  const running = () =>
    page.evaluate(() =>
      document
        .getAnimations()
        .filter((a) => a.playState === "running" && a.effect.getComputedTiming().activeDuration > 1)
        .map((a) => `${a.animationName ?? a.transitionProperty} on ${a.effect.target?.className?.baseVal ?? a.effect.target?.className}`),
    );
  for (const p of PAGES) {
    await open(page, p);
    expect(await running(), `${p.name} after load`).toEqual([]);
    // The next 60 s refresh of live.json (new markers, pill, alerts) starts nothing either.
    await page.clock.fastForward(60_000);
    await expect(page.locator(".nav [data-freshness]")).not.toHaveAttribute("data-state", "loading");
    expect(await running(), `${p.name} after a refresh`).toEqual([]);
  }
});
