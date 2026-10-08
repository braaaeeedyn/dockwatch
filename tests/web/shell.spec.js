// F1 shell: layout at every width, accessibility, skip link, theme, nav overlay, freshness pill.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { PAGES, prepare } from "./helpers.js";

const WIDTHS = [320, 390, 768, 1024, 1440, 2560];
const AXE_WIDTHS = [320, 768, 1440];
const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

async function open(page, path) {
  await page.goto(path);
  // The pill leaves its "loading" state once live.json has been read.
  await expect(page.locator(".nav [data-freshness]")).not.toHaveAttribute("data-state", "loading");
}

test("no horizontal scroll from 320 to 2560 px on every page", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  for (const { path } of PAGES) {
    for (const width of WIDTHS) {
      await page.setViewportSize({ width, height: 900 });
      await open(page, path);
      const overflow = await page.evaluate(() => ({
        scroll: document.documentElement.scrollWidth,
        client: document.documentElement.clientWidth,
      }));
      expect(overflow.scroll, `${path} at ${width} px`).toBeLessThanOrEqual(overflow.client);
    }
  }
});

test("axe finds no violations at 320, 768 and 1440 px in light and dark", async ({ page }) => {
  test.setTimeout(180_000);
  await prepare(page);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    for (const { path } of PAGES) {
      for (const width of AXE_WIDTHS) {
        await page.setViewportSize({ width, height: 900 });
        await open(page, path);
        const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
        const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
        expect(summary, `${path} at ${width} px, ${colorScheme}`).toEqual([]);
      }
    }
  }
});

test("axe finds no violations in the open nav menu", async ({ page }) => {
  await prepare(page);
  await page.setViewportSize({ width: 390, height: 844 });
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    await open(page, "/index.html");
    await page.getByRole("button", { name: "Open menu" }).click();
    await expect(page.locator("#nav-menu")).toBeVisible();
    const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
    expect(results.violations.map((v) => v.id), colorScheme).toEqual([]);
  }
});

test("skip link is the first focusable element", async ({ page }) => {
  await prepare(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  for (const { path, skip, target } of PAGES) {
    await open(page, path);
    await page.keyboard.press("Tab");
    const focused = page.locator(":focus");
    await expect(focused).toHaveText(skip);
    await expect(focused).toHaveClass(/skip-link/);
    await expect(focused).toBeInViewport();
    await page.keyboard.press("Enter");
    await expect(page.locator(":focus")).toHaveAttribute("id", target);
  }
});

test("theme toggle switches theme and is remembered", async ({ page }) => {
  await prepare(page);
  await page.emulateMedia({ colorScheme: "light" });
  await page.setViewportSize({ width: 1440, height: 900 });
  await open(page, "/index.html");

  const html = page.locator("html");
  const canvas = () => page.evaluate(() => getComputedStyle(document.body).backgroundColor);
  expect(await canvas()).toBe("rgb(255, 255, 255)");

  const toggle = page.locator(".nav [data-theme-toggle]");
  await expect(toggle).toHaveAttribute("aria-label", "Switch to dark theme");
  await toggle.click();
  await expect(html).toHaveAttribute("data-theme", "dark");
  expect(await canvas()).toBe("rgb(14, 17, 22)");
  await expect(toggle).toHaveAttribute("aria-label", "Switch to light theme");

  // Remembered across reloads and pages, even though the system still prefers light.
  await page.reload();
  await expect(html).toHaveAttribute("data-theme", "dark");
  await open(page, "/about.html");
  await expect(html).toHaveAttribute("data-theme", "dark");
  expect(await canvas()).toBe("rgb(14, 17, 22)");

  // And back again.
  await page.locator(".nav [data-theme-toggle]").click();
  await expect(html).toHaveAttribute("data-theme", "light");
  await page.reload();
  await expect(html).toHaveAttribute("data-theme", "light");
  expect(await canvas()).toBe("rgb(255, 255, 255)");
});

test("theme follows the system preference until one is picked", async ({ page }) => {
  await prepare(page);
  await page.emulateMedia({ colorScheme: "dark" });
  await open(page, "/index.html");
  await expect(page.locator("html")).not.toHaveAttribute("data-theme", /.+/);
  expect(await page.evaluate(() => getComputedStyle(document.body).backgroundColor)).toBe("rgb(14, 17, 22)");
});

test("nav menu opens below 1120 px, traps focus and closes with Escape", async ({ page }) => {
  await prepare(page);
  await page.emulateMedia({ reducedMotion: "reduce" });

  // Desktop: links inline, no menu button.
  await page.setViewportSize({ width: 1440, height: 900 });
  await open(page, "/index.html");
  await expect(page.locator(".nav__links")).toBeVisible();
  await expect(page.getByRole("button", { name: "Open menu" })).toBeHidden();

  for (const width of [390, 768, 1024]) {
    await page.setViewportSize({ width, height: 900 });
    await open(page, "/insights.html");
    await expect(page.locator(".nav__links")).toBeHidden();
    const menuButton = page.getByRole("button", { name: "Open menu" });
    await expect(menuButton).toBeVisible();
    await expect(menuButton).toHaveAttribute("aria-expanded", "false");

    await menuButton.click();
    const menu = page.locator("#nav-menu");
    await expect(menu).toBeVisible();
    await expect(menuButton).toHaveAttribute("aria-expanded", "true");
    await expect(menu.getByRole("link", { name: "Pipeline" })).toBeVisible();
    expect(await page.evaluate(() => getComputedStyle(document.documentElement).overflow)).toBe("hidden");

    const focusInMenu = () => page.evaluate(() => document.getElementById("nav-menu").contains(document.activeElement));
    expect(await focusInMenu(), `focus moves into the menu at ${width} px`).toBe(true);
    for (let i = 0; i < 10; i += 1) {
      await page.keyboard.press("Tab");
      expect(await focusInMenu(), `Tab ${i + 1} at ${width} px`).toBe(true);
    }
    for (let i = 0; i < 10; i += 1) {
      await page.keyboard.press("Shift+Tab");
      expect(await focusInMenu(), `Shift+Tab ${i + 1} at ${width} px`).toBe(true);
    }

    await page.keyboard.press("Escape");
    await expect(menu).toBeHidden();
    await expect(menuButton).toBeFocused();
    await expect(menuButton).toHaveAttribute("aria-expanded", "false");
    expect(await page.evaluate(() => getComputedStyle(document.documentElement).overflow)).not.toBe("hidden");
  }
});

test("freshness pill says Live, Delayed or Paused", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const pill = page.locator(".nav [data-freshness]");

  await prepare(page, { ageMs: 30_000 });
  await page.goto("/index.html");
  await expect(pill).toHaveAttribute("data-state", "live");
  await expect(pill).toContainText("Live");
  await expect(pill).toContainText("Data as of");

  // Time passes with no new export: the pill turns Delayed, then Paused, without a reload.
  await page.clock.fastForward(3 * 60_000);
  await expect(pill).toHaveAttribute("data-state", "delayed");
  await expect(pill).toContainText("Delayed");
  await page.clock.fastForward(4 * 60_000);
  await expect(pill).toHaveAttribute("data-state", "paused");
  await expect(pill).toContainText("Paused");
});

test("freshness pill reads Paused when data is old on load, and the footer shows the data date", async ({ page }) => {
  await prepare(page, { ageMs: 10 * 60_000 });
  await page.goto("/pipeline.html");
  await expect(page.locator(".nav [data-freshness]")).toHaveAttribute("data-state", "paused");
  await expect(page.locator("[data-data-through]")).toContainText(/Data through \w{3} \d{1,2}, \d{4}/);
  await expect(page.locator("footer")).toContainText("Not affiliated with Lyft or Bay Wheels");
});
