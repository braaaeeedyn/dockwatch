// Touch targets (DESIGN.md §8, v1.0.1): on a coarse pointer, the List view's sort buttons and station-name buttons
// are hit areas of at least 44 x 44 px. Checked by hit-testing points around each button, not only by its box.
import { expect, test } from "@playwright/test";
import { prepare } from "./helpers.js";

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
