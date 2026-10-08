// Shared page shell: theme, nav overlay, freshness pill and footer. Every page entry calls initShell().

import { initFreshness } from "./freshness.js";
import { initNav } from "./nav.js";
import { initTheme } from "./theme.js";

export function initShell() {
  initTheme();
  initNav();
  initFreshness();
}
