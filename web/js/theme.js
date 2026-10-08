// Theme: follows prefers-color-scheme until the visitor picks one with the toggle; the pick is remembered.
// The inline script in each page's <head> applies the stored pick before first paint (no flash).

const STORAGE_KEY = "dockwatch-theme";
const darkQuery = window.matchMedia("(prefers-color-scheme: dark)");

export function storedTheme() {
  try {
    const value = window.localStorage.getItem(STORAGE_KEY);
    return value === "light" || value === "dark" ? value : null;
  } catch {
    return null; // storage blocked (privacy mode, sandboxed iframe)
  }
}

function storeTheme(theme) {
  try {
    window.localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // Not remembered, but the switch still works for this page view.
  }
}

/** The theme in effect right now: the stored pick, else the system preference. */
export function effectiveTheme() {
  return document.documentElement.dataset.theme || (darkQuery.matches ? "dark" : "light");
}

function syncControls() {
  const next = effectiveTheme() === "dark" ? "light" : "dark";
  const label = `Switch to ${next} theme`;
  for (const button of document.querySelectorAll("[data-theme-toggle]")) {
    const text = button.querySelector("[data-theme-toggle-label]");
    if (text) text.textContent = label;
    else button.setAttribute("aria-label", label);
  }
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute("content", getComputedStyle(document.documentElement).getPropertyValue("--canvas").trim());
}

export function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  storeTheme(theme);
  syncControls();
}

export function initTheme() {
  const stored = storedTheme();
  if (stored) document.documentElement.dataset.theme = stored;
  for (const button of document.querySelectorAll("[data-theme-toggle]")) {
    button.addEventListener("click", () => setTheme(effectiveTheme() === "dark" ? "light" : "dark"));
  }
  darkQuery.addEventListener("change", syncControls);
  syncControls();
}
