// Playwright config for the static site in web/ (no build step). Run: npm ci && npx playwright test
import { defineConfig, devices } from "@playwright/test";

const PORT = 5179; // 4173 and 8765 are taken on the dev host

export default defineConfig({
  testDir: "tests/web",
  fullyParallel: true,
  // Few workers: the host is memory-tight (Spark, Kafka). See tests/web/serve.py for why it isn't plain http.server.
  workers: 2,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "line" : "list",
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `python tests/web/serve.py ${PORT} web`,
    url: `http://127.0.0.1:${PORT}/index.html`,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
  },
});
