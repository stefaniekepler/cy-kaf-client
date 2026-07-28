
import path from "node:path";
import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  outputDir: "../test-results/playwright",
  use: {
    baseURL: "http://127.0.0.1:4176",
    headless: true,
    screenshot: "off",
    trace: "off",
    video: "off",
  },
  webServer: {
    command:
      "pnpm --dir frontend exec vite --mode development --host 127.0.0.1 --port 4176 --strictPort --no-open",
    cwd: path.resolve(__dirname, "../.."),
    url: "http://127.0.0.1:4176",
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
