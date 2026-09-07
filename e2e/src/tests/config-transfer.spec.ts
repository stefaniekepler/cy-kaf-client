import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";

const yaml = "kafka:\n  clusters:\n    - name: team\n      bootstrapServers: broker.invalid:9092\n";

for (const theme of ["light_theme", "dark_theme"]) {
  test(`configuration export and selected conflict import (${theme})`, async({ page }) => {
    test.setTimeout(60_000);
    page.on("pageerror", (error) => console.error("fixture page error:", error.message));
    await page.setViewportSize({ width: 1024, height: 700 });
    await page.addInitScript((mode) => localStorage.setItem("mode", mode), theme);
    const submitted: unknown[] = [];
    const unexpected: string[] = [];
    await page.route("**/__desktop/**", (route) => route.fulfill({ status: 404, body: "{}", contentType: "application/json" }));
    await page.route("http://127.0.0.1:4176/api/**", async(route) => {
      const path = new URL(route.request().url()).pathname;
      const fixtures: Record<string, unknown> = {
        "/api/info": { enabledFeatures: ["DYNAMIC_CONFIG"] },
        "/api/authorization": { rbacEnabled: false, userInfo: { username: "fixture", permissions: [] } },
        "/api/clusters": [],
        "/api/config": { properties: { kafka: { clusters: [] } } },
      };
      let body: unknown = fixtures[path];
      if (path === "/api/config/export") {
        await route.fulfill({ contentType: "application/yaml", body: yaml });
        return;
      }
      if (path === "/api/config/import/preview") {
        body = { revision: "fixture-revision", entries: [
          { index: 0, name: "team", bootstrapServers: "broker.invalid:9092", conflicts: [{ name: "local-team", bootstrapServers: "broker.invalid:9092", reason: "address" }], overlaps: [] },
          { index: 1, name: "new-environment", bootstrapServers: "new.invalid:9092", conflicts: [], overlaps: [] },
        ] };
      }
      if (path === "/api/config/import") {
        submitted.push(route.request().postDataJSON());
        body = { added: 1, replaced: 1, skipped: 0 };
      }
      if (body === undefined) { unexpected.push(path); await route.abort(); return; }
      await route.fulfill({ contentType: "application/json", body: JSON.stringify(body) });
    });
    await page.goto("/ui/clusters");
    await page.getByRole("button", { name: "Settings", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Settings", exact: true });
    const downloadPromise = page.waitForEvent("download");
    await dialog.getByRole("button", { name: "一键导出配置" }).click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toMatch(/^kafka-environments-.*\.yaml$/);
    expect(await readFile((await download.path())!, "utf8")).toBe(yaml);
    await dialog.getByLabel("选择 Kafka 配置文件").setInputFiles({ name: "team.yaml", mimeType: "application/yaml", buffer: Buffer.from(yaml) });
    await expect(dialog.getByRole("checkbox", { name: "导入 team", exact: true })).not.toBeChecked();
    await expect(dialog.getByRole("checkbox", { name: "导入 new-environment", exact: true })).toBeChecked();
    expect(submitted).toEqual([]);
    await dialog.getByRole("checkbox", { name: "导入 team", exact: true }).check();
    await dialog.getByRole("button", { name: "确认导入（2）" }).click();
    await expect(dialog.getByRole("status")).toHaveText("新增 1 个，替换 1 个本地环境，跳过 0 个");
    expect(submitted).toEqual([{ content: yaml, selected: [0, 1], revision: "fixture-revision" }]);
    expect(await dialog.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
    expect(unexpected).toEqual([]);
  });
}
