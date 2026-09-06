import { performance } from "node:perf_hooks";
import { expect, Page, Route, test } from "@playwright/test";

const CLUSTER_NAME = "reminder-cluster";
const FRONTEND_URL = "http://127.0.0.1:4176";
const BROKERS_PATH = `/ui/clusters/${CLUSTER_NAME}/brokers`;
// Unmount the SPA before teardown without invalidating seeded localStorage.
const STATIC_DOCUMENT_URL = `${FRONTEND_URL}/robots.txt`;
const API_QUIET_WINDOW_MS = 100;
const CLOCK_PAUSE_MARGIN_MS = 1_000;
const REMINDER_TITLE = "返回全部集群";
const REMINDER_BODY =
  "点击左侧 All clusters，可返回并添加或管理集群。";
const API_FIXTURES: Record<string, unknown> = {
  "/api/info": { enabledFeatures: [] },
  "/api/authorization": {
    rbacEnabled: false,
    userInfo: { username: "reminder-layout-test", permissions: [] },
  },
  "/api/clusters": [
    {
      name: CLUSTER_NAME,
      defaultCluster: true,
      status: "ONLINE",
      brokerCount: 0,
      onlinePartitionCount: 0,
      topicCount: 0,
      bytesInPerSec: 0,
      bytesOutPerSec: 0,
      readOnly: true,
      version: "3.7.0",
      features: [],
      controller: "KRAFT",
    },
  ],
  [`/api/clusters/${CLUSTER_NAME}/stats`]: {
    brokerCount: 0,
    activeControllers: 0,
    onlinePartitionCount: 0,
    offlinePartitionCount: 0,
    inSyncReplicasCount: 0,
    outOfSyncReplicasCount: 0,
    underReplicatedPartitionCount: 0,
    diskUsage: [],
    version: "3.7.0",
  },
  [`/api/clusters/${CLUSTER_NAME}/brokers`]: [],
};

const fulfillJson = (route: Route, body: unknown) =>
  route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(body),
  });

async function installReadOnlyApiFixtures(
  page: Page,
  fixtures: Record<string, unknown> = API_FIXTURES
) {
  const unexpectedRequests: string[] = [];
  let pendingRequests = 0;
  let activityVersion = 0;
  let latestActivityAt = performance.now();

  const recordActivity = () => {
    activityVersion += 1;
    latestActivityAt = performance.now();
  };

  await page.route(`${FRONTEND_URL}/api/**`, async(route) => {
    const request = route.request();
    const url = new URL(request.url());
    const requestPath = `${url.pathname}${url.search}`;
    const requestLabel = `${request.method()} ${requestPath}`;

    pendingRequests += 1;
    recordActivity();
    try {
      if (request.method() !== "GET") {
        unexpectedRequests.push(requestLabel);
        await route.abort("blockedbyclient");
        return;
      }

      const fixture = fixtures[requestPath];
      if (fixture !== undefined) {
        await fulfillJson(route, fixture);
        return;
      }

      unexpectedRequests.push(requestLabel);
      await route.fulfill({
        status: 404,
        contentType: "application/json",
        body: JSON.stringify({ message: "No fixture for read-only request" }),
      });
    } finally {
      pendingRequests -= 1;
      recordActivity();
    }
  });

  const waitForQuiet = async() => {
    let observedVersion = activityVersion;
    let quietSince = Math.max(performance.now(), latestActivityAt);

    await expect
      .poll(
        () => {
          const now = performance.now();

          if (pendingRequests !== 0) {
            observedVersion = activityVersion;
            quietSince = now;
            return false;
          }

          if (observedVersion !== activityVersion) {
            observedVersion = activityVersion;
            quietSince = Math.max(now, latestActivityAt);
            return false;
          }

          return now - Math.max(quietSince, latestActivityAt) >=
            API_QUIET_WINDOW_MS;
        },
        {
          intervals: [20],
          message: "intercepted API requests did not reach a quiet state",
          timeout: 5_000,
        }
      )
      .toBe(true);
  };

  return { unexpectedRequests, waitForQuiet };
}

const overflowClusterFixtures = Array.from({ length: 36 }, (_, index) => ({
  name: `overflow-cluster-${String(index + 1).padStart(2, "0")}`,
  defaultCluster: false,
  status: "ONLINE",
  brokerCount: 0,
  onlinePartitionCount: 0,
  topicCount: 0,
  bytesInPerSec: 0,
  bytesOutPerSec: 0,
  readOnly: true,
  version: "3.7.0",
  features: [],
  controller: "KRAFT",
}));
const overflowClusters: Array<Record<string, unknown>> = [
  ...overflowClusterFixtures.slice(0, 18),
  ...(API_FIXTURES["/api/clusters"] as Array<Record<string, unknown>>),
  ...overflowClusterFixtures.slice(18),
];

async function seedTheme(page: Page, mode: "light_theme" | "dark_theme") {
  await page.addInitScript((themeMode) => {
    localStorage.setItem("mode", themeMode);
  }, mode);
}

const allClustersItem = (page: Page) =>
  page
    .getByRole("link", { name: "All clusters", exact: true })
    .getByRole("menuitem");

const reminder = (page: Page) =>
  page.getByRole("status").filter({ hasText: REMINDER_BODY });

async function clickClusterRow(page: Page) {
  const clusterRow = page.getByRole("row").filter({ hasText: CLUSTER_NAME });

  await expect(clusterRow).toHaveCount(1);
  await clusterRow.click();
  await expect(page).toHaveURL(`${FRONTEND_URL}${BROKERS_PATH}`);
}

async function assertReminderContent(page: Page) {
  const allClustersReminder = reminder(page);

  await expect(allClustersReminder).toBeVisible();
  await expect(
    allClustersReminder.getByText(REMINDER_BODY, { exact: true })
  ).toBeVisible();
  await expect(
    allClustersReminder.getByText(REMINDER_TITLE, { exact: true })
  ).toBeVisible();
}

for (const theme of [
  {
    mode: "light_theme" as const,
    activeBackground: "rgba(11, 13, 14, 0.05)",
    inactiveBackground: "rgba(11, 13, 14, 0.03)",
    reminderBackground: "rgb(249, 250, 250)",
    reminderBorder: "rgb(227, 230, 232)",
    reminderColor: "rgb(92, 105, 112)",
    reminderTitleColor: "rgb(69, 79, 84)",
    reminderShadow: "rgba(0, 0, 0, 0.1) 0px 2px 8px 0px",
  },
  {
    mode: "dark_theme" as const,
    activeBackground: "rgba(255, 255, 255, 0.1)",
    inactiveBackground: "rgba(255, 255, 255, 0.05)",
    reminderBackground: "rgb(34, 40, 42)",
    reminderBorder: "rgb(57, 66, 70)",
    reminderColor: "rgb(171, 181, 186)",
    reminderTitleColor: "rgb(199, 206, 209)",
    reminderShadow: "rgba(0, 0, 0, 0.1) 0px 2px 8px 0px",
  },
]) {
  test(`anchors the reminder beside All clusters in ${theme.mode}`, async({
    page,
  }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    const apiAudit = await installReadOnlyApiFixtures(page);
    await seedTheme(page, theme.mode);

    await page.goto("/");

    const item = allClustersItem(page);
    expect(await page.getByText("Overview", { exact: true }).count()).toBe(0);
    await expect(item).toBeVisible();
    const activeBackground = await item.evaluate(
      (element) => getComputedStyle(element).backgroundColor
    );
    expect(activeBackground).toBe(theme.activeBackground);

    await clickClusterRow(page);
    await assertReminderContent(page);
    const reminderAppearance = await reminder(page).evaluate((element) => {
      const styles = getComputedStyle(element);
      return {
        background: styles.backgroundColor,
        border: styles.borderColor,
        shadow: styles.boxShadow,
        text: styles.color,
      };
    });
    expect(reminderAppearance).toEqual({
      background: theme.reminderBackground,
      border: theme.reminderBorder,
      shadow: theme.reminderShadow,
      text: theme.reminderColor,
    });
    const reminderTitleColor = await reminder(page)
      .getByText(REMINDER_TITLE, { exact: true })
      .evaluate((element) => getComputedStyle(element).color);
    expect(reminderTitleColor).toBe(theme.reminderTitleColor);
    const inactiveBackground = await item.evaluate(
      (element) => getComputedStyle(element).backgroundColor
    );
    expect(inactiveBackground).toBe(theme.inactiveBackground);

    const itemBox = await item.boundingBox();
    const reminderBox = await reminder(page).boundingBox();
    expect(itemBox).not.toBeNull();
    expect(reminderBox).not.toBeNull();
    if (!itemBox || !reminderBox) {
      throw new Error("All clusters item and reminder must have measurable bounds");
    }

    expect(reminderBox.x).toBeGreaterThan(itemBox.x + itemBox.width);
    expect(
      reminderBox.y < itemBox.y + itemBox.height &&
        reminderBox.y + reminderBox.height > itemBox.y
    ).toBe(true);

    await apiAudit.waitForQuiet();
    await page.goto(STATIC_DOCUMENT_URL);
    await apiAudit.waitForQuiet();
    expect(pageErrors).toEqual([]);
    expect(apiAudit.unexpectedRequests).toEqual([]);
  });
}

test("dismisses and rearms the reminder across its route lifecycle", async({
  page,
}) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  const apiAudit = await installReadOnlyApiFixtures(page);
  await seedTheme(page, "light_theme");
  await page.clock.install();

  await page.goto("/");
  await clickClusterRow(page);
  await assertReminderContent(page);

  await allClustersItem(page).click();
  await expect(page).toHaveURL(`${FRONTEND_URL}/`);
  await expect(reminder(page)).toHaveCount(0);

  await clickClusterRow(page);
  await assertReminderContent(page);
  await page.clock.fastForward(4000);
  await expect(reminder(page)).toHaveAttribute("data-state", "leaving");
  await page.clock.fastForward(180);
  await expect(reminder(page)).toBeHidden();

  await allClustersItem(page).click();
  await expect(page).toHaveURL(`${FRONTEND_URL}/`);
  await clickClusterRow(page);
  await assertReminderContent(page);

  await apiAudit.waitForQuiet();
  await page.goto(STATIC_DOCUMENT_URL);
  await apiAudit.waitForQuiet();
  expect(pageErrors).toEqual([]);
  expect(apiAudit.unexpectedRequests).toEqual([]);
});

test.describe("overflowing sidebar", () => {
  test.use({ viewport: { width: 1280, height: 560 } });

  test("keeps global navigation and the active cluster fully separated", async({
    page,
  }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    const apiAudit = await installReadOnlyApiFixtures(page, {
      ...API_FIXTURES,
      "/api/clusters": overflowClusters,
    });
    await seedTheme(page, "light_theme");

    await page.goto("/");
    await clickClusterRow(page);
    await assertReminderContent(page);

    const sidebarBox = await page
      .getByLabel("Sidebar", { exact: true })
      .boundingBox();
    const itemBox = await allClustersItem(page).boundingBox();
    const reminderBox = await reminder(page).boundingBox();
    const sidebarMenu = page.getByLabel("Sidebar Menu", { exact: true });
    const activeClusterTitleBox = await sidebarMenu
      .getByText(CLUSTER_NAME, { exact: true })
      .boundingBox();
    const activeClusterMenuBox = await sidebarMenu
      .getByRole("menu")
      .filter({ has: page.getByText(CLUSTER_NAME, { exact: true }) })
      .boundingBox();
    expect(sidebarBox).not.toBeNull();
    expect(itemBox).not.toBeNull();
    expect(reminderBox).not.toBeNull();
    expect(activeClusterTitleBox).not.toBeNull();
    expect(activeClusterMenuBox).not.toBeNull();
    if (
      !sidebarBox ||
      !itemBox ||
      !reminderBox ||
      !activeClusterTitleBox ||
      !activeClusterMenuBox
    ) {
      throw new Error(
        "Sidebar, global navigation, reminder, and active cluster must have measurable bounds"
      );
    }

    expect(itemBox.y).toBeGreaterThanOrEqual(sidebarBox.y);
    expect(itemBox.y + itemBox.height).toBeLessThanOrEqual(
      sidebarBox.y + sidebarBox.height
    );
    expect(reminderBox.x).toBeGreaterThan(itemBox.x + itemBox.width);
    expect(
      reminderBox.y < itemBox.y + itemBox.height &&
        reminderBox.y + reminderBox.height > itemBox.y
    ).toBe(true);
    expect(activeClusterTitleBox.y).toBeGreaterThanOrEqual(
      itemBox.y + itemBox.height
    );
    expect(
      activeClusterMenuBox.y + activeClusterMenuBox.height
    ).toBeLessThanOrEqual(sidebarBox.y + sidebarBox.height);

    await apiAudit.waitForQuiet();
    await page.goto(STATIC_DOCUMENT_URL);
    await apiAudit.waitForQuiet();
    expect(pageErrors).toEqual([]);
    expect(apiAudit.unexpectedRequests).toEqual([]);
  });
});

test.describe("1024x700", () => {
  test.use({ viewport: { width: 1024, height: 700 } });

  test("waits for the narrow sidebar before showing the reminder", async({
    page,
  }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    const apiAudit = await installReadOnlyApiFixtures(page);
    await seedTheme(page, "light_theme");
    await page.clock.install();

    await page.goto("/");
    await clickClusterRow(page);
    const sidebar = page.getByLabel("Sidebar", { exact: true });
    await expect(sidebar).not.toBeInViewport();
    await expect(reminder(page)).toHaveCount(0);

    await page.clock.fastForward(5000);
    await expect(sidebar).not.toBeInViewport();
    await expect(reminder(page)).toHaveCount(0);
    const pauseTime = await page.evaluate(
      (pauseMarginMs) => Date.now() + pauseMarginMs,
      CLOCK_PAUSE_MARGIN_MS
    );
    await page.clock.pauseAt(pauseTime);

    const pageHeader = page.getByRole("navigation", { name: "Page Header" });
    await pageHeader
      .getByRole("button", { name: "Toggle sidebar", exact: true })
      .click();
    await expect(sidebar).toBeInViewport({ ratio: 0.99 });
    await expect(allClustersItem(page)).toBeVisible();
    await expect(reminder(page)).toHaveAttribute("data-state", "visible");

    await page.clock.fastForward(3999);
    await expect(reminder(page)).toHaveAttribute("data-state", "visible");
    await page.clock.fastForward(1);
    await expect(reminder(page)).toHaveAttribute("data-state", "leaving");
    await page.clock.fastForward(180);
    await expect(reminder(page)).toHaveCount(0);

    await apiAudit.waitForQuiet();
    await page.goto(STATIC_DOCUMENT_URL);
    await apiAudit.waitForQuiet();
    expect(pageErrors).toEqual([]);
    expect(apiAudit.unexpectedRequests).toEqual([]);
  });
});
