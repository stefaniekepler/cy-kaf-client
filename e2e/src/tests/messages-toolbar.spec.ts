import { expect, Page, Route, test } from "@playwright/test";

const CLUSTER_NAME = "layout-cluster";
const TOPIC_NAME = "layout-topic";
const LONG_FILTER_NAME = `persisted-${"unbroken".repeat(80)}`;
const FRONTEND_URL = "http://127.0.0.1:4176";
const TOPIC_API_PATH = `/api/clusters/${CLUSTER_NAME}/topics/${TOPIC_NAME}`;
const MESSAGES_PATH =
  `/ui/clusters/${CLUSTER_NAME}/all-topics/${TOPIC_NAME}/messages` +
  `?activeFilterId=${encodeURIComponent(LONG_FILTER_NAME)}`;
const API_FIXTURES: Record<string, unknown> = {
  "/api/info": { enabledFeatures: [] },
  "/api/authorization": {
    rbacEnabled: false,
    userInfo: { username: "toolbar-layout-test", permissions: [] },
  },
  "/api/clusters": [
    {
      name: CLUSTER_NAME,
      defaultCluster: true,
      status: "ONLINE",
      readOnly: true,
      features: [],
      controller: "KRAFT",
    },
  ],
  [TOPIC_API_PATH]: {
    name: TOPIC_NAME,
    internal: false,
    partitions: [
      {
        partition: 0,
        leader: 1,
        replicas: [{ broker: 1, leader: true, inSync: true }],
        offsetMax: 0,
        offsetMin: 0,
      },
    ],
    partitionCount: 1,
    replicationFactor: 1,
    replicas: 1,
    inSyncReplicas: 1,
    cleanUpPolicy: "DELETE",
  },
  [`${TOPIC_API_PATH}/connectors`]: [],
  [`${TOPIC_API_PATH}/serdes`]: { key: [], value: [] },
};

const fulfillJson = (route: Route, body: unknown) =>
  route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(body),
  });

async function installReadOnlyApiFixtures(page: Page) {
  const unexpectedRequests: string[] = [];

  await page.route(`${FRONTEND_URL}/api/**`, async(route) => {
    const request = route.request();
    const url = new URL(request.url());
    const requestLabel = `${request.method()} ${url.pathname}${url.search}`;

    if (request.method() !== "GET") {
      unexpectedRequests.push(requestLabel);
      await route.abort("blockedbyclient");
      return;
    }

    if (url.pathname === `${TOPIC_API_PATH}/messages/v2`) {
      await route.fulfill({
        status: 200,
        headers: {
          "cache-control": "no-cache",
          "content-type": "text/event-stream",
        },
        body: 'data: {"type":"DONE"}\n\n',
      });
      return;
    }

    const fixture = API_FIXTURES[url.pathname];
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
  });

  return unexpectedRequests;
}

async function seedPersistedFilter(page: Page) {
  await page.addInitScript(
    ({ filterName }) => {
      localStorage.setItem(
        "kafbat-ui-message-filters",
        JSON.stringify({
          state: {
            filters: {
              [filterName]: {
                id: filterName,
                value: "record.partition == 0",
                filterCode: "record.partition == 0",
              },
            },
          },
          version: 0,
        })
      );
    },
    { filterName: LONG_FILTER_NAME }
  );
}

async function measureToolbar(page: Page) {
  const toolbar = page.getByTestId("messages-toolbar");

  return toolbar.evaluate((element) => {
    const root = element as HTMLElement;
    const activeFilter = root.querySelector<HTMLElement>(
      '[data-testid="activeSmartFilter"]'
    );
    const search = root.querySelector<HTMLInputElement>(
      'input[placeholder="Search"]'
    );
    const buttons = Array.from(root.querySelectorAll<HTMLButtonElement>("button"));
    const addFilters = buttons.find((button) =>
      button.textContent?.includes("Add Filters")
    );
    const refresh = buttons.find(
      (button) => button.textContent?.trim() === "Refresh"
    );
    const toolbarControls = activeFilter?.parentElement;
    const label = activeFilter?.querySelector<HTMLElement>("[title]");

    if (
      !activeFilter ||
      !search ||
      !addFilters ||
      !refresh ||
      !toolbarControls ||
      !label
    ) {
      throw new Error("Messages toolbar is missing a required control");
    }

    const toRect = (item: Element, name: string) => {
      const rect = item.getBoundingClientRect();
      return {
        name,
        left: rect.left,
        right: rect.right,
        top: rect.top,
        bottom: rect.bottom,
        width: rect.width,
        height: rect.height,
      };
    };
    const controlRects = Array.from(toolbarControls.children).map(
      (child, index) => toRect(child, `toolbar-control-${index}`)
    );
    const requiredRects = [
      toRect(addFilters, "Add Filters"),
      toRect(activeFilter, "active filter"),
      toRect(refresh, "Refresh"),
      toRect(search, "Search"),
    ];
    const overlapCandidates = [
      ...controlRects,
      toRect(search, "Search"),
    ].filter(({ width, height }) => width > 0 && height > 0);
    const overlapPairs: string[] = [];

    overlapCandidates.forEach((first, firstIndex) => {
      overlapCandidates.slice(firstIndex + 1).forEach((second) => {
        const overlaps =
          first.left < second.right &&
          first.right > second.left &&
          first.top < second.bottom &&
          first.bottom > second.top;
        if (overlaps) overlapPairs.push(`${first.name}/${second.name}`);
      });
    });

    const documentElement = document.documentElement;
    const toolbarRect = root.getBoundingClientRect();
    const searchRect = search.getBoundingClientRect();
    const follows = (first: Element, second: Element) =>
      Boolean(
        first.compareDocumentPosition(second) &
          Node.DOCUMENT_POSITION_FOLLOWING
      );

    return {
      document: {
        clientWidth: documentElement.clientWidth,
        scrollWidth: documentElement.scrollWidth,
      },
      toolbar: {
        clientWidth: root.clientWidth,
        scrollWidth: root.scrollWidth,
      },
      controls: {
        clientWidth: toolbarControls.clientWidth,
        scrollWidth: toolbarControls.scrollWidth,
      },
      label: {
        clientWidth: label.clientWidth,
        scrollWidth: label.scrollWidth,
        title: label.getAttribute("title"),
      },
      requiredRects,
      requiredOrder:
        follows(addFilters, activeFilter) &&
        follows(activeFilter, refresh) &&
        follows(refresh, search),
      requiredWithinViewport: requiredRects.every(
        ({ left, right, top, bottom }) =>
          left >= 0 &&
          right <= documentElement.clientWidth &&
          top >= 0 &&
          bottom <= window.innerHeight
      ),
      searchRightGap: Math.abs(toolbarRect.right - searchRect.right),
      overlapPairs,
    };
  });
}

for (const viewport of [
  { width: 1024, height: 700 },
  { width: 1440, height: 900 },
]) {
  test.describe(`${viewport.width}x${viewport.height}`, () => {
    test.use({ viewport });

    test("keeps a persisted unbroken filter inside the real Messages toolbar", async({
      page,
    }) => {
      const pageErrors: string[] = [];
      page.on("pageerror", (error) => pageErrors.push(error.message));
      const unexpectedRequests = await installReadOnlyApiFixtures(page);
      await seedPersistedFilter(page);

      await page.goto(MESSAGES_PATH);

      const toolbar = page.getByTestId("messages-toolbar");
      const addFilters = toolbar.getByRole("button", { name: "Add Filters" });
      const activeFilter = toolbar.getByTestId("activeSmartFilter");
      const refresh = toolbar.getByRole("button", { name: "Refresh" });
      const search = toolbar.getByPlaceholder("Search");
      const label = toolbar.getByTitle(LONG_FILTER_NAME);

      await expect(toolbar).toBeVisible();
      await expect(addFilters).toBeVisible();
      await expect(activeFilter).toBeVisible();
      await expect(refresh).toBeVisible();
      await expect(search).toBeVisible();
      await expect(label).toHaveText(LONG_FILTER_NAME);
      await expect(label).toHaveAttribute("title", LONG_FILTER_NAME);
      await page.waitForLoadState("networkidle");

      const metrics = await measureToolbar(page);
      console.log(
        `messages-toolbar ${viewport.width}x${viewport.height} ${JSON.stringify(
          metrics
        )}`
      );

      expect(metrics.document.scrollWidth).toBeLessThanOrEqual(
        metrics.document.clientWidth
      );
      expect(metrics.toolbar.scrollWidth).toBeLessThanOrEqual(
        metrics.toolbar.clientWidth
      );
      expect(metrics.controls.scrollWidth).toBeLessThanOrEqual(
        metrics.controls.clientWidth
      );
      expect(metrics.requiredWithinViewport).toBe(true);
      expect(metrics.requiredOrder).toBe(true);
      expect(metrics.searchRightGap).toBeLessThanOrEqual(1);
      expect(metrics.overlapPairs).toEqual([]);
      expect(metrics.label.scrollWidth).toBeGreaterThan(
        metrics.label.clientWidth
      );
      expect(metrics.label.title).toBe(LONG_FILTER_NAME);
      expect(pageErrors).toEqual([]);
      expect(unexpectedRequests).toEqual([]);
    });
  });
}
