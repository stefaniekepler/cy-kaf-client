import { act, renderHook } from '@testing-library/react';
import {
  clusterBrokersPath,
  clusterNewConfigPath,
  clusterTopicsPath,
} from 'lib/paths';
import useAllClustersReminder, {
  getIsClusterRoute,
  resetAllClustersReminderSession,
} from 'components/PageContainer/useAllClustersReminder';

type HookProps = {
  isClusterRoute: boolean;
  isSidebarVisible: boolean;
  isLarge: boolean;
};

const renderReminder = (initialProps: HookProps) =>
  renderHook((props: HookProps) => useAllClustersReminder(props), {
    initialProps,
  });

describe('getIsClusterRoute', () => {
  it.each([
    ['/', false],
    ['/ui', false],
    ['/ui/clusters', false],
    ['/ui/clusters/', false],
    [clusterNewConfigPath, false],
    [`${clusterNewConfigPath}/`, false],
    [`${clusterNewConfigPath}/advanced`, false],
    ['/UI/CLUSTERS/CREATE-NEW-CLUSTER', false],
    ['/ui/clusters/Create-New-Cluster/advanced', false],
    [clusterBrokersPath('local'), true],
    [clusterTopicsPath('local'), true],
  ])('classifies %s as cluster route: %s', (pathname, expected) => {
    expect(getIsClusterRoute(pathname)).toBe(expected);
  });
});

describe('useAllClustersReminder', () => {
  beforeEach(() => resetAllClustersReminderSession());

  it('stays hidden outside cluster context', () => {
    const { result } = renderReminder({
      isClusterRoute: false,
      isSidebarVisible: true,
      isLarge: true,
    });

    expect(result.current.isAllClustersReminderVisible).toBe(false);
  });

  it('displays immediately on an initial wide cluster route', () => {
    const { result } = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });

    expect(result.current.isAllClustersReminderVisible).toBe(true);
  });

  it('hides on the render that leaves cluster context while visible', () => {
    const leavingRenderVisibility: boolean[] = [];
    const { result, rerender } = renderHook(
      (props: HookProps) => {
        const reminder = useAllClustersReminder(props);

        if (!props.isClusterRoute) {
          leavingRenderVisibility.push(reminder.isAllClustersReminderVisible);
        }

        return reminder;
      },
      {
        initialProps: {
          isClusterRoute: true,
          isSidebarVisible: true,
          isLarge: true,
        },
      }
    );

    expect(result.current.isAllClustersReminderVisible).toBe(true);

    rerender({
      isClusterRoute: false,
      isSidebarVisible: true,
      isLarge: true,
    });

    expect(leavingRenderVisibility[0]).toBe(false);
    expect(result.current.isAllClustersReminderVisible).toBe(false);
  });

  it('waits on an initial narrow cluster route until the hidden sidebar opens', () => {
    const { result, rerender } = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: false,
      isLarge: false,
    });

    expect(result.current.isAllClustersReminderVisible).toBe(false);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });

    expect(result.current.isAllClustersReminderVisible).toBe(true);
  });

  it('consumes a visible reminder when the sidebar closes', () => {
    const { result, rerender } = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: false,
      isLarge: false,
    });

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(true);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: false,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(false);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(false);
  });

  it('waits for a narrow open sidebar to close before displaying on a subsequent open', () => {
    const { result, rerender } = renderReminder({
      isClusterRoute: false,
      isSidebarVisible: true,
      isLarge: false,
    });

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(false);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: false,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(false);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(true);
  });

  it('displays when an awaiting narrow layout becomes wide before the sidebar closes', () => {
    const { result, rerender } = renderReminder({
      isClusterRoute: false,
      isSidebarVisible: true,
      isLarge: false,
    });

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(false);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(true);
  });

  it('does not display twice after dismissal and sidebar hide/show in one entry cycle', () => {
    const { result, rerender } = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: false,
      isLarge: false,
    });

    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(true);

    act(() => result.current.dismissAllClustersReminder());
    expect(result.current.isAllClustersReminderVisible).toBe(false);

    rerender({
      isClusterRoute: true,
      isSidebarVisible: false,
      isLarge: false,
    });
    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: false,
    });

    expect(result.current.isAllClustersReminderVisible).toBe(false);
  });

  it('stays dismissed for the rest of the session once it has been shown', () => {
    const { result, rerender } = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });
    expect(result.current.isAllClustersReminderVisible).toBe(true);

    act(() => result.current.dismissAllClustersReminder());

    // 离开集群上下文后再进入另一个集群：本次 App 会话内不再提醒
    rerender({
      isClusterRoute: false,
      isSidebarVisible: true,
      isLarge: true,
    });
    rerender({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });

    expect(result.current.isAllClustersReminderVisible).toBe(false);
  });

  it('does not remind a freshly mounted hook after an earlier cluster visit', () => {
    const first = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });
    expect(first.result.current.isAllClustersReminderVisible).toBe(true);
    first.unmount();

    const second = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });
    expect(second.result.current.isAllClustersReminderVisible).toBe(false);
  });

  it('reminds again in a new app session', () => {
    const first = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });
    expect(first.result.current.isAllClustersReminderVisible).toBe(true);
    first.unmount();

    resetAllClustersReminderSession();

    const second = renderReminder({
      isClusterRoute: true,
      isSidebarVisible: true,
      isLarge: true,
    });
    expect(second.result.current.isAllClustersReminderVisible).toBe(true);
  });
});
