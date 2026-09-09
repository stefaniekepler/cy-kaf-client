import { useCallback, useEffect, useRef, useState } from 'react';
import { matchPath } from 'react-router-dom';
import { clusterNewConfigPath, clusterPath } from 'lib/paths';

type ReminderPhase =
  | 'idle'
  | 'awaiting-sidebar-close'
  | 'pending'
  | 'visible'
  | 'shown';

type UseAllClustersReminderProps = {
  isClusterRoute: boolean;
  isSidebarVisible: boolean;
  isLarge: boolean;
};

// 每次打开 App（一次页面会话）只提醒一次：模块级标记随刷新/重启自然复位。
let hasRemindedThisSession = false;

export const resetAllClustersReminderSession = () => {
  hasRemindedThisSession = false;
};

const normalizePathname = (pathname: string) =>
  pathname.replace(/\/+$/, '') || '/';

export const getIsClusterRoute = (pathname: string): boolean => {
  const normalizedPathname = normalizePathname(pathname);

  if (matchPath(`${clusterNewConfigPath}/*`, normalizedPathname)) {
    return false;
  }

  return matchPath(`${clusterPath()}/*`, normalizedPathname) !== null;
};

const getEntryPhase = ({
  isClusterRoute,
  isSidebarVisible,
  isLarge,
}: UseAllClustersReminderProps): ReminderPhase => {
  if (!isClusterRoute) return 'idle';
  if (hasRemindedThisSession) return 'shown';
  if (isLarge && isSidebarVisible) return 'visible';
  if (!isLarge && isSidebarVisible) return 'awaiting-sidebar-close';
  return 'pending';
};

const useAllClustersReminder = ({
  isClusterRoute,
  isSidebarVisible,
  isLarge,
}: UseAllClustersReminderProps) => {
  const [phase, setPhase] = useState<ReminderPhase>(() =>
    getEntryPhase({ isClusterRoute, isSidebarVisible, isLarge })
  );
  const previousIsClusterRoute = useRef(isClusterRoute);

  useEffect(() => {
    const wasClusterRoute = previousIsClusterRoute.current;
    previousIsClusterRoute.current = isClusterRoute;

    if (!isClusterRoute) {
      setPhase('idle');
      return;
    }

    if (!wasClusterRoute) {
      setPhase(getEntryPhase({ isClusterRoute, isSidebarVisible, isLarge }));
    }
  }, [isClusterRoute, isSidebarVisible, isLarge]);

  useEffect(() => {
    if (!isClusterRoute) return;

    setPhase((currentPhase) => {
      if (currentPhase === 'awaiting-sidebar-close' && isLarge) {
        return isSidebarVisible ? 'visible' : 'pending';
      }

      if (currentPhase === 'awaiting-sidebar-close' && !isSidebarVisible) {
        return 'pending';
      }

      if (currentPhase === 'visible' && !isSidebarVisible) {
        return 'shown';
      }

      if (currentPhase === 'pending' && isSidebarVisible) {
        return 'visible';
      }

      return currentPhase;
    });
  }, [isClusterRoute, isSidebarVisible, isLarge]);

  useEffect(() => {
    if (phase === 'visible') {
      hasRemindedThisSession = true;
    }
  }, [phase]);

  const dismissAllClustersReminder = useCallback(() => {
    setPhase('shown');
  }, []);

  return {
    isAllClustersReminderVisible:
      isClusterRoute && isSidebarVisible && phase === 'visible',
    dismissAllClustersReminder,
  };
};

export default useAllClustersReminder;
