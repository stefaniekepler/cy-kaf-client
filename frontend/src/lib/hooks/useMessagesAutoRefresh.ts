import { useEffect, useRef } from 'react';
import { useLocalStorage } from 'lib/hooks/useLocalStorage';

/**
 * Periodically re-triggers a topic's messages refresh (the same `refreshData`
 * the manual Refresh button calls) while a non-zero refresh rate is selected.
 * The rate lives in localStorage under `topic-<topicName>-refresh-rate`, the
 * key written by RefreshRateSelect. refreshData is held in a ref so the timer
 * isn't torn down and recreated on every render (refreshData changes identity
 * each render).
 */
export const useMessagesAutoRefresh = (
  topicName: string,
  refreshData: () => void
) => {
  const [refreshRate] = useLocalStorage<number>(
    `topic-${topicName}-refresh-rate`,
    0
  );
  const refreshDataRef = useRef(refreshData);
  refreshDataRef.current = refreshData;

  useEffect(() => {
    if (refreshRate <= 0) {
      return undefined;
    }
    const id = setInterval(() => refreshDataRef.current(), refreshRate * 1000);
    return () => clearInterval(id);
  }, [refreshRate]);
};
