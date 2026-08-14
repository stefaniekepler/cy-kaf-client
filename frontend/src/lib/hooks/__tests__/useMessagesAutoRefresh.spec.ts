import { act, renderHook } from '@testing-library/react';
import { useMessagesAutoRefresh } from 'lib/hooks/useMessagesAutoRefresh';
import { LOCAL_STORAGE_KEY_PREFIX } from 'lib/constants';

const storageKey = (topicName: string) =>
  `${LOCAL_STORAGE_KEY_PREFIX}-topic-${topicName}-refresh-rate`;

describe('useMessagesAutoRefresh', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    localStorage.clear();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('calls refreshData on the configured interval', () => {
    localStorage.setItem(storageKey('orders'), '2');
    const refreshData = jest.fn();

    renderHook(() => useMessagesAutoRefresh('orders', refreshData));

    act(() => {
      jest.advanceTimersByTime(2000);
    });
    expect(refreshData).toHaveBeenCalledTimes(1);

    act(() => {
      jest.advanceTimersByTime(2000);
    });
    expect(refreshData).toHaveBeenCalledTimes(2);
  });

  it('does not schedule a timer when the rate is off', () => {
    localStorage.setItem(storageKey('orders'), '0');
    const refreshData = jest.fn();

    renderHook(() => useMessagesAutoRefresh('orders', refreshData));

    act(() => {
      jest.advanceTimersByTime(30000);
    });
    expect(refreshData).not.toHaveBeenCalled();
  });
});
