import React, { PropsWithChildren } from 'react';
import { act, renderHook, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { fetchEventSource } from '@microsoft/fetch-event-source';
import { renderQueryHook } from 'lib/testHelpers';
import * as hooks from 'lib/hooks/api/topicMessages';
import fetchMock from 'fetch-mock';
import { UseQueryResult, UseSuspenseQueryResult } from '@tanstack/react-query';
import {
  PollingMode,
  SerdeUsage,
  TopicMessageEventTypeEnum,
} from 'generated-sources';
import { LOCAL_STORAGE_KEY_PREFIX, MessagesFilterKeys } from 'lib/constants';
import { clusterTopicMessagesPath } from 'lib/paths';
import { useMessagesFilters } from 'lib/hooks/useMessagesFilters';

const clusterName = 'test-cluster';
const topicName = 'test-topic';

const expectQueryWorks = async (
  mock: fetchMock.FetchMockStatic,
  result: {
    current:
      | UseQueryResult<unknown, unknown>
      | UseSuspenseQueryResult<unknown, unknown>;
  }
) => {
  await waitFor(() => expect(result.current.isFetched).toBeTruthy());
  expect(mock.calls()).toHaveLength(1);
  expect(result.current.data).toBeDefined();
};

jest.mock('lib/errorHandling', () => ({
  ...jest.requireActual('lib/errorHandling'),
  showServerError: jest.fn(),
}));

jest.mock('@microsoft/fetch-event-source', () => ({
  fetchEventSource: jest.fn(),
}));

const fetchEventSourceMock = jest.mocked(fetchEventSource);
let sourceInit: Parameters<typeof fetchEventSource>[1] | undefined;
let frameCallback: FrameRequestCallback | undefined;

const messageEvent = (offset: number) => ({
  id: '',
  event: '',
  data: JSON.stringify({
    type: TopicMessageEventTypeEnum.MESSAGE,
    message: {
      partition: 0,
      offset,
      timestamp: '2026-07-24T00:00:00.000Z',
    },
  }),
});

const renderMessagePageHooks = (query = '') => {
  const entry = `${clusterTopicMessagesPath(clusterName, topicName)}${query}`;
  const wrapper = ({ children }: PropsWithChildren) =>
    React.createElement(
      MemoryRouter,
      {
        initialEntries: [entry],
        future: {
          v7_relativeSplatPath: true,
          v7_startTransition: true,
        },
      },
      React.createElement(
        Routes,
        null,
        React.createElement(Route, {
          path: clusterTopicMessagesPath(),
          element: children,
        })
      )
    );

  return renderHook(
    () => {
      const filters = useMessagesFilters(topicName);
      const messages = hooks.useTopicMessages({
        clusterName,
        topicName,
        enabled: filters.isInitialized,
      });
      return { filters, messages };
    },
    { wrapper }
  );
};

const expectSingleRequest = async (
  result: ReturnType<typeof renderMessagePageHooks>['result'],
  expectedMode: string
) => {
  await waitFor(() => expect(result.current.filters.isInitialized).toBe(true));
  await waitFor(() => expect(fetchEventSourceMock).toHaveBeenCalledTimes(1));
  const [requestUrl, requestInit] = fetchEventSourceMock.mock.calls[0];
  const parsedUrl = new URL(String(requestUrl), 'http://localhost');

  expect(requestInit?.method).toBe('GET');
  expect(requestInit?.signal?.aborted).toBe(false);
  expect(parsedUrl.searchParams.get(MessagesFilterKeys.mode)).toBe(
    expectedMode
  );
  expect(fetchEventSourceMock).toHaveBeenCalledTimes(1);

  return parsedUrl;
};

describe('Topic Messages hooks', () => {
  beforeEach(() => {
    fetchMock.restore();
    fetchEventSourceMock.mockReset();
    sourceInit = undefined;
    frameCallback = undefined;
    fetchEventSourceMock.mockImplementation(async (_url, init) => {
      sourceInit = init;
    });
    jest.spyOn(window, 'requestAnimationFrame').mockImplementation((cb) => {
      frameCallback = cb;
      return 1;
    });
    jest.spyOn(window, 'cancelAnimationFrame').mockImplementation(jest.fn());
    localStorage.clear();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('starts one LATEST request after default filters are initialized', async () => {
    const { result } = renderMessagePageHooks();
    await expectSingleRequest(result, PollingMode.LATEST);
  });

  it('starts only the persisted request without an earlier LATEST read', async () => {
    const storageKey = `${LOCAL_STORAGE_KEY_PREFIX}-message-filters-fields`;
    const persistedFilters = {
      [`${topicName}:${clusterName}`]: {
        mode: PollingMode.FROM_OFFSET,
        offset: '7',
      },
    };
    localStorage.setItem(storageKey, JSON.stringify(persistedFilters));
    expect(JSON.parse(localStorage.getItem(storageKey) || '{}')).toEqual(
      persistedFilters
    );

    const { result } = renderMessagePageHooks();
    await waitFor(() =>
      expect(result.current.filters.isInitialized).toBe(true)
    );
    expect(JSON.parse(localStorage.getItem(storageKey) || '{}')).toEqual(
      persistedFilters
    );
    const requestUrl = await expectSingleRequest(
      result,
      PollingMode.FROM_OFFSET
    );
    expect(requestUrl.searchParams.get(MessagesFilterKeys.offset)).toBe('7');
  });

  it.each([
    ['valid', PollingMode.EARLIEST],
    ['empty', ''],
    ['unknown', 'UNKNOWN_MODE'],
  ])('preserves one explicit %s mode request', async (_label, mode) => {
    const { result } = renderMessagePageHooks(
      `?${MessagesFilterKeys.mode}=${encodeURIComponent(mode)}`
    );
    await expectSingleRequest(result, mode);
  });

  it('commits a burst of messages in one animation-frame batch', async () => {
    const { result } = renderMessagePageHooks();
    await waitFor(() => expect(sourceInit?.onmessage).toBeDefined());

    act(() => {
      for (let offset = 0; offset < 100; offset += 1) {
        sourceInit?.onmessage?.(messageEvent(offset));
      }
    });

    expect(result.current.messages.messages).toHaveLength(0);
    expect(window.requestAnimationFrame).toHaveBeenCalledTimes(1);

    act(() => frameCallback?.(0));

    expect(result.current.messages.messages).toHaveLength(100);
    expect(result.current.messages.messages.map((m) => m.offset)).toEqual(
      Array.from({ length: 100 }, (_, offset) => offset)
    );
  });

  it('keeps newest-first order when a TAILING batch is flushed', async () => {
    const { result } = renderMessagePageHooks(
      `?${MessagesFilterKeys.mode}=${PollingMode.TAILING}`
    );
    await waitFor(() => expect(sourceInit?.onmessage).toBeDefined());

    act(() => {
      [1, 2, 3].forEach((offset) =>
        sourceInit?.onmessage?.(messageEvent(offset))
      );
      frameCallback?.(0);
    });

    expect(result.current.messages.messages.map((m) => m.offset)).toEqual([
      3, 2, 1,
    ]);
  });

  it('drops a pending batch when the request is aborted', async () => {
    const { result } = renderMessagePageHooks();
    await waitFor(() => expect(sourceInit?.onmessage).toBeDefined());

    act(() => {
      sourceInit?.onmessage?.(messageEvent(1));
      result.current.messages.abortFetchData();
      frameCallback?.(0);
    });

    expect(window.cancelAnimationFrame).toHaveBeenCalledWith(1);
    expect(result.current.messages.messages).toHaveLength(0);
  });

  it('handles useSerdes', async () => {
    const path = `/api/clusters/${clusterName}/topics/${topicName}/serdes?use=SERIALIZE`;

    const mock = fetchMock.getOnce(path, {});
    const { result } = renderQueryHook(() =>
      hooks.useSerdes({ clusterName, topicName, use: SerdeUsage.SERIALIZE })
    );
    await expectQueryWorks(mock, result);
  });
});
