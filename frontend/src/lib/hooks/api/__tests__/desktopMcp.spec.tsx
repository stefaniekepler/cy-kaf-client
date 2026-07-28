import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import fetchMock from 'fetch-mock';
import React, { PropsWithChildren } from 'react';
import {
  desktopMcpQueryKeys,
  useConfigureDesktopMCP,
  useDesktopMCPIntegrations,
  useDesktopMCPSettings,
  useUpdateDesktopMCPSettings,
} from 'lib/hooks/api/desktopMcp';

const settingsPath = '/__desktop/mcp-settings';
const integrationsPath = '/__desktop/mcp-integrations';

const settings = { enabled: true, allowWrites: false };
const integration = {
  client: 'codex',
  status: 'configured',
  canConfigure: true,
  manualCommand: 'codex mcp add cy-kaf',
  message: '',
};

const createWrapper =
  (queryClient: QueryClient): React.FC<PropsWithChildren<unknown>> =>
  ({ children }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

describe('desktop MCP hooks', () => {
  beforeEach(() => fetchMock.restore());

  it('maps the settings response from the desktop endpoint', async () => {
    fetchMock.getOnce(settingsPath, settings);
    const queryClient = new QueryClient();
    const { result } = renderHook(() => useDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current.data).toEqual({ available: true, settings }));
    expect(fetchMock.calls(settingsPath)).toHaveLength(1);
  });

  it('treats a missing desktop endpoint as unavailable without retrying', async () => {
    fetchMock.getOnce(settingsPath, 404);
    const queryClient = new QueryClient();
    const { result } = renderHook(() => useDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current.data).toEqual({ available: false }));
    expect(result.current.isError).toBe(false);
    expect(fetchMock.calls(settingsPath)).toHaveLength(1);
  });

  it('returns a fixed error for a malformed successful response', async () => {
    fetchMock.getOnce(settingsPath, { enabled: 'yes', allowWrites: false });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(() => useDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error?.message).toBe('Invalid desktop MCP response.');
  });

  it.each([
    ['settings', settingsPath, { ...settings, path: '/unexpected' }],
    [
      'integration envelope',
      integrationsPath,
      { clients: [integration], version: 'unexpected' },
    ],
    [
      'integration record',
      integrationsPath,
      { clients: [{ ...integration, env: 'unexpected' }] },
    ],
  ])('rejects a successful %s response with extra fields', async (_, path, body) => {
    fetchMock.getOnce(path, body);
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(
      () => (path === settingsPath ? useDesktopMCPSettings() : useDesktopMCPIntegrations(true)),
      { wrapper: createWrapper(queryClient) }
    );

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error?.message).toBe('Invalid desktop MCP response.');
  });

  it('rejects malformed integrations and does not expose an unexpected error body', async () => {
    fetchMock.getOnce(integrationsPath, {
      clients: [{ ...integration, status: 'unknown_status' }],
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(() => useDesktopMCPIntegrations(true), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error?.message).toBe('Invalid desktop MCP response.');
  });

  it('uses a fixed error when a failed response is outside the error contract', async () => {
    fetchMock.getOnce(settingsPath, {
      status: 500,
      body: { message: 'unexpected response content' },
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(() => useDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error?.message).toBe('Desktop MCP request failed.');
  });

  it.each([
    { code: 'mcp_disabled', message: 'Unexpected message.' },
    { code: 'unknown_code', message: 'MCP is disabled.' },
    { code: 'mcp_disabled', message: 'MCP is disabled.', path: '/unexpected' },
  ])('does not reflect an unrecognized desktop error payload', async (body) => {
    fetchMock.getOnce(settingsPath, { status: 409, body });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(() => useDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error?.message).toBe('Desktop MCP request failed.');
  });

  it('sends only confirmed settings fields and invalidates desktop probes', async () => {
    fetchMock.putOnce(settingsPath, settings);
    const queryClient = new QueryClient();
    queryClient.setQueryData(desktopMcpQueryKeys.settings, settings);
    queryClient.setQueryData(desktopMcpQueryKeys.integrations, [integration]);
    const { result } = renderHook(() => useUpdateDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await act(async () => {
      await result.current.mutateAsync({
        ...settings,
        confirmWrites: false,
      });
    });

    expect(fetchMock.lastCall(settingsPath)?.[1]?.body).toBe(
      JSON.stringify({ ...settings, confirmWrites: false })
    );
    expect(fetchMock.lastCall(settingsPath)?.[1]?.headers).toEqual({
      Accept: 'application/json',
      'Content-Type': 'application/json',
    });
    expect(queryClient.getQueryState(desktopMcpQueryKeys.settings)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(desktopMcpQueryKeys.integrations)?.isInvalidated).toBe(true);
  });

  it('uses the fixed client route and invalidates probes after configuration', async () => {
    fetchMock.postOnce(`${integrationsPath}/codex/configure`, integration);
    const queryClient = new QueryClient();
    queryClient.setQueryData(desktopMcpQueryKeys.settings, settings);
    queryClient.setQueryData(desktopMcpQueryKeys.integrations, [integration]);
    const { result } = renderHook(() => useConfigureDesktopMCP(), {
      wrapper: createWrapper(queryClient),
    });

    await act(async () => {
      await result.current.mutateAsync({ client: 'codex', replace: true });
    });

    expect(fetchMock.calls(`${integrationsPath}/codex/configure`)).toHaveLength(1);
    expect(queryClient.getQueryState(desktopMcpQueryKeys.settings)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(desktopMcpQueryKeys.integrations)?.isInvalidated).toBe(true);
  });

  it('rejects invalid settings mutation input without fetching', async () => {
    const queryClient = new QueryClient();
    const { result } = renderHook(() => useUpdateDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await act(async () => {
      await expect(
        result.current.mutateAsync({
          enabled: 'yes',
          allowWrites: false,
          confirmWrites: false,
        } as never)
      ).rejects.toThrow('Desktop MCP request failed.');
    });

    expect(fetchMock.calls()).toHaveLength(0);
  });

  it('rejects expanded settings mutation input without fetching', async () => {
    const queryClient = new QueryClient();
    const { result } = renderHook(() => useUpdateDesktopMCPSettings(), {
      wrapper: createWrapper(queryClient),
    });

    await act(async () => {
      await expect(
        result.current.mutateAsync({
          ...settings,
          confirmWrites: false,
          path: '/unexpected',
        } as never)
      ).rejects.toThrow('Desktop MCP request failed.');
    });

    expect(fetchMock.calls()).toHaveLength(0);
  });

  it('rejects invalid client mutation input without fetching', async () => {
    const queryClient = new QueryClient();
    const { result } = renderHook(() => useConfigureDesktopMCP(), {
      wrapper: createWrapper(queryClient),
    });

    await act(async () => {
      await expect(
        result.current.mutateAsync({ client: 'other', replace: 'yes' } as never)
      ).rejects.toThrow('Desktop MCP request failed.');
    });

    expect(fetchMock.calls()).toHaveLength(0);
  });

  it('does not request integrations while the probe is disabled', () => {
    const queryClient = new QueryClient();
    renderHook(() => useDesktopMCPIntegrations(false), {
      wrapper: createWrapper(queryClient),
    });

    expect(fetchMock.calls(integrationsPath)).toHaveLength(0);
  });
});
