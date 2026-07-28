import {
  useMutation,
  useQuery,
  useQueryClient,
  UseMutationResult,
  UseQueryResult,
} from '@tanstack/react-query';

export type MCPSettings = {
  enabled: boolean;
  allowWrites: boolean;
};

export type ClientKind = 'codex' | 'claude-code';
export type ClientStatus =
  | 'client_not_found'
  | 'not_configured'
  | 'configured'
  | 'configuration_conflict'
  | 'repair_required'
  | 'error';

export type DesktopMCPState =
  | { available: false }
  | { available: true; settings: MCPSettings };

export type UpdateMCPSettingsRequest = MCPSettings & {
  confirmWrites: boolean;
};

export type ClientIntegration = {
  client: ClientKind;
  status: ClientStatus;
  canConfigure: boolean;
  manualCommand: string;
  message: string;
};

export type ConfigureClientRequest = {
  client: ClientKind;
  replace: boolean;
};

const invalidResponseError = 'Invalid desktop MCP response.';
const genericRequestError = 'Desktop MCP request failed.';

const configurePaths: Record<ClientKind, string> = {
  codex: '/__desktop/mcp-integrations/codex/configure',
  'claude-code': '/__desktop/mcp-integrations/claude-code/configure',
};

const clientStatuses: ReadonlySet<ClientStatus> = new Set([
  'client_not_found',
  'not_configured',
  'configured',
  'configuration_conflict',
  'repair_required',
  'error',
]);

const desktopMcpErrorMessages = {
  invalid_request: 'Invalid request.',
  desktop_only: 'Desktop only.',
  mcp_disabled: 'MCP is disabled.',
  write_confirmation_required: 'Confirm write access before enabling it.',
  client_not_found: 'Client not found.',
  concurrent_modification: 'Configuration changed externally.',
  rollback_failed: 'Rollback failed.',
  verification_failed: 'Configuration verification failed.',
  client_command_failed: 'Client command failed.',
} as const;

export const desktopMcpQueryKeys = {
  settings: ['desktop-mcp', 'settings'] as const,
  integrations: ['desktop-mcp', 'integrations'] as const,
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

const hasExactKeys = (
  value: unknown,
  keys: readonly string[]
): value is Record<string, unknown> =>
  isRecord(value) &&
  Object.keys(value).length === keys.length &&
  keys.every((key) => Object.prototype.hasOwnProperty.call(value, key));

const isMCPSettings = (value: unknown): value is MCPSettings =>
  hasExactKeys(value, ['enabled', 'allowWrites']) &&
  typeof value.enabled === 'boolean' &&
  typeof value.allowWrites === 'boolean';

const isClientKind = (value: unknown): value is ClientKind =>
  value === 'codex' || value === 'claude-code';

const isClientIntegration = (value: unknown): value is ClientIntegration =>
  hasExactKeys(value, [
    'client',
    'status',
    'canConfigure',
    'manualCommand',
    'message',
  ]) &&
  isClientKind(value.client) &&
  typeof value.status === 'string' &&
  clientStatuses.has(value.status as ClientStatus) &&
  typeof value.canConfigure === 'boolean' &&
  typeof value.manualCommand === 'string' &&
  typeof value.message === 'string';

const isUpdateMCPSettingsRequest = (
  value: unknown
): value is UpdateMCPSettingsRequest =>
  hasExactKeys(value, ['enabled', 'allowWrites', 'confirmWrites']) &&
  typeof value.enabled === 'boolean' &&
  typeof value.allowWrites === 'boolean' &&
  typeof value.confirmWrites === 'boolean';

const isConfigureClientRequest = (
  value: unknown
): value is ConfigureClientRequest =>
  hasExactKeys(value, ['client', 'replace']) &&
  isClientKind(value.client) &&
  typeof value.replace === 'boolean';

const isIntegrationsResponse = (
  value: unknown
): value is { clients: ClientIntegration[] } =>
  hasExactKeys(value, ['clients']) &&
  Array.isArray(value.clients) &&
  value.clients.every(isClientIntegration);

const safeError = (value: unknown): Error => {
  if (
    hasExactKeys(value, ['code', 'message']) &&
    typeof value.code === 'string' &&
    typeof value.message === 'string' &&
    Object.prototype.hasOwnProperty.call(desktopMcpErrorMessages, value.code) &&
    desktopMcpErrorMessages[
      value.code as keyof typeof desktopMcpErrorMessages
    ] === value.message
  ) {
    return new Error(value.message);
  }

  return new Error(genericRequestError);
};

const readJSON = async (response: Response): Promise<unknown> => {
  try {
    return await response.json();
  } catch {
    throw new Error(response.ok ? invalidResponseError : genericRequestError);
  }
};

const request = (path: string, init?: RequestInit) => {
  const { headers, ...requestInit } = init || {};
  return fetch(path, {
    credentials: 'same-origin',
    ...requestInit,
    headers: {
      Accept: 'application/json',
      ...headers,
    },
  });
};

const getSettings = async (): Promise<DesktopMCPState> => {
  const response = await request('/__desktop/mcp-settings');
  if (response.status === 404) return { available: false };

  const body = await readJSON(response);
  if (!response.ok) throw safeError(body);
  if (!isMCPSettings(body)) throw new Error(invalidResponseError);

  return { available: true, settings: body };
};

const updateSettings = async (
  input: UpdateMCPSettingsRequest
): Promise<MCPSettings> => {
  if (!isUpdateMCPSettingsRequest(input)) {
    throw new Error(genericRequestError);
  }
  const { enabled, allowWrites, confirmWrites } = input;
  const response = await request('/__desktop/mcp-settings', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled, allowWrites, confirmWrites }),
  });
  const body = await readJSON(response);
  if (!response.ok) throw safeError(body);
  if (!isMCPSettings(body)) throw new Error(invalidResponseError);

  return body;
};

const getIntegrations = async (): Promise<ClientIntegration[]> => {
  const response = await request('/__desktop/mcp-integrations');
  const body = await readJSON(response);
  if (!response.ok) throw safeError(body);
  if (!isIntegrationsResponse(body)) {
    throw new Error(invalidResponseError);
  }

  return body.clients;
};

const configureClient = async (
  input: ConfigureClientRequest
): Promise<ClientIntegration> => {
  if (!isConfigureClientRequest(input)) {
    throw new Error(genericRequestError);
  }
  const { client, replace } = input;
  const response = await request(configurePaths[client], {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ replace }),
  });
  const body = await readJSON(response);
  if (!response.ok) throw safeError(body);
  if (!isClientIntegration(body)) throw new Error(invalidResponseError);

  return body;
};

const invalidateDesktopMcpQueries = (queryClient: ReturnType<typeof useQueryClient>) =>
  Promise.all([
    queryClient.invalidateQueries({ queryKey: desktopMcpQueryKeys.settings }),
    queryClient.invalidateQueries({ queryKey: desktopMcpQueryKeys.integrations }),
  ]);

export function useDesktopMCPSettings(): UseQueryResult<DesktopMCPState, Error> {
  return useQuery<DesktopMCPState, Error>({
    queryKey: desktopMcpQueryKeys.settings,
    queryFn: getSettings,
    retry: false,
  });
}

export function useUpdateDesktopMCPSettings(): UseMutationResult<
  MCPSettings,
  Error,
  UpdateMCPSettingsRequest
> {
  const queryClient = useQueryClient();
  return useMutation<MCPSettings, Error, UpdateMCPSettingsRequest>({
    mutationFn: updateSettings,
    onSuccess: () => invalidateDesktopMcpQueries(queryClient),
  });
}

export function useDesktopMCPIntegrations(
  enabled: boolean
): UseQueryResult<ClientIntegration[], Error> {
  return useQuery<ClientIntegration[], Error>({
    queryKey: desktopMcpQueryKeys.integrations,
    queryFn: getIntegrations,
    enabled,
  });
}

export function useConfigureDesktopMCP(): UseMutationResult<
  ClientIntegration,
  Error,
  ConfigureClientRequest
> {
  const queryClient = useQueryClient();
  return useMutation<ClientIntegration, Error, ConfigureClientRequest>({
    mutationFn: configureClient,
    onSuccess: () => invalidateDesktopMcpQueries(queryClient),
  });
}
