import React from 'react';
import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import SettingsModal from 'components/Settings/SettingsModal';
import { render } from 'lib/testHelpers';
import {
  ClientIntegration,
  ClientKind,
  DesktopMCPState,
  useConfigureDesktopMCP,
  useDesktopMCPIntegrations,
  useDesktopMCPSettings,
  useUpdateDesktopMCPSettings,
} from 'lib/hooks/api/desktopMcp';

jest.mock('lib/hooks/api/desktopMcp');

const mockUseSettings = jest.mocked(useDesktopMCPSettings);
const mockUseIntegrations = jest.mocked(useDesktopMCPIntegrations);
const mockUseUpdateSettings = jest.mocked(useUpdateDesktopMCPSettings);
const mockUseConfigure = jest.mocked(useConfigureDesktopMCP);

const settingsRefetch = jest.fn();
const integrationsRefetch = jest.fn();
const updateSettings = jest.fn();
const configureClient = jest.fn();
const resetUpdate = jest.fn();
const resetConfigure = jest.fn();
const clipboardWriteText = jest.fn();
const locationAssign = jest.fn();

let settingsState: DesktopMCPState | undefined;
let settingsError: Error | null;
let settingsLoading: boolean;
let integrations: ClientIntegration[] | undefined;
let integrationsError: Error | null;
let integrationsLoading: boolean;
let updatePending: boolean;
let configurePending: boolean;
let configureData: ClientIntegration | undefined;
let configureVariables: { client: ClientKind; replace: boolean } | undefined;

const integration = (
  client: ClientKind,
  status: ClientIntegration['status'],
  overrides: Partial<ClientIntegration> = {}
): ClientIntegration => ({
  client,
  status,
  canConfigure: status !== 'configured' && status !== 'client_not_found',
  manualCommand: `${client} mcp add cy-kaf-client`,
  message: status === 'error' ? 'Client status could not be read.' : '',
  ...overrides,
});

const triggerRef = React.createRef<HTMLButtonElement>();

const renderModal = (isOpen = true, onClose = jest.fn()) =>
  render(
    <SettingsModal isOpen={isOpen} onClose={onClose} triggerRef={triggerRef} />
  );

describe('SettingsModal', () => {
  const originalLocation = window.location;

  beforeAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { assign: locationAssign },
    });
  });

  afterAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: originalLocation,
    });
  });

  beforeEach(() => {
    settingsState = {
      available: true,
      settings: { enabled: true, allowWrites: false },
    };
    settingsError = null;
    settingsLoading = false;
    integrations = [
      integration('codex', 'configured'),
      integration('claude-code', 'configured'),
    ];
    integrationsError = null;
    integrationsLoading = false;
    updatePending = false;
    configurePending = false;
    configureData = undefined;
    configureVariables = undefined;

    settingsRefetch.mockResolvedValue(undefined);
    integrationsRefetch.mockResolvedValue(undefined);
    updateSettings.mockResolvedValue({
      enabled: true,
      allowWrites: false,
    });
    configureClient.mockImplementation(async (request) => {
      configureVariables = request;
      configureData = integration(request.client, 'configured');
      return configureData;
    });
    resetConfigure.mockImplementation(() => {
      configureData = undefined;
      configureVariables = undefined;
    });
    clipboardWriteText.mockResolvedValue(undefined);

    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText: clipboardWriteText },
    });

    mockUseSettings.mockImplementation(
      () =>
        ({
          data: settingsState,
          error: settingsError,
          isLoading: settingsLoading,
          refetch: settingsRefetch,
        }) as unknown as ReturnType<typeof useDesktopMCPSettings>
    );
    mockUseIntegrations.mockImplementation(
      () =>
        ({
          data: integrations,
          error: integrationsError,
          isLoading: integrationsLoading,
          refetch: integrationsRefetch,
        }) as unknown as ReturnType<typeof useDesktopMCPIntegrations>
    );
    mockUseUpdateSettings.mockImplementation(
      () =>
        ({
          mutateAsync: updateSettings,
          isPending: updatePending,
          reset: resetUpdate,
        }) as unknown as ReturnType<typeof useUpdateDesktopMCPSettings>
    );
    mockUseConfigure.mockImplementation(
      () =>
        ({
          mutateAsync: configureClient,
          isPending: configurePending,
          data: configureData,
          variables: configureVariables,
          reset: resetConfigure,
        }) as unknown as ReturnType<typeof useConfigureDesktopMCP>
    );
  });

  it('keeps both sections visible but makes desktop-only controls unavailable', () => {
    settingsState = { available: false };

    renderModal();

    const dialog = screen.getByRole('dialog', { name: 'Settings' });
    expect(within(dialog).getByRole('heading', { name: 'MCP' })).toBeVisible();
    expect(
      within(dialog).getByRole('heading', { name: 'Diagnostics' })
    ).toBeVisible();
    expect(
      within(dialog).getAllByText('Available in the desktop app only.')
    ).toHaveLength(2);
    expect(
      within(dialog).queryByRole('button', { name: /Configure|Copy|Open log/ })
    ).not.toBeInTheDocument();
    expect(within(dialog).queryByRole('checkbox')).not.toBeInTheDocument();
    expect(updateSettings).not.toHaveBeenCalled();
    expect(configureClient).not.toHaveBeenCalled();
    expect(locationAssign).not.toHaveBeenCalled();
  });

  it('enables MCP with the read-only policy and persists write and MCP disablement immediately', async () => {
    settingsState = {
      available: true,
      settings: { enabled: false, allowWrites: false },
    };
    const { rerender } = renderModal();

    await userEvent.click(screen.getByRole('checkbox', { name: 'Enable MCP' }));
    expect(updateSettings).toHaveBeenLastCalledWith({
      enabled: true,
      allowWrites: false,
      confirmWrites: false,
    });

    settingsState = {
      available: true,
      settings: { enabled: true, allowWrites: true },
    };
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );

    await userEvent.click(
      screen.getByRole('checkbox', { name: 'Allow write operations' })
    );
    expect(updateSettings).toHaveBeenLastCalledWith({
      enabled: true,
      allowWrites: false,
      confirmWrites: false,
    });

    await userEvent.click(screen.getByRole('checkbox', { name: 'Enable MCP' }));
    expect(updateSettings).toHaveBeenLastCalledWith({
      enabled: false,
      allowWrites: false,
      confirmWrites: false,
    });
  });

  it('associates both visible policy labels with their switches and actions', async () => {
    settingsState = {
      available: true,
      settings: { enabled: false, allowWrites: false },
    };
    const { rerender } = renderModal();

    const enableSwitch = screen.getByRole('checkbox', { name: 'Enable MCP' });
    expect(enableSwitch).toHaveAttribute(
      'aria-labelledby',
      'desktop-mcp-enabled-label'
    );
    await userEvent.click(
      screen.getByText('Enable MCP', { selector: 'label' })
    );
    expect(updateSettings).toHaveBeenLastCalledWith({
      enabled: true,
      allowWrites: false,
      confirmWrites: false,
    });

    settingsState = {
      available: true,
      settings: { enabled: true, allowWrites: false },
    };
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );
    const writesSwitch = screen.getByRole('checkbox', {
      name: 'Allow write operations',
    });
    expect(writesSwitch).toHaveAttribute(
      'aria-labelledby',
      'desktop-mcp-writes-label'
    );

    await userEvent.click(
      screen.getByText('Allow write operations', { selector: 'label' })
    );

    expect(
      screen.getByRole('dialog', { name: 'Enable write operations?' })
    ).toBeInTheDocument();
  });

  it('keeps only the write confirmation active, focuses it, and restores the switch on cancel', async () => {
    renderModal();

    await userEvent.click(
      screen.getByRole('checkbox', { name: 'Allow write operations' })
    );

    const confirmation = screen.getByRole('dialog', {
      name: 'Enable write operations?',
    });
    expect(screen.getAllByRole('dialog')).toHaveLength(1);
    expect(
      screen.queryByRole('dialog', { name: 'Settings' })
    ).not.toBeInTheDocument();
    await waitFor(() =>
      expect(
        within(confirmation).getByRole('button', { name: 'Cancel' })
      ).toHaveFocus()
    );

    await userEvent.click(
      within(confirmation).getByRole('button', { name: 'Cancel' })
    );

    await waitFor(() =>
      expect(
        screen.getByRole('checkbox', { name: 'Allow write operations' })
      ).toHaveFocus()
    );
    expect(
      screen.getByRole('dialog', { name: 'Settings' })
    ).toBeInTheDocument();
  });

  it('requires confirmation for every write enable and does not optimistically check the switch', async () => {
    let resolveUpdate:
      | ((value: { enabled: boolean; allowWrites: boolean }) => void)
      | undefined;
    updateSettings.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveUpdate = resolve;
        })
    );
    renderModal();

    const writeSwitch = screen.getByRole('checkbox', {
      name: 'Allow write operations',
    });
    await userEvent.click(writeSwitch);

    const confirmation = screen.getByRole('dialog', {
      name: 'Enable write operations?',
    });
    expect(
      within(confirmation).getByText(
        'Write tools can create, modify, or delete Kafka and related service state.'
      )
    ).toBeVisible();
    expect(updateSettings).not.toHaveBeenCalled();
    expect(
      screen.queryByRole('checkbox', { name: 'Allow write operations' })
    ).not.toBeInTheDocument();

    await userEvent.click(
      within(confirmation).getByRole('button', { name: 'Cancel' })
    );
    expect(updateSettings).not.toHaveBeenCalled();
    expect(
      screen.getByRole('checkbox', { name: 'Allow write operations' })
    ).not.toBeChecked();

    await userEvent.click(
      screen.getByRole('checkbox', { name: 'Allow write operations' })
    );
    await userEvent.click(
      within(
        screen.getByRole('dialog', { name: 'Enable write operations?' })
      ).getByRole('button', { name: 'Enable write operations' })
    );
    expect(updateSettings).toHaveBeenCalledWith({
      enabled: true,
      allowWrites: true,
      confirmWrites: true,
    });
    expect(
      screen.queryByRole('checkbox', { name: 'Allow write operations' })
    ).not.toBeInTheDocument();

    await act(async () => {
      resolveUpdate?.({ enabled: true, allowWrites: true });
    });
    expect(
      screen.getByRole('checkbox', { name: 'Allow write operations' })
    ).not.toBeChecked();
  });

  it.each([
    ['client_not_found', 'Client not found', 'Copy manual command'],
    ['not_configured', 'Not configured', 'Configure'],
    ['configured', 'Configured', null],
    ['configuration_conflict', 'Configuration conflict', 'Review and replace'],
    ['repair_required', 'Repair required', 'Repair configuration'],
  ] as const)('maps %s to its status and action', (status, label, action) => {
    integrations = [integration('codex', status)];

    renderModal();

    const client = screen.getByRole('group', { name: 'Codex' });
    expect(within(client).getByText(label)).toBeVisible();
    if (action) {
      expect(
        within(client).getByRole('button', { name: action })
      ).toBeVisible();
    } else {
      expect(within(client).queryByRole('button')).not.toBeInTheDocument();
    }
  });

  it('shows a sanitized client status alert and Retry only refetches integrations', async () => {
    integrations = [integration('codex', 'error')];

    renderModal();

    const client = screen.getByRole('group', { name: 'Codex' });
    const alert = within(client).getByRole('alert');
    expect(alert).toHaveTextContent('Client status could not be read.');
    expect(within(alert).queryByRole('button')).not.toBeInTheDocument();
    await userEvent.click(
      within(client).getByRole('button', { name: 'Retry' })
    );
    expect(integrationsRefetch).toHaveBeenCalledTimes(2);
    expect(configureClient).not.toHaveBeenCalled();
  });

  it('copies only the returned manual command and reports fixed clipboard outcomes', async () => {
    integrations = [integration('codex', 'client_not_found')];

    renderModal();
    await userEvent.click(
      screen.getByRole('button', { name: 'Copy manual command' })
    );

    expect(clipboardWriteText).toHaveBeenCalledWith(
      'codex mcp add cy-kaf-client'
    );
    expect(screen.getByText('Copied')).toBeVisible();
    expect(locationAssign).not.toHaveBeenCalled();

    clipboardWriteText.mockRejectedValueOnce(new Error('private OS failure'));
    await userEvent.click(
      screen.getByRole('button', { name: 'Copy manual command' })
    );
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Unable to copy the manual command.'
    );
    expect(screen.queryByText('private OS failure')).not.toBeInTheDocument();
  });

  it('configures a client without replacement and shows client-specific success', async () => {
    integrations = [
      integration('codex', 'not_configured'),
      integration('claude-code', 'not_configured'),
    ];

    renderModal();
    await userEvent.click(
      within(screen.getByRole('group', { name: 'Codex' })).getByRole('button', {
        name: 'Configure',
      })
    );

    expect(configureClient).toHaveBeenCalledWith({
      client: 'codex',
      replace: false,
    });
    expect(
      screen.getByText('Open a new Codex window to apply this configuration.')
    ).toBeVisible();

    await userEvent.click(
      within(screen.getByRole('group', { name: 'Claude Code' })).getByRole(
        'button',
        { name: 'Configure' }
      )
    );
    expect(configureClient).toHaveBeenLastCalledWith({
      client: 'claude-code',
      replace: false,
    });
    expect(
      screen.getByText(
        'Open a new Claude Code window to apply this configuration.'
      )
    ).toBeVisible();
  });

  it('submits only fixed policy and client selector fields', async () => {
    settingsState = {
      available: true,
      settings: { enabled: false, allowWrites: false },
    };
    integrations = [integration('codex', 'not_configured')];
    const { rerender } = renderModal();

    await userEvent.click(screen.getByRole('checkbox', { name: 'Enable MCP' }));
    const policyPayload =
      updateSettings.mock.calls[updateSettings.mock.calls.length - 1][0];
    expect(Object.keys(policyPayload).sort()).toEqual([
      'allowWrites',
      'confirmWrites',
      'enabled',
    ]);

    settingsState = {
      available: true,
      settings: { enabled: true, allowWrites: false },
    };
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );
    await userEvent.click(screen.getByRole('button', { name: 'Configure' }));
    const clientPayload =
      configureClient.mock.calls[configureClient.mock.calls.length - 1][0];
    expect(Object.keys(clientPayload).sort()).toEqual(['client', 'replace']);

    [policyPayload, clientPayload].forEach((payload) => {
      expect(payload).not.toHaveProperty('executable');
      expect(payload).not.toHaveProperty('path');
      expect(payload).not.toHaveProperty('env');
    });
  });

  it.each(['configuration_conflict', 'repair_required'] as const)(
    'requires a fixed replacement confirmation for %s',
    async (status) => {
      integrations = [integration('codex', status)];
      renderModal();

      await userEvent.click(
        screen.getByRole('button', {
          name:
            status === 'configuration_conflict'
              ? 'Review and replace'
              : 'Repair configuration',
        })
      );

      const confirmation = screen.getByRole('dialog', {
        name: 'Replace existing configuration?',
      });
      expect(
        within(confirmation).getByText(
          'This replaces only the cy-kaf-client MCP server entry in this client.'
        )
      ).toBeVisible();
      await userEvent.click(
        within(confirmation).getByRole('button', { name: 'Cancel' })
      );
      expect(configureClient).not.toHaveBeenCalled();

      await userEvent.click(
        screen.getByRole('button', {
          name:
            status === 'configuration_conflict'
              ? 'Review and replace'
              : 'Repair configuration',
        })
      );
      await userEvent.click(
        within(
          screen.getByRole('dialog', {
            name: 'Replace existing configuration?',
          })
        ).getByRole('button', { name: 'Replace configuration' })
      );
      expect(configureClient).toHaveBeenCalledWith({
        client: 'codex',
        replace: true,
      });
    }
  );

  it('keeps only the replacement confirmation active, focuses it, and restores the originating client action on Escape', async () => {
    integrations = [
      integration('codex', 'configuration_conflict'),
      integration('claude-code', 'configuration_conflict'),
    ];
    renderModal();

    await userEvent.click(
      within(screen.getByRole('group', { name: 'Claude Code' })).getByRole(
        'button',
        { name: 'Review and replace' }
      )
    );

    const confirmation = screen.getByRole('dialog', {
      name: 'Replace existing configuration?',
    });
    expect(screen.getAllByRole('dialog')).toHaveLength(1);
    expect(
      screen.queryByRole('dialog', { name: 'Settings' })
    ).not.toBeInTheDocument();
    await waitFor(() =>
      expect(
        within(confirmation).getByRole('button', { name: 'Cancel' })
      ).toHaveFocus()
    );

    await userEvent.keyboard('{Escape}');

    await waitFor(() =>
      expect(
        within(screen.getByRole('group', { name: 'Claude Code' })).getByRole(
          'button',
          { name: 'Review and replace' }
        )
      ).toHaveFocus()
    );
    expect(
      screen.getByRole('dialog', { name: 'Settings' })
    ).toBeInTheDocument();
  });

  it('disables configuration while MCP is off and while a submission is loading', () => {
    settingsState = {
      available: true,
      settings: { enabled: false, allowWrites: false },
    };
    integrations = [integration('codex', 'not_configured')];
    const { rerender } = renderModal();

    const disabledForPolicy = screen.getByRole('button', { name: 'Configure' });
    expect(disabledForPolicy).toBeDisabled();
    expect(disabledForPolicy).toHaveAttribute(
      'title',
      'Enable MCP before configuring a client.'
    );

    settingsState = {
      available: true,
      settings: { enabled: true, allowWrites: false },
    };
    configurePending = true;
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );
    expect(screen.getByRole('button', { name: 'Configure' })).toBeDisabled();
  });

  it('disables automatic configuration when the client cannot be configured', async () => {
    integrations = [
      integration('codex', 'not_configured', { canConfigure: false }),
    ];
    renderModal();

    const configure = screen.getByRole('button', { name: 'Configure' });
    expect(configure).toBeDisabled();
    expect(configure).toHaveAttribute(
      'title',
      'Automatic configuration is unavailable.'
    );

    await userEvent.click(configure);
    expect(configureClient).not.toHaveBeenCalled();
  });

  it('disables write confirmation actions while pending and blocks duplicate writes', async () => {
    let resolveUpdate:
      | ((value: { enabled: boolean; allowWrites: boolean }) => void)
      | undefined;
    updateSettings.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveUpdate = resolve;
        })
    );
    renderModal();

    await userEvent.click(
      screen.getByRole('checkbox', { name: 'Allow write operations' })
    );
    const confirmation = screen.getByRole('dialog', {
      name: 'Enable write operations?',
    });
    const confirm = within(confirmation).getByRole('button', {
      name: 'Enable write operations',
    });
    await userEvent.dblClick(confirm);

    expect(updateSettings).toHaveBeenCalledTimes(1);
    expect(confirm).toBeDisabled();
    expect(
      within(confirmation).getByRole('button', { name: 'Cancel' })
    ).toBeDisabled();

    await act(async () => {
      resolveUpdate?.({ enabled: true, allowWrites: true });
    });
  });

  it('disables replacement confirmation actions while pending and blocks duplicate configuration', async () => {
    integrations = [integration('codex', 'configuration_conflict')];
    let resolveConfigure: ((value: ClientIntegration) => void) | undefined;
    configureClient.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveConfigure = resolve;
        })
    );
    renderModal();

    await userEvent.click(
      screen.getByRole('button', { name: 'Review and replace' })
    );
    const confirmation = screen.getByRole('dialog', {
      name: 'Replace existing configuration?',
    });
    const replace = within(confirmation).getByRole('button', {
      name: 'Replace configuration',
    });
    await userEvent.dblClick(replace);

    expect(configureClient).toHaveBeenCalledTimes(1);
    expect(replace).toBeDisabled();
    expect(
      within(confirmation).getByRole('button', { name: 'Cancel' })
    ).toBeDisabled();

    await act(async () => {
      resolveConfigure?.(integration('codex', 'configured'));
    });
  });

  it('blocks duplicate configure submits and shows only sanitized mutation errors', async () => {
    integrations = [integration('codex', 'not_configured')];
    let rejectConfigure: ((reason: Error) => void) | undefined;
    configureClient.mockImplementation(
      () =>
        new Promise((_, reject) => {
          rejectConfigure = reject;
        })
    );
    renderModal();

    const configure = screen.getByRole('button', { name: 'Configure' });
    await userEvent.dblClick(configure);
    expect(configureClient).toHaveBeenCalledTimes(1);
    expect(configure).toBeDisabled();

    await act(async () => {
      rejectConfigure?.(new Error('Configuration verification failed.'));
    });
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Configuration verification failed.'
    );
  });

  it('refetches both server sources on every reopen and clears prior success', async () => {
    integrations = [integration('codex', 'not_configured')];

    const Harness = () => {
      const [open, setOpen] = React.useState(false);
      const opener = React.useRef<HTMLButtonElement>(null);
      return (
        <>
          <button ref={opener} type="button" onClick={() => setOpen(true)}>
            Open settings
          </button>
          <SettingsModal
            isOpen={open}
            onClose={() => setOpen(false)}
            triggerRef={opener}
          />
        </>
      );
    };

    render(<Harness />);
    await userEvent.click(
      screen.getByRole('button', { name: 'Open settings' })
    );
    await userEvent.click(screen.getByRole('button', { name: 'Configure' }));
    expect(
      screen.getByText('Open a new Codex window to apply this configuration.')
    ).toBeVisible();
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    await userEvent.click(
      screen.getByRole('button', { name: 'Open settings' })
    );

    expect(settingsRefetch).toHaveBeenCalledTimes(2);
    expect(integrationsRefetch).toHaveBeenCalledTimes(2);
    expect(
      screen.queryByText('Open a new Codex window to apply this configuration.')
    ).not.toBeInTheDocument();
  });

  it('renders the settings loading state and sanitized settings query error', () => {
    settingsState = undefined;
    settingsLoading = true;
    const { rerender } = renderModal();

    expect(screen.getByText('Loading MCP settings...')).toBeVisible();
    expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();

    settingsLoading = false;
    settingsError = new Error('Sanitized settings request failed.');
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(
      'Sanitized settings request failed.'
    );
    expect(
      within(screen.getByRole('alert')).queryByRole('button')
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText('Loading MCP settings...')
    ).not.toBeInTheDocument();
  });

  it('renders integration loading and sanitized query error states whose Retry only refetches', async () => {
    integrations = undefined;
    integrationsLoading = true;
    const { rerender } = renderModal();

    expect(screen.getByText('Loading client integrations...')).toBeVisible();

    integrationsLoading = false;
    integrationsError = new Error('Sanitized integrations request failed.');
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );
    integrationsRefetch.mockClear();

    const alert = screen.getByRole('alert');
    expect(alert).toHaveTextContent('Sanitized integrations request failed.');
    expect(within(alert).queryByRole('button')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(integrationsRefetch).toHaveBeenCalledTimes(1);
    expect(configureClient).not.toHaveBeenCalled();
  });

  it('uses only the fixed log action and handles the fixed opener error while open', async () => {
    const onClose = jest.fn();
    const { unmount } = renderModal(true, onClose);

    await userEvent.click(
      screen.getByRole('button', { name: 'Open log directory' })
    );
    expect(locationAssign).toHaveBeenCalledWith('cy-kaf-action://open-logs');

    act(() => {
      window.dispatchEvent(new Event('cy-kaf-open-logs-error'));
    });
    const diagnostics = screen
      .getByRole('heading', { name: 'Diagnostics' })
      .closest('section');
    expect(
      within(diagnostics as HTMLElement).getByRole('alert')
    ).toHaveTextContent('Unable to open the log directory.');

    unmount();
    act(() => {
      window.dispatchEvent(new Event('cy-kaf-open-logs-error'));
    });
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('removes the exact log error listener when closed without retaining a stale alert', async () => {
    const addListener = jest.spyOn(window, 'addEventListener');
    const removeListener = jest.spyOn(window, 'removeEventListener');
    const { rerender, container } = renderModal();

    expect(container).not.toHaveTextContent(/[\u3400-\u9fff]/);
    const listenerCall = addListener.mock.calls.find(
      ([type]) => type === 'cy-kaf-open-logs-error'
    );
    expect(listenerCall).toBeDefined();

    rerender(
      <SettingsModal
        isOpen={false}
        onClose={jest.fn()}
        triggerRef={triggerRef}
      />
    );
    expect(removeListener).toHaveBeenCalledWith(
      'cy-kaf-open-logs-error',
      listenerCall?.[1]
    );

    act(() => {
      window.dispatchEvent(new Event('cy-kaf-open-logs-error'));
    });
    rerender(
      <SettingsModal isOpen onClose={jest.fn()} triggerRef={triggerRef} />
    );

    expect(
      screen.queryByText('Unable to open the log directory.')
    ).not.toBeInTheDocument();
    await waitFor(() => expect(settingsRefetch).toHaveBeenCalledTimes(2));
  });
});
