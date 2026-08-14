import React from 'react';
import Alert from 'components/common/Alert/Alert';
import { Button } from 'components/common/Button/Button';
import Modal from 'components/common/Modal/Modal';
import Switch from 'components/common/Switch/Switch';
import {
  ClientIntegration,
  ClientKind,
  useConfigureDesktopMCP,
  useDesktopMCPIntegrations,
  useDesktopMCPSettings,
  useUpdateDesktopMCPSettings,
} from 'lib/hooks/api/desktopMcp';
import { useImportConfig } from 'lib/hooks/api/appConfig';

import * as S from './SettingsModal.styled';

type Props = {
  isOpen: boolean;
  onClose: () => void;
  triggerRef: React.RefObject<HTMLButtonElement>;
};

type ModalError = {
  section: 'mcp' | 'diagnostics';
  message: string;
};

type WriteConfirmationState = false | 'confirming' | 'submitting';

const clientNames: Record<ClientKind, string> = {
  codex: 'Codex',
  'claude-code': 'Claude Code',
};

const statusLabels: Record<ClientIntegration['status'], string> = {
  client_not_found: 'Client not found',
  not_configured: 'Not configured',
  configured: 'Configured',
  configuration_conflict: 'Configuration conflict',
  repair_required: 'Repair required',
  error: 'Error',
};

const actionLabels: Partial<Record<ClientIntegration['status'], string>> = {
  client_not_found: 'Copy manual command',
  not_configured: 'Configure',
  configuration_conflict: 'Review and replace',
  repair_required: 'Repair configuration',
  error: 'Retry',
};

const safeMutationMessage = (error: unknown) =>
  error instanceof Error ? error.message : 'Desktop MCP request failed.';

const SettingsModal: React.FC<Props> = ({ isOpen, onClose, triggerRef }) => {
  const settingsQuery = useDesktopMCPSettings();
  const available = settingsQuery.data?.available === true;
  const settings = settingsQuery.data?.available
    ? settingsQuery.data.settings
    : undefined;
  const integrationsQuery = useDesktopMCPIntegrations(isOpen && available);
  const updateMutation = useUpdateDesktopMCPSettings();
  const configureMutation = useConfigureDesktopMCP();
  const importMutation = useImportConfig();
  const configFileInputRef = React.useRef<HTMLInputElement>(null);

  const [confirmingWrites, setConfirmingWrites] =
    React.useState<WriteConfirmationState>(false);
  const [confirmingClientReplacement, setConfirmingClientReplacement] =
    React.useState<ClientKind | null>(null);
  const [submittingClient, setSubmittingClient] =
    React.useState<ClientKind | null>(null);
  const [copiedClient, setCopiedClient] = React.useState<ClientKind | null>(
    null
  );
  const [error, setError] = React.useState<ModalError | null>(null);
  const wasOpen = React.useRef(false);
  const writesSwitchRef = React.useRef<HTMLInputElement>(null);
  const writesCancelRef = React.useRef<HTMLButtonElement>(null);
  const replacementCancelRef = React.useRef<HTMLButtonElement>(null);
  const codexActionRef = React.useRef<HTMLButtonElement>(null);
  const claudeCodeActionRef = React.useRef<HTMLButtonElement>(null);
  const clientActionRefs: Record<
    ClientKind,
    React.RefObject<HTMLButtonElement>
  > = {
    codex: codexActionRef,
    'claude-code': claudeCodeActionRef,
  };

  const clearTransientState = React.useCallback(() => {
    setConfirmingWrites(false);
    setConfirmingClientReplacement(null);
    setSubmittingClient(null);
    setCopiedClient(null);
    setError(null);
  }, []);

  React.useEffect(() => {
    if (isOpen && !wasOpen.current) {
      configureMutation.reset();
      updateMutation.reset();
      settingsQuery.refetch();
      if (available) integrationsQuery.refetch();
    } else if (!isOpen && wasOpen.current) {
      clearTransientState();
      configureMutation.reset();
      updateMutation.reset();
    }
    wasOpen.current = isOpen;
  }, [
    available,
    clearTransientState,
    configureMutation,
    integrationsQuery,
    isOpen,
    settingsQuery,
    updateMutation,
  ]);

  React.useEffect(() => {
    if (!isOpen) return undefined;

    const onOpenLogsError = () => {
      setError({
        section: 'diagnostics',
        message: 'Unable to open the log directory.',
      });
    };
    window.addEventListener('cy-kaf-open-logs-error', onOpenLogsError);
    return () =>
      window.removeEventListener('cy-kaf-open-logs-error', onOpenLogsError);
  }, [isOpen]);

  const persistSettings = async (
    next: { enabled: boolean; allowWrites: boolean },
    confirmWrites: boolean
  ) => {
    setError(null);
    try {
      await updateMutation.mutateAsync({ ...next, confirmWrites });
    } catch (mutationError) {
      setError({
        section: 'mcp',
        message: safeMutationMessage(mutationError),
      });
    }
  };

  const toggleMCP = () => {
    if (!settings || updateMutation.isPending) return;
    if (settings.enabled) {
      persistSettings({ enabled: false, allowWrites: false }, false);
      return;
    }
    persistSettings({ enabled: true, allowWrites: false }, false);
  };

  const toggleWrites = () => {
    if (!settings?.enabled || updateMutation.isPending) return;
    if (settings.allowWrites) {
      persistSettings({ enabled: true, allowWrites: false }, false);
      return;
    }
    setConfirmingWrites('confirming');
  };

  const confirmWrites = async () => {
    if (confirmingWrites === 'submitting' || updateMutation.isPending) return;
    setConfirmingWrites('submitting');
    setError(null);
    try {
      await updateMutation.mutateAsync({
        enabled: true,
        allowWrites: true,
        confirmWrites: true,
      });
      setConfirmingWrites(false);
    } catch (mutationError) {
      setError({
        section: 'mcp',
        message: safeMutationMessage(mutationError),
      });
      setConfirmingWrites(false);
    }
  };

  const submitClient = async (client: ClientKind, replace: boolean) => {
    if (
      !settings?.enabled ||
      submittingClient !== null ||
      configureMutation.isPending
    ) {
      return;
    }
    setSubmittingClient(client);
    setCopiedClient(null);
    setError(null);
    configureMutation.reset();
    try {
      await configureMutation.mutateAsync({ client, replace });
      setConfirmingClientReplacement(null);
    } catch (mutationError) {
      setError({
        section: 'mcp',
        message: safeMutationMessage(mutationError),
      });
      setConfirmingClientReplacement(null);
    } finally {
      setSubmittingClient(null);
    }
  };

  const copyManualCommand = async (client: ClientIntegration) => {
    setCopiedClient(null);
    setError(null);
    try {
      await navigator.clipboard.writeText(client.manualCommand);
      setCopiedClient(client.client);
    } catch {
      setError({
        section: 'mcp',
        message: 'Unable to copy the manual command.',
      });
    }
  };

  const renderClientAction = (client: ClientIntegration) => {
    const action = actionLabels[client.status];
    if (!action) return null;

    if (client.status === 'error') {
      return (
        <Button
          ref={clientActionRefs[client.client]}
          buttonType="secondary"
          buttonSize="S"
          onClick={() => integrationsQuery.refetch()}
        >
          {action}
        </Button>
      );
    }

    if (client.status === 'client_not_found') {
      return (
        <Button
          ref={clientActionRefs[client.client]}
          buttonType="secondary"
          buttonSize="S"
          onClick={() => copyManualCommand(client)}
        >
          {action}
        </Button>
      );
    }

    const disabled =
      !settings?.enabled || !client.canConfigure || configureMutation.isPending;
    let disabledReason: string | undefined;
    if (!settings?.enabled) {
      disabledReason = 'Enable MCP before configuring a client.';
    } else if (!client.canConfigure) {
      disabledReason = 'Automatic configuration is unavailable.';
    }
    const replace =
      client.status === 'configuration_conflict' ||
      client.status === 'repair_required';

    return (
      <Button
        ref={clientActionRefs[client.client]}
        buttonType="secondary"
        buttonSize="S"
        aria-label={action}
        disabled={disabled}
        inProgress={submittingClient === client.client}
        title={disabledReason}
        onClick={() => {
          if (replace) {
            setConfirmingClientReplacement(client.client);
          } else {
            submitClient(client.client, false);
          }
        }}
      >
        {action}
      </Button>
    );
  };

  const successClient =
    configureMutation.data?.status === 'configured'
      ? configureMutation.variables?.client
      : undefined;
  const confirmationOpen =
    confirmingWrites !== false || confirmingClientReplacement !== null;
  const writesPending =
    confirmingWrites === 'submitting' || updateMutation.isPending;
  const closeWritesConfirmation = () => {
    if (!writesPending) setConfirmingWrites(false);
  };
  const closeReplacementConfirmation = () => {
    if (submittingClient === null) setConfirmingClientReplacement(null);
  };

  return (
    <>
      <Modal
        isOpen={isOpen && !confirmationOpen}
        onClose={onClose}
        title="Settings"
        restoreFocusRef={triggerRef}
        maxWidth="640px"
        footer={
          <Button buttonType="secondary" buttonSize="M" onClick={onClose}>
            Close
          </Button>
        }
      >
        <S.Content>
          <S.Section>
            <S.SectionHeading>MCP</S.SectionHeading>
            {settingsQuery.isLoading && <p>Loading MCP settings...</p>}
            {settingsQuery.error && (
              <S.Alerts>
                <Alert
                  title="Settings error"
                  type="error"
                  message={settingsQuery.error.message}
                />
              </S.Alerts>
            )}
            {settingsQuery.data?.available === false && (
              <S.Limitation>Available in the desktop app only.</S.Limitation>
            )}
            {settings && (
              <>
                <S.Controls>
                  <S.ControlRow>
                    <S.ControlLabel
                      id="desktop-mcp-enabled-label"
                      htmlFor="desktop-mcp-enabled"
                    >
                      Enable MCP
                    </S.ControlLabel>
                    <Switch
                      id="desktop-mcp-enabled"
                      name="desktop-mcp-enabled"
                      ariaLabel="Enable MCP"
                      ariaLabelledBy="desktop-mcp-enabled-label"
                      checked={settings.enabled}
                      disabled={updateMutation.isPending}
                      onChange={toggleMCP}
                    />
                  </S.ControlRow>
                  <S.ControlRow>
                    <S.ControlLabel
                      id="desktop-mcp-writes-label"
                      htmlFor="desktop-mcp-writes"
                    >
                      Allow write operations
                    </S.ControlLabel>
                    <Switch
                      ref={writesSwitchRef}
                      id="desktop-mcp-writes"
                      name="desktop-mcp-writes"
                      ariaLabel="Allow write operations"
                      ariaLabelledBy="desktop-mcp-writes-label"
                      checked={settings.allowWrites}
                      disabled={!settings.enabled || updateMutation.isPending}
                      onChange={toggleWrites}
                    />
                  </S.ControlRow>
                </S.Controls>

                {integrationsQuery.isLoading && (
                  <p>Loading client integrations...</p>
                )}
                {integrationsQuery.error && (
                  <S.Alerts>
                    <Alert
                      title="Client integrations error"
                      type="error"
                      message={integrationsQuery.error.message}
                    />
                    <S.Actions>
                      <Button
                        buttonType="secondary"
                        buttonSize="S"
                        onClick={() => integrationsQuery.refetch()}
                      >
                        Retry
                      </Button>
                    </S.Actions>
                  </S.Alerts>
                )}
                {integrationsQuery.data && (
                  <S.ClientList>
                    {integrationsQuery.data.map((client) => (
                      <S.Client
                        key={client.client}
                        role="group"
                        aria-label={clientNames[client.client]}
                      >
                        <S.ClientHeader>
                          <S.ClientName>
                            {clientNames[client.client]}
                          </S.ClientName>
                          <S.Status>{statusLabels[client.status]}</S.Status>
                        </S.ClientHeader>
                        {client.status === 'error' && (
                          <S.Alerts>
                            <Alert
                              title="Client error"
                              type="error"
                              message={client.message}
                            />
                          </S.Alerts>
                        )}
                        <S.Actions>
                          {renderClientAction(client)}
                          {copiedClient === client.client && (
                            <span>Copied</span>
                          )}
                        </S.Actions>
                        {successClient === client.client && (
                          <p>
                            Open a new {clientNames[client.client]} window to
                            apply this configuration.
                          </p>
                        )}
                      </S.Client>
                    ))}
                  </S.ClientList>
                )}
              </>
            )}
            {error?.section === 'mcp' && (
              <S.Alerts>
                <Alert
                  title="Error"
                  type="error"
                  message={error.message}
                  onDissmiss={() => setError(null)}
                />
              </S.Alerts>
            )}
          </S.Section>

          <S.Section>
            <S.SectionHeading>Configuration</S.SectionHeading>
            {!available ? (
              <S.Limitation>Available in the desktop app only.</S.Limitation>
            ) : (
              <>
                <S.Actions>
                  <Button
                    buttonType="secondary"
                    buttonSize="M"
                    onClick={() =>
                      window.location.assign('cy-kaf-action://reveal-config')
                    }
                  >
                    Export configuration
                  </Button>
                  <Button
                    buttonType="secondary"
                    buttonSize="M"
                    disabled={importMutation.isPending}
                    inProgress={importMutation.isPending}
                    onClick={() => configFileInputRef.current?.click()}
                  >
                    Import configuration
                  </Button>
                  <input
                    ref={configFileInputRef}
                    type="file"
                    accept=".yaml,.yml"
                    style={{ display: 'none' }}
                    onChange={(e) => {
                      const file = e.target.files?.[0];
                      if (!file) return;
                      const form = new FormData();
                      form.append('file', file);
                      importMutation.mutateAsync(form).catch(() => undefined);
                    }}
                  />
                </S.Actions>
                {importMutation.isError && (
                  <S.Alerts>
                    <Alert
                      title="Import failed"
                      type="error"
                      message={safeMutationMessage(importMutation.error)}
                    />
                  </S.Alerts>
                )}
              </>
            )}
          </S.Section>

          <S.Section>
            <S.SectionHeading>Diagnostics</S.SectionHeading>
            {settingsQuery.data?.available === false ? (
              <S.Limitation>Available in the desktop app only.</S.Limitation>
            ) : (
              available && (
                <>
                  <Button
                    buttonType="secondary"
                    buttonSize="M"
                    onClick={() =>
                      window.location.assign('cy-kaf-action://open-logs')
                    }
                  >
                    Open log directory
                  </Button>
                  {error?.section === 'diagnostics' && (
                    <S.Alerts>
                      <Alert
                        title="Diagnostics error"
                        type="error"
                        message={error.message}
                        onDissmiss={() => setError(null)}
                      />
                    </S.Alerts>
                  )}
                </>
              )
            )}
          </S.Section>
        </S.Content>
      </Modal>

      <Modal
        isOpen={isOpen && confirmingWrites !== false}
        onClose={closeWritesConfirmation}
        title="Enable write operations?"
        initialFocusRef={writesCancelRef}
        restoreFocusRef={writesSwitchRef}
        closeOnEscape={!writesPending}
        maxWidth="480px"
        footer={
          <S.ConfirmationActions>
            <Button
              ref={writesCancelRef}
              buttonType="secondary"
              buttonSize="M"
              disabled={writesPending}
              onClick={closeWritesConfirmation}
            >
              Cancel
            </Button>
            <Button
              buttonType="primary"
              buttonSize="M"
              inProgress={writesPending}
              onClick={() => confirmWrites()}
            >
              Enable write operations
            </Button>
          </S.ConfirmationActions>
        }
      >
        <S.ConfirmationCopy>
          Write tools can create, modify, or delete Kafka and related service
          state.
        </S.ConfirmationCopy>
      </Modal>

      <Modal
        isOpen={isOpen && confirmingClientReplacement !== null}
        onClose={closeReplacementConfirmation}
        title="Replace existing configuration?"
        initialFocusRef={replacementCancelRef}
        restoreFocusRef={
          confirmingClientReplacement
            ? clientActionRefs[confirmingClientReplacement]
            : undefined
        }
        closeOnEscape={submittingClient === null}
        maxWidth="480px"
        footer={
          <S.ConfirmationActions>
            <Button
              ref={replacementCancelRef}
              buttonType="secondary"
              buttonSize="M"
              disabled={submittingClient !== null}
              onClick={closeReplacementConfirmation}
            >
              Cancel
            </Button>
            <Button
              buttonType="primary"
              buttonSize="M"
              inProgress={submittingClient !== null}
              onClick={() => {
                if (confirmingClientReplacement) {
                  submitClient(confirmingClientReplacement, true);
                }
              }}
            >
              Replace configuration
            </Button>
          </S.ConfirmationActions>
        }
      >
        <S.ConfirmationCopy>
          This replaces only the cy-kaf-client MCP server entry in this client.
        </S.ConfirmationCopy>
      </Modal>
    </>
  );
};

export default SettingsModal;
