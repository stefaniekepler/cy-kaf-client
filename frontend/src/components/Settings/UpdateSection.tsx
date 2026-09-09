import React from 'react';
import { Button } from 'components/common/Button/Button';
import {
  sendDesktopUpdateAction,
  useDesktopUpdates,
} from 'lib/hooks/api/desktopUpdates';

import * as S from './SettingsModal.styled';

type Props = { active: boolean };

const UpdateSection: React.FC<Props> = ({ active }) => {
  const { hosted, status } = useDesktopUpdates(active);
  const [confirmingVersion, setConfirmingVersion] = React.useState<
    string | null
  >(null);
  const installRef = React.useRef<HTMLButtonElement>(null);
  const cancelRef = React.useRef<HTMLButtonElement>(null);
  const restoreInstallFocus = React.useRef(false);

  const ready = status?.available === true && status.status === 'ready';
  const confirmationValid =
    confirmingVersion !== null && ready && status.version === confirmingVersion;

  React.useEffect(() => {
    if (confirmationValid) cancelRef.current?.focus();
    else if (restoreInstallFocus.current) {
      restoreInstallFocus.current = false;
      installRef.current?.focus();
    }
  }, [confirmationValid]);

  React.useEffect(() => {
    if (confirmingVersion !== null && !confirmationValid) {
      setConfirmingVersion(null);
    }
  }, [confirmationValid, confirmingVersion]);

  React.useEffect(() => {
    if (!active) setConfirmingVersion(null);
  }, [active]);

  if (!hosted) return null;

  const busy =
    status?.status === 'checking' ||
    status?.status === 'downloading' ||
    status?.status === 'installing';
  const progress =
    status?.status === 'downloading' &&
    status.totalBytes !== undefined &&
    status.totalBytes > 0
      ? Math.min(
          100,
          Math.floor((status.downloadedBytes / status.totalBytes) * 100)
        )
      : null;

  const cancelInstall = () => {
    restoreInstallFocus.current = true;
    setConfirmingVersion(null);
  };

  return (
    <S.Section>
      <S.SectionHeading>更新</S.SectionHeading>
      {!status && <p>Loading update status...</p>}
      {status?.status === 'idle' && <p>Version {status.currentVersion}</p>}
      {status?.status === 'checking' && <p>Checking for updates...</p>}
      {status?.status === 'downloading' && (
        <p>
          {progress === null
            ? 'Downloading update...'
            : `Downloading update: ${progress}%`}
        </p>
      )}
      {status?.status === 'installing' && <p>Installing update...</p>}
      {(status?.status === 'unavailable' || status?.available === false) && (
        <p>{status.message || 'Updates are unavailable.'}</p>
      )}
      {status?.status === 'error' && (
        <p>{status.message || 'The update check failed.'}</p>
      )}
      {ready && (
        <>
          <p>Version {status.version} is ready to install.</p>
          {status.notes && <p>{status.notes}</p>}
          {status.message && <p>{status.message}</p>}
        </>
      )}

      {confirmationValid ? (
        <div role="group" aria-label="Confirm update installation">
          <p>Install version {status?.version} and restart the application?</p>
          {status?.notes && <p>{status.notes}</p>}
          <S.Actions>
            <Button
              ref={cancelRef}
              buttonType="secondary"
              buttonSize="S"
              onClick={cancelInstall}
            >
              Cancel
            </Button>
            <Button
              buttonType="primary"
              buttonSize="S"
              onClick={() => {
                if (
                  status?.available === true &&
                  status.status === 'ready' &&
                  status.version === confirmingVersion
                ) {
                  sendDesktopUpdateAction('updates-install');
                }
              }}
            >
              Confirm install and restart
            </Button>
          </S.Actions>
        </div>
      ) : (
        <S.Actions>
          <Button
            buttonType="secondary"
            buttonSize="S"
            disabled={!status?.available || busy || ready}
            onClick={() => sendDesktopUpdateAction('updates-check')}
          >
            Check for updates
          </Button>
          {status?.status === 'up_to_date' && (
            <S.UpToDate>You are up to date.</S.UpToDate>
          )}
          {ready && (
            <>
              <Button
                ref={installRef}
                buttonType="primary"
                buttonSize="S"
                onClick={() => setConfirmingVersion(status.version || null)}
              >
                Install and restart
              </Button>
              <Button
                buttonType="secondary"
                buttonSize="S"
                onClick={() =>
                  sendDesktopUpdateAction(
                    status.scheduled
                      ? 'updates-cancel-schedule'
                      : 'updates-schedule'
                  )
                }
              >
                {status.scheduled
                  ? 'Cancel scheduled install'
                  : 'Install next launch'}
              </Button>
            </>
          )}
        </S.Actions>
      )}
    </S.Section>
  );
};

export default UpdateSection;
