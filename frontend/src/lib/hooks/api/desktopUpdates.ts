import React from 'react';

export type DesktopUpdateStatusName =
  | 'idle'
  | 'checking'
  | 'downloading'
  | 'ready'
  | 'up_to_date'
  | 'error'
  | 'installing'
  | 'unavailable';

export type DesktopUpdateStatus = {
  available: boolean;
  currentVersion: string;
  status: DesktopUpdateStatusName;
  version?: string;
  notes?: string;
  downloadedBytes: number;
  totalBytes?: number;
  scheduled: boolean;
  message?: string;
};

export type DesktopUpdateAction =
  | 'updates-status'
  | 'updates-check'
  | 'updates-install'
  | 'updates-schedule'
  | 'updates-cancel-schedule';

const statusNames: DesktopUpdateStatusName[] = [
  'idle',
  'checking',
  'downloading',
  'ready',
  'up_to_date',
  'error',
  'installing',
  'unavailable',
];

const isOptionalString = (value: unknown) =>
  value === undefined || typeof value === 'string';

export const isDesktopUpdateHost = () =>
  Reflect.get(window, '__CY_KAF_DESKTOP_UPDATES__') === true;

export const readDesktopUpdateStatus = (
  value: unknown
): DesktopUpdateStatus | null => {
  if (!value || typeof value !== 'object') return null;
  const candidate = value as Record<string, unknown>;
  if (
    typeof candidate.available !== 'boolean' ||
    typeof candidate.currentVersion !== 'string' ||
    !statusNames.includes(candidate.status as DesktopUpdateStatusName) ||
    typeof candidate.downloadedBytes !== 'number' ||
    !Number.isFinite(candidate.downloadedBytes) ||
    candidate.downloadedBytes < 0 ||
    (candidate.totalBytes !== undefined &&
      (typeof candidate.totalBytes !== 'number' ||
        !Number.isFinite(candidate.totalBytes) ||
        candidate.totalBytes < 0)) ||
    typeof candidate.scheduled !== 'boolean' ||
    !isOptionalString(candidate.version) ||
    !isOptionalString(candidate.notes) ||
    !isOptionalString(candidate.message)
  ) {
    return null;
  }
  return candidate as DesktopUpdateStatus;
};

export const subscribeToDesktopUpdates = (
  listener: (status: DesktopUpdateStatus) => void
) => {
  const onStatus = (event: Event) => {
    const status = readDesktopUpdateStatus(
      (event as CustomEvent<unknown>).detail
    );
    if (status) listener(status);
  };
  window.addEventListener('cy-kaf-update-status', onStatus);
  return () => window.removeEventListener('cy-kaf-update-status', onStatus);
};

export const sendDesktopUpdateAction = (action: DesktopUpdateAction) => {
  window.location.assign(`cy-kaf-action://${action}`);
};

export const useDesktopUpdates = (active: boolean) => {
  const hosted = isDesktopUpdateHost();
  const [status, setStatus] = React.useState<DesktopUpdateStatus | null>(null);

  React.useEffect(() => {
    if (!active || !hosted) return undefined;
    const unsubscribe = subscribeToDesktopUpdates(setStatus);
    sendDesktopUpdateAction('updates-status');
    return unsubscribe;
  }, [active, hosted]);

  return { hosted, status };
};
