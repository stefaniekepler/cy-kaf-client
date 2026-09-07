import {
  isDesktopUpdateHost,
  readDesktopUpdateStatus,
  subscribeToDesktopUpdates,
} from 'lib/hooks/api/desktopUpdates';

const readyStatus = {
  available: true,
  currentVersion: '1.2.3',
  status: 'ready',
  version: '1.3.0',
  notes: 'A safe update.',
  downloadedBytes: 2048,
  totalBytes: 2048,
  scheduled: false,
};

describe('desktopUpdates', () => {
  afterEach(() => {
    Reflect.deleteProperty(window, '__CY_KAF_DESKTOP_UPDATES__');
  });

  it('recognizes only a Tauri-hosted page as a desktop update host', () => {
    expect(isDesktopUpdateHost()).toBe(false);

    Object.defineProperty(window, '__CY_KAF_DESKTOP_UPDATES__', {
      configurable: true,
      value: true,
    });

    expect(isDesktopUpdateHost()).toBe(true);
  });

  it('accepts the complete native status contract', () => {
    expect(readDesktopUpdateStatus(readyStatus)).toEqual(readyStatus);
  });

  it.each([
    null,
    { ...readyStatus, status: 'complete' },
    { ...readyStatus, downloadedBytes: -1 },
    { ...readyStatus, scheduled: 'yes' },
    { ...readyStatus, version: 4 },
  ])('rejects malformed native status payload %#', (payload) => {
    expect(readDesktopUpdateStatus(payload)).toBeNull();
  });

  it('ignores malformed events while delivering valid status events', () => {
    const listener = jest.fn();
    const unsubscribe = subscribeToDesktopUpdates(listener);

    window.dispatchEvent(
      new CustomEvent('cy-kaf-update-status', {
        detail: { ...readyStatus, status: 'complete' },
      })
    );
    window.dispatchEvent(
      new CustomEvent('cy-kaf-update-status', { detail: readyStatus })
    );

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledWith(readyStatus);

    unsubscribe();
    window.dispatchEvent(
      new CustomEvent('cy-kaf-update-status', { detail: readyStatus })
    );
    expect(listener).toHaveBeenCalledTimes(1);
  });
});
