import React from 'react';
import { act, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import UpdateSection from 'components/Settings/UpdateSection';
import { render } from 'lib/testHelpers';

const status = (state: string, overrides: Record<string, unknown> = {}) => ({
  available: true,
  currentVersion: '1.2.3',
  status: state,
  downloadedBytes: 0,
  scheduled: false,
  ...overrides,
});

describe('UpdateSection', () => {
  const originalLocation = window.location;
  const locationAssign = jest.fn();

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
    locationAssign.mockClear();
    Object.defineProperty(window, '__CY_KAF_DESKTOP_UPDATES__', {
      configurable: true,
      value: true,
    });
  });

  afterEach(() => {
    Reflect.deleteProperty(window, '__CY_KAF_DESKTOP_UPDATES__');
  });

  it('is hidden outside the desktop host and does not request status', () => {
    Reflect.deleteProperty(window, '__CY_KAF_DESKTOP_UPDATES__');

    render(<UpdateSection active />);

    expect(
      screen.queryByRole('heading', { name: '更新' })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: 'Check for updates' })
    ).not.toBeInTheDocument();
    expect(locationAssign).not.toHaveBeenCalled();
  });

  it('subscribes only while active and requests the current status', () => {
    const { rerender } = render(<UpdateSection active={false} />);
    expect(locationAssign).not.toHaveBeenCalled();

    rerender(<UpdateSection active />);
    expect(locationAssign).toHaveBeenCalledWith(
      'cy-kaf-action://updates-status'
    );

    rerender(<UpdateSection active={false} />);
    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('up_to_date'),
        })
      );
    });
    expect(screen.queryByText('You are up to date.')).not.toBeInTheDocument();
  });

  it('shows the up-to-date notice only once a check confirmed it', () => {
    render(<UpdateSection active />);

    const push = (state: string, overrides: Record<string, unknown> = {}) =>
      act(() => {
        window.dispatchEvent(
          new CustomEvent('cy-kaf-update-status', {
            detail: status(state, overrides),
          })
        );
      });

    push('idle');
    expect(screen.queryByText('You are up to date.')).not.toBeInTheDocument();

    push('checking');
    expect(screen.queryByText('You are up to date.')).not.toBeInTheDocument();

    push('ready', { version: '1.3.0' });
    expect(screen.queryByText('You are up to date.')).not.toBeInTheDocument();

    push('up_to_date');
    expect(screen.getByText('You are up to date.')).toBeVisible();
  });

  it('renders the up-to-date notice in muted green after the check button', () => {
    render(<UpdateSection active />);

    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('up_to_date'),
        })
      );
    });

    expect(screen.getByRole('heading', { name: '更新' })).toBeVisible();
    const notice = screen.getByText('You are up to date.');
    expect(notice).toHaveStyleRule('color', '#29a352');

    const check = screen.getByRole('button', { name: 'Check for updates' });
    expect(check.nextElementSibling).toBe(notice);
  });

  it('uses the native disabled state until update checks are available', () => {
    render(<UpdateSection active />);
    const check = screen.getByRole('button', { name: 'Check for updates' });
    expect(check).toBeDisabled();

    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('unavailable', {
            available: false,
            message: 'Updater is unavailable for this build.',
          }),
        })
      );
    });

    expect(check).toBeDisabled();
  });

  it('updates progress without stealing focus or issuing an action', () => {
    render(
      <div>
        <input aria-label="Unsaved setting" defaultValue="draft" />
        <UpdateSection active />
      </div>
    );
    locationAssign.mockClear();
    const input = screen.getByRole('textbox', { name: 'Unsaved setting' });
    input.focus();

    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('downloading', {
            version: '1.3.0',
            downloadedBytes: 512,
            totalBytes: 1024,
          }),
        })
      );
    });

    expect(input).toHaveValue('draft');
    expect(input).toHaveFocus();
    expect(screen.getByText('Downloading update: 50%')).toBeVisible();
    expect(locationAssign).not.toHaveBeenCalled();
  });

  it('requires inline confirmation before explicitly installing', async () => {
    render(<UpdateSection active />);
    locationAssign.mockClear();
    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('ready', {
            version: '1.3.0',
            notes: 'Security and stability fixes.',
            downloadedBytes: 1024,
            totalBytes: 1024,
          }),
        })
      );
    });

    expect(
      screen.getByRole('button', { name: 'Check for updates' })
    ).toBeDisabled();
    expect(
      screen.getByRole('button', { name: 'Install and restart' })
    ).toBeEnabled();
    expect(
      screen.getByRole('button', { name: 'Install next launch' })
    ).toBeEnabled();

    await userEvent.click(
      screen.getByRole('button', { name: 'Install and restart' })
    );
    expect(locationAssign).not.toHaveBeenCalled();
    const confirmation = screen.getByRole('group', {
      name: 'Confirm update installation',
    });
    expect(
      within(confirmation).getByText(/Security and stability/)
    ).toBeVisible();

    await userEvent.click(
      within(confirmation).getByRole('button', {
        name: 'Confirm install and restart',
      })
    );
    expect(locationAssign).toHaveBeenCalledWith(
      'cy-kaf-action://updates-install'
    );
  });

  it.each([
    status('downloading', {
      version: '1.3.0',
      downloadedBytes: 512,
      totalBytes: 1024,
    }),
    status('ready', { version: '1.4.0', downloadedBytes: 1024 }),
  ])(
    'invalidates install confirmation when the ready package changes %#',
    async (nextStatus) => {
      render(<UpdateSection active />);
      act(() => {
        window.dispatchEvent(
          new CustomEvent('cy-kaf-update-status', {
            detail: status('ready', {
              version: '1.3.0',
              downloadedBytes: 1024,
            }),
          })
        );
      });
      await userEvent.click(
        screen.getByRole('button', { name: 'Install and restart' })
      );

      act(() => {
        window.dispatchEvent(
          new CustomEvent('cy-kaf-update-status', { detail: nextStatus })
        );
      });

      expect(
        screen.queryByRole('group', { name: 'Confirm update installation' })
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole('button', { name: 'Confirm install and restart' })
      ).not.toBeInTheDocument();
      expect(locationAssign).not.toHaveBeenCalledWith(
        'cy-kaf-action://updates-install'
      );
    }
  );

  it('cancels confirmation and restores focus to the install button', async () => {
    render(<UpdateSection active />);
    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('ready', {
            version: '1.3.0',
            downloadedBytes: 1024,
          }),
        })
      );
    });
    const install = screen.getByRole('button', { name: 'Install and restart' });
    await userEvent.click(install);
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(
      screen.getByRole('button', { name: 'Install and restart' })
    ).toHaveFocus();
    expect(locationAssign).not.toHaveBeenCalledWith(
      'cy-kaf-action://updates-install'
    );
  });

  it('schedules and cancels installation using explicit controls', async () => {
    render(<UpdateSection active />);
    locationAssign.mockClear();
    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('ready', { version: '1.3.0', downloadedBytes: 1024 }),
        })
      );
    });

    await userEvent.click(
      screen.getByRole('button', { name: 'Install next launch' })
    );
    expect(locationAssign).toHaveBeenLastCalledWith(
      'cy-kaf-action://updates-schedule'
    );

    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('ready', {
            version: '1.3.0',
            downloadedBytes: 1024,
            scheduled: true,
          }),
        })
      );
    });
    await userEvent.click(
      screen.getByRole('button', { name: 'Cancel scheduled install' })
    );
    expect(locationAssign).toHaveBeenLastCalledWith(
      'cy-kaf-action://updates-cancel-schedule'
    );
  });

  it('shows a ready-state persistence error without navigation or focus change', () => {
    render(<UpdateSection active />);
    locationAssign.mockClear();
    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('ready', {
            version: '1.3.0',
            downloadedBytes: 1024,
            message: 'Unable to save the installation schedule.',
          }),
        })
      );
    });
    const install = screen.getByRole('button', { name: 'Install and restart' });
    install.focus();

    act(() => {
      window.dispatchEvent(
        new CustomEvent('cy-kaf-update-status', {
          detail: status('ready', {
            version: '1.3.0',
            downloadedBytes: 1024,
            message: 'Unable to save the installation schedule. Try again.',
          }),
        })
      );
    });

    expect(
      screen.getByText('Unable to save the installation schedule. Try again.')
    ).toBeVisible();
    expect(install).toHaveFocus();
    expect(locationAssign).not.toHaveBeenCalled();
  });
});
