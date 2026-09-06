import React from 'react';
import { render } from 'lib/testHelpers';
import NavBar from 'components/NavBar/NavBar';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  useConfigureDesktopMCP,
  useDesktopMCPIntegrations,
  useDesktopMCPSettings,
  useUpdateDesktopMCPSettings,
} from 'lib/hooks/api/desktopMcp';

jest.mock('components/Version/Version', () => () => (
  <a href="/commit/mock-revision">Version</a>
));
jest.mock('components/NavBar/UserInfo/UserInfo', () => () => (
  <div>UserInfo</div>
));
jest.mock('lib/hooks/api/desktopMcp');

const mockUseSettings = jest.mocked(useDesktopMCPSettings);
const mockUseIntegrations = jest.mocked(useDesktopMCPIntegrations);
const mockUseUpdateSettings = jest.mocked(useUpdateDesktopMCPSettings);
const mockUseConfigure = jest.mocked(useConfigureDesktopMCP);

describe('NavBar', () => {
  const onBurgerClick = jest.fn();

  beforeEach(() => {
    onBurgerClick.mockClear();
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: jest.fn().mockImplementation(() => ({
        matches: false,
        addListener: jest.fn(),
      })),
    });

    mockUseSettings.mockReturnValue({
      data: { available: false },
      refetch: jest.fn(),
    } as unknown as ReturnType<typeof useDesktopMCPSettings>);
    mockUseIntegrations.mockReturnValue({
      data: [],
      refetch: jest.fn(),
    } as unknown as ReturnType<typeof useDesktopMCPIntegrations>);
    mockUseUpdateSettings.mockReturnValue({
      mutateAsync: jest.fn(),
      reset: jest.fn(),
    } as unknown as ReturnType<typeof useUpdateDesktopMCPSettings>);
    mockUseConfigure.mockReturnValue({
      mutateAsync: jest.fn(),
      reset: jest.fn(),
    } as unknown as ReturnType<typeof useConfigureDesktopMCP>);

    render(<NavBar onBurgerClick={onBurgerClick} />);
  });

  it('exposes a named sidebar toggle that invokes the burger callback', async () => {
    const header = screen.getByRole('navigation', { name: 'Page Header' });
    const sidebarToggle = within(header).getByRole('button', {
      name: 'Toggle sidebar',
    });

    await userEvent.click(sidebarToggle);

    expect(onBurgerClick).toHaveBeenCalledTimes(1);
  });

  it('renders only the retained header controls', () => {
    const header = screen.getByLabelText('Page Header');

    expect(header).toBeInTheDocument();
    expect(within(header).getByText('Cy KafClient')).toBeInTheDocument();
    expect(within(header).getByText('UserInfo')).toBeInTheDocument();
    expect(within(header).queryByText(/^cy$/)).not.toBeInTheDocument();
    expect(
      header.querySelector('a[href*="github.com"]')
    ).not.toBeInTheDocument();
    expect(header.querySelector('a[href*="/commit/"]')).not.toBeInTheDocument();
  });

  it('places the keyboard-focusable Settings control after Theme and before UserInfo', async () => {
    const header = screen.getByLabelText('Page Header');
    const timezone = within(header).getByRole('button', {
      name: 'user-timezone-dropdown',
    });
    const theme = within(header).getByRole('listbox');
    const settings = within(header).getByRole('button', { name: 'Settings' });
    const userInfo = within(header).getByText('UserInfo');

    expect(timezone.compareDocumentPosition(theme)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING
    );
    expect(theme.compareDocumentPosition(settings)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING
    );
    expect(settings.compareDocumentPosition(userInfo)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING
    );
    expect(settings).toHaveTextContent('Settings');
    expect(settings.querySelector('svg')).toBeInTheDocument();

    settings.focus();
    expect(settings).toHaveFocus();
  });

  it('opens one named Settings dialog and closes it back to the trigger', async () => {
    const settings = screen.getByRole('button', { name: 'Settings' });

    await userEvent.click(settings);
    expect(screen.getAllByRole('dialog', { name: 'Settings' })).toHaveLength(1);

    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(
      screen.queryByRole('dialog', { name: 'Settings' })
    ).not.toBeInTheDocument();
    expect(settings).toHaveFocus();
  });
});
