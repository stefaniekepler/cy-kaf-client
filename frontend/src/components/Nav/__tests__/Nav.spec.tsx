import React from 'react';
import Nav, { NavProps } from 'components/Nav/Nav';
import { act, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useLocation } from 'react-router-dom';
import { render } from 'lib/testHelpers';
import { Cluster } from 'generated-sources';
import { useClusters } from 'lib/hooks/api/clusters';
import { theme } from 'theme/theme';
import {
  offlineClusterPayload,
  onlineClusterPayload,
} from 'lib/fixtures/clusters';

jest.mock('lib/hooks/api/clusters', () => ({
  useClusters: jest.fn(),
}));

const LocationProbe = () => {
  const location = useLocation();
  return <output data-testid="location">{location.pathname}</output>;
};

describe('Nav', () => {
  const getComponent = (props: NavProps = {}) => (
    <>
      <Nav {...props} />
      <LocationProbe />
    </>
  );

  const renderComponent = (
    payload: Cluster[] = [],
    initialEntry = '/',
    isSuccess = true,
    props: NavProps = {}
  ) => {
    (useClusters as jest.Mock).mockImplementation(() => ({
      data: payload,
      isSuccess,
    }));
    return render(getComponent(props), { initialEntries: [initialEntry] });
  };

  const getAllClustersLink = () =>
    screen.getByRole('link', { name: 'All clusters' });

  const getAllClustersReminder = () =>
    screen
      .queryAllByRole('status')
      .find((element) => element.getAttribute('aria-live') === 'polite');

  const getMenuItemsCount = () => screen.getAllByRole('menuitem').length;

  it('keeps the global navigation visible while clusters are unavailable', () => {
    renderComponent([], '/', false);

    const clustersLabel = screen.getByText('Clusters');

    expect(getMenuItemsCount()).toEqual(1);
    expect(screen.queryByText('Overview')).not.toBeInTheDocument();
    expect(clustersLabel).toBeVisible();
    expect(clustersLabel).toHaveStyle({
      color: theme.menu.primary.color.active,
    });
    expect(getAllClustersLink()).toHaveAttribute('href', '/');
  });

  it('keeps global navigation outside the independently scrolling cluster list', () => {
    renderComponent([onlineClusterPayload, offlineClusterPayload]);

    const globalNavigation = getAllClustersLink().closest('ul')?.parentElement;
    const clusterNavigation = screen.getByText('Clusters').parentElement;

    expect(globalNavigation).toBeInTheDocument();
    expect(globalNavigation).toContainElement(getAllClustersLink());
    expect(globalNavigation).not.toContainElement(screen.getByText('Clusters'));
    expect(globalNavigation).toHaveStyleRule('flex', '0 0 auto');
    expect(globalNavigation).toHaveStyleRule(
      'background-color',
      theme.default.backgroundColor
    );
    expect(clusterNavigation).toHaveStyleRule('min-height', '0');
    expect(clusterNavigation).toHaveStyleRule('overflow-y', 'auto');
  });

  it('renders a decorative theme-colored overview glyph', () => {
    renderComponent();

    const icon = getAllClustersLink().querySelector('svg');

    expect(icon).toHaveAttribute('aria-hidden', 'true');
    expect(icon).toHaveAttribute('viewBox', '0 0 16 16');
    expect(icon?.querySelectorAll('[fill="currentColor"]')).toHaveLength(4);
  });

  it.each(['/', '/ui', '/ui/', '/ui/clusters', '/ui/clusters/'])(
    'marks All clusters active for the %s alias',
    (initialEntry) => {
      renderComponent([], initialEntry);

      const link = getAllClustersLink();

      expect(link).toHaveAttribute('aria-current', 'page');
      expect(within(link).getByRole('menuitem')).toHaveStyle({
        color: theme.menu.primary.color.active,
      });
    }
  );

  it('does not mark All clusters active on a cluster resource page', () => {
    renderComponent([], '/ui/clusters/local/brokers');

    const link = getAllClustersLink();

    expect(link).not.toHaveAttribute('aria-current');
    expect(within(link).getByRole('menuitem')).toHaveStyle({
      color: theme.menu.primary.color.normal,
    });
  });

  it('softly emphasizes All clusters on a cluster resource page', () => {
    renderComponent([], '/ui/clusters/local/brokers');

    const menuItem = within(getAllClustersLink()).getByRole('menuitem');

    expect(menuItem).toHaveStyleRule(
      'background-color',
      theme.menu.secondary.backgroundColor.hover
    );
    expect(menuItem).toHaveStyleRule(
      'box-shadow',
      `inset 0 0 0 1px ${theme.layout.stuffBorderColor}`
    );
  });

  it('uses the active emphasis background on the All clusters route', () => {
    renderComponent([], '/');

    expect(within(getAllClustersLink()).getByRole('menuitem')).toHaveStyleRule(
      'background-color',
      theme.menu.secondary.backgroundColor.active
    );
  });

  it('renders the controlled All clusters reminder only while visible', () => {
    const onDismiss = jest.fn();
    const { rerender } = renderComponent([], '/', true, {
      isAllClustersReminderVisible: true,
      onAllClustersReminderDismiss: onDismiss,
    });

    expect(getAllClustersReminder()).toHaveTextContent(
      '点击左侧 All clusters，可返回并添加或管理集群。'
    );
    expect(screen.getByText('返回全部集群')).toBeInTheDocument();

    rerender(
      getComponent({
        isAllClustersReminderVisible: false,
        onAllClustersReminderDismiss: onDismiss,
      })
    );

    expect(getAllClustersReminder()).toBeUndefined();
  });

  it('navigates from a cluster resource page to the all-clusters route', async () => {
    const user = userEvent.setup();
    renderComponent([], '/ui/clusters/local/brokers');

    await act(async () => {
      await user.click(getAllClustersLink());
    });

    expect(screen.getByTestId('location')).toHaveTextContent('/');
  });

  it('preserves existing cluster menus', () => {
    renderComponent([onlineClusterPayload, offlineClusterPayload]);

    expect(screen.getAllByRole('menu')).toHaveLength(3);
    expect(getMenuItemsCount()).toEqual(3);
    expect(getAllClustersLink()).toBeInTheDocument();
    expect(screen.getByText(onlineClusterPayload.name)).toBeInTheDocument();
    expect(screen.getByText(offlineClusterPayload.name)).toBeInTheDocument();
  });
});
