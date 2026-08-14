import React from 'react';
import Nav from 'components/Nav/Nav';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from 'lib/testHelpers';
import { Cluster } from 'generated-sources';
import { useClusters } from 'lib/hooks/api/clusters';
import {
  offlineClusterPayload,
  onlineClusterPayload,
} from 'lib/fixtures/clusters';
import { LOCAL_STORAGE_KEY_PREFIX } from 'lib/constants';

jest.mock('lib/hooks/api/clusters', () => ({
  useClusters: jest.fn(),
}));

/*
 Due to jsdom doesnt know about scrollIntoView
*/
window.HTMLElement.prototype.scrollIntoView = jest.fn();

describe('Nav', () => {
  const renderComponent = (payload: Cluster[] = []) => {
    (useClusters as jest.Mock).mockImplementation(() => ({
      data: payload,
      isSuccess: true,
    }));
    render(<Nav />);
  };

  const getHome = () => screen.getByRole('link', { name: 'Home' });

  const getMenuItemsCount = () => screen.queryAllByRole('menuitem').length;

  const clusterMenuStorageKey = (name: string) =>
    `${LOCAL_STORAGE_KEY_PREFIX}-clusterMenu-${name}-isOpen`;

  beforeEach(() => {
    localStorage.clear();
  });
  it('renders loader', () => {
    renderComponent();

    expect(getMenuItemsCount()).toEqual(0);
    expect(getHome()).toBeInTheDocument();
  });

  it('renders ClusterMenu', () => {
    renderComponent([onlineClusterPayload, offlineClusterPayload]);
    expect(screen.getAllByRole('menu').length).toEqual(2);
    expect(getMenuItemsCount()).toEqual(2);
    expect(getHome()).toBeInTheDocument();
    expect(screen.getByText(onlineClusterPayload.name)).toBeInTheDocument();
    expect(screen.getByText(offlineClusterPayload.name)).toBeInTheDocument();
  });

  describe('collapse all clusters', () => {
    const getCollapseAllButton = () =>
      screen.getByRole('button', { name: 'Collapse all clusters' });

    it('renders the collapse-all button when clusters are present', () => {
      renderComponent([onlineClusterPayload]);

      expect(getCollapseAllButton()).toBeInTheDocument();
    });

    it('collapses every expanded cluster menu and persists the state', async () => {
      localStorage.setItem(
        clusterMenuStorageKey(onlineClusterPayload.name),
        'true'
      );
      localStorage.setItem(
        clusterMenuStorageKey(offlineClusterPayload.name),
        'true'
      );
      renderComponent([onlineClusterPayload, offlineClusterPayload]);

      // 2 cluster tabs + 2 * 3 (Brokers/Topics/Consumers)
      expect(getMenuItemsCount()).toEqual(8);

      await userEvent.click(getCollapseAllButton());

      // collapsed cluster tabs only
      expect(getMenuItemsCount()).toEqual(2);
      expect(
        localStorage.getItem(clusterMenuStorageKey(onlineClusterPayload.name))
      ).toEqual('false');
      expect(
        localStorage.getItem(clusterMenuStorageKey(offlineClusterPayload.name))
      ).toEqual('false');
    });
  });
});
