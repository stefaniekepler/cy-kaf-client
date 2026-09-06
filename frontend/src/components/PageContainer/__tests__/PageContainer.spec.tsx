import React from 'react';
import { act, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouterProps } from 'react-router-dom';
import { render } from 'lib/testHelpers';
import PageContainer from 'components/PageContainer/PageContainer';
import { useClusters } from 'lib/hooks/api/clusters';
import { Cluster, ControllerType, ServerStatus } from 'generated-sources';
import { useGetUserInfo } from 'lib/hooks/api/roles';
import { clusterBrokersPath } from 'lib/paths';

jest.mock('components/Version/Version', () => () => <div>Version</div>);

interface DataType {
  data: Cluster[] | undefined;
}

jest.mock('lib/hooks/api/clusters');
jest.mock('lib/hooks/api/roles');
const mockedNavigate = jest.fn();
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => mockedNavigate,
}));
describe('Page Container', () => {
  const originalInnerWidthDescriptor = Object.getOwnPropertyDescriptor(
    window,
    'innerWidth'
  );

  const setWindowWidth = (width: number) => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: width,
    });
  };

  const renderComponent = (
    hasDynamicConfig: boolean,
    data: DataType,
    initialEntries: MemoryRouterProps['initialEntries'] = ['/']
  ) => {
    const useClustersMock = useClusters as jest.Mock;
    useClustersMock.mockReturnValue(data);
    const useGetUserInfoMock = useGetUserInfo as jest.Mock;
    useGetUserInfoMock.mockReturnValue({
      data: { rbacEnabled: false },
    });
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: jest.fn().mockImplementation(() => ({
        matches: false,
        addListener: jest.fn(),
      })),
    });
    return render(
      <PageContainer>
        <div>child</div>
      </PageContainer>,
      {
        globalSettings: { hasDynamicConfig },
        initialEntries,
      }
    );
  };

  beforeEach(() => {
    mockedNavigate.mockClear();
    setWindowWidth(1280);
  });

  afterAll(() => {
    if (originalInnerWidthDescriptor) {
      Object.defineProperty(window, 'innerWidth', originalInnerWidthDescriptor);
    } else {
      Reflect.deleteProperty(window, 'innerWidth');
    }
  });

  it('render the inner container', async () => {
    renderComponent(false, { data: undefined });
    expect(screen.getByText('child')).toBeInTheDocument();
  });

  it('shows the All clusters reminder on a wide cluster route entry', () => {
    renderComponent(false, { data: undefined }, [clusterBrokersPath('local')]);

    const reminder = screen.getByRole('status');
    expect(reminder).toHaveTextContent(
      '点击左侧 All clusters，可返回并添加或管理集群。'
    );
    expect(within(reminder).getByText('返回全部集群')).toBeInTheDocument();
  });

  it('shows the All clusters reminder after opening the sidebar on a small screen', async () => {
    const user = userEvent.setup();
    setWindowWidth(800);
    renderComponent(false, { data: undefined }, [clusterBrokersPath('local')]);

    expect(screen.queryByRole('status')).not.toBeInTheDocument();

    const pageHeader = screen.getByRole('navigation', {
      name: 'Page Header',
    });
    const sidebarToggle = within(pageHeader).getByRole('button', {
      name: 'Toggle sidebar',
    });
    await act(async () => {
      await user.click(sidebarToggle);
    });

    expect(screen.getByRole('status')).toBeInTheDocument();
  });

  describe('Redirect to the Wizard page', () => {
    it('redirects to new cluster configuration page if there are no clusters and dynamic config is enabled', async () => {
      renderComponent(true, { data: [] });

      expect(mockedNavigate).toHaveBeenCalled();
    });

    it('should not navigate to new cluster config page when there are clusters', async () => {
      renderComponent(true, {
        data: [
          {
            name: 'Cluster 1',
            status: ServerStatus.ONLINE,
            controller: ControllerType.KRAFT,
          },
        ],
      });

      expect(mockedNavigate).not.toHaveBeenCalled();
    });

    it('should not navigate to new cluster config page when there are no clusters and hasDynamicConfig is false', async () => {
      renderComponent(false, {
        data: [],
      });

      expect(mockedNavigate).not.toHaveBeenCalled();
    });
  });
});
