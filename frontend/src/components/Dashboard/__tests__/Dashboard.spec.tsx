import React from 'react';
import { screen } from '@testing-library/react';
import Dashboard from 'components/Dashboard/Dashboard';
import { render } from 'lib/testHelpers';
import { useClusters } from 'lib/hooks/api/clusters';
import { useGetUserInfo } from 'lib/hooks/api/roles';

jest.mock('lib/hooks/api/clusters', () => ({
  useClusters: jest.fn(),
}));

jest.mock('lib/hooks/api/roles', () => ({
  useGetUserInfo: jest.fn(),
}));

jest.mock('components/common/NewTable', () => ({
  __esModule: true,
  default: () => <div role="table" />,
  SizeCell: () => null,
}));

describe('Dashboard', () => {
  beforeEach(() => {
    jest.mocked(useClusters).mockReturnValue({
      data: [],
      isFetched: true,
    } as unknown as ReturnType<typeof useClusters>);
    jest.mocked(useGetUserInfo).mockReturnValue({
      data: { rbacEnabled: false },
    } as unknown as ReturnType<typeof useGetUserInfo>);
  });

  it('uses the Clusters heading and enables cluster configuration when available', () => {
    render(<Dashboard />, {
      globalSettings: { hasDynamicConfig: true },
    });

    expect(
      screen.getByRole('heading', { name: 'Clusters' })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: 'Configure new cluster' })
    ).toBeEnabled();
  });

  it('hides cluster configuration when dynamic configuration is unavailable', () => {
    render(<Dashboard />, {
      globalSettings: { hasDynamicConfig: false },
    });

    expect(
      screen.queryByRole('button', { name: 'Configure new cluster' })
    ).not.toBeInTheDocument();
  });

  it('disables cluster configuration without the required RBAC permission', () => {
    jest.mocked(useGetUserInfo).mockReturnValue({
      data: {
        rbacEnabled: true,
        userInfo: { permissions: [] },
      },
    } as unknown as ReturnType<typeof useGetUserInfo>);

    render(<Dashboard />, {
      globalSettings: { hasDynamicConfig: true },
    });

    expect(
      screen.getByRole('button', { name: 'Configure new cluster' })
    ).toBeDisabled();
  });
});
