import React from 'react';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CellContext } from '@tanstack/react-table';
import { Cluster } from 'generated-sources';
import ClusterTableActionsCell from 'components/Dashboard/ClusterTableActionsCell';
import { render } from 'lib/testHelpers';
import { useClusters } from 'lib/hooks/api/clusters';
import { useGetUserInfo } from 'lib/hooks/api/roles';
import {
  useDeleteAppConfigCluster,
  useDuplicateAppConfigCluster,
} from 'lib/hooks/api/appConfig';
import { showAlert, showSuccessAlert } from 'lib/errorHandling';

jest.mock('lib/hooks/api/clusters', () => ({ useClusters: jest.fn() }));
jest.mock('lib/hooks/api/roles', () => ({ useGetUserInfo: jest.fn() }));
jest.mock('lib/hooks/api/appConfig', () => ({
  useDuplicateAppConfigCluster: jest.fn(),
  useDeleteAppConfigCluster: jest.fn(),
}));
jest.mock('lib/errorHandling', () => ({
  ...jest.requireActual('lib/errorHandling'),
  showAlert: jest.fn(),
  showSuccessAlert: jest.fn(),
}));

const mutateAsync = jest.fn();
const deleteAsync = jest.fn();

const cellProps = (name: string) =>
  ({ row: { original: { name } } }) as unknown as CellContext<Cluster, unknown>;

const setClusters = (names: string[]) =>
  jest.mocked(useClusters).mockReturnValue({
    data: names.map((name) => ({ name })),
  } as unknown as ReturnType<typeof useClusters>);

const renderCell = (name = 'prod', onRowClick = jest.fn()) => {
  render(
    <div onClick={onRowClick} role="presentation">
      <ClusterTableActionsCell {...cellProps(name)} />
    </div>
  );
  return onRowClick;
};

describe('ClusterTableActionsCell', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mutateAsync.mockResolvedValue('prod-copy');
    setClusters(['prod']);
    jest.mocked(useGetUserInfo).mockReturnValue({
      data: { rbacEnabled: false },
    } as unknown as ReturnType<typeof useGetUserInfo>);
    jest.mocked(useDuplicateAppConfigCluster).mockReturnValue({
      mutateAsync,
      isPending: false,
    } as unknown as ReturnType<typeof useDuplicateAppConfigCluster>);
    deleteAsync.mockResolvedValue(undefined);
    jest.mocked(useDeleteAppConfigCluster).mockReturnValue({
      mutateAsync: deleteAsync,
      isPending: false,
    } as unknown as ReturnType<typeof useDeleteAppConfigCluster>);
  });

  it('offers Configure, Duplicate and Delete in that order', () => {
    renderCell();

    expect(screen.getByRole('link', { name: 'Configure' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Duplicate' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Delete' })).toBeEnabled();
    expect(screen.getAllByRole('button').map((b) => b.dataset.action)).toEqual([
      'configure',
      'duplicate',
      'delete',
    ]);
  });

  it('confirms with the name the copy will take before writing it', async () => {
    setClusters(['prod', 'prod-copy']);
    mutateAsync.mockResolvedValue('prod-copy-2');
    renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Duplicate' }));

    const dialog = screen.getByRole('dialog', { name: 'Confirmation Dialog' });
    expect(dialog).toHaveTextContent('prod');
    expect(dialog).toHaveTextContent('prod-copy-2');
    expect(mutateAsync).not.toHaveBeenCalled();

    await userEvent.click(screen.getByRole('button', { name: 'Confirm' }));

    await waitFor(() => expect(mutateAsync).toHaveBeenCalledWith('prod'));
    expect(showSuccessAlert).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining('prod-copy-2'),
      })
    );
    expect(showAlert).not.toHaveBeenCalled();
  });

  it('writes nothing when the confirmation is dismissed', async () => {
    renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Duplicate' }));
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it('reports a failed duplication without a success alert', async () => {
    mutateAsync.mockRejectedValue(new Error('boom'));
    renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Duplicate' }));
    await userEvent.click(screen.getByRole('button', { name: 'Confirm' }));

    await waitFor(() =>
      expect(showAlert).toHaveBeenCalledWith('error', expect.anything())
    );
    expect(showSuccessAlert).not.toHaveBeenCalled();
  });

  it('does not trigger the row navigation behind the cell', async () => {
    const onRowClick = renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Duplicate' }));

    expect(onRowClick).not.toHaveBeenCalled();
  });

  it('deletes the cluster only after the confirmation is accepted', async () => {
    renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Delete' }));
    const dialog = screen.getByRole('dialog', { name: 'Confirmation Dialog' });
    expect(dialog).toHaveTextContent('确认删除环境 prod');
    expect(dialog).toHaveTextContent('该操作不可撤销');
    expect(dialog).toHaveTextContent(
      '该操作仅删除本地配置，不影响 Kafka 服务端'
    );
    expect(deleteAsync).not.toHaveBeenCalled();

    await userEvent.click(screen.getByRole('button', { name: 'Confirm' }));

    await waitFor(() => expect(deleteAsync).toHaveBeenCalledWith('prod'));
    expect(showSuccessAlert).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining('prod') })
    );
    expect(showAlert).not.toHaveBeenCalled();
  });

  it('keeps the cluster when the delete confirmation is dismissed', async () => {
    renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));

    expect(deleteAsync).not.toHaveBeenCalled();
  });

  it('reports a failed deletion without a success alert', async () => {
    deleteAsync.mockRejectedValue(new Error('boom'));
    renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await userEvent.click(screen.getByRole('button', { name: 'Confirm' }));

    await waitFor(() =>
      expect(showAlert).toHaveBeenCalledWith('error', expect.anything())
    );
    expect(showSuccessAlert).not.toHaveBeenCalled();
  });

  it('does not trigger the row navigation from Delete', async () => {
    const onRowClick = renderCell();

    await userEvent.click(screen.getByRole('button', { name: 'Delete' }));

    expect(onRowClick).not.toHaveBeenCalled();
  });

  it('keeps Delete busy while the removal is being written', () => {
    jest.mocked(useDeleteAppConfigCluster).mockReturnValue({
      mutateAsync: deleteAsync,
      isPending: true,
    } as unknown as ReturnType<typeof useDeleteAppConfigCluster>);

    renderCell();

    expect(screen.getByRole('button', { name: 'Delete' })).toBeDisabled();
  });

  it('disables both actions without the application config permission', () => {
    jest.mocked(useGetUserInfo).mockReturnValue({
      data: { rbacEnabled: true, userInfo: { permissions: [] } },
    } as unknown as ReturnType<typeof useGetUserInfo>);

    renderCell();

    expect(screen.getByRole('button', { name: 'Duplicate' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Delete' })).toBeDisabled();
  });

  it('keeps Duplicate busy while the copy is being written', () => {
    jest.mocked(useDuplicateAppConfigCluster).mockReturnValue({
      mutateAsync,
      isPending: true,
    } as unknown as ReturnType<typeof useDuplicateAppConfigCluster>);

    renderCell();

    expect(screen.getByRole('button', { name: 'Duplicate' })).toBeDisabled();
  });
});
