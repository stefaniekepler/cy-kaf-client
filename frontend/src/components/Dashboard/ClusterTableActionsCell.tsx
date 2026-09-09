import React, { useMemo } from 'react';
import { Cluster, ResourceType } from 'generated-sources';
import { CellContext } from '@tanstack/react-table';
import { clusterConfigPath } from 'lib/paths';
import { useGetUserInfo } from 'lib/hooks/api/roles';
import { useClusters } from 'lib/hooks/api/clusters';
import {
  useDeleteAppConfigCluster,
  useDuplicateAppConfigCluster,
} from 'lib/hooks/api/appConfig';
import { nextDuplicateName } from 'lib/duplicateClusterName';
import { useConfirm } from 'lib/hooks/useConfirm';
import { showAlert, showSuccessAlert } from 'lib/errorHandling';
import { ActionCanButton } from 'components/common/ActionComponent';

import * as S from './Dashboard.styled';

type Props = CellContext<Cluster, unknown>;

const ClusterTableActionsCell: React.FC<Props> = ({ row }) => {
  const { name } = row.original;
  const { data } = useGetUserInfo();
  const clusters = useClusters();
  const duplicate = useDuplicateAppConfigCluster();
  const remove = useDeleteAppConfigCluster();
  const confirm = useConfirm();
  const confirmDangerous = useConfirm(true);

  const hasPermissions = useMemo(() => {
    if (!data?.rbacEnabled) return true;
    return !!data?.userInfo?.permissions.some(
      (permission) => permission.resource === ResourceType.APPLICATIONCONFIG
    );
  }, [data]);

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
  };

  const handleDuplicate = (e: React.MouseEvent) => {
    e.stopPropagation();
    // Previewed from the loaded cluster list; the mutation recomputes the name
    // against the freshest config before writing it.
    const preview = nextDuplicateName(
      name,
      (clusters.data || []).map((cluster) => cluster.name)
    );
    confirm(`确认克隆环境 ${name}？副本将命名为 ${preview}。`, async () => {
      try {
        const created = await duplicate.mutateAsync(name);
        showSuccessAlert({ message: `已克隆为 ${created}` });
      } catch {
        showAlert('error', {
          id: 'app-config-duplicate-error',
          title: '克隆环境失败',
          message: '克隆环境配置失败，请重试',
        });
      }
    });
  };

  const handleDelete = (e: React.MouseEvent) => {
    e.stopPropagation();
    confirmDangerous(
      <>
        确认删除环境 {name}？该操作不可撤销。
        <S.ConfirmHint>
          该操作仅删除本地配置，不影响 Kafka 服务端。
        </S.ConfirmHint>
      </>,
      async () => {
        try {
          await remove.mutateAsync(name);
          showSuccessAlert({ message: `已删除环境 ${name}` });
        } catch {
          showAlert('error', {
            id: 'app-config-delete-error',
            title: '删除环境失败',
            message: '删除环境配置失败，请重试',
          });
        }
      }
    );
  };

  return (
    <S.ClusterActions>
      <ActionCanButton
        buttonType="secondary"
        buttonSize="S"
        to={clusterConfigPath(name)}
        canDoAction={hasPermissions}
        data-action="configure"
        onClick={handleClick}
      >
        Configure
      </ActionCanButton>
      <ActionCanButton
        buttonType="secondary"
        buttonSize="S"
        canDoAction={hasPermissions}
        disabled={duplicate.isPending}
        inProgress={duplicate.isPending}
        data-action="duplicate"
        onClick={handleDuplicate}
      >
        Duplicate
      </ActionCanButton>
      <ActionCanButton
        buttonType="secondary"
        buttonSize="S"
        canDoAction={hasPermissions}
        disabled={remove.isPending}
        inProgress={remove.isPending}
        data-action="delete"
        onClick={handleDelete}
      >
        Delete
      </ActionCanButton>
    </S.ClusterActions>
  );
};

export default ClusterTableActionsCell;
