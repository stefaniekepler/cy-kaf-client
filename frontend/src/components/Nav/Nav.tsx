import React, { type FC } from 'react';
import { useClusters } from 'lib/hooks/api/clusters';
import useCurrentClusterName from 'lib/hooks/useCurrentClusterName';
import { setLocalStorageValue } from 'lib/hooks/useLocalStorage';

import * as S from './Nav.styled';
import MenuItem from './Menu/MenuItem';
import ClusterMenu from './ClusterMenu/ClusterMenu';

const Nav: FC = () => {
  const clusters = useClusters();
  const clusterName = useCurrentClusterName();

  const collapseAllClusters = () => {
    clusters.data?.forEach((cluster) => {
      setLocalStorageValue(`clusterMenu-${cluster.name}-isOpen`, false);
    });
  };

  return (
    <aside aria-label="Sidebar Menu">
      <S.SidebarHeader>
        <S.List>
          <MenuItem variant="primary" to="/" title="Dashboard" />
        </S.List>
        {clusters.isSuccess && clusters.data.length > 0 && (
          <S.CollapseAllButton
            type="button"
            aria-label="Collapse all clusters"
            title="Collapse all clusters"
            onClick={collapseAllClusters}
          >
            <svg
              viewBox="0 0 10 6"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
            >
              <path d="M8.99988 5L4.99988 1L0.999878 5" fill="none" />
            </svg>
          </S.CollapseAllButton>
        )}
      </S.SidebarHeader>
      {clusters.isSuccess &&
        clusters.data.map((cluster) => (
          <ClusterMenu
            key={cluster.name}
            name={cluster.name}
            status={cluster.status}
            features={cluster.features}
            opened={clusters.data.length === 1 || cluster.name === clusterName}
          />
        ))}
    </aside>
  );
};

export default Nav;
