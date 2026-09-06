import React, { type FC } from 'react';
import { useClusters } from 'lib/hooks/api/clusters';
import useCurrentClusterName from 'lib/hooks/useCurrentClusterName';
import { useLocation } from 'react-router-dom';
import AllClustersIcon from 'components/common/Icons/AllClustersIcon';

import * as S from './Nav.styled';
import MenuItem from './Menu/MenuItem';
import ClusterMenu from './ClusterMenu/ClusterMenu';
import AllClustersReminder from './AllClustersReminder/AllClustersReminder';

const ALL_CLUSTERS_PATHS = new Set(['/', '/ui', '/ui/clusters']);

export interface NavProps {
  isAllClustersReminderVisible?: boolean;
  onAllClustersReminderDismiss?: () => void;
}

const Nav: FC<NavProps> = ({
  isAllClustersReminderVisible = false,
  onAllClustersReminderDismiss,
}) => {
  const clusters = useClusters();
  const clusterName = useCurrentClusterName();
  const location = useLocation();
  const normalizedPathname = location.pathname.replace(/\/+$/, '') || '/';
  const isAllClustersActive = ALL_CLUSTERS_PATHS.has(normalizedPathname);

  return (
    <>
      <S.Navigation aria-label="Sidebar Menu">
        <S.GlobalNavigation>
          <S.List>
            <MenuItem
              variant="primary"
              to="/"
              title="All clusters"
              icon={<AllClustersIcon />}
              isActive={isAllClustersActive}
              isEmphasized
            />
          </S.List>
        </S.GlobalNavigation>
        <S.ClustersNavigation>
          <S.SectionLabel>Clusters</S.SectionLabel>
          {clusters.isSuccess &&
            clusters.data.map((cluster) => (
              <ClusterMenu
                key={cluster.name}
                name={cluster.name}
                status={cluster.status}
                features={cluster.features}
                opened={
                  clusters.data.length === 1 || cluster.name === clusterName
                }
              />
            ))}
        </S.ClustersNavigation>
      </S.Navigation>
      {isAllClustersReminderVisible && onAllClustersReminderDismiss && (
        <AllClustersReminder onDismiss={onAllClustersReminderDismiss} />
      )}
    </>
  );
};

export default Nav;
