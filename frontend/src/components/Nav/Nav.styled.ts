import styled from 'styled-components';
import { ClusterColorKey } from 'theme/theme';

export const SidebarHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

export const CollapseAllButton = styled.button`
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  padding: 0;
  margin-right: 4px;
  border: none;
  border-radius: 6px;
  background: transparent;
  color: ${({ theme }) => theme.menu.primary.color.normal};
  cursor: pointer;

  & svg {
    width: 10px;
    height: 12px;
    stroke: currentColor;
  }

  &:hover {
    background-color: ${({ theme }) =>
      theme.menu.primary.backgroundColor.hover};
  }
`;

export const List = styled.ul.attrs({ role: 'menu' })`
  & > & {
    padding: 0 0 0 8px;
  }

  & * {
    margin-bottom: 2px;
  }
`;

export const ClusterList = styled.ul.attrs<{ $colorKey: ClusterColorKey }>({
  role: 'menu',
})`
  border-radius: 8px;
  padding: 2px 4px;
  margin-bottom: 2px;
  background-color: ${({ theme, $colorKey }) =>
    theme.clusterMenu.backgroundColor[$colorKey]};
`;
