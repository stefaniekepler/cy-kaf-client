import styled from 'styled-components';
import { ClusterColorKey } from 'theme/theme';

export const Navigation = styled.aside`
  display: flex;
  flex: 1 1 auto;
  flex-direction: column;
  min-height: 0;
  width: 100%;
`;

export const GlobalNavigation = styled.div`
  flex: 0 0 auto;
  background-color: ${({ theme }) => theme.default.backgroundColor};
`;

export const ClustersNavigation = styled.div`
  flex: 1 1 auto;
  min-height: 0;
  padding-top: 12px;
  scrollbar-gutter: stable;
  scrollbar-width: thin;
  overflow-y: auto;

  &::-webkit-scrollbar {
    width: 8px;
  }

  &::-webkit-scrollbar-track {
    background-color: ${({ theme }) => theme.scrollbar.trackColor.normal};
  }

  &::-webkit-scrollbar-thumb {
    width: 8px;
    background-color: ${({ theme }) => theme.scrollbar.thumbColor.normal};
    border-radius: 4px;
  }

  &:hover::-webkit-scrollbar-thumb {
    background: ${({ theme }) => theme.scrollbar.thumbColor.active};
  }

  &:hover::-webkit-scrollbar-track {
    background-color: ${({ theme }) => theme.scrollbar.trackColor.active};
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

export const SectionLabel = styled.div`
  margin: 12px 8px 4px;
  color: ${({ theme }) => theme.menu.primary.color.active};
  font-size: 11px;
  font-weight: 500;
  line-height: 16px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  user-select: none;

  &:first-child {
    margin-top: 0;
  }
`;
