import styled from 'styled-components';

export const Toolbar = styled.div`
  padding: 8px 16px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  color: ${({ theme }) => theme.default.color.normal};
`;

export const ClusterActions = styled.div`
  display: flex;
  gap: 8px;

  button[data-action='configure'] {
    background: ${({ theme }) => theme.tag.backgroundColor.blue};
    color: ${({ theme }) => theme.tag.color};
  }

  button[data-action='delete'] {
    background: ${({ theme }) => theme.tag.backgroundColor.red};
    color: ${({ theme }) => theme.tag.color};
  }

  button[data-action='configure']:hover:not(:disabled),
  button[data-action='delete']:hover:not(:disabled) {
    color: ${({ theme }) => theme.tag.color};
    filter: brightness(0.94);
  }

  button[data-action='configure']:hover:not(:disabled) {
    background: ${({ theme }) => theme.tag.backgroundColor.blue};
  }

  button[data-action='delete']:hover:not(:disabled) {
    background: ${({ theme }) => theme.tag.backgroundColor.red};
  }
`;

export const ConfirmHint = styled.p`
  margin: 8px 0 0;
`;
