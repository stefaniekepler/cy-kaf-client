import styled from 'styled-components';

export const Content = styled.div`
  width: min(560px, calc(100vw - 80px));

  @media (max-width: 480px) {
    width: calc(100vw - 64px);
  }
`;

export const Section = styled.section`
  & + & {
    border-top: 1px solid ${({ theme }) => theme.modal.border.bottom};
    margin-top: 24px;
    padding-top: 20px;
  }
`;

export const SectionHeading = styled.h2`
  color: ${({ theme }) => theme.modal.color};
  font-size: 16px;
  margin: 0 0 14px;
`;

export const UpToDate = styled.p`
  align-self: center;
  color: #29a352;
  margin: 0 0 0 4px;
`;

export const Limitation = styled.p`
  color: ${({ theme }) => theme.modal.contentColor};
  margin: 0;
`;

export const Controls = styled.div`
  display: grid;
  gap: 14px;
`;

export const ControlRow = styled.div`
  align-items: center;
  display: flex;
  justify-content: space-between;
  min-height: 32px;

  input:focus-visible + span {
    outline: 2px solid ${({ theme }) => theme.select.borderColor.active};
    outline-offset: 3px;
  }
`;

export const ControlLabel = styled.label`
  cursor: pointer;
  font-size: 14px;
  font-weight: 500;
`;

export const ClientList = styled.div`
  display: grid;
  gap: 12px;
  margin-top: 20px;
`;

export const Client = styled.div`
  border: 1px solid ${({ theme }) => theme.modal.border.contrast};
  border-radius: 6px;
  display: grid;
  gap: 10px;
  padding: 14px;
`;

export const ClientHeader = styled.div`
  align-items: center;
  display: flex;
  gap: 10px;
  justify-content: space-between;
`;

export const ClientName = styled.h3`
  font-size: 14px;
  margin: 0;
`;

export const Status = styled.span`
  color: ${({ theme }) => theme.modal.contentColor};
  font-size: 13px;
`;

export const Actions = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
`;

export const Alerts = styled.div`
  & > [role='alert'] {
    max-width: 100%;
    width: auto;
  }
`;

export const ConfirmationCopy = styled.p`
  margin: 0;
`;

export const ConfirmationActions = styled.div`
  display: flex;
  gap: 8px;
  justify-content: flex-end;
`;
