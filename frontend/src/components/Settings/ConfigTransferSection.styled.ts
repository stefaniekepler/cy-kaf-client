import styled from 'styled-components';

export const Description = styled.p`
  color: ${({ theme }) => theme.modal.contentColor};
  line-height: 1.6;
  margin: 12px 0;
`;
export const Review = styled.div`
  margin-top: 20px;
  h3 {
    font-size: 15px;
    margin-bottom: 8px;
  }
`;
export const EntryList = styled.div`
  display: grid;
  gap: 10px;
  max-height: 320px;
  overflow: auto;
  margin: 12px 0;
`;
export const Entry = styled.div`
  border: 1px solid ${({ theme }) => theme.modal.border.contrast};
  border-radius: 6px;
  padding: 12px;
  min-width: 0;
  overflow-wrap: anywhere;
`;
export const EntryLabel = styled.label`
  display: flex;
  gap: 8px;
  align-items: baseline;
  cursor: pointer;
  input {
    flex-shrink: 0;
  }
`;
export const Detail = styled.p`
  color: ${({ theme }) => theme.modal.contentColor};
  margin: 6px 0 0 22px;
  font-size: 13px;
  line-height: 1.5;
`;
