import styled, { css, keyframes } from 'styled-components';

export const REMINDER_FADE_DURATION_MS = 180;

const fadeIn = keyframes`
  from {
    opacity: 0;
    transform: translateX(-4px);
  }

  to {
    opacity: 1;
    transform: translateX(0);
  }
`;

const fadeOut = keyframes`
  from {
    opacity: 1;
    transform: translateX(0);
  }

  to {
    opacity: 0;
    transform: translateX(-4px);
  }
`;

export const Reminder = styled.div<{ $isLeaving: boolean }>`
  position: fixed;
  top: calc(${({ theme }) => theme.layout.navBarHeight} + 18px);
  left: calc(${({ theme }) => theme.layout.navBarWidth} + 8px);
  z-index: 1100;
  width: 220px;
  padding: 10px 12px;
  border: 1px solid ${({ theme }) => theme.allClustersReminder.borderColor};
  border-radius: 8px;
  background-color: ${({ theme }) => theme.allClustersReminder.backgroundColor};
  color: ${({ theme }) => theme.allClustersReminder.color};
  box-shadow: 0 2px 8px ${({ theme }) => theme.allClustersReminder.shadow};
  font-size: 12px;
  line-height: 18px;
  pointer-events: none;

  ${({ $isLeaving }) => css`
    animation: ${$isLeaving ? fadeOut : fadeIn} ${REMINDER_FADE_DURATION_MS}ms
      ease-out both;
  `}

  &::before {
    position: absolute;
    top: 14px;
    left: -5px;
    z-index: -1;
    width: 8px;
    height: 8px;
    border: 1px solid ${({ theme }) => theme.allClustersReminder.borderColor};
    background-color: ${({ theme }) =>
      theme.allClustersReminder.backgroundColor};
    content: '';
    transform: rotate(45deg);
  }

  @media (prefers-reduced-motion: reduce) {
    animation: none;
    opacity: ${({ $isLeaving }) => ($isLeaving ? 0 : 1)};
    transform: none;
  }
`;

export const Title = styled.div`
  margin-bottom: 2px;
  color: ${({ theme }) => theme.allClustersReminder.titleColor};
  font-size: 13px;
  font-weight: 600;
  line-height: 18px;
`;

export const Message = styled.div`
  font-size: 12px;
  line-height: 18px;
`;
