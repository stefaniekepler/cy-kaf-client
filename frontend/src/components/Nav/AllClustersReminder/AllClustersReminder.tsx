import React, { type FC, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

import * as S from './AllClustersReminder.styled';

export const REMINDER_VISIBLE_DURATION_MS = 4000;

interface AllClustersReminderProps {
  onDismiss: () => void;
}

const AllClustersReminder: FC<AllClustersReminderProps> = ({ onDismiss }) => {
  const [isLeaving, setIsLeaving] = useState(false);
  const onDismissRef = useRef(onDismiss);

  useEffect(() => {
    onDismissRef.current = onDismiss;
  }, [onDismiss]);

  useEffect(() => {
    const visibleTimer = window.setTimeout(() => {
      setIsLeaving(true);
    }, REMINDER_VISIBLE_DURATION_MS);

    return () => window.clearTimeout(visibleTimer);
  }, []);

  useEffect(() => {
    if (!isLeaving) {
      return undefined;
    }

    const fadeTimer = window.setTimeout(
      () => onDismissRef.current(),
      S.REMINDER_FADE_DURATION_MS
    );

    return () => window.clearTimeout(fadeTimer);
  }, [isLeaving]);

  return createPortal(
    <S.Reminder
      $isLeaving={isLeaving}
      role="status"
      aria-live="polite"
      aria-atomic="true"
      data-state={isLeaving ? 'leaving' : 'visible'}
    >
      <S.Title>返回全部集群</S.Title>
      <S.Message>
        点击左侧 <span lang="en">All clusters</span>，可返回并添加或管理集群。
      </S.Message>
    </S.Reminder>,
    document.body
  );
};

export default AllClustersReminder;
