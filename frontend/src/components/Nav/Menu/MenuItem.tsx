import React, { type FC, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

import * as S from './styled';

export interface MenuItemProps {
  to: string;
  title: string;
  variant?: 'primary' | 'secondary';
  isActive?: boolean;
  isEmphasized?: boolean;
  icon?: ReactNode;
  onClick?: () => void;
}

const MenuItem: FC<MenuItemProps> = ({
  title,
  to,
  isActive,
  isEmphasized = false,
  variant = 'secondary',
  icon,
  onClick,
}) => (
  <Link
    to={to}
    title={title}
    onClick={onClick}
    aria-current={isActive ? 'page' : undefined}
    className={isActive ? 'active' : undefined}
  >
    <S.MenuItem
      $isActive={isActive}
      $isEmphasized={isEmphasized}
      $variant={variant}
    >
      {icon ? (
        <S.MenuItemContent>
          <S.LeadingIcon aria-hidden="true">{icon}</S.LeadingIcon>
          <span>{title}</span>
        </S.MenuItemContent>
      ) : (
        title
      )}
    </S.MenuItem>
  </Link>
);

export default MenuItem;
