import React, { type ButtonHTMLAttributes } from 'react';
import { Link } from 'react-router-dom';
import Spinner from 'components/common/Spinner/Spinner';

import StyledButton, { ButtonProps } from './Button.styled';

export interface Props
  extends ButtonHTMLAttributes<HTMLButtonElement>, ButtonProps {
  to?: string | object;
  inProgress?: boolean;
  className?: string;
}

export const Button = React.forwardRef<HTMLButtonElement, Props>(
  ({ to, children, disabled, inProgress, ...props }, ref) => {
    if (to) {
      return (
        <Link to={to} className={props.className}>
          <StyledButton ref={ref} disabled={disabled} type="button" {...props}>
            {children}
          </StyledButton>
        </Link>
      );
    }

    return (
      <StyledButton
        ref={ref}
        type="button"
        disabled={disabled || inProgress}
        {...props}
      >
        {children}{' '}
        {inProgress ? (
          <Spinner size={16} borderWidth={2} marginLeft={2} emptyBorderColor />
        ) : null}
      </StyledButton>
    );
  }
);

Button.displayName = 'Button';
