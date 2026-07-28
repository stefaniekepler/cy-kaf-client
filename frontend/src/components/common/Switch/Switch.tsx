import React from 'react';

import * as S from './Switch.styled';

export interface SwitchProps {
  onChange(): void;
  checked: boolean;
  name: string;
  ariaLabel: string;
  id?: string;
  ariaLabelledBy?: string;
  disabled?: boolean;
}
const Switch = React.forwardRef<HTMLInputElement, SwitchProps>(
  (
    {
      id,
      name,
      checked,
      onChange,
      ariaLabel,
      ariaLabelledBy,
      disabled = false,
    },
    ref
  ) => {
    return (
      <S.StyledLabel>
        <S.StyledInput
          ref={ref}
          id={id ?? name}
          name={name}
          type="checkbox"
          onChange={onChange}
          checked={checked}
          aria-label={ariaLabel}
          aria-labelledby={ariaLabelledBy}
          disabled={disabled}
        />
        <S.StyledSlider checked={checked} />
      </S.StyledLabel>
    );
  }
);

Switch.displayName = 'Switch';

export default Switch;
