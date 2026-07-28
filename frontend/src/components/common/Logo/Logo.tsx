import React from 'react';

import * as S from './Logo.styled';

const Logo: React.FC = () => {
  return (
    <S.Logo
      width="23"
      height="30"
      viewBox="0 0 23 30"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <text
        x="50%"
        y="50%"
        dominantBaseline="central"
        textAnchor="middle"
        fontFamily="Inter, -apple-system, sans-serif"
        fontWeight={700}
        fontSize={12}
      >
        cy
      </text>
    </S.Logo>
  );
};

export default Logo;
