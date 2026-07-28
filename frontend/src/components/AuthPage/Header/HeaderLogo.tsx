import React from 'react';

import * as S from './Header.styled';

const HeaderLogo = () => (
  <S.StyledSVG
    width="125"
    height="41"
    viewBox="0 0 125 41"
    xmlns="http://www.w3.org/2000/svg"
  >
    <text
      x="50%"
      y="50%"
      dominantBaseline="central"
      textAnchor="middle"
      fontFamily="Inter, -apple-system, sans-serif"
      fontWeight={700}
      fontSize={14}
      fill="currentColor"
    >
      cy-kaf
    </text>
  </S.StyledSVG>
);

export default HeaderLogo;
