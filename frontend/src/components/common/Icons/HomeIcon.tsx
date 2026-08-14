import React from 'react';

interface HomeIconProps {
  fill?: string;
  width?: string;
  height?: string;
  viewBox?: string;
}

const HomeIcon: React.FC<HomeIconProps> = ({
  fill,
  width,
  height,
  viewBox,
}) => (
  <svg
    width={width || '16'}
    height={height || '16'}
    viewBox={viewBox || '0 0 16 16'}
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M8 1L15 7.5V14.5C15 15.0523 14.5523 15.5 14 15.5H10.5V10.5H5.5V15.5H2C1.44772 15.5 1 15.0523 1 14.5V7.5L8 1Z"
      fill={fill || 'currentColor'}
    />
  </svg>
);

export default HomeIcon;
