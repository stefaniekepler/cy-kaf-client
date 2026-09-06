import React from 'react';
import MenuItem, { MenuItemProps } from 'components/Nav/Menu/MenuItem';
import { screen, within } from '@testing-library/react';
import { render } from 'lib/testHelpers';
import { theme } from 'theme/theme';

describe('MenuItem', () => {
  const setupComponent = (props: Partial<MenuItemProps> = {}) => (
    <ul>
      <MenuItem to="/test" title="Test title" {...props} />
    </ul>
  );

  const getMenuItem = () => screen.getByRole('menuitem');
  const getLink = () => screen.getByRole('link');

  it('renders component with correct title', () => {
    const testTitle = 'My Test Title';
    render(setupComponent({ title: testTitle }));
    expect(screen.getByText(testTitle)).toBeInTheDocument();
  });

  it('renders primary variant component with correct styles', () => {
    render(setupComponent({ variant: 'primary' }));
    expect(getMenuItem()).toHaveStyle({ fontWeight: '500' });
  });

  it('keeps the default primary background without an inset border', () => {
    render(setupComponent({ variant: 'primary' }));

    expect(getMenuItem()).toHaveStyleRule(
      'background-color',
      theme.menu.primary.backgroundColor.normal
    );
    expect(getMenuItem()).toHaveStyleRule('box-shadow', 'none');
  });

  it('renders an inactive emphasized primary item with persistent styling', () => {
    render(setupComponent({ variant: 'primary', isEmphasized: true }));

    expect(getMenuItem()).toHaveStyleRule(
      'background-color',
      theme.menu.secondary.backgroundColor.hover
    );
    expect(getMenuItem()).toHaveStyleRule(
      'box-shadow',
      `inset 0 0 0 1px ${theme.layout.stuffBorderColor}`
    );
    expect(getMenuItem()).toHaveStyleRule(
      'background-color',
      theme.menu.secondary.backgroundColor.active,
      { modifier: '&:hover' }
    );
    expect(getMenuItem()).toHaveStyleRule(
      'background-color',
      theme.menu.secondary.backgroundColor.active,
      { modifier: '&:active' }
    );
  });

  it('renders an active emphasized primary item with the active background', () => {
    render(
      setupComponent({
        variant: 'primary',
        isEmphasized: true,
        isActive: true,
      })
    );

    expect(getMenuItem()).toHaveStyleRule(
      'background-color',
      theme.menu.secondary.backgroundColor.active
    );
  });

  it('renders secondary variant component with correct styles', () => {
    render(setupComponent({ variant: 'secondary' }));
    expect(getMenuItem()).toHaveStyle({ fontWeight: '400' });
  });

  it('renders list item with link inside', () => {
    render(setupComponent({ to: '/my-cluster' }));
    const menuItem = getMenuItem();
    const link = getLink();

    expect(menuItem).toBeInTheDocument();
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', '/my-cluster');
  });

  it('renders an optional leading icon without changing the link name', () => {
    render(
      setupComponent({
        icon: <svg data-testid="leading-icon" aria-label="Ignored icon" />,
      })
    );

    const link = screen.getByRole('link', { name: 'Test title' });

    expect(within(link).getByTestId('leading-icon')).toBeInTheDocument();
    expect(link).not.toHaveAccessibleName(/Ignored icon/);
  });

  it('marks an active link as the current page', () => {
    render(setupComponent({ isActive: true }));

    expect(screen.getByRole('link', { name: 'Test title' })).toHaveAttribute(
      'aria-current',
      'page'
    );
    expect(getLink()).toHaveClass('active');
  });
});
