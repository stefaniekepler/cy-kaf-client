import React from 'react';
import { act, cleanup, render, screen, within } from '@testing-library/react';
import { ThemeProvider } from 'styled-components';
import { darkTheme, theme } from 'theme/theme';
import AllClustersReminder from 'components/Nav/AllClustersReminder/AllClustersReminder';

describe('AllClustersReminder', () => {
  const renderComponent = (
    onDismiss: () => void = jest.fn(),
    selectedTheme = theme
  ) =>
    render(
      <ThemeProvider theme={selectedTheme}>
        <AllClustersReminder onDismiss={onDismiss} />
      </ThemeProvider>
    );

  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    cleanup();
    jest.useRealTimers();
  });

  it('renders the approved title and sentence as a non-interactive polite atomic status in a portal', () => {
    const { container } = renderComponent();
    const reminder = screen.getByRole('status');

    expect(within(reminder).getByText('返回全部集群')).toBeInTheDocument();
    expect(reminder).toHaveTextContent(
      '点击左侧 All clusters，可返回并添加或管理集群。'
    );
    expect(within(reminder).getByText('All clusters')).toHaveAttribute(
      'lang',
      'en'
    );
    expect(reminder).toHaveAttribute('aria-live', 'polite');
    expect(reminder).toHaveAttribute('aria-atomic', 'true');
    expect(reminder).toHaveAttribute('data-state', 'visible');
    expect(reminder).toHaveStyleRule('pointer-events', 'none');
    expect(document.body).toContainElement(reminder);
    expect(container).not.toContainElement(reminder);
  });

  it.each([
    {
      mode: 'light',
      selectedTheme: theme,
      backgroundColor: '#f9fafa',
      borderColor: '#E3E6E8',
      color: '#5C6970',
      titleColor: '#454F54',
      shadowColor: 'rgba(0, 0, 0, 0.1)',
    },
    {
      mode: 'dark',
      selectedTheme: darkTheme,
      backgroundColor: '#22282A',
      borderColor: '#394246',
      color: '#ABB5BA',
      titleColor: '#C7CED1',
      shadowColor: 'rgba(0, 0, 0, 0.1)',
    },
  ])(
    'uses a quiet differentiated neutral treatment in $mode mode',
    ({
      selectedTheme,
      backgroundColor,
      borderColor,
      color,
      titleColor,
      shadowColor,
    }) => {
      renderComponent(jest.fn(), selectedTheme);
      const reminder = screen.getByRole('status');

      expect(reminder).toHaveStyleRule('background-color', backgroundColor);
      expect(reminder).toHaveStyleRule('border', `1px solid ${borderColor}`);
      expect(reminder).toHaveStyleRule('color', color);
      expect(reminder).toHaveStyleRule(
        'box-shadow',
        `0 2px 8px ${shadowColor}`
      );
      expect(within(reminder).getByText('返回全部集群')).toHaveStyleRule(
        'color',
        titleColor
      );
    }
  );

  it('stays visible for 4000ms, fades for 180ms, then dismisses once', () => {
    const onDismiss = jest.fn();
    renderComponent(onDismiss);
    const reminder = screen.getByRole('status');

    act(() => {
      jest.advanceTimersByTime(3999);
    });
    expect(reminder).toHaveAttribute('data-state', 'visible');
    expect(onDismiss).not.toHaveBeenCalled();

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(reminder).toHaveAttribute('data-state', 'leaving');
    expect(onDismiss).not.toHaveBeenCalled();

    act(() => {
      jest.advanceTimersByTime(180);
    });
    expect(onDismiss).toHaveBeenCalledTimes(1);

    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });

  it('keeps the fade deadline while using the latest dismiss callback', () => {
    const onDismissA = jest.fn();
    const onDismissB = jest.fn();
    const { rerender } = renderComponent(onDismissA);

    act(() => {
      jest.advanceTimersByTime(4000);
    });
    expect(screen.getByRole('status')).toHaveAttribute('data-state', 'leaving');

    act(() => {
      jest.advanceTimersByTime(100);
    });
    rerender(
      <ThemeProvider theme={theme}>
        <AllClustersReminder onDismiss={onDismissB} />
      </ThemeProvider>
    );

    act(() => {
      jest.advanceTimersByTime(79);
    });
    expect(onDismissA).not.toHaveBeenCalled();
    expect(onDismissB).not.toHaveBeenCalled();

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(onDismissA).not.toHaveBeenCalled();
    expect(onDismissB).toHaveBeenCalledTimes(1);

    act(() => {
      jest.runOnlyPendingTimers();
    });
    expect(onDismissA).not.toHaveBeenCalled();
    expect(onDismissB).toHaveBeenCalledTimes(1);
  });

  it('does not dismiss after unmounting before the visible timer completes', () => {
    const onDismiss = jest.fn();
    const { unmount } = renderComponent(onDismiss);

    act(() => {
      jest.advanceTimersByTime(3999);
    });
    unmount();
    act(() => {
      jest.runOnlyPendingTimers();
    });

    expect(onDismiss).not.toHaveBeenCalled();
  });

  it('does not dismiss after unmounting during the fade timer', () => {
    const onDismiss = jest.fn();
    const { unmount } = renderComponent(onDismiss);

    act(() => {
      jest.advanceTimersByTime(4000);
    });
    expect(screen.getByRole('status')).toHaveAttribute('data-state', 'leaving');
    unmount();
    act(() => {
      jest.runOnlyPendingTimers();
    });

    expect(onDismiss).not.toHaveBeenCalled();
  });

  it('removes animation and transform when reduced motion is preferred', () => {
    renderComponent();
    const reminder = screen.getByRole('status');
    const mediaRule = Array.from(document.styleSheets)
      .flatMap((sheet) => Array.from(sheet.cssRules))
      .find(
        (rule) =>
          (rule as CSSMediaRule).conditionText ===
          '(prefers-reduced-motion: reduce)'
      ) as CSSMediaRule | undefined;
    const reducedMotionRule = Array.from(mediaRule?.cssRules ?? []).find(
      (rule) => {
        const { selectorText } = rule as CSSStyleRule;
        return selectorText ? reminder.matches(selectorText) : false;
      }
    ) as CSSStyleRule | undefined;

    expect(reducedMotionRule).toBeDefined();
    expect(reducedMotionRule?.style.getPropertyValue('animation')).toBe('none');
    expect(reducedMotionRule?.style.getPropertyValue('transform')).toBe('none');
  });
});
