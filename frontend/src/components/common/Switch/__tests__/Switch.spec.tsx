import React, { useState } from 'react';
import userEvent from '@testing-library/user-event';
import { fireEvent, screen } from '@testing-library/react';
import Switch from 'components/common/Switch/Switch';
import { render } from 'lib/testHelpers';

describe('Switch', () => {
  it('uses its name as the input id and visible label as the accessible name', async () => {
    const user = userEvent.setup();
    const inputRef = React.createRef<HTMLInputElement>();
    const onChange = jest.fn();

    render(
      <>
        <label id="desktop-mcp-enabled-label" htmlFor="desktop-mcp-enabled">
          Enable MCP from visible label
        </label>
        <Switch
          ref={inputRef}
          name="desktop-mcp-enabled"
          ariaLabel="Fallback switch name"
          ariaLabelledBy="desktop-mcp-enabled-label"
          checked={false}
          onChange={onChange}
        />
      </>
    );

    const checkbox = screen.getByRole('checkbox', {
      name: 'Enable MCP from visible label',
    });
    expect(checkbox).toHaveAttribute('id', 'desktop-mcp-enabled');
    expect(checkbox).toHaveAttribute(
      'aria-labelledby',
      'desktop-mcp-enabled-label'
    );
    expect(inputRef.current).toBe(checkbox);

    await user.click(
      screen.getByText('Enable MCP from visible label', { selector: 'label' })
    );

    expect(onChange).toHaveBeenCalledTimes(1);
    expect(checkbox).toHaveFocus();
  });

  it('exposes its accessible name and toggles with the keyboard', async () => {
    const user = userEvent.setup();
    const onChange = jest.fn();
    render(
      <Switch
        name="internalTopics"
        ariaLabel="Show Internal Topics"
        checked={false}
        onChange={onChange}
      />
    );

    const checkbox = screen.getByRole('checkbox', {
      name: 'Show Internal Topics',
    });
    await user.tab();
    await user.keyboard(' ');

    expect(checkbox).toHaveFocus();
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it('does not notify callers when disabled', async () => {
    const user = userEvent.setup();
    const onChange = jest.fn();
    render(
      <Switch
        name="offlineClusters"
        ariaLabel="Only offline clusters"
        checked={false}
        disabled
        onChange={onChange}
      />
    );

    await user.click(
      screen.getByRole('checkbox', { name: 'Only offline clusters' })
    );

    expect(onChange).not.toHaveBeenCalled();
  });

  it('reflects controlled checked state after a caller update', () => {
    const Harness = () => {
      const [checked, setChecked] = useState(false);
      return (
        <Switch
          name="keepContents"
          ariaLabel="Keep contents after producing a message"
          checked={checked}
          onChange={() => setChecked((current) => !current)}
        />
      );
    };

    render(<Harness />);
    const checkbox = screen.getByRole('checkbox', {
      name: 'Keep contents after producing a message',
    });
    expect(checkbox).not.toBeChecked();

    fireEvent.click(checkbox);

    expect(checkbox).toBeChecked();
  });
});
