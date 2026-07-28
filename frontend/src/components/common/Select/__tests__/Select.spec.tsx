import Select, { SelectProps } from 'components/common/Select/Select';
import React from 'react';
import { render } from 'lib/testHelpers';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

jest.mock('react-hook-form', () => ({
  useFormContext: () => ({
    register: jest.fn(),
  }),
}));

const options = [
  { label: 'test-label1', value: 'test-value1', disabled: false },
  { label: 'test-label2', value: 'test-value2', disabled: true },
];

const renderComponent = (props?: Partial<SelectProps<string>>) =>
  render(<Select name="test" {...props} />);

describe('Custom Select', () => {
  describe('when isLive is not specified', () => {
    beforeEach(() => {
      renderComponent({
        options,
      });
    });

    const getListbox = () => screen.getByRole('listbox');
    const getOption = () => screen.getByRole('option');

    it('renders component', () => {
      expect(getListbox()).toBeInTheDocument();
    });

    it('show select options when select is being clicked', async () => {
      expect(getOption()).toBeInTheDocument();
      await userEvent.click(getListbox());
      expect(screen.getAllByRole('option')).toHaveLength(3);
    });

    it('checking select option change', async () => {
      const optionLabel = 'test-label1';

      await userEvent.click(getListbox());
      await userEvent.selectOptions(getListbox(), [optionLabel]);

      expect(getOption()).toHaveTextContent(optionLabel);
    });

    it('trying to select disabled option does not trigger change', async () => {
      const normalOptionLabel = 'test-label1';
      const disabledOptionLabel = 'test-label2';

      await userEvent.click(getListbox());
      await userEvent.selectOptions(getListbox(), [normalOptionLabel]);
      await userEvent.click(getListbox());
      await userEvent.selectOptions(getListbox(), [disabledOptionLabel]);

      expect(getOption()).toHaveTextContent(normalOptionLabel);
    });

    it.each([
      [false, '0', 'auto'],
      [true, 'auto', '0'],
    ])(
      'aligns the option list for theme mode=%s',
      async (isThemeMode, expectedLeft, expectedRight) => {
        renderComponent({ options, isThemeMode });

        await userEvent.click(screen.getAllByRole('listbox')[1]);
        const optionList = screen.getByText('test-label1').parentElement;

        expect(optionList).toHaveStyleRule('left', expectedLeft);
        expect(optionList).toHaveStyleRule('right', expectedRight);
        expect(optionList).toHaveStyleRule('width', 'max-content');
      }
    );
  });
});
