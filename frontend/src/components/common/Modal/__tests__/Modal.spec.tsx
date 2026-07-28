import React, { useRef, useState } from 'react';
import { fireEvent, screen } from '@testing-library/react';
import Modal from 'components/common/Modal/Modal';
import { render } from 'lib/testHelpers';

describe('Modal', () => {
  it('uses its title as the accessible dialog name for existing callers', () => {
    render(
      <Modal isOpen onClose={jest.fn()} title="Connection details">
        <p>Details</p>
      </Modal>
    );

    const dialog = screen.getByRole('dialog', { name: 'Connection details' });
    expect(dialog).toBeInTheDocument();
    expect(dialog).not.toHaveAttribute('maxwidth');
    expect(dialog).not.toHaveAttribute('maxheight');
  });

  it('closes on Escape and restores focus to the opening trigger', () => {
    const onClose = jest.fn();
    const Harness = () => {
      const [isOpen, setIsOpen] = useState(false);
      const initialFocusRef = useRef<HTMLButtonElement>(null);
      return (
        <>
          <button type="button" onClick={() => setIsOpen(true)}>
            Open details
          </button>
          <Modal
            isOpen={isOpen}
            onClose={() => {
              onClose();
              setIsOpen(false);
            }}
            ariaLabel="Details dialog"
            initialFocusRef={initialFocusRef}
          >
            <button ref={initialFocusRef} type="button">
              First action
            </button>
          </Modal>
        </>
      );
    };

    render(<Harness />);
    const trigger = screen.getByRole('button', { name: 'Open details' });
    trigger.focus();
    fireEvent.click(trigger);

    expect(screen.getByRole('dialog', { name: 'Details dialog' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'First action' })).toHaveFocus();

    fireEvent.keyDown(document, { key: 'Escape' });

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(trigger).toHaveFocus();
  });

  it('does not restore focus while an open caller rerenders', () => {
    const Harness = () => {
      const [isOpen, setIsOpen] = useState(false);
      const [, setVersion] = useState(0);
      const initialFocusRef = useRef<HTMLButtonElement>(null);
      return (
        <>
          <button type="button" onClick={() => setIsOpen(true)}>
            Open details
          </button>
          <button type="button" onClick={() => setVersion((version) => version + 1)}>
            Rerender caller
          </button>
          <Modal
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            initialFocusRef={initialFocusRef}
          >
            <button ref={initialFocusRef} type="button">
              First action
            </button>
          </Modal>
        </>
      );
    };

    render(<Harness />);
    const trigger = screen.getByRole('button', { name: 'Open details' });
    trigger.focus();
    fireEvent.click(trigger);
    const focus = jest.spyOn(trigger, 'focus');

    fireEvent.click(screen.getByRole('button', { name: 'Rerender caller' }));

    expect(focus).not.toHaveBeenCalled();
  });

  it('does not restore focus when open accessibility props change', () => {
    const Harness = () => {
      const [isOpen, setIsOpen] = useState(false);
      const [useUpdatedProps, setUseUpdatedProps] = useState(false);
      const initialFocusRef = useRef<HTMLButtonElement>(null);
      const updatedInitialFocusRef = useRef<HTMLButtonElement>(null);
      const restoreFocusRef = useRef<HTMLButtonElement>(null);
      return (
        <>
          <button type="button" onClick={() => setIsOpen(true)}>
            Open details
          </button>
          <button type="button" onClick={() => setUseUpdatedProps(true)}>
            Update accessibility props
          </button>
          <button type="button" onClick={() => setIsOpen(false)}>
            Close details
          </button>
          <button ref={restoreFocusRef} type="button">
            Restore target
          </button>
          <Modal
            isOpen={isOpen}
            onClose={() => setIsOpen(false)}
            closeOnEscape={!useUpdatedProps}
            initialFocusRef={
              useUpdatedProps ? updatedInitialFocusRef : initialFocusRef
            }
            restoreFocusRef={useUpdatedProps ? restoreFocusRef : undefined}
          >
            <button ref={initialFocusRef} type="button">
              First action
            </button>
            <button ref={updatedInitialFocusRef} type="button">
              Updated action
            </button>
          </Modal>
        </>
      );
    };

    render(<Harness />);
    const trigger = screen.getByRole('button', { name: 'Open details' });
    trigger.focus();
    fireEvent.click(trigger);
    const focus = jest.spyOn(trigger, 'focus');

    fireEvent.click(screen.getByRole('button', { name: 'Update accessibility props' }));

    expect(focus).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'First action' })).toHaveFocus();

    fireEvent.click(screen.getByRole('button', { name: 'Close details' }));

    expect(screen.getByRole('button', { name: 'Restore target' })).toHaveFocus();
  });

  it('closes when the overlay is clicked but not when dialog content is clicked', () => {
    const onClose = jest.fn();
    render(
      <Modal isOpen onClose={onClose} ariaLabel="Example dialog">
        <button type="button">Dialog action</button>
      </Modal>
    );

    fireEvent.click(screen.getByRole('button', { name: 'Dialog action' }));
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('dialog', { name: 'Example dialog' }).parentElement!);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
