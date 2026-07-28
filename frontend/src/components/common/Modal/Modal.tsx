import React from 'react';

import * as S from './Modal.styled';

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title?: string;
  ariaLabel?: string;
  initialFocusRef?: React.RefObject<HTMLElement>;
  restoreFocusRef?: React.RefObject<HTMLElement>;
  closeOnEscape?: boolean;
  children: React.ReactNode;
  footer?: React.ReactNode;
  maxWidth?: string;
  maxHeight?: string;
}

const Modal: React.FC<ModalProps> = ({
  isOpen,
  onClose,
  title,
  ariaLabel,
  initialFocusRef,
  restoreFocusRef,
  closeOnEscape = true,
  children,
  footer,
  maxWidth = '65vw',
  maxHeight = '80vh',
}) => {
  const triggerFocusRef = React.useRef<HTMLElement | null>(null);
  const hasRestoredFocus = React.useRef(false);
  const onCloseRef = React.useRef(onClose);
  const initialFocusRefRef = React.useRef(initialFocusRef);
  const restoreFocusRefRef = React.useRef(restoreFocusRef);

  React.useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  React.useEffect(() => {
    initialFocusRefRef.current = initialFocusRef;
    restoreFocusRefRef.current = restoreFocusRef;
  }, [initialFocusRef, restoreFocusRef]);

  React.useEffect(() => {
    if (!isOpen) return undefined;

    triggerFocusRef.current =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    hasRestoredFocus.current = false;
    initialFocusRefRef.current?.current?.focus();

    return () => {
      if (!hasRestoredFocus.current) {
        (
          restoreFocusRefRef.current?.current || triggerFocusRef.current
        )?.focus();
        hasRestoredFocus.current = true;
      }
    };
  }, [isOpen]);

  React.useEffect(() => {
    if (!isOpen) return undefined;

    const onKeyDown = (event: KeyboardEvent) => {
      if (closeOnEscape && event.key === 'Escape') onCloseRef.current();
    };
    document.addEventListener('keydown', onKeyDown);

    return () => document.removeEventListener('keydown', onKeyDown);
  }, [closeOnEscape, isOpen]);

  if (!isOpen) return null;

  return (
    <S.ModalOverlay onClick={onClose}>
      <S.ModalContent
        onClick={(e: React.MouseEvent) => e.stopPropagation()}
        $maxWidth={maxWidth}
        $maxHeight={maxHeight}
        role="dialog"
        aria-modal="true"
        aria-label={title || ariaLabel || 'Modal'}
      >
        {title && (
          <S.ModalHeader>
            <S.ModalTitle>{title}</S.ModalTitle>
          </S.ModalHeader>
        )}

        <S.ModalBody>{children}</S.ModalBody>

        {footer && <S.ModalFooter>{footer}</S.ModalFooter>}
      </S.ModalContent>
    </S.ModalOverlay>
  );
};

export default Modal;
