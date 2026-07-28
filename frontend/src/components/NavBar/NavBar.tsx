import React, { useContext, useRef, useState } from 'react';
import Select from 'components/common/Select/Select';
import AutoIcon from 'components/common/Icons/AutoIcon';
import SunIcon from 'components/common/Icons/SunIcon';
import MoonIcon from 'components/common/Icons/MoonIcon';
import SettingsIcon from 'components/common/Icons/SettingsIcon';
import SettingsModal from 'components/Settings/SettingsModal';
import { ThemeModeContext } from 'components/contexts/ThemeModeContext';
import { Button } from 'components/common/Button/Button';
import MenuIcon from 'components/common/Icons/MenuIcon';

import { UserTimezone } from './UserTimezone/UserTimezone';
import UserInfo from './UserInfo/UserInfo';
import * as S from './NavBar.styled';

interface Props {
  onBurgerClick: () => void;
}

export type ThemeDropDownValue = 'auto_theme' | 'light_theme' | 'dark_theme';

const options = [
  {
    label: (
      <>
        <AutoIcon />
        <div>Auto theme</div>
      </>
    ),
    value: 'auto_theme',
  },
  {
    label: (
      <>
        <SunIcon />
        <div>Light theme</div>
      </>
    ),
    value: 'light_theme',
  },
  {
    label: (
      <>
        <MoonIcon />
        <div>Dark theme</div>
      </>
    ),
    value: 'dark_theme',
  },
];

const NavBar: React.FC<Props> = ({ onBurgerClick }) => {
  const { themeMode, setThemeMode } = useContext(ThemeModeContext);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const settingsTriggerRef = useRef<HTMLButtonElement | null>(null);

  return (
    <S.Navbar role="navigation" aria-label="Page Header">
      <S.NavbarBrand>
        <S.NavbarBrand>
          <Button buttonType="text" buttonSize="S" onClick={onBurgerClick}>
            <MenuIcon />
          </Button>

          <S.Hyperlink to="/">Cy KafClient</S.Hyperlink>
        </S.NavbarBrand>
      </S.NavbarBrand>
      <S.NavbarSocial>
        <UserTimezone />

        <Select
          options={options}
          value={themeMode}
          onChange={setThemeMode}
          isThemeMode
        />
        <S.SettingsButton
          buttonType="text"
          buttonSize="L"
          aria-label="Settings"
          onClick={(event) => {
            settingsTriggerRef.current = event.currentTarget;
            setSettingsOpen(true);
          }}
        >
          <SettingsIcon />
          Settings
        </S.SettingsButton>
        <UserInfo />
      </S.NavbarSocial>
      <SettingsModal
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        triggerRef={settingsTriggerRef}
      />
    </S.Navbar>
  );
};

export default NavBar;
