use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DesktopSettings {
    pub port: Option<u16>,
}

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("could not read desktop settings")]
    Read(#[source] io::Error),
    #[error("desktop settings are invalid")]
    Invalid(#[source] serde_json::Error),
    #[error("desktop port must be nonzero")]
    InvalidPort,
    #[error("could not create the desktop settings directory")]
    CreateDirectory(#[source] io::Error),
    #[error("could not write desktop settings")]
    Write(#[source] io::Error),
    #[error("could not replace desktop settings")]
    Replace(#[source] io::Error),
}

pub fn load_settings(path: &Path) -> Result<DesktopSettings, SettingsError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(DesktopSettings::default());
        }
        Err(error) => return Err(SettingsError::Read(error)),
    };
    let settings =
        serde_json::from_slice::<DesktopSettings>(&bytes).map_err(SettingsError::Invalid)?;
    validate_settings(&settings)?;
    Ok(settings)
}

pub fn save_settings(path: &Path, settings: &DesktopSettings) -> Result<(), SettingsError> {
    validate_settings(settings)?;
    let directory = settings_directory(path);
    fs::create_dir_all(directory).map_err(SettingsError::CreateDirectory)?;
    restrict_directory(directory)?;

    let bytes = serde_json::to_vec_pretty(settings).map_err(SettingsError::Invalid)?;
    let temporary = temporary_path(path);
    let write_result = write_temporary(&temporary, &bytes).and_then(|()| {
        fs::rename(&temporary, path).map_err(SettingsError::Replace)?;
        restrict_file(path)
    });

    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn validate_settings(settings: &DesktopSettings) -> Result<(), SettingsError> {
    if settings.port == Some(0) {
        Err(SettingsError::InvalidPort)
    } else {
        Ok(())
    }
}

fn settings_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    temporary.into()
}

fn write_temporary(path: &Path, bytes: &[u8]) -> Result<(), SettingsError> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let mut file = options.open(path).map_err(SettingsError::Write)?;
    restrict_file(path)?;
    file.write_all(bytes).map_err(SettingsError::Write)?;
    file.write_all(b"\n").map_err(SettingsError::Write)?;
    file.flush().map_err(SettingsError::Write)?;
    file.sync_all().map_err(SettingsError::Write)
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> Result<(), SettingsError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(SettingsError::CreateDirectory)
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> Result<(), SettingsError> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file(path: &Path) -> Result<(), SettingsError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(SettingsError::Write)
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> Result<(), SettingsError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn missing_settings_file_uses_dynamic_port() {
        let temp = tempdir().expect("temp directory");
        let settings =
            load_settings(&temp.path().join("desktop.json")).expect("missing file is valid");

        assert_eq!(settings, DesktopSettings { port: None });
    }

    #[test]
    fn saved_port_can_be_loaded_without_leaving_temp_file() {
        let temp = tempdir().expect("temp directory");
        let path = temp.path().join("state").join("desktop.json");

        save_settings(&path, &DesktopSettings { port: Some(43127) }).expect("save settings");

        assert_eq!(
            load_settings(&path).expect("load settings"),
            DesktopSettings { port: Some(43127) }
        );
        assert!(!path.with_file_name("desktop.json.tmp").exists());
    }

    #[test]
    fn invalid_port_and_invalid_json_are_rejected() {
        let temp = tempdir().expect("temp directory");
        let path = temp.path().join("desktop.json");

        fs::write(&path, br#"{"port":0}"#).expect("write invalid port");
        assert!(load_settings(&path).is_err());

        fs::write(&path, b"{not-json").expect("write invalid JSON");
        assert!(load_settings(&path).is_err());

        assert!(save_settings(&path, &DesktopSettings { port: Some(0) }).is_err());
        assert_eq!(
            fs::read(&path).expect("invalid save must preserve current file"),
            b"{not-json"
        );
    }

    #[cfg(unix)]
    #[test]
    fn save_restricts_directory_and_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("temp directory");
        let directory = temp.path().join("state");
        let path = directory.join("desktop.json");

        save_settings(&path, &DesktopSettings { port: None }).expect("save settings");

        assert_eq!(
            fs::metadata(&directory)
                .expect("directory metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path)
                .expect("file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
