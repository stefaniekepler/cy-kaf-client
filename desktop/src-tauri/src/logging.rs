use crate::protocol::SESSION_COOKIE;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;

const LOG_FILE_NAME: &str = "desktop.log";
const MAX_LOG_BYTES: u64 = 2 * 1024 * 1024;
const MAX_LINE_BYTES: usize = 16 * 1024;
const MAX_BACKUPS: usize = 3;
const MAX_MARKER_BYTES: usize = 32;
const STATIC_SENSITIVE_MARKERS: [&[u8]; 3] = [b"CY_KAF_INIT", b"CY_KAF_READY", b"sessionToken"];

#[derive(Debug, Error)]
pub enum LogError {
    #[error("could not create the desktop log directory")]
    CreateDirectory(#[source] io::Error),
    #[error("could not open the desktop log")]
    Open(#[source] io::Error),
    #[error("could not write the desktop log")]
    Write(#[source] io::Error),
    #[error("could not rotate the desktop log")]
    Rotate(#[source] io::Error),
}

pub struct SidecarLogger {
    path: PathBuf,
    file: Option<File>,
    current_size: u64,
    pending: Vec<u8>,
    marker_tail: Vec<u8>,
    line_sensitive: bool,
}

impl SidecarLogger {
    pub fn open(directory: &Path) -> Result<Self, LogError> {
        fs::create_dir_all(directory).map_err(LogError::CreateDirectory)?;
        restrict_directory(directory)?;

        let path = directory.join(LOG_FILE_NAME);
        let file = open_log_file(&path)?;
        let current_size = file.metadata().map_err(LogError::Open)?.len();
        let mut logger = Self {
            path,
            file: Some(file),
            current_size,
            pending: Vec::with_capacity(MAX_LINE_BYTES),
            marker_tail: Vec::with_capacity(MAX_MARKER_BYTES),
            line_sensitive: false,
        };
        if logger.current_size >= MAX_LOG_BYTES {
            logger.rotate()?;
        }
        Ok(logger)
    }

    pub fn append_sidecar_stderr(&mut self, bytes: &[u8]) -> Result<(), LogError> {
        for &byte in bytes {
            if byte == b'\n' {
                self.finish_line()?;
                continue;
            }

            if self.pending.len() < MAX_LINE_BYTES {
                self.pending.push(byte);
            }
            self.scan_sensitive_marker(byte);
        }
        Ok(())
    }

    fn scan_sensitive_marker(&mut self, byte: u8) {
        if self.line_sensitive {
            return;
        }

        self.marker_tail.push(byte);
        if self.marker_tail.len() > MAX_MARKER_BYTES {
            self.marker_tail.remove(0);
        }
        self.line_sensitive = STATIC_SENSITIVE_MARKERS
            .iter()
            .any(|marker| self.marker_tail.ends_with(marker))
            || self.marker_tail.ends_with(SESSION_COOKIE.as_bytes());
    }

    fn finish_line(&mut self) -> Result<(), LogError> {
        if !self.line_sensitive {
            if self.pending.last() == Some(&b'\r') {
                self.pending.pop();
            }
            let bytes_to_write = self.pending.len() as u64 + 1;
            if self.current_size > 0 && self.current_size + bytes_to_write > MAX_LOG_BYTES {
                self.rotate()?;
            }
            let file = self
                .file
                .as_mut()
                .ok_or_else(|| LogError::Write(io::Error::other("desktop log is not open")))?;
            file.write_all(&self.pending).map_err(LogError::Write)?;
            file.write_all(b"\n").map_err(LogError::Write)?;
            self.current_size += bytes_to_write;
        }

        self.pending.clear();
        self.marker_tail.clear();
        self.line_sensitive = false;
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), LogError> {
        if let Some(mut file) = self.file.take() {
            file.flush().map_err(LogError::Write)?;
        }

        remove_if_exists(&backup_path(&self.path, MAX_BACKUPS))?;
        for index in (2..=MAX_BACKUPS).rev() {
            rename_if_exists(
                &backup_path(&self.path, index - 1),
                &backup_path(&self.path, index),
            )?;
        }
        rename_if_exists(&self.path, &backup_path(&self.path, 1))?;

        self.file = Some(open_log_file(&self.path)?);
        self.current_size = 0;
        Ok(())
    }
}

impl Drop for SidecarLogger {
    fn drop(&mut self) {
        if !self.pending.is_empty() || self.line_sensitive {
            let _ = self.finish_line();
        }
        if let Some(file) = self.file.as_mut() {
            let _ = file.flush();
        }
    }
}

fn open_log_file(path: &Path) -> Result<File, LogError> {
    let mut options = OpenOptions::new();
    options.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let file = options.open(path).map_err(LogError::Open)?;
    restrict_file(path)?;
    Ok(file)
}

fn backup_path(path: &Path, index: usize) -> PathBuf {
    let mut backup = path.as_os_str().to_owned();
    backup.push(format!(".{index}"));
    backup.into()
}

fn remove_if_exists(path: &Path) -> Result<(), LogError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LogError::Rotate(error)),
    }
}

fn rename_if_exists(from: &Path, to: &Path) -> Result<(), LogError> {
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LogError::Rotate(error)),
    }
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> Result<(), LogError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(LogError::CreateDirectory)
}

#[cfg(not(unix))]
fn restrict_directory(_path: &Path) -> Result<(), LogError> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file(path: &Path) -> Result<(), LogError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(LogError::Open)
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> Result<(), LogError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn rotates_at_two_mib_and_keeps_at_most_three_backups() {
        let temp = tempdir().expect("temp directory");
        let mut logger = SidecarLogger::open(temp.path()).expect("open logger");
        let mut line = vec![b'x'; MAX_LINE_BYTES - 1];
        line.push(b'\n');

        for _ in 0..650 {
            logger
                .append_sidecar_stderr(&line)
                .expect("append sidecar stderr");
        }
        drop(logger);

        for name in [
            LOG_FILE_NAME.to_owned(),
            format!("{LOG_FILE_NAME}.1"),
            format!("{LOG_FILE_NAME}.2"),
            format!("{LOG_FILE_NAME}.3"),
        ] {
            let metadata = fs::metadata(temp.path().join(name)).expect("expected log file");
            assert!(metadata.len() <= MAX_LOG_BYTES);
        }
        assert!(!temp.path().join(format!("{LOG_FILE_NAME}.4")).exists());
    }

    #[test]
    fn drops_entire_sensitive_lines_even_when_marker_crosses_chunks() {
        let temp = tempdir().expect("temp directory");
        let mut logger = SidecarLogger::open(temp.path()).expect("open logger");

        logger
            .append_sidecar_stderr(b"safe diagnostic\nCY_KAF_IN")
            .expect("first chunk");
        logger
            .append_sidecar_stderr(
                b"IT {\"protocol\":1}\nCY_KAF_READY hidden\nfield=sessionToken secret\n",
            )
            .expect("second chunk");
        logger
            .append_sidecar_stderr(b"Cookie: cy_kaf_desktop_session=hidden\nsafe tail\n")
            .expect("third chunk");
        drop(logger);

        let contents = fs::read(temp.path().join(LOG_FILE_NAME)).expect("read current log file");
        assert_eq!(contents, b"safe diagnostic\nsafe tail\n");
    }

    #[test]
    fn truncates_each_logged_line_to_sixteen_kibibytes() {
        let temp = tempdir().expect("temp directory");
        let mut logger = SidecarLogger::open(temp.path()).expect("open logger");
        let mut oversized = vec![b'z'; MAX_LINE_BYTES + 4096];
        oversized.push(b'\n');

        logger
            .append_sidecar_stderr(&oversized)
            .expect("append oversized line");
        drop(logger);

        let contents = fs::read(temp.path().join(LOG_FILE_NAME)).expect("read log");
        assert_eq!(contents.len(), MAX_LINE_BYTES + 1);
        assert_eq!(contents.last(), Some(&b'\n'));
    }

    #[cfg(unix)]
    #[test]
    fn restricts_log_directory_and_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("temp directory");
        let directory = temp.path().join("logs");
        let mut logger = SidecarLogger::open(&directory).expect("open logger");
        logger
            .append_sidecar_stderr(b"diagnostic\n")
            .expect("append log");
        drop(logger);

        assert_eq!(
            fs::metadata(&directory)
                .expect("directory metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(directory.join(LOG_FILE_NAME))
                .expect("file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
