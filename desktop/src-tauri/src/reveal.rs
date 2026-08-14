use std::io;
use std::path::Path;

/// Reveals the config file in the platform file manager (does not launch an
/// editor). Linux can only open the containing directory — `xdg-open` has no
/// select-file mode.
#[cfg(target_os = "macos")]
pub fn reveal_in_file_manager(path: &Path) -> io::Result<()> {
    let status = std::process::Command::new("open")
        .arg("-R")
        .arg(path)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Other, "open -R failed"))
    }
}

#[cfg(target_os = "windows")]
pub fn reveal_in_file_manager(path: &Path) -> io::Result<()> {
    let select = format!("/select,{}", path.display());
    let status = std::process::Command::new("explorer").arg(select).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Other, "explorer /select failed"))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub fn reveal_in_file_manager(path: &Path) -> io::Result<()> {
    let dir = path.parent().unwrap_or(path);
    let status = std::process::Command::new("xdg-open").arg(dir).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Other, "xdg-open failed"))
    }
}
