use base64::{Engine, engine::general_purpose::STANDARD};
use minisign_verify::{PublicKey, Signature};
use semver::Version;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;

pub const MAX_PACKAGE_BYTES: u64 = 256 << 20;
const MAX_METADATA_BYTES: u64 = 32 << 10;
const MAX_SIGNATURE_BYTES: usize = 16 << 10;
const READY_FILE: &str = "ready.cache";
const SCHEDULE_FILE: &str = "schedule.json";

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Update cache is unavailable")]
    FileAccess,
    #[error("Update cache is invalid")]
    InvalidData,
    #[error("Update package signature is invalid")]
    InvalidSignature,
    #[error("Update version is invalid")]
    InvalidVersion,
    #[error("No verified update is ready")]
    NotReady,
}

#[derive(Debug)]
pub struct CachedUpdate {
    pub version: String,
    pub signature: String,
    pub scheduled: bool,
    pub bytes: Vec<u8>,
}

pub struct Cache {
    root: PathBuf,
    pubkey: String,
    access: Mutex<()>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    version: String,
    signature: String,
    package_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Schedule {
    version: String,
    signature: String,
    scheduled: bool,
}

impl Cache {
    pub fn new(root: PathBuf, pubkey: String) -> Self {
        Self {
            root,
            pubkey,
            access: Mutex::new(()),
        }
    }

    pub fn save(&self, version: &str, signature: &str, bytes: &[u8]) -> Result<(), CacheError> {
        let _access = self.access.lock().map_err(|_| CacheError::FileAccess)?;
        parse_version(version)?;
        verify_package(bytes, signature, &self.pubkey)?;
        let metadata = serde_json::to_vec(&Metadata {
            version: version.into(),
            signature: signature.into(),
            package_bytes: bytes.len() as u64,
        })
        .map_err(|_| CacheError::InvalidData)?;
        if metadata.len() as u64 > MAX_METADATA_BYTES {
            return Err(CacheError::InvalidData);
        }
        self.prepare_directory()?;
        // A schedule belongs to one ready package. Cancel before replacement so
        // a crash can never apply an old opt-in to newly downloaded bytes.
        remove_file_if_present(&self.root.join(SCHEDULE_FILE))?;
        let length = (metadata.len() as u64).to_le_bytes();
        self.atomic_write(READY_FILE, &[&length, &metadata, bytes])
    }

    pub fn load(&self, current_version: &str) -> Result<Option<CachedUpdate>, CacheError> {
        let _access = self.access.lock().map_err(|_| CacheError::FileAccess)?;
        let current = parse_version(current_version)?;
        if !self.check_paths()? {
            return Ok(None);
        }
        let Some(mut ready) = self.read_ready_clean()? else {
            return Ok(None);
        };
        if !parse_version(&ready.version)?
            .cmp_precedence(&current)
            .is_gt()
        {
            self.clear_files()?;
            return Ok(None);
        }
        ready.scheduled = self.read_schedule(&ready)?;
        Ok(Some(ready))
    }

    pub fn set_scheduled(&self, scheduled: bool) -> Result<(), CacheError> {
        let _access = self.access.lock().map_err(|_| CacheError::FileAccess)?;
        if !self.check_paths()? {
            return if scheduled {
                Err(CacheError::NotReady)
            } else {
                Ok(())
            };
        }
        if !scheduled {
            return remove_file_if_present(&self.root.join(SCHEDULE_FILE));
        }
        let ready = self.read_ready_clean()?.ok_or(CacheError::NotReady)?;
        let schedule = serde_json::to_vec(&Schedule {
            version: ready.version,
            signature: ready.signature,
            scheduled: true,
        })
        .map_err(|_| CacheError::InvalidData)?;
        self.atomic_write(SCHEDULE_FILE, &[&schedule])
    }

    pub fn clear(&self) -> Result<(), CacheError> {
        let _access = self.access.lock().map_err(|_| CacheError::FileAccess)?;
        if self.check_paths()? {
            self.clear_files()?;
        }
        Ok(())
    }

    fn check_paths(&self) -> Result<bool, CacheError> {
        if !self.root.is_absolute() || self.root.components().any(|c| c == Component::ParentDir) {
            return Err(CacheError::FileAccess);
        }
        let mut path = PathBuf::new();
        for component in self.root.components() {
            path.push(component.as_os_str());
            match fs::symlink_metadata(&path) {
                Ok(metadata) if !is_link(&metadata) && metadata.is_dir() => (),
                Ok(_) => return Err(CacheError::FileAccess),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(_) => return Err(CacheError::FileAccess),
            }
        }
        for filename in [READY_FILE, SCHEDULE_FILE] {
            match fs::symlink_metadata(self.root.join(filename)) {
                Ok(metadata) if !is_link(&metadata) && metadata.is_file() => (),
                Ok(_) => return Err(CacheError::FileAccess),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(_) => return Err(CacheError::FileAccess),
            }
        }
        Ok(true)
    }

    fn prepare_directory(&self) -> Result<(), CacheError> {
        if !self.check_paths()? {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder
                .create(&self.root)
                .map_err(|_| CacheError::FileAccess)?;
        }
        if !self.check_paths()? {
            return Err(CacheError::FileAccess);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700))
                .map_err(|_| CacheError::FileAccess)?;
        }
        Ok(())
    }

    fn read_ready_clean(&self) -> Result<Option<CachedUpdate>, CacheError> {
        match self.read_ready() {
            Ok(ready) => Ok(ready),
            Err(error) => {
                // Invalid caches are consumed once. No startup attempt should
                // repeatedly encounter a scheduled, unverified package.
                self.clear_files()?;
                Err(error)
            }
        }
    }

    fn read_ready(&self) -> Result<Option<CachedUpdate>, CacheError> {
        let mut file = match File::open(self.root.join(READY_FILE)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(CacheError::FileAccess),
        };
        let total = file.metadata().map_err(|_| CacheError::FileAccess)?.len();
        if !(8..=MAX_PACKAGE_BYTES + MAX_METADATA_BYTES + 8).contains(&total) {
            return Err(CacheError::InvalidData);
        }
        let mut header = [0; 8];
        file.read_exact(&mut header)
            .map_err(|_| CacheError::InvalidData)?;
        let length = u64::from_le_bytes(header);
        if length == 0 || length > MAX_METADATA_BYTES || length > total - 8 {
            return Err(CacheError::InvalidData);
        }
        let mut encoded = vec![0; length as usize];
        file.read_exact(&mut encoded)
            .map_err(|_| CacheError::InvalidData)?;
        let metadata: Metadata =
            serde_json::from_slice(&encoded).map_err(|_| CacheError::InvalidData)?;
        parse_version(&metadata.version)?;
        if metadata.package_bytes == 0
            || metadata.package_bytes > MAX_PACKAGE_BYTES
            || metadata.package_bytes != total - 8 - length
        {
            return Err(CacheError::InvalidData);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(metadata.package_bytes as usize)
            .map_err(|_| CacheError::InvalidData)?;
        file.take(metadata.package_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CacheError::FileAccess)?;
        if bytes.len() as u64 != metadata.package_bytes {
            return Err(CacheError::InvalidData);
        }
        verify_package(&bytes, &metadata.signature, &self.pubkey)?;
        Ok(Some(CachedUpdate {
            version: metadata.version,
            signature: metadata.signature,
            scheduled: false,
            bytes,
        }))
    }

    fn read_schedule(&self, ready: &CachedUpdate) -> Result<bool, CacheError> {
        let path = self.root.join(SCHEDULE_FILE);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(_) => return Err(CacheError::FileAccess),
        };
        let mut bytes = Vec::new();
        file.take(MAX_METADATA_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CacheError::FileAccess)?;
        let schedule = if bytes.len() as u64 <= MAX_METADATA_BYTES {
            serde_json::from_slice::<Schedule>(&bytes).ok()
        } else {
            None
        };
        if let Some(schedule) = schedule
            && schedule.version == ready.version
            && schedule.signature == ready.signature
        {
            return Ok(schedule.scheduled);
        }
        remove_file_if_present(&path)?;
        Ok(false)
    }

    fn clear_files(&self) -> Result<(), CacheError> {
        remove_file_if_present(&self.root.join(SCHEDULE_FILE))?;
        remove_file_if_present(&self.root.join(READY_FILE))
    }

    fn atomic_write(&self, filename: &str, chunks: &[&[u8]]) -> Result<(), CacheError> {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|_| CacheError::FileAccess)?;
        let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = self.root.join(format!(".cache-{suffix}.tmp"));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).map_err(|_| CacheError::FileAccess)?;
        let temp = TemporaryFile(path);
        for chunk in chunks {
            file.write_all(chunk).map_err(|_| CacheError::FileAccess)?;
        }
        file.sync_all().map_err(|_| CacheError::FileAccess)?;
        drop(file);
        fs::rename(&temp.0, self.root.join(filename)).map_err(|_| CacheError::FileAccess)?;
        #[cfg(unix)]
        File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| CacheError::FileAccess)?;
        Ok(())
    }
}

pub fn verify_package(bytes: &[u8], signature: &str, pubkey: &str) -> Result<(), CacheError> {
    if bytes.is_empty()
        || bytes.len() as u64 > MAX_PACKAGE_BYTES
        || signature.len() > MAX_SIGNATURE_BYTES
        || pubkey.len() > 4096
    {
        return Err(CacheError::InvalidSignature);
    }
    let key_text = STANDARD
        .decode(pubkey)
        .map_err(|_| CacheError::InvalidSignature)?;
    let key_text = std::str::from_utf8(&key_text).map_err(|_| CacheError::InvalidSignature)?;
    let public_key = PublicKey::decode(key_text).map_err(|_| CacheError::InvalidSignature)?;
    let signature_text = STANDARD
        .decode(signature)
        .map_err(|_| CacheError::InvalidSignature)?;
    let signature_text =
        std::str::from_utf8(&signature_text).map_err(|_| CacheError::InvalidSignature)?;
    let signature = Signature::decode(signature_text).map_err(|_| CacheError::InvalidSignature)?;
    public_key
        .verify(bytes, &signature, true)
        .map_err(|_| CacheError::InvalidSignature)
}

fn parse_version(version: &str) -> Result<Version, CacheError> {
    if version.len() > 256 {
        return Err(CacheError::InvalidVersion);
    }
    Version::parse(version).map_err(|_| CacheError::InvalidVersion)
}

fn remove_file_if_present(path: &Path) -> Result<(), CacheError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(CacheError::FileAccess),
    }
}

fn is_link(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        // Include junctions and other reparse points, not just symbolic links.
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    metadata.file_type().is_symlink()
}

struct TemporaryFile(PathBuf);
impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
