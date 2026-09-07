use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use std::fs;

// Generated with `cargo tauri signer` for these exact bytes. The ephemeral test
// private key was discarded; this is not the application's release key.
const PACKAGE: &[u8] = b"Cy KafClient signed update cache fixture\n";
const PUBLIC_KEY: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDMwMzY0NjU1MDkyMUVCNTIKUldSUzZ5RUpWVVkyTVBmVWtDSWRVL3d5WTg5WUU3bS9tMWtrL3ZTQkRTNjh4TUNOYm5qc2RTWjEK";
const SIGNATURE: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IHNpZ25hdHVyZSBmcm9tIHRhdXJpIHNlY3JldCBrZXkKUlVSUzZ5RUpWVVkyTVAwVFNPQ1hFcmFEUlZqaUhmVms0WWQ0dkZmSEx2Yi9USXlySUgrRTBCcDRQdXh2eDJPSGlvT0wrVmdCc0d2TXNVQjB5MkpnVTVDamV0c3FxQUtKRVFZPQp0cnVzdGVkIGNvbW1lbnQ6IHRpbWVzdGFtcDoxNzg4NzYxNTEyCWZpbGU6cGFja2FnZS5iaW4KS1BCYUNaNXVVc25CSlZVRlcrRkVidjFuNXhRaEo2SDFOSFNRVmFtdnhhbzBEYkc1WGdBRDJVWXVlTEdyaW01NUZ5VVEvY2RrMGJDZHF6cUdvUmZlQkE9PQo=";

fn fixture() -> (tempfile::TempDir, PathBuf, Cache) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("updates");
    let cache = Cache::new(root.clone(), PUBLIC_KEY.into());
    (temp, root, cache)
}

#[test]
fn verifies_real_tauri_signature_and_rejects_tampered_data_signature_and_key() {
    verify_package(PACKAGE, SIGNATURE, PUBLIC_KEY).unwrap();
    assert!(verify_package(b"altered installer", SIGNATURE, PUBLIC_KEY).is_err());
    let mut signature = STANDARD.decode(SIGNATURE).unwrap();
    let position = signature.iter().position(|value| *value == b'\n').unwrap() + 15;
    signature[position] = if signature[position] == b'A' {
        b'B'
    } else {
        b'A'
    };
    assert!(verify_package(PACKAGE, &STANDARD.encode(signature), PUBLIC_KEY).is_err());
    assert!(verify_package(PACKAGE, SIGNATURE, "private-invalid-key").is_err());
    assert!(verify_package(PACKAGE, "private-invalid-signature", PUBLIC_KEY).is_err());
    let error = verify_package(PACKAGE, "private-invalid-signature", PUBLIC_KEY).unwrap_err();
    assert!(!error.to_string().contains("private-invalid"));
    assert!(!format!("{error:?}").contains("private-invalid"));
}

#[test]
fn saved_signed_package_survives_reopening_with_schedule_initially_off() {
    let (_temp, root, cache) = fixture();
    assert!(cache.load("1.0.0").unwrap().is_none());
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    let reopened = Cache::new(root, PUBLIC_KEY.into());
    let ready = reopened.load("1.0.0").unwrap().unwrap();
    assert_eq!(ready.version, "1.2.0");
    assert_eq!(ready.signature, SIGNATURE);
    assert_eq!(ready.bytes, PACKAGE);
    assert!(!ready.scheduled);
}

#[test]
fn schedule_and_cancel_survive_reopening_without_discarding_package() {
    let (_temp, root, cache) = fixture();
    assert!(cache.set_scheduled(true).is_err());
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    cache.set_scheduled(true).unwrap();
    let reopened = Cache::new(root, PUBLIC_KEY.into());
    assert!(reopened.load("1.0.0").unwrap().unwrap().scheduled);
    reopened.set_scheduled(false).unwrap();
    let ready = cache.load("1.0.0").unwrap().unwrap();
    assert!(!ready.scheduled);
    assert_eq!(ready.bytes, PACKAGE);
}

#[test]
fn replacing_ready_package_cancels_the_previous_schedule() {
    let (_temp, _root, cache) = fixture();
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    cache.set_scheduled(true).unwrap();
    cache.save("1.3.0", SIGNATURE, PACKAGE).unwrap();
    assert!(!cache.load("1.0.0").unwrap().unwrap().scheduled);
}

#[test]
fn accepts_only_strictly_newer_semver_and_clears_stale_schedule() {
    for (available, current, newer) in [
        ("1.2.0", "1.1.9", true),
        ("1.2.0", "1.2.0", false),
        ("1.2.0", "1.3.0", false),
        ("1.2.0+build2", "1.2.0+build1", false),
        ("1.2.0-beta.1", "1.2.0", false),
        ("1.2.0", "1.2.0-beta.1", true),
    ] {
        let (_temp, _root, cache) = fixture();
        cache.save(available, SIGNATURE, PACKAGE).unwrap();
        cache.set_scheduled(true).unwrap();
        assert_eq!(cache.load(current).unwrap().is_some(), newer);
        if !newer {
            assert!(cache.load("0.0.1").unwrap().is_none());
        }
    }
}

#[test]
fn invalid_replacement_preserves_last_good_package_and_schedule() {
    let (_temp, _root, cache) = fixture();
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    cache.set_scheduled(true).unwrap();
    assert!(
        cache
            .save("../private-invalid-version", SIGNATURE, PACKAGE)
            .is_err()
    );
    assert!(cache.save("1.3.0", SIGNATURE, b"tampered").is_err());
    let ready = cache.load("1.0.0").unwrap().unwrap();
    assert_eq!(ready.version, "1.2.0");
    assert!(ready.scheduled);
}

#[test]
fn corrupted_cached_package_is_rejected_then_removed_without_restart_loop() {
    let (_temp, root, cache) = fixture();
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    cache.set_scheduled(true).unwrap();
    let path = root.join("ready.cache");
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(cache.load("1.0.0").is_err());
    assert!(cache.load("1.0.0").unwrap().is_none());
}

#[test]
fn truncated_or_oversized_metadata_never_exposes_ready_package() {
    for invalid in [b"not a cache".to_vec(), u64::MAX.to_le_bytes().to_vec()] {
        let (_temp, root, cache) = fixture();
        cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
        fs::write(root.join("ready.cache"), invalid).unwrap();
        assert!(cache.load("1.0.0").is_err());
        assert!(cache.load("1.0.0").unwrap().is_none());
    }
}

#[test]
fn malformed_or_mismatched_schedule_defaults_to_not_scheduled() {
    for invalid in [
        "not json",
        "{\"version\":\"9.0.0\",\"signature\":\"old\",\"scheduled\":true}",
    ] {
        let (_temp, root, cache) = fixture();
        cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
        fs::write(root.join("schedule.json"), invalid).unwrap();
        assert!(!cache.load("1.0.0").unwrap().unwrap().scheduled);
    }
}

#[test]
fn interrupted_temporary_write_is_never_loaded_and_clear_is_idempotent() {
    let (_temp, root, cache) = fixture();
    fs::create_dir(&root).unwrap();
    fs::write(root.join(".partial.tmp"), PACKAGE).unwrap();
    assert!(cache.load("1.0.0").unwrap().is_none());
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    cache.set_scheduled(true).unwrap();
    cache.clear().unwrap();
    cache.clear().unwrap();
    assert!(cache.load("1.0.0").unwrap().is_none());
}

#[test]
fn oversized_cached_package_is_rejected_before_allocating_it() {
    let (_temp, root, cache) = fixture();
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(root.join("ready.cache"))
        .unwrap()
        .set_len(MAX_PACKAGE_BYTES + 65536)
        .unwrap();
    assert!(cache.load("1.0.0").is_err());
    assert!(cache.load("1.0.0").unwrap().is_none());
}

#[cfg(unix)]
#[test]
fn cache_directory_and_files_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let (_temp, root, cache) = fixture();
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    cache.set_scheduled(true).unwrap();
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for filename in ["ready.cache", "schedule.json"] {
        assert_eq!(
            fs::metadata(root.join(filename))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[cfg(unix)]
#[test]
fn rejects_symlink_root_ancestor_and_cache_files_without_touching_target() {
    use std::os::unix::fs::symlink;
    let (temp, root, cache) = fixture();
    let target = temp.path().canonicalize().unwrap().join("outside");
    fs::create_dir(&target).unwrap();
    symlink(&target, &root).unwrap();
    assert!(cache.save("1.2.0", SIGNATURE, PACKAGE).is_err());
    assert!(cache.load("1.0.0").is_err());
    assert!(cache.clear().is_err());
    let nested = Cache::new(root.join("nested"), PUBLIC_KEY.into());
    assert!(nested.save("1.2.0", SIGNATURE, PACKAGE).is_err());
    assert!(!target.join("nested").exists());
    fs::remove_file(&root).unwrap();
    cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    let outside = target.join("untouched");
    fs::write(&outside, b"private sentinel").unwrap();
    for filename in ["ready.cache", "schedule.json"] {
        let path = root.join(filename);
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }
        symlink(&outside, &path).unwrap();
        assert!(cache.load("1.0.0").is_err());
        assert!(cache.save("1.3.0", SIGNATURE, PACKAGE).is_err());
        assert!(cache.set_scheduled(true).is_err());
        assert_eq!(fs::read(&outside).unwrap(), b"private sentinel");
        fs::remove_file(path).unwrap();
        cache.save("1.2.0", SIGNATURE, PACKAGE).unwrap();
    }
}
