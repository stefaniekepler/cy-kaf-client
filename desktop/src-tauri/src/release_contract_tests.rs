use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn repository_file(path: &str) -> String {
    fs::read_to_string(repository_root().join(path)).unwrap_or_default()
}

#[test]
fn workflow_declares_all_six_native_desktop_targets() {
    let workflow = repository_file(".github/workflows/ci.yml");
    let desktop_matrix = workflow
        .split_once("  desktop-package:")
        .and_then(|(_, desktop_job)| desktop_job.split_once("    steps:"))
        .map(|(matrix, _)| matrix)
        .expect("desktop-package matrix");

    for required in [
        "runner: macos-15-intel",
        "target: x86_64-apple-darwin",
        "runner: macos-15",
        "target: aarch64-apple-darwin",
        "runner: windows-2025",
        "target: x86_64-pc-windows-msvc",
        "runner: windows-11-arm",
        "target: aarch64-pc-windows-msvc",
        "runner: ubuntu-24.04",
        "target: x86_64-unknown-linux-gnu",
        "runner: ubuntu-24.04-arm",
        "target: aarch64-unknown-linux-gnu",
    ] {
        assert!(desktop_matrix.contains(required), "missing {required}");
    }
    assert_eq!(desktop_matrix.matches("          - runner:").count(), 6);
    assert_eq!(desktop_matrix.matches("            target:").count(), 6);
    assert_eq!(desktop_matrix.matches("            asset:").count(), 6);
}

#[test]
fn release_requires_six_installers_and_one_checksum_file() {
    let workflow = repository_file(".github/workflows/ci.yml");

    for suffix in [
        "macos-x86_64.dmg",
        "macos-aarch64.dmg",
        "windows-x86_64-setup.exe",
        "windows-aarch64-setup.exe",
        "linux-x86_64.AppImage",
        "linux-aarch64.AppImage",
    ] {
        assert!(workflow.contains(suffix), "missing {suffix}");
        assert_eq!(
            workflow.matches(suffix).count(),
            2,
            "{suffix} must appear in both release allowlists"
        );
    }
    assert!(workflow.contains("\"SHA256SUMS.txt\""));
}

#[test]
fn linux_packages_have_native_smoke_and_user_documentation() {
    let workflow = repository_file(".github/workflows/ci.yml");
    let smoke = repository_file("scripts/desktop-smoke-linux.sh");
    let readme = repository_file("README.md");

    assert!(workflow.contains("Smoke Linux AppImage"));
    assert!(workflow.contains("scripts/desktop-smoke-linux.sh"));
    assert!(workflow.contains("openbox"));
    assert!(smoke.contains("APPIMAGE_EXTRACT_AND_RUN=1"));
    assert!(smoke.contains(r#""--desktop""#));
    assert!(smoke.contains(r#""--no-browser""#));
    assert!(smoke.contains("openbox --sm-disable"));
    assert!(smoke.contains("xdotool getwindowpid"));
    assert!(smoke.contains("candidate_desktop_pid"));
    assert!(smoke.contains("candidate_sidecar_command"));
    assert!(smoke.contains("pgrep -f 'cy-kaf-client'"));
    assert!(smoke.contains("process diagnostics:"));
    assert!(smoke.contains("window diagnostics:"));
    assert!(smoke.contains("for _ in $(seq 1 180); do"));
    assert!(smoke.contains("within 90 seconds"));
    assert!(!smoke.contains("pgrep -f '/cy-kaf-client --desktop --no-browser'"));
    assert!(smoke.contains(r#"--pid "$desktop_pid""#));
    assert!(!smoke.contains("process_is_descendant"));
    assert!(!smoke.contains(r#"--pid "$shell_pid""#));
    assert!(readme.contains("linux-x86_64.AppImage"));
    assert!(readme.contains("linux-aarch64.AppImage"));
}
