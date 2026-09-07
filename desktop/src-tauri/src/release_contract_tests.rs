use std::{fs, path::PathBuf, process::Command};

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
fn release_requires_six_installers_updater_payloads_and_one_checksum_file() {
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
    }
    for suffix in [
        "macos-x86_64.app.tar.gz",
        "macos-aarch64.app.tar.gz",
        "windows-x86_64-setup.exe.sig",
        "windows-aarch64-setup.exe.sig",
        "linux-x86_64.AppImage.sig",
        "linux-aarch64.AppImage.sig",
    ] {
        assert!(workflow.contains(suffix), "missing {suffix}");
    }
    assert!(workflow.contains("latest.json"));
    assert!(workflow.contains("sha256sum -c SHA256SUMS.txt"));
    // All three native signing steps must explicitly pass even an empty
    // password, otherwise Tauri attempts an interactive terminal prompt in CI.
    assert_eq!(
        workflow
            .matches("TAURI_SIGNING_PRIVATE_KEY_PASSWORD:")
            .count(),
        3
    );
}

#[test]
fn updater_manifest_command_reads_real_signature_contents_for_all_targets() {
    let temp = tempfile::tempdir().expect("temp artifact directory");
    let version = "1.2.3";
    let artifacts = [
        "macos-x86_64.app.tar.gz",
        "macos-aarch64.app.tar.gz",
        "windows-x86_64-setup.exe",
        "windows-aarch64-setup.exe",
        "linux-x86_64.AppImage",
        "linux-aarch64.AppImage",
    ];
    for (index, suffix) in artifacts.iter().enumerate() {
        let artifact = temp.path().join(format!("Cy-KafClient_{version}_{suffix}"));
        fs::write(&artifact, format!("package-{index}")).expect("write package fixture");
        fs::write(
            artifact.with_file_name(format!(
                "{}.sig",
                artifact.file_name().unwrap().to_string_lossy()
            )),
            format!("real-signature-{index}\n"),
        )
        .expect("write signature fixture");
    }
    let output = temp.path().join("latest.json");
    let status = Command::new("go")
        .current_dir(repository_root())
        .args([
            "run",
            "./scripts/updatermanifest",
            "-dir",
            temp.path().to_str().unwrap(),
            "-version",
            version,
            "-tag",
            "v1.2.3",
            "-notes",
            "release notes",
            "-pub-date",
            "2026-09-07T08:09:10Z",
            "-output",
            output.to_str().unwrap(),
        ])
        .status()
        .expect("run updater manifest helper");
    assert!(status.success());

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(output).expect("read generated manifest"))
            .expect("parse generated manifest");
    let platforms = manifest["platforms"].as_object().expect("platform map");
    assert_eq!(platforms.len(), 6);
    for (index, target) in [
        "darwin-x86_64",
        "darwin-aarch64",
        "windows-x86_64",
        "windows-aarch64",
        "linux-x86_64",
        "linux-aarch64",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            platforms[*target]["signature"],
            format!("real-signature-{index}")
        );
    }
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
