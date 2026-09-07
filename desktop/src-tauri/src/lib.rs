pub mod downloads;
pub mod host;
pub mod lifecycle;
pub mod logging;
pub mod navigation;
pub mod protocol;
pub mod settings;
pub mod shutdown;
pub mod tauri_host;
pub mod updates;

use crate::{
    host::{SupervisorTimings, desktop_data_directory, resolve_test_config, run_supervisor_loop},
    lifecycle::SupervisorCommand,
    navigation::{DesktopAction, Navigation, classify_navigation},
    tauri_host::{DesktopViewState, SharedOrigin, SharedViewState, TauriHostIo, apply_view_state},
};
use std::{
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Sender},
    },
    time::Duration,
};
use tauri::{
    Manager, RunEvent, WebviewUrl, WebviewWindowBuilder, WindowEvent,
    webview::{NewWindowResponse, PageLoadEvent},
};
use url::Origin;

const MAIN_WINDOW: &str = "main";
const APP_DISPLAY_NAME: &str = "Cy KafClient";
const TEST_CONFIG_ENV: &str = "CY_KAF_DESKTOP_TEST_CONFIG";
const SUPERVISOR_POLL: Duration = Duration::from_millis(50);
const OPEN_LOGS_ERROR_SCRIPT: &str = "window.dispatchEvent(new Event('cy-kaf-open-logs-error'))";

type SharedCommandSender = Arc<Mutex<Option<Sender<SupervisorCommand>>>>;

pub fn run() {
    let command_sender: SharedCommandSender = Arc::new(Mutex::new(None));
    let allow_exit = Arc::new(AtomicBool::new(false));
    let setup_sender = Arc::clone(&command_sender);
    let setup_allow_exit = Arc::clone(&allow_exit);
    let close_sender = Arc::clone(&command_sender);
    let close_allow_exit = Arc::clone(&allow_exit);

    let app = tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            if let Some(window) = app.get_webview_window(MAIN_WINDOW) {
                let _ = window.show();
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .on_window_event(move |window, event| {
            if window.label() == MAIN_WINDOW
                && matches!(event, WindowEvent::CloseRequested { .. })
                && !close_allow_exit.load(Ordering::SeqCst)
            {
                if let WindowEvent::CloseRequested { api, .. } = event {
                    api.prevent_close();
                }
                send_supervisor_command(&close_sender, SupervisorCommand::StopAndExit);
            }
        })
        .setup(move |app| {
            let test_config = resolve_test_config(std::env::var_os(TEST_CONFIG_ENV))?;
            let data_directory =
                desktop_data_directory(&app.path().config_dir()?, test_config.as_deref());
            let settings_path = data_directory.join("desktop.json");
            let log_directory = data_directory.join("logs");

            let (sender, receiver) = mpsc::channel();
            *setup_sender
                .lock()
                .map_err(|_| "desktop command channel is poisoned")? = Some(sender.clone());

            let app_origin: SharedOrigin = Arc::new(RwLock::new(None));
            let sidecar_origin: SharedOrigin = Arc::new(RwLock::new(None));
            let view_state: SharedViewState = Arc::new(RwLock::new(DesktopViewState::starting()));
            std::fs::create_dir_all(&data_directory)?;
            let updater = updates::runtime::UpdateManager::new(
                app.handle().clone(),
                std::fs::canonicalize(&data_directory)?,
                Arc::clone(&sidecar_origin),
                sender.clone(),
                Arc::clone(&setup_allow_exit),
                test_config.is_some(),
            )?;
            app.manage(updater.clone());

            let navigation_app = Arc::clone(&app_origin);
            let navigation_sidecar = Arc::clone(&sidecar_origin);
            let navigation_sender = sender.clone();
            let navigation_logs = log_directory.clone();
            let navigation_app_handle = app.handle().clone();
            let new_window_app = Arc::clone(&app_origin);
            let new_window_sidecar = Arc::clone(&sidecar_origin);
            let page_view_state = Arc::clone(&view_state);
            let download_sidecar = Arc::clone(&sidecar_origin);
            let download_directory = match test_config.as_ref() {
                Some(config) => config.parent().map(|parent| parent.join("downloads")),
                None => app.path().download_dir().ok(),
            };

            let window =
                WebviewWindowBuilder::new(app, MAIN_WINDOW, WebviewUrl::App("index.html".into()))
                    .title(APP_DISPLAY_NAME)
                    .inner_size(1440.0, 900.0)
                    .min_inner_size(1024.0, 700.0)
                    .resizable(true)
                    .devtools(cfg!(debug_assertions))
                    .initialization_script("Object.defineProperty(window, '__CY_KAF_DESKTOP_UPDATES__', {value: true});")
                    .on_navigation(move |url| {
                        handle_navigation(
                            url,
                            &navigation_app,
                            &navigation_sidecar,
                            &navigation_sender,
                            &navigation_logs,
                            &navigation_app_handle,
                        )
                    })
                    .on_download(move |webview, event| {
                        match event {
                            tauri::webview::DownloadEvent::Requested { url, destination } => {
                                let allowed = download_sidecar.read().ok().and_then(|origin| {
                                    downloads::config_download_destination(
                                        &url, origin.as_ref(), download_directory.as_deref()?, destination,
                                    )
                                });
                                if let Some(path) = allowed
                                    && path.parent().is_some_and(|parent| std::fs::create_dir_all(parent).is_ok())
                                {
                                    *destination = path;
                                    return true;
                                }
                                let _ = webview.eval("window.dispatchEvent(new CustomEvent('cy-kaf-config-export-finished', {detail: {success: false}}));");
                                false
                            }
                            tauri::webview::DownloadEvent::Finished { success, .. } => {
                                let script = format!("window.dispatchEvent(new CustomEvent('cy-kaf-config-export-finished', {{detail: {{success: {success}}}}}));");
                                let _ = webview.eval(&script);
                                true
                            }
                            _ => false,
                        }
                    })
                    .on_new_window(move |url, _features| {
                        handle_new_window(&url, &new_window_app, &new_window_sidecar);
                        NewWindowResponse::Deny
                    })
                    .on_page_load(move |window, payload| {
                        if payload.event() == PageLoadEvent::Finished
                            && is_bootstrap_url(payload.url())
                            && let Ok(state) = page_view_state.read()
                        {
                            let _ = apply_view_state(&window, &state);
                        }
                    })
                    .build()?;

            let app_url = window.url()?;
            *app_origin
                .write()
                .map_err(|_| "desktop app origin is poisoned")? = Some(app_url.origin());

            let host = TauriHostIo::new(
                app.handle().clone(),
                window,
                app_url,
                sidecar_origin,
                view_state,
                Arc::clone(&setup_allow_exit),
            );
            std::thread::Builder::new()
                .name("cy-kaf-supervisor".into())
                .spawn(move || {
                    if updater.begin_session() {
                        return;
                    }
                    run_supervisor_loop(
                        host,
                        receiver,
                        settings_path,
                        log_directory,
                        test_config,
                        SupervisorTimings::default(),
                        SUPERVISOR_POLL,
                    );
                })?;
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("failed to build cy-kaf-client desktop shell");

    app.run(move |_app, event| {
        if let RunEvent::ExitRequested { api, .. } = event
            && !allow_exit.load(Ordering::SeqCst)
        {
            api.prevent_exit();
            send_supervisor_command(&command_sender, SupervisorCommand::StopAndExit);
        }
    });
}

fn handle_navigation(
    url: &url::Url,
    app_origin: &SharedOrigin,
    sidecar_origin: &SharedOrigin,
    sender: &Sender<SupervisorCommand>,
    log_directory: &std::path::Path,
    app_handle: &tauri::AppHandle,
) -> bool {
    if url.scheme() == "cy-kaf-action"
        && url
            .host_str()
            .is_some_and(|host| host.starts_with("updates-"))
    {
        if let Some(updater) = app_handle.try_state::<updates::runtime::UpdateManager>() {
            updater.handle_navigation(url);
        }
        return false;
    }
    let app = match captured_app_origin(app_origin, url) {
        Some(origin) => origin,
        None => return false,
    };
    let sidecar = match sidecar_origin.read() {
        Ok(origin) => origin.clone(),
        Err(_) => return false,
    };
    match classify_navigation(url, &app, sidecar.as_ref()) {
        Navigation::Allow => true,
        Navigation::Action(action) => {
            if should_dispatch_action(action, sidecar.is_some())
                && let Err(error) = dispatch_action(action, sender, log_directory)
            {
                report_action_error(app_handle, error);
            }
            false
        }
        Navigation::ExternalHttps(_) | Navigation::Deny => false,
    }
}

fn handle_new_window(url: &url::Url, app_origin: &SharedOrigin, sidecar_origin: &SharedOrigin) {
    let Some(app) = captured_app_origin(app_origin, url) else {
        return;
    };
    let Ok(sidecar) = sidecar_origin.read() else {
        return;
    };
    if let Navigation::ExternalHttps(external) = classify_navigation(url, &app, sidecar.as_ref()) {
        let _ = open::that_detached(external.as_str());
    }
}

fn captured_app_origin(shared: &SharedOrigin, candidate: &url::Url) -> Option<Origin> {
    if let Ok(origin) = shared.read()
        && let Some(origin) = origin.as_ref()
    {
        return Some(origin.clone());
    }
    if !is_bootstrap_url(candidate) {
        return None;
    }
    let observed = candidate.origin();
    if let Ok(mut origin) = shared.write() {
        *origin = Some(observed.clone());
    }
    Some(observed)
}

fn is_bootstrap_url(url: &url::Url) -> bool {
    url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
        && matches!(
            (url.scheme(), url.host_str()),
            ("tauri", Some("localhost")) | ("http", Some("tauri.localhost"))
        )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActionDispatchError {
    OpenLogs,
}

fn action_allowed_while_ready(action: DesktopAction) -> bool {
    matches!(action, DesktopAction::OpenLogs)
}

fn should_dispatch_action(action: DesktopAction, sidecar_ready: bool) -> bool {
    !sidecar_ready || action_allowed_while_ready(action)
}

fn dispatch_action(
    action: DesktopAction,
    sender: &Sender<SupervisorCommand>,
    log_directory: &std::path::Path,
) -> Result<(), ActionDispatchError> {
    dispatch_action_with_opener(action, sender, log_directory, |directory| {
        open::that_detached(directory)
    })
}

fn dispatch_action_with_opener<F>(
    action: DesktopAction,
    sender: &Sender<SupervisorCommand>,
    log_directory: &std::path::Path,
    open_logs: F,
) -> Result<(), ActionDispatchError>
where
    F: FnOnce(&std::path::Path) -> std::io::Result<()>,
{
    match action {
        DesktopAction::Retry => {
            let _ = sender.send(SupervisorCommand::Retry);
        }
        DesktopAction::ReassignPort => {
            let _ = sender.send(SupervisorCommand::ReassignPort);
        }
        DesktopAction::OpenLogs => {
            open_logs(log_directory).map_err(|_| ActionDispatchError::OpenLogs)?;
        }
        DesktopAction::Quit => {
            let _ = sender.send(SupervisorCommand::StopAndExit);
        }
    }
    Ok(())
}

fn report_action_error(app: &tauri::AppHandle, error: ActionDispatchError) {
    match error {
        ActionDispatchError::OpenLogs => {
            let Some(window) = app.get_webview_window(MAIN_WINDOW) else {
                eprintln!("open_logs_error_notification_unavailable");
                return;
            };
            if window.eval(OPEN_LOGS_ERROR_SCRIPT).is_err() {
                eprintln!("open_logs_error_notification_failed");
            }
        }
    }
}

fn send_supervisor_command(shared: &SharedCommandSender, command: SupervisorCommand) {
    if let Ok(sender) = shared.lock()
        && let Some(sender) = sender.as_ref()
    {
        let _ = sender.send(command);
    }
}

#[cfg(test)]
mod action_dispatch_tests {
    use super::{
        ActionDispatchError, DesktopAction, OPEN_LOGS_ERROR_SCRIPT, SupervisorCommand,
        dispatch_action_with_opener, should_dispatch_action,
    };
    use std::{cell::Cell, io, path::Path, sync::mpsc};

    #[test]
    fn dispatches_all_actions_before_ready_and_only_open_logs_when_ready() {
        for (sidecar_ready, action, expected) in [
            (false, DesktopAction::Retry, true),
            (false, DesktopAction::ReassignPort, true),
            (false, DesktopAction::OpenLogs, true),
            (false, DesktopAction::Quit, true),
            (true, DesktopAction::Retry, false),
            (true, DesktopAction::ReassignPort, false),
            (true, DesktopAction::OpenLogs, true),
            (true, DesktopAction::Quit, false),
        ] {
            let (sender, receiver) = mpsc::channel();
            let opened = Cell::new(false);

            if should_dispatch_action(action, sidecar_ready) {
                dispatch_action_with_opener(action, &sender, Path::new("/synthetic/logs"), |_| {
                    opened.set(true);
                    Ok(())
                })
                .expect("allowed action should dispatch");
            }

            assert_eq!(
                should_dispatch_action(action, sidecar_ready),
                expected,
                "unexpected dispatch eligibility for {action:?} while ready={sidecar_ready}"
            );
            match action {
                DesktopAction::Retry if expected => {
                    assert_eq!(receiver.try_recv(), Ok(SupervisorCommand::Retry));
                }
                DesktopAction::ReassignPort if expected => {
                    assert_eq!(receiver.try_recv(), Ok(SupervisorCommand::ReassignPort));
                }
                DesktopAction::Quit if expected => {
                    assert_eq!(receiver.try_recv(), Ok(SupervisorCommand::StopAndExit));
                }
                DesktopAction::OpenLogs => assert_eq!(opened.get(), expected),
                _ => assert!(receiver.try_recv().is_err()),
            }
        }
    }

    #[test]
    fn returns_a_fixed_open_logs_error_when_the_injected_opener_fails() {
        let (sender, _receiver) = mpsc::channel();

        let error = dispatch_action_with_opener(
            DesktopAction::OpenLogs,
            &sender,
            Path::new("/synthetic/logs"),
            |_| Err(io::Error::other("test opener failure")),
        );

        assert_eq!(error, Err(ActionDispatchError::OpenLogs));
    }

    #[test]
    fn open_logs_notification_has_an_exact_fixed_no_data_script() {
        assert_eq!(
            OPEN_LOGS_ERROR_SCRIPT,
            "window.dispatchEvent(new Event('cy-kaf-open-logs-error'))"
        );
        assert!(!OPEN_LOGS_ERROR_SCRIPT.contains("detail"));
        assert!(!OPEN_LOGS_ERROR_SCRIPT.contains("/synthetic/logs"));
    }
}

#[cfg(test)]
mod display_name_tests {
    use super::APP_DISPLAY_NAME;

    #[test]
    fn visible_names_are_unified_without_renaming_internal_identifiers() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        assert_eq!(APP_DISPLAY_NAME, "Cy KafClient");
        assert_eq!(config["productName"].as_str(), Some(APP_DISPLAY_NAME));
        assert_eq!(config["identifier"].as_str(), Some("com.cykaf.client"));
        assert_eq!(
            config["bundle"]["externalBin"][0].as_str(),
            Some("binaries/cy-kaf-client")
        );

        let bootstrap = include_str!("../../bootstrap/index.html");
        assert!(bootstrap.contains("<title>Cy KafClient</title>"));
        assert!(bootstrap.contains("<h1>Cy KafClient</h1>"));
        assert!(
            include_str!("../../../frontend/index.html").contains("<title>Cy KafClient</title>")
        );

        let manifest: serde_json::Value =
            serde_json::from_str(include_str!("../../../frontend/public/manifest.json")).unwrap();
        assert_eq!(manifest["name"].as_str(), Some(APP_DISPLAY_NAME));
    }

    #[test]
    fn windows_smoke_keeps_internal_executable_separate_from_visible_names() {
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).unwrap();
        let smoke = include_str!("../../../scripts/desktop-smoke.ps1");

        assert_eq!(env!("CARGO_PKG_NAME"), "cy-kaf-client-desktop");
        assert!(config.get("mainBinaryName").is_none());
        assert!(smoke.contains(r#"$ExpectedExecutableName = "cy-kaf-client-desktop.exe""#));
        assert!(smoke.contains(r#"$ExpectedDisplayName = "Cy KafClient""#));
        assert!(
            smoke.contains("$ApplicationPath = Join-Path $InstallPath $ExpectedExecutableName")
        );
        assert!(smoke.contains("$ProductRegistration.DisplayName -ne $ExpectedDisplayName"));
        assert!(smoke.contains(
            r#"$DesktopShortcutPath = Join-Path $DesktopDirectory "$ExpectedDisplayName.lnk""#
        ));
    }

    #[test]
    fn windows_ci_installs_below_the_system_temp_directory() {
        let workflow = include_str!("../../../.github/workflows/ci.yml");

        assert!(workflow.contains(
            r#"$InstallDir = Join-Path ([System.IO.Path]::GetTempPath()) ("cy-kaf-client-smoke-" + [System.Guid]::NewGuid())"#
        ));
        assert!(
            !workflow.contains(r#"$InstallDir = Join-Path $env:RUNNER_TEMP "cy-kaf-client-smoke""#)
        );
    }

    #[test]
    fn macos_smoke_uses_an_isolated_native_saved_state_home() {
        let smoke = include_str!("../../../scripts/desktop-smoke.sh");

        assert!(smoke.contains(
            r#"CFFIXED_USER_HOME="$smoke_directory" CY_KAF_DESKTOP_TEST_CONFIG="$config_path""#
        ));
    }
}

#[cfg(test)]
mod release_contract_tests;
