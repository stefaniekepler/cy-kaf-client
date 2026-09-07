use super::{
    cache::Cache,
    state::Phase,
    transport,
    worker::{Action, Backend, Offer, ReadyOffer, Snapshot, Worker},
};
use crate::{lifecycle::SupervisorCommand, tauri_host::SharedOrigin};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc::{self, Sender, SyncSender},
    },
    time::{Duration, Instant},
};
use tauri::{AppHandle, Manager};
use tauri_plugin_updater::Update;

const BUSY_ERROR: &str = "其他客户端或 MCP 进程正在使用程序，请结束后重新检查更新。";
const INSTALL_ERROR: &str = "暂时无法安装更新，当前版本可以继续使用，请重新检查更新。";
const FIRST_CHECK: Duration = Duration::from_secs(60);
const CHECK_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

struct Prepared {
    update: Update,
    bytes: Vec<u8>,
}
struct Context {
    app: AppHandle,
    cache: Cache,
    current_version: String,
    origin: SharedOrigin,
    supervisor: Sender<SupervisorCommand>,
    allow_exit: Arc<AtomicBool>,
    sidecar_pid: AtomicU32,
    startup_done: AtomicBool,
    prepared: Mutex<Option<Prepared>>,
    test_mode: bool,
}

#[derive(Clone)]
pub(crate) struct UpdateManager {
    context: Arc<Context>,
    actions: SyncSender<Action>,
    snapshot: Arc<Mutex<Snapshot>>,
}

impl UpdateManager {
    pub(crate) fn new(
        app: AppHandle,
        directory: PathBuf,
        origin: SharedOrigin,
        supervisor: Sender<SupervisorCommand>,
        allow_exit: Arc<AtomicBool>,
        test_mode: bool,
    ) -> std::io::Result<Self> {
        let pubkey = app
            .config()
            .plugins
            .0
            .get("updater")
            .and_then(|c| c.get("pubkey"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        let available = !pubkey.is_empty() && (supports_install(&app) || test_mode);
        let context = Arc::new(Context {
            current_version: app.package_info().version.to_string(),
            app,
            cache: Cache::new(directory.join("updates"), pubkey),
            origin,
            supervisor,
            allow_exit,
            sidecar_pid: AtomicU32::new(0),
            startup_done: AtomicBool::new(false),
            prepared: Mutex::new(None),
            test_mode,
        });
        let notify_context = Arc::clone(&context);
        let mut worker = Worker::new(
            NativeBackend {
                context: Arc::clone(&context),
                runtime: None,
                offer: None,
            },
            context.current_version.clone(),
            available,
            move |snapshot| emit(&notify_context, snapshot),
        );
        let snapshot = Arc::clone(&worker.shared);
        let (actions, receiver) = mpsc::sync_channel(1);
        let worker_context = Arc::clone(&context);
        std::thread::Builder::new()
            .name("cy-kaf-updater".into())
            .spawn(move || {
                while !worker_context.startup_done.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(100));
                }
                worker.restore();
                let mut next_check = None;
                loop {
                    if next_check.is_none()
                        && worker_context
                            .origin
                            .read()
                            .is_ok_and(|origin| origin.is_some())
                    {
                        next_check = Some(Instant::now() + FIRST_CHECK);
                    }
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(action) => {
                            worker.handle(action);
                            if matches!(action, Action::Check) {
                                next_check = Some(Instant::now() + CHECK_INTERVAL);
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if next_check.is_some_and(|deadline| Instant::now() >= deadline) {
                                worker.handle(Action::Check);
                                next_check = Some(Instant::now() + CHECK_INTERVAL);
                            }
                        }
                    }
                }
            })?;
        Ok(Self {
            context,
            actions,
            snapshot,
        })
    }

    pub(crate) fn record_sidecar(&self, pid: u32) {
        self.context.sidecar_pid.store(pid, Ordering::Release);
    }

    // Runs before the supervisor creates any sidecar. No scheduled installation
    // can be retried later in this session, even after a transient network error.
    pub(crate) fn begin_session(&self) -> bool {
        let installed = !self.context.test_mode && self.try_scheduled_install();
        self.context.startup_done.store(true, Ordering::Release);
        installed
    }

    fn try_scheduled_install(&self) -> bool {
        let Ok(Some(cached)) = self.context.cache.load(&self.context.current_version) else {
            return false;
        };
        if !cached.scheduled || processes_busy(&self.context) {
            return false;
        }
        let Ok(runtime) = background_runtime() else {
            return false;
        };
        let Ok(update) =
            runtime.block_on(transport::check(&self.context.app, Duration::from_secs(5)))
        else {
            return false;
        };
        let Some(update) = update else {
            let _ = self.context.cache.clear();
            return false;
        };
        if !transport::same_release(
            &cached.version,
            &cached.signature,
            &update.version,
            &update.signature,
        ) {
            let _ = self.context.cache.clear();
            return false;
        }
        // Consume the opt-in before touching the installed application: a failed
        // install/restart can never put the next launch into an installation loop.
        if self.context.cache.set_scheduled(false).is_err() || processes_busy(&self.context) {
            return false;
        }
        if update.install(&cached.bytes).is_err() {
            return false;
        }
        let _ = self.context.cache.clear();
        self.context.allow_exit.store(true, Ordering::SeqCst);
        self.context.app.restart();
    }

    // Called only after our supervisor has stopped its own child and released
    // its listening port. Ordinary window close has no prepared installation.
    pub(crate) fn finish_explicit_install(&self, code: i32) -> bool {
        let prepared = self
            .context
            .prepared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let Some(prepared) = prepared else {
            return false;
        };
        if code == 0
            && !processes_busy(&self.context)
            && prepared.update.install(&prepared.bytes).is_ok()
        {
            let _ = self.context.cache.clear();
        }
        // On install failure restart the existing application after its child
        // was stopped. The schedule was cleared before shutdown, so no retry loop.
        self.context.allow_exit.store(true, Ordering::SeqCst);
        self.context.app.restart();
    }

    pub(crate) fn handle_navigation(&self, url: &url::Url) -> bool {
        let page = self
            .context
            .app
            .get_webview_window("main")
            .and_then(|w| w.url().ok());
        let origin = self.context.origin.read().ok().and_then(|o| o.clone());
        if !trusted_command(url, page.as_ref(), origin.as_ref()) {
            return false;
        }
        let snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let action = match url.host_str() {
            Some("updates-status") => {
                emit(&self.context, &snapshot);
                return true;
            }
            Some("updates-check")
                if matches!(snapshot.phase, Phase::Idle | Phase::UpToDate | Phase::Error) =>
            {
                Action::Check
            }
            Some("updates-install") if snapshot.phase == Phase::Ready => Action::Install,
            Some("updates-schedule") if snapshot.phase == Phase::Ready => Action::Schedule,
            Some("updates-cancel-schedule") if snapshot.phase != Phase::Installing => {
                Action::CancelSchedule
            }
            _ => return true,
        };
        let _ = self.actions.try_send(action);
        true
    }
}

fn background_runtime() -> Result<tokio::runtime::Runtime, &'static str> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| transport::CHECK_ERROR)
}

struct NativeBackend {
    context: Arc<Context>,
    runtime: Option<tokio::runtime::Runtime>,
    offer: Option<Update>,
}
impl NativeBackend {
    fn runtime(&mut self) -> Result<&tokio::runtime::Runtime, &'static str> {
        if self.runtime.is_none() {
            self.runtime = Some(background_runtime()?);
        }
        Ok(self.runtime.as_ref().expect("initialized updater runtime"))
    }
}
impl Backend for NativeBackend {
    fn restore(&mut self) -> Result<Option<ReadyOffer>, &'static str> {
        if self.context.test_mode {
            return Ok(None);
        }
        self.context
            .cache
            .load(&self.context.current_version)
            .map(|cached| {
                cached.map(|c| ReadyOffer {
                    offer: Offer {
                        version: c.version,
                        notes: String::new(),
                    },
                    bytes: c.bytes.len() as u64,
                    scheduled: c.scheduled,
                })
            })
            .map_err(|_| transport::CACHE_ERROR)
    }
    fn check(&mut self) -> Result<Option<Offer>, &'static str> {
        if self.context.test_mode {
            return Ok(None);
        }
        let context = Arc::clone(&self.context);
        self.offer = self
            .runtime()?
            .block_on(transport::check(&context.app, Duration::from_secs(10)))?;
        Ok(self.offer.as_ref().map(|u| Offer {
            version: u.version.clone(),
            notes: u.body.clone().unwrap_or_default(),
        }))
    }
    fn download(
        &mut self,
        _: &Offer,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<u64, &'static str> {
        let update = self.offer.take().ok_or(transport::CHECK_ERROR)?;
        let context = Arc::clone(&self.context);
        if let Ok(Some(cached)) = context.cache.load(&context.current_version)
            && transport::same_release(
                &cached.version,
                &cached.signature,
                &update.version,
                &update.signature,
            )
        {
            return Ok(cached.bytes.len() as u64);
        }
        self.runtime()?
            .block_on(transport::download(&update, &context.cache, progress))
    }
    fn schedule(&mut self, value: bool) -> Result<(), &'static str> {
        self.context
            .cache
            .set_scheduled(value)
            .map_err(|_| transport::CACHE_ERROR)
    }
    fn prepare_install(&mut self) -> Result<(), &'static str> {
        if self.context.test_mode {
            return Err(INSTALL_ERROR);
        }
        let context = Arc::clone(&self.context);
        if processes_busy(&context) {
            return Err(BUSY_ERROR);
        }
        let cached = context
            .cache
            .load(&context.current_version)
            .map_err(|_| transport::CACHE_ERROR)?
            .ok_or(transport::CACHE_ERROR)?;
        let update = self
            .runtime()?
            .block_on(transport::check(&context.app, Duration::from_secs(10)))?
            .ok_or(transport::CACHE_ERROR)?;
        if !transport::same_release(
            &cached.version,
            &cached.signature,
            &update.version,
            &update.signature,
        ) {
            return Err(transport::CACHE_ERROR);
        }
        context
            .cache
            .set_scheduled(false)
            .map_err(|_| transport::CACHE_ERROR)?;
        if processes_busy(&context) {
            return Err(BUSY_ERROR);
        }
        *context.prepared.lock().map_err(|_| INSTALL_ERROR)? = Some(Prepared {
            update,
            bytes: cached.bytes,
        });
        if context
            .supervisor
            .send(SupervisorCommand::StopAndExit)
            .is_err()
        {
            *context.prepared.lock().map_err(|_| INSTALL_ERROR)? = None;
            return Err(INSTALL_ERROR);
        }
        Ok(())
    }
}

fn trusted_command(
    action: &url::Url,
    page: Option<&url::Url>,
    origin: Option<&url::Origin>,
) -> bool {
    action.scheme() == "cy-kaf-action"
        && action.username().is_empty()
        && action.password().is_none()
        && action.port().is_none()
        && action.path().is_empty()
        && action.query().is_none()
        && action.fragment().is_none()
        && matches!(
            action.host_str(),
            Some(
                "updates-status"
                    | "updates-check"
                    | "updates-install"
                    | "updates-schedule"
                    | "updates-cancel-schedule"
            )
        )
        && page.zip(origin).is_some_and(|(page, origin)| {
            page.username().is_empty() && page.password().is_none() && &page.origin() == origin
        })
}

fn status_json(snapshot: &Snapshot) -> serde_json::Value {
    let mut value = serde_json::json!({"available":snapshot.available,"currentVersion":snapshot.current_version,
        "status":snapshot.phase.as_str(),"downloadedBytes":snapshot.downloaded_bytes,"scheduled":snapshot.scheduled});
    for (key, optional) in [
        ("version", &snapshot.version),
        ("notes", &snapshot.notes),
        ("message", &snapshot.message),
    ] {
        if let Some(text) = optional {
            value[key] = text.clone().into();
        }
    }
    if let Some(total) = snapshot.total_bytes {
        value["totalBytes"] = total.into();
    }
    value
}

fn emit(context: &Context, snapshot: &Snapshot) {
    let Some(window) = context.app.get_webview_window("main") else {
        return;
    };
    let Ok(page) = window.url() else {
        return;
    };
    if !context
        .origin
        .read()
        .is_ok_and(|o| o.as_ref() == Some(&page.origin()))
    {
        return;
    }
    let json = status_json(snapshot);
    let _ = window.eval(format!(
        "window.dispatchEvent(new CustomEvent('cy-kaf-update-status',{{detail:{json}}}));"
    ));
}

fn process_matches(
    exe: Option<&Path>,
    name: &std::ffi::OsStr,
    shell: &Path,
    sidecar: &Path,
) -> bool {
    match exe {
        Some(path) => path == shell || path == sidecar,
        None => shell.file_name() == Some(name) || sidecar.file_name() == Some(name),
    }
}

fn processes_busy(context: &Context) -> bool {
    use sysinfo::{ProcessRefreshKind, RefreshKind, System, UpdateKind};
    let Ok(shell) = std::env::current_exe() else {
        return true;
    };
    let Some(parent) = shell.parent() else {
        return true;
    };
    let sidecar = parent.join(if cfg!(windows) {
        "cy-kaf-client.exe"
    } else {
        "cy-kaf-client"
    });
    let system = System::new_with_specifics(
        RefreshKind::nothing()
            .with_processes(ProcessRefreshKind::nothing().with_exe(UpdateKind::Always)),
    );
    if system.processes().is_empty() {
        return true;
    }
    system.processes().iter().any(|(pid, process)| {
        pid.as_u32() != std::process::id()
            && pid.as_u32() != context.sidecar_pid.load(Ordering::Acquire)
            && process_matches(process.exe(), process.name(), &shell, &sidecar)
    })
}

fn supports_install(app: &AppHandle) -> bool {
    #[cfg(target_os = "linux")]
    {
        app.env().appimage.is_some()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = app;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_processes_using_the_installed_shell_or_sidecar_block_installation() {
        let shell = Path::new("/app/cy-kaf-client-desktop");
        let sidecar = Path::new("/app/cy-kaf-client");
        assert!(process_matches(
            Some(sidecar),
            "cy-kaf-client".as_ref(),
            shell,
            sidecar
        ));
        assert!(process_matches(
            Some(shell),
            "cy-kaf-client-desktop".as_ref(),
            shell,
            sidecar
        ));
        assert!(!process_matches(
            Some(Path::new("/other/cy-kaf-client")),
            "cy-kaf-client".as_ref(),
            shell,
            sidecar
        ));
        assert!(!process_matches(
            None,
            "WebKit.Networking".as_ref(),
            shell,
            sidecar
        ));
        assert!(process_matches(
            None,
            "cy-kaf-client".as_ref(),
            shell,
            sidecar
        ));
    }

    #[test]
    fn commands_require_the_current_ready_sidecar_and_exact_action_url() {
        let page = url::Url::parse("http://127.0.0.1:54321/ui").unwrap();
        let origin = page.origin();
        let action = url::Url::parse("cy-kaf-action://updates-check").unwrap();
        assert!(trusted_command(&action, Some(&page), Some(&origin)));
        assert!(!trusted_command(&action, None, Some(&origin)));
        assert!(!trusted_command(&action, Some(&page), None));
        let other = url::Url::parse("http://127.0.0.1:54322/ui").unwrap();
        assert!(!trusted_command(&action, Some(&other), Some(&origin)));
        for raw in [
            "cy-kaf-action://updates-check?url=evil",
            "cy-kaf-action://updates-install/path",
            "cy-kaf-action://updates-unknown",
            "cy-kaf-action://user@updates-check",
        ] {
            assert!(!trusted_command(
                &url::Url::parse(raw).unwrap(),
                Some(&page),
                Some(&origin)
            ));
        }
    }

    #[test]
    fn bridge_omits_unknown_optional_fields_and_uses_capability_availability() {
        let worker = Worker::new(NoopBackend, "0.1.3".into(), true, |_| {});
        let json = status_json(&worker.snapshot());
        assert_eq!(json["available"], true);
        assert_eq!(json["currentVersion"], "0.1.3");
        assert_eq!(json["status"], "idle");
        assert!(json.get("version").is_none());
        assert!(json.get("totalBytes").is_none());
    }

    struct NoopBackend;
    impl Backend for NoopBackend {
        fn restore(&mut self) -> Result<Option<ReadyOffer>, &'static str> {
            Ok(None)
        }
        fn check(&mut self) -> Result<Option<Offer>, &'static str> {
            Ok(None)
        }
        fn download(
            &mut self,
            _: &Offer,
            _: &mut dyn FnMut(u64, Option<u64>),
        ) -> Result<u64, &'static str> {
            unreachable!()
        }
        fn schedule(&mut self, _: bool) -> Result<(), &'static str> {
            unreachable!()
        }
        fn prepare_install(&mut self) -> Result<(), &'static str> {
            unreachable!()
        }
    }
}
