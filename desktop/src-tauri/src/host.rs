use crate::{
    lifecycle::{FailureCode, LifecycleEvent, LifecycleState, SupervisorCommand, transition},
    logging::SidecarLogger,
    protocol::{SidecarErrorCode, SidecarEvent, init_line, new_session_token, parse_sidecar_line},
    settings::{DesktopSettings, load_settings, save_settings},
};
use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, TryRecvError},
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Terminated(Option<i32>),
    Error,
}

pub trait ChildHandle: Send {
    fn pid(&self) -> u32;
    fn write_stdin(&mut self, bytes: &[u8]) -> Result<(), HostError>;
    fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<ChildEvent>, HostError>;
    fn kill(self: Box<Self>) -> Result<(), HostError>;
}

pub trait HostIo: Send + Sync + 'static {
    fn spawn_sidecar(
        &self,
        port: u16,
        test_config: Option<&Path>,
    ) -> Result<Box<dyn ChildHandle>, HostError>;
    fn probe_health(&self, origin: &Url, timeout: Duration) -> Result<(), HostError>;
    fn set_session_cookie(&self, origin: &Url, token: &str) -> Result<(), HostError>;
    fn clear_session_cookie(&self, origin: &Url) -> Result<(), HostError>;
    fn navigate_app(&self) -> Result<(), HostError>;
    fn navigate_sidecar(&self, origin: &Url) -> Result<(), HostError>;
    fn render_failure(&self, code: FailureCode) -> Result<(), HostError>;
    fn request_shutdown(
        &self,
        origin: &Url,
        token: &str,
        timeout: Duration,
    ) -> Result<(), HostError>;
    fn port_released(&self, origin: &Url) -> Result<bool, HostError>;
    fn exit(&self, code: i32);
}

#[derive(Clone, Copy, Debug)]
pub struct SupervisorTimings {
    pub startup: Duration,
    pub health: Duration,
    pub shutdown: Duration,
    pub port_release: Duration,
    pub port_poll: Duration,
}

impl Default for SupervisorTimings {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(15),
            health: Duration::from_secs(2),
            shutdown: Duration::from_secs(5),
            port_release: Duration::from_secs(2),
            port_poll: Duration::from_millis(50),
        }
    }
}

impl SupervisorTimings {
    #[cfg(test)]
    fn for_tests() -> Self {
        Self {
            startup: Duration::from_millis(10),
            health: Duration::from_millis(10),
            shutdown: Duration::from_millis(10),
            port_release: Duration::from_millis(10),
            port_poll: Duration::ZERO,
        }
    }
}

#[derive(Debug, Error)]
pub enum HostError {
    #[error("could not spawn the desktop sidecar")]
    Spawn,
    #[error("desktop sidecar I/O failed")]
    Child,
    #[error("desktop sidecar protocol failed")]
    Protocol,
    #[error("desktop settings operation failed")]
    Settings,
    #[error("desktop logging failed")]
    Logging,
    #[error("desktop health check failed")]
    Health,
    #[error("desktop session cookie operation failed")]
    Cookie,
    #[error("desktop navigation failed")]
    Navigation,
    #[error("desktop failure view could not be rendered")]
    Render,
    #[error("desktop shutdown request failed")]
    Shutdown,
    #[error("desktop lifecycle state is invalid")]
    State,
    #[error("desktop test config is invalid")]
    TestConfig,
    #[error("desktop sidecar port is still listening")]
    PortStillListening,
}

pub struct Supervisor<I: HostIo> {
    io: I,
    state: LifecycleState,
    child: Option<Box<dyn ChildHandle>>,
    token: Option<String>,
    startup_deadline: Option<Instant>,
    settings: DesktopSettings,
    settings_path: PathBuf,
    test_config: Option<PathBuf>,
    logger: SidecarLogger,
    timings: SupervisorTimings,
}

impl<I: HostIo> Supervisor<I> {
    pub fn new(
        io: I,
        settings_path: PathBuf,
        log_directory: PathBuf,
        test_config: Option<PathBuf>,
        timings: SupervisorTimings,
    ) -> Result<Self, HostError> {
        let settings = load_settings(&settings_path).map_err(|_| HostError::Settings)?;
        let logger = SidecarLogger::open(&log_directory).map_err(|_| HostError::Logging)?;
        Ok(Self {
            io,
            state: LifecycleState::Stopped,
            child: None,
            token: None,
            startup_deadline: None,
            settings,
            settings_path,
            test_config,
            logger,
            timings,
        })
    }

    pub fn state(&self) -> &LifecycleState {
        &self.state
    }

    pub fn handle_command(&mut self, command: SupervisorCommand) -> Result<bool, HostError> {
        match command {
            SupervisorCommand::Start | SupervisorCommand::Retry => {
                self.start();
                Ok(false)
            }
            SupervisorCommand::ReassignPort => {
                if !matches!(self.state, LifecycleState::Failed { .. }) {
                    return Ok(false);
                }
                self.settings.port = None;
                save_settings(&self.settings_path, &self.settings)
                    .map_err(|_| HostError::Settings)?;
                self.start();
                Ok(false)
            }
            SupervisorCommand::StopAndExit => {
                self.stop_and_exit()?;
                Ok(true)
            }
        }
    }

    pub fn poll_child(&mut self, timeout: Duration) -> Result<(), HostError> {
        let timeout = if matches!(self.state, LifecycleState::Starting) {
            let Some(deadline) = self.startup_deadline else {
                return self.fail(FailureCode::StartFailed, None);
            };
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return self.fail(FailureCode::StartupTimeout, None);
            };
            timeout.min(remaining)
        } else {
            timeout
        };
        let event = match self.child.as_mut() {
            Some(child) => match child.recv_timeout(timeout) {
                Ok(event) => event,
                Err(_) => {
                    let origin = self.ready_origin();
                    return self.fail(FailureCode::StartFailed, origin.as_ref());
                }
            },
            None => return Ok(()),
        };
        let Some(event) = event else {
            if matches!(self.state, LifecycleState::Starting)
                && self
                    .startup_deadline
                    .is_none_or(|deadline| Instant::now() >= deadline)
            {
                return self.fail(FailureCode::StartupTimeout, None);
            }
            return Ok(());
        };
        match event {
            ChildEvent::Stderr(bytes) => {
                if self.append_stderr(&bytes).is_err() {
                    let origin = self.ready_origin();
                    self.fail(FailureCode::StartFailed, origin.as_ref())
                } else {
                    Ok(())
                }
            }
            ChildEvent::Terminated(_) => self.handle_unexpected_exit(),
            ChildEvent::Stdout(bytes) => {
                if matches!(self.state, LifecycleState::Starting) {
                    match parse_sidecar_line(&bytes) {
                        Ok(Some(SidecarEvent::Ready { origin })) => {
                            self.complete_ready(origin);
                            Ok(())
                        }
                        Ok(Some(SidecarEvent::Error { code })) => {
                            self.fail(map_sidecar_error(code), None)
                        }
                        Ok(None) => Ok(()),
                        Err(_) => self.fail(FailureCode::InitInvalid, None),
                    }
                } else {
                    match parse_sidecar_line(&bytes) {
                        Ok(None) => Ok(()),
                        Ok(Some(_)) | Err(_) => {
                            let origin = self.ready_origin();
                            self.fail(FailureCode::InitInvalid, origin.as_ref())
                        }
                    }
                }
            }
            ChildEvent::Error => {
                let origin = self.ready_origin();
                self.fail(FailureCode::StartFailed, origin.as_ref())
            }
        }
    }

    fn start(&mut self) {
        let restarting = matches!(self.state, LifecycleState::Failed { .. });
        if transition(&mut self.state, LifecycleEvent::StartRequested).is_err() {
            return;
        }
        if restarting && self.io.navigate_app().is_err() {
            let _ = self.fail(FailureCode::StartFailed, None);
            return;
        }

        let token = match new_session_token() {
            Ok(token) => token,
            Err(_) => {
                let _ = self.fail(FailureCode::StartFailed, None);
                return;
            }
        };
        let init = match init_line(&token) {
            Ok(init) => init,
            Err(_) => {
                let _ = self.fail(FailureCode::InitInvalid, None);
                return;
            }
        };
        let port = self.settings.port.unwrap_or(0);
        let child = match self.io.spawn_sidecar(port, self.test_config.as_deref()) {
            Ok(child) => child,
            Err(_) => {
                let _ = self.fail(FailureCode::StartFailed, None);
                return;
            }
        };
        self.child = Some(child);
        self.token = Some(token);
        if self
            .child
            .as_mut()
            .expect("sidecar was just stored")
            .write_stdin(&init)
            .is_err()
        {
            let _ = self.fail(FailureCode::StartFailed, None);
            return;
        }

        let Some(deadline) = Instant::now().checked_add(self.timings.startup) else {
            let _ = self.fail(FailureCode::StartupTimeout, None);
            return;
        };
        self.startup_deadline = Some(deadline);
    }

    fn complete_ready(&mut self, origin: Url) {
        if self.io.probe_health(&origin, self.timings.health).is_err() {
            let _ = self.fail(FailureCode::StartFailed, None);
            return;
        }

        self.settings.port = origin.port();
        if save_settings(&self.settings_path, &self.settings).is_err() {
            let _ = self.fail(FailureCode::StartFailed, None);
            return;
        }

        let Some(token) = self.token.as_deref() else {
            let _ = self.fail(FailureCode::StartFailed, None);
            return;
        };
        if self.io.set_session_cookie(&origin, token).is_err() {
            let _ = self.io.clear_session_cookie(&origin);
            let _ = self.fail(FailureCode::StartFailed, None);
            return;
        }
        if self.io.navigate_sidecar(&origin).is_err() {
            let _ = self.fail(FailureCode::StartFailed, Some(&origin));
            return;
        }

        let pid = self.child.as_ref().map_or(0, |child| child.pid());
        if transition(&mut self.state, LifecycleEvent::Ready { origin, pid }).is_err() {
            let _ = self.fail(FailureCode::StartFailed, None);
        } else {
            self.startup_deadline = None;
        }
    }

    fn append_stderr(&mut self, bytes: &[u8]) -> Result<(), HostError> {
        self.logger
            .append_sidecar_stderr(bytes)
            .map_err(|_| HostError::Logging)?;
        self.logger
            .append_sidecar_stderr(b"\n")
            .map_err(|_| HostError::Logging)
    }

    fn fail(&mut self, code: FailureCode, cookie_origin: Option<&Url>) -> Result<(), HostError> {
        if let Some(origin) = cookie_origin {
            let _ = self.io.clear_session_cookie(origin);
        }
        transition(&mut self.state, LifecycleEvent::StartFailed { code })
            .map_err(|_| HostError::State)?;
        if let Some(child) = self.child.take() {
            let _ = child.kill();
        }
        self.token = None;
        self.startup_deadline = None;
        self.io.navigate_app().map_err(|_| HostError::Navigation)?;
        self.io.render_failure(code).map_err(|_| HostError::Render)
    }

    fn handle_unexpected_exit(&mut self) -> Result<(), HostError> {
        self.child.take();
        self.startup_deadline = None;
        match self.state.clone() {
            LifecycleState::Ready { origin, .. } => {
                self.io
                    .clear_session_cookie(&origin)
                    .map_err(|_| HostError::Cookie)?;
                transition(&mut self.state, LifecycleEvent::ChildExited)
                    .map_err(|_| HostError::State)?;
                self.token = None;
                self.io.navigate_app().map_err(|_| HostError::Navigation)?;
                self.io
                    .render_failure(FailureCode::Exited)
                    .map_err(|_| HostError::Render)
            }
            LifecycleState::Stopping { .. } => {
                transition(&mut self.state, LifecycleEvent::ChildExited)
                    .map_err(|_| HostError::State)
            }
            _ => self.fail(FailureCode::Exited, None),
        }
    }

    fn stop_and_exit(&mut self) -> Result<(), HostError> {
        let LifecycleState::Ready { origin, .. } = self.state.clone() else {
            if let Some(child) = self.child.take() {
                let _ = child.kill();
            }
            self.token = None;
            self.startup_deadline = None;
            self.io.exit(0);
            return Ok(());
        };
        transition(&mut self.state, LifecycleEvent::StopRequested).map_err(|_| HostError::State)?;

        let shutdown_deadline = Instant::now().checked_add(self.timings.shutdown);
        let graceful = self.token.as_deref().is_some_and(|token| {
            shutdown_deadline
                .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                .is_some_and(|timeout| self.io.request_shutdown(&origin, token, timeout).is_ok())
        });
        let terminated = match (graceful, shutdown_deadline) {
            (true, Some(deadline)) => self.wait_for_termination(deadline)?,
            _ => false,
        };
        if !terminated {
            if let Some(child) = self.child.take() {
                let _ = child.kill();
            }
            transition(&mut self.state, LifecycleEvent::ChildExited)
                .map_err(|_| HostError::State)?;
        }

        self.io
            .clear_session_cookie(&origin)
            .map_err(|_| HostError::Cookie)?;
        self.token = None;
        self.wait_for_port_release(&origin)?;
        self.io.exit(0);
        Ok(())
    }

    fn wait_for_termination(&mut self, deadline: Instant) -> Result<bool, HostError> {
        loop {
            let Some(timeout) = deadline.checked_duration_since(Instant::now()) else {
                return Ok(false);
            };
            let event = match self.child.as_mut() {
                Some(child) => child.recv_timeout(timeout)?,
                None => return Ok(true),
            };
            match event {
                Some(ChildEvent::Terminated(_)) => {
                    self.child.take();
                    transition(&mut self.state, LifecycleEvent::ChildExited)
                        .map_err(|_| HostError::State)?;
                    return Ok(true);
                }
                Some(ChildEvent::Stderr(bytes)) => self.append_stderr(&bytes)?,
                Some(ChildEvent::Stdout(_)) | Some(ChildEvent::Error) => {}
                None => return Ok(false),
            }
        }
    }

    fn wait_for_port_release(&self, origin: &Url) -> Result<(), HostError> {
        let deadline = Instant::now()
            .checked_add(self.timings.port_release)
            .ok_or(HostError::PortStillListening)?;
        loop {
            if self.io.port_released(origin)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(HostError::PortStillListening);
            }
            if !self.timings.port_poll.is_zero() {
                thread::sleep(self.timings.port_poll);
            }
        }
    }

    fn ready_origin(&self) -> Option<Url> {
        match &self.state {
            LifecycleState::Ready { origin, .. } => Some(origin.clone()),
            _ => None,
        }
    }
}

pub(crate) fn run_supervisor_loop<I: HostIo + Clone>(
    io: I,
    receiver: Receiver<SupervisorCommand>,
    settings_path: PathBuf,
    log_directory: PathBuf,
    test_config: Option<PathBuf>,
    timings: SupervisorTimings,
    poll_interval: Duration,
) {
    let mut supervisor = loop {
        match Supervisor::new(
            io.clone(),
            settings_path.clone(),
            log_directory.clone(),
            test_config.clone(),
            timings,
        ) {
            Ok(supervisor) => break supervisor,
            Err(_) => {
                let _ = io.navigate_app();
                let _ = io.render_failure(FailureCode::StartFailed);
                match receiver.recv() {
                    Ok(SupervisorCommand::StopAndExit) => {
                        io.exit(0);
                        return;
                    }
                    Ok(
                        SupervisorCommand::Start
                        | SupervisorCommand::Retry
                        | SupervisorCommand::ReassignPort,
                    ) => {}
                    Err(_) => return,
                }
            }
        }
    };
    let _ = supervisor.handle_command(SupervisorCommand::Start);

    loop {
        match receiver.try_recv() {
            Ok(command) => match supervisor.handle_command(command) {
                Ok(true) => return,
                Err(_) if command == SupervisorCommand::StopAndExit => {
                    io.exit(1);
                    return;
                }
                Ok(false) | Err(_) => {}
            },
            Err(TryRecvError::Disconnected) => return,
            Err(TryRecvError::Empty) => {}
        }
        let _ = supervisor.poll_child(poll_interval);
    }
}

fn map_sidecar_error(code: SidecarErrorCode) -> FailureCode {
    match code {
        SidecarErrorCode::InitInvalid => FailureCode::InitInvalid,
        SidecarErrorCode::ConfigInvalid => FailureCode::ConfigInvalid,
        SidecarErrorCode::PortUnavailable => FailureCode::PortUnavailable,
        SidecarErrorCode::StartFailed => FailureCode::StartFailed,
    }
}

pub(crate) fn resolve_test_config(raw: Option<OsString>) -> Result<Option<PathBuf>, HostError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        return Err(HostError::TestConfig);
    }
    let canonical = fs::canonicalize(path).map_err(|_| HostError::TestConfig)?;
    let metadata = fs::metadata(&canonical).map_err(|_| HostError::TestConfig)?;
    if metadata.is_file() {
        Ok(Some(canonical))
    } else {
        Err(HostError::TestConfig)
    }
}

pub(crate) fn desktop_data_directory(config_root: &Path, test_config: Option<&Path>) -> PathBuf {
    match test_config {
        Some(path) => {
            let mut runtime_path = path.as_os_str().to_owned();
            runtime_path.push(".runtime");
            runtime_path.into()
        }
        None => config_root.join("cy-kaf-client"),
    }
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
