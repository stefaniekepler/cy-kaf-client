use crate::{
    host::{ChildEvent, ChildHandle, HostError, HostIo},
    lifecycle::FailureCode,
    protocol::{SESSION_COOKIE, init_line},
    shutdown::send_shutdown,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    ffi::OsString,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream},
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tauri::{AppHandle, WebviewWindow, async_runtime::Receiver, webview::Cookie};
use tauri_plugin_shell::{
    ShellExt,
    process::{CommandChild, CommandEvent},
};
use url::{Origin, Url};

const MAX_HEALTH_RESPONSE_BYTES: usize = 64 * 1024;
const PORT_PROBE_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DesktopViewState {
    kind: &'static str,
    message: &'static str,
}

impl DesktopViewState {
    pub(crate) fn starting() -> Self {
        Self {
            kind: "starting",
            message: "",
        }
    }

    fn failed(code: FailureCode) -> Self {
        Self {
            kind: "failed",
            message: code.user_message(),
        }
    }
}

pub(crate) type SharedOrigin = Arc<RwLock<Option<Origin>>>;
pub(crate) type SharedViewState = Arc<RwLock<DesktopViewState>>;

#[derive(Clone)]
pub(crate) struct TauriHostIo {
    app: AppHandle,
    window: WebviewWindow,
    app_url: Url,
    sidecar_origin: SharedOrigin,
    view_state: SharedViewState,
    allow_exit: Arc<AtomicBool>,
}

impl TauriHostIo {
    pub(crate) fn new(
        app: AppHandle,
        window: WebviewWindow,
        app_url: Url,
        sidecar_origin: SharedOrigin,
        view_state: SharedViewState,
        allow_exit: Arc<AtomicBool>,
    ) -> Self {
        Self {
            app,
            window,
            app_url,
            sidecar_origin,
            view_state,
            allow_exit,
        }
    }

    fn set_sidecar_origin(&self, origin: Option<Origin>) -> Result<(), HostError> {
        *self
            .sidecar_origin
            .write()
            .map_err(|_| HostError::Navigation)? = origin;
        Ok(())
    }

    fn set_view_state(&self, state: DesktopViewState) -> Result<(), HostError> {
        *self.view_state.write().map_err(|_| HostError::Render)? = state;
        Ok(())
    }
}

impl HostIo for TauriHostIo {
    fn spawn_sidecar(
        &self,
        port: u16,
        test_config: Option<&Path>,
    ) -> Result<Box<dyn ChildHandle>, HostError> {
        let mut args = vec![
            OsString::from("--desktop"),
            OsString::from("--no-browser"),
            OsString::from("--port"),
            OsString::from(port.to_string()),
        ];
        if let Some(path) = test_config {
            if !path.is_absolute() || !path.is_file() {
                return Err(HostError::TestConfig);
            }
            args.push(OsString::from("--config"));
            args.push(path.as_os_str().to_owned());
        }

        let command = self
            .app
            .shell()
            .sidecar("cy-kaf-client")
            .map_err(|_| HostError::Spawn)?
            .args(args);
        let (receiver, child) = command.spawn().map_err(|_| HostError::Spawn)?;
        Ok(Box::new(TauriChild { receiver, child }))
    }

    fn probe_health(&self, origin: &Url, timeout: Duration) -> Result<(), HostError> {
        probe_health_http(origin, timeout)
    }

    fn set_session_cookie(&self, _origin: &Url, token: &str) -> Result<(), HostError> {
        self.window
            .set_cookie(build_session_cookie(token)?)
            .map_err(|_| HostError::Cookie)
    }

    fn clear_session_cookie(&self, _origin: &Url) -> Result<(), HostError> {
        let cookie = Cookie::build((SESSION_COOKIE, ""))
            .domain("127.0.0.1")
            .path("/")
            .build();
        self.window
            .delete_cookie(cookie)
            .map_err(|_| HostError::Cookie)
    }

    fn navigate_app(&self) -> Result<(), HostError> {
        self.set_sidecar_origin(None)?;
        self.set_view_state(DesktopViewState::starting())?;
        self.window
            .navigate(self.app_url.clone())
            .map_err(|_| HostError::Navigation)
    }

    fn navigate_sidecar(&self, origin: &Url) -> Result<(), HostError> {
        self.set_sidecar_origin(Some(origin.origin()))?;
        self.window
            .navigate(origin.clone())
            .map_err(|_| HostError::Navigation)
    }

    fn render_failure(&self, code: FailureCode) -> Result<(), HostError> {
        let state = DesktopViewState::failed(code);
        self.set_view_state(state.clone())?;
        apply_view_state(&self.window, &state)
    }

    fn request_shutdown(
        &self,
        origin: &Url,
        token: &str,
        timeout: Duration,
    ) -> Result<(), HostError> {
        send_shutdown(origin, token, timeout).map_err(|_| HostError::Shutdown)
    }

    fn port_released(&self, origin: &Url) -> Result<bool, HostError> {
        is_port_released(origin)
    }

    fn exit(&self, code: i32) {
        self.allow_exit.store(true, Ordering::SeqCst);
        self.app.exit(code);
    }
}

struct TauriChild {
    receiver: Receiver<CommandEvent>,
    child: CommandChild,
}

impl ChildHandle for TauriChild {
    fn pid(&self) -> u32 {
        self.child.pid()
    }

    fn write_stdin(&mut self, bytes: &[u8]) -> Result<(), HostError> {
        self.child.write(bytes).map_err(|_| HostError::Child)
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<ChildEvent>, HostError> {
        recv_command_event(&mut self.receiver, timeout)
    }

    fn kill(self: Box<Self>) -> Result<(), HostError> {
        let Self { child, .. } = *self;
        child.kill().map_err(|_| HostError::Child)
    }
}

fn recv_command_event(
    receiver: &mut Receiver<CommandEvent>,
    timeout: Duration,
) -> Result<Option<ChildEvent>, HostError> {
    let event = tauri::async_runtime::block_on(async {
        tokio::time::timeout(timeout, receiver.recv()).await
    });
    Ok(match event {
        Err(_) => None,
        Ok(None) => Some(ChildEvent::Error),
        Ok(Some(CommandEvent::Stdout(bytes))) => Some(ChildEvent::Stdout(bytes)),
        Ok(Some(CommandEvent::Stderr(bytes))) => Some(ChildEvent::Stderr(bytes)),
        Ok(Some(CommandEvent::Terminated(payload))) => Some(ChildEvent::Terminated(payload.code)),
        Ok(Some(CommandEvent::Error(_))) => Some(ChildEvent::Error),
        Ok(Some(_)) => Some(ChildEvent::Error),
    })
}

pub(crate) fn apply_view_state(
    window: &WebviewWindow,
    state: &DesktopViewState,
) -> Result<(), HostError> {
    let payload = serde_json::to_string(state).map_err(|_| HostError::Render)?;
    window
        .eval(format!("window.setDesktopState({payload})"))
        .map_err(|_| HostError::Render)
}

fn build_session_cookie(token: &str) -> Result<Cookie<'static>, HostError> {
    init_line(token).map_err(|_| HostError::Cookie)?;
    Cookie::parse(format!(
        "{SESSION_COOKIE}={token}; Domain=127.0.0.1; Path=/; HttpOnly; SameSite=Strict"
    ))
    .map(Cookie::into_owned)
    .map_err(|_| HostError::Cookie)
}

fn probe_health_http(origin: &Url, timeout: Duration) -> Result<(), HostError> {
    let (address, port) = loopback_address(origin)?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(HostError::Health)?;
    let mut stream = TcpStream::connect_timeout(&address, remaining(deadline)?)
        .map_err(|_| HostError::Health)?;
    stream
        .set_write_timeout(Some(remaining(deadline)?))
        .map_err(|_| HostError::Health)?;
    let request = format!(
        "GET /actuator/health HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|_| HostError::Health)?;

    let response = read_health_response(&mut stream, deadline)?;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .ok_or(HostError::Health)?;
    let status_end = response
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or(HostError::Health)?;
    if &response[..status_end] != b"HTTP/1.1 200 OK" {
        return Err(HostError::Health);
    }
    let body: Value =
        serde_json::from_slice(&response[header_end..]).map_err(|_| HostError::Health)?;
    if body.get("status").and_then(Value::as_str) == Some("UP") {
        Ok(())
    } else {
        Err(HostError::Health)
    }
}

fn read_health_response(stream: &mut TcpStream, deadline: Instant) -> Result<Vec<u8>, HostError> {
    let mut response = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        stream
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|_| HostError::Health)?;
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => response.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(HostError::Health),
        }
        if response.len() > MAX_HEALTH_RESPONSE_BYTES {
            return Err(HostError::Health);
        }
    }
    if response.is_empty() {
        Err(HostError::Health)
    } else {
        Ok(response)
    }
}

fn is_port_released(origin: &Url) -> Result<bool, HostError> {
    let (address, _) = loopback_address(origin)?;
    match TcpStream::connect_timeout(&address, PORT_PROBE_TIMEOUT) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(true),
        Err(_) => Ok(false),
    }
}

fn loopback_address(origin: &Url) -> Result<(SocketAddr, u16), HostError> {
    let port = origin.port().filter(|port| *port != 0);
    let valid = origin.scheme() == "http"
        && origin.username().is_empty()
        && origin.password().is_none()
        && origin.host_str() == Some("127.0.0.1")
        && port.is_some()
        && origin.path() == "/"
        && origin.query().is_none()
        && origin.fragment().is_none();
    match (valid, port) {
        (true, Some(port)) => Ok((
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
            port,
        )),
        _ => Err(HostError::Health),
    }
}

fn remaining(deadline: Instant) -> Result<Duration, HostError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or(HostError::Health)
}

#[cfg(test)]
#[path = "tauri_host_tests.rs"]
mod tests;
