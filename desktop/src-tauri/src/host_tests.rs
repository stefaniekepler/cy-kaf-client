use super::*;
use crate::{
    lifecycle::{FailureCode, LifecycleState, SupervisorCommand},
    protocol::init_line,
    settings::{DesktopSettings, load_settings, save_settings},
};
use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver},
    },
    thread,
    time::Duration,
};
use tempfile::{TempDir, tempdir};
use url::Url;

#[derive(Clone)]
struct ChildPlan {
    pid: u32,
    events: VecDeque<ChildEvent>,
    wait_on_empty: bool,
}

#[derive(Default)]
struct FakeState {
    plans: VecDeque<ChildPlan>,
    trace: Vec<String>,
    spawn_ports: Vec<u16>,
    spawn_configs: Vec<Option<PathBuf>>,
    stdin_writes: usize,
    child_drops: usize,
    kills: usize,
    health_fails: bool,
    cookie_sets: usize,
    cookie_clears: usize,
    navigated_sidecars: Vec<Url>,
    rendered_failures: Vec<FailureCode>,
    shutdown_fails: bool,
    shutdown_delay: Duration,
    release_results: VecDeque<bool>,
    exits: Vec<i32>,
    settings_path: Option<PathBuf>,
}

#[derive(Clone)]
struct FakeIo {
    state: Arc<Mutex<FakeState>>,
}

struct FakeChild {
    state: Arc<Mutex<FakeState>>,
    pid: u32,
    events: VecDeque<ChildEvent>,
    wait_on_empty: bool,
}

impl Drop for FakeChild {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.child_drops += 1;
        }
    }
}

impl ChildHandle for FakeChild {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn write_stdin(&mut self, bytes: &[u8]) -> Result<(), HostError> {
        let mut state = self.state.lock().expect("fake state");
        assert!(bytes.starts_with(b"CY_KAF_INIT "));
        assert_eq!(bytes.last(), Some(&b'\n'));
        state.stdin_writes += 1;
        state.trace.push("stdin:init".into());
        Ok(())
    }

    fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<ChildEvent>, HostError> {
        let event = self.events.pop_front();
        if event.is_none() && self.wait_on_empty && !timeout.is_zero() {
            thread::sleep(timeout);
        }
        Ok(event)
    }

    fn kill(self: Box<Self>) -> Result<(), HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.kills += 1;
        state.trace.push("child:kill".into());
        drop(state);
        Ok(())
    }
}

impl HostIo for FakeIo {
    fn spawn_sidecar(
        &self,
        port: u16,
        test_config: Option<&Path>,
    ) -> Result<Box<dyn ChildHandle>, HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.trace.push(format!("spawn:{port}"));
        state.spawn_ports.push(port);
        state.spawn_configs.push(test_config.map(Path::to_path_buf));
        let plan = state.plans.pop_front().ok_or(HostError::Spawn)?;
        Ok(Box::new(FakeChild {
            state: Arc::clone(&self.state),
            pid: plan.pid,
            events: plan.events,
            wait_on_empty: plan.wait_on_empty,
        }))
    }

    fn probe_health(&self, _origin: &Url, _timeout: Duration) -> Result<(), HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.trace.push("health".into());
        if state.health_fails {
            Err(HostError::Health)
        } else {
            Ok(())
        }
    }

    fn set_session_cookie(&self, origin: &Url, token: &str) -> Result<(), HostError> {
        assert!(init_line(token).is_ok());
        let mut state = self.state.lock().expect("fake state");
        let saved = load_settings(
            state
                .settings_path
                .as_deref()
                .expect("settings path configured"),
        )
        .map_err(|_| HostError::Cookie)?;
        if saved.port != origin.port() {
            return Err(HostError::Cookie);
        }
        state.cookie_sets += 1;
        state.trace.push("cookie:set".into());
        Ok(())
    }

    fn clear_session_cookie(&self, _origin: &Url) -> Result<(), HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.cookie_clears += 1;
        state.trace.push("cookie:clear".into());
        Ok(())
    }

    fn navigate_app(&self) -> Result<(), HostError> {
        self.state
            .lock()
            .expect("fake state")
            .trace
            .push("navigate:app".into());
        Ok(())
    }

    fn navigate_sidecar(&self, origin: &Url) -> Result<(), HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.trace.push("navigate:sidecar".into());
        state.navigated_sidecars.push(origin.clone());
        Ok(())
    }

    fn render_failure(&self, code: FailureCode) -> Result<(), HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.trace.push(format!("failure:{code:?}"));
        state.rendered_failures.push(code);
        Ok(())
    }

    fn request_shutdown(
        &self,
        _origin: &Url,
        token: &str,
        _timeout: Duration,
    ) -> Result<(), HostError> {
        assert!(init_line(token).is_ok());
        let mut state = self.state.lock().expect("fake state");
        state.trace.push("shutdown".into());
        let fails = state.shutdown_fails;
        let delay = state.shutdown_delay;
        drop(state);
        if !delay.is_zero() {
            thread::sleep(delay);
        }
        if fails {
            Err(HostError::Shutdown)
        } else {
            Ok(())
        }
    }

    fn port_released(&self, _origin: &Url) -> Result<bool, HostError> {
        let mut state = self.state.lock().expect("fake state");
        state.trace.push("port:check".into());
        Ok(state.release_results.pop_front().unwrap_or(true))
    }

    fn exit(&self, code: i32) {
        let mut state = self.state.lock().expect("fake state");
        state.trace.push(format!("exit:{code}"));
        state.exits.push(code);
    }
}

struct Harness {
    _temp: TempDir,
    supervisor: Supervisor<FakeIo>,
    fake: Arc<Mutex<FakeState>>,
    settings_path: PathBuf,
}

fn ready(port: u16) -> ChildEvent {
    ChildEvent::Stdout(
        format!(r#"CY_KAF_READY {{"protocol":1,"origin":"http://127.0.0.1:{port}"}}"#).into_bytes(),
    )
}

fn sidecar_error(code: &str) -> ChildEvent {
    ChildEvent::Stdout(format!(r#"CY_KAF_ERROR {{"protocol":1,"code":"{code}"}}"#).into_bytes())
}

fn plan(pid: u32, events: impl IntoIterator<Item = ChildEvent>) -> ChildPlan {
    ChildPlan {
        pid,
        events: events.into_iter().collect(),
        wait_on_empty: false,
    }
}

fn hanging_plan(pid: u32) -> ChildPlan {
    ChildPlan {
        pid,
        events: VecDeque::new(),
        wait_on_empty: true,
    }
}

fn harness(
    plans: Vec<ChildPlan>,
    saved_port: Option<u16>,
    test_config: Option<PathBuf>,
) -> Harness {
    let temp = tempdir().expect("temp directory");
    let settings_path = temp.path().join("state").join("desktop.json");
    if saved_port.is_some() {
        save_settings(&settings_path, &DesktopSettings { port: saved_port })
            .expect("seed settings");
    }
    let fake = Arc::new(Mutex::new(FakeState {
        plans: plans.into(),
        settings_path: Some(settings_path.clone()),
        ..FakeState::default()
    }));
    let supervisor = Supervisor::new(
        FakeIo {
            state: Arc::clone(&fake),
        },
        settings_path.clone(),
        temp.path().join("logs"),
        test_config,
        SupervisorTimings::for_tests(),
    )
    .expect("create supervisor");

    Harness {
        _temp: temp,
        supervisor,
        fake,
        settings_path,
    }
}

fn fallback_fixture(
    settings_path: PathBuf,
) -> (FakeIo, Arc<Mutex<FakeState>>, Receiver<SupervisorCommand>) {
    let fake = Arc::new(Mutex::new(FakeState {
        settings_path: Some(settings_path),
        ..FakeState::default()
    }));
    let io = FakeIo {
        state: Arc::clone(&fake),
    };
    let (sender, receiver) = mpsc::channel();
    sender
        .send(SupervisorCommand::StopAndExit)
        .expect("queue close request");
    (io, fake, receiver)
}

fn poll_once(harness: &mut Harness) {
    harness
        .supervisor
        .poll_child(Duration::ZERO)
        .expect("poll sidecar");
}

fn start_and_poll_once(harness: &mut Harness) {
    assert!(
        !harness
            .supervisor
            .handle_command(SupervisorCommand::Start)
            .expect("start")
    );
    poll_once(harness);
}

#[test]
fn dynamic_port_start_probes_saves_cookie_then_navigates_and_keeps_stdin_open() {
    let mut harness = harness(vec![plan(1234, [ready(43127)])], None, None);

    start_and_poll_once(&mut harness);

    assert_eq!(
        harness.supervisor.state(),
        &LifecycleState::Ready {
            origin: Url::parse("http://127.0.0.1:43127").expect("origin"),
            pid: 1234,
        }
    );
    assert_eq!(
        load_settings(&harness.settings_path).expect("settings"),
        DesktopSettings { port: Some(43127) }
    );
    {
        let state = harness.fake.lock().expect("fake state");
        assert_eq!(state.spawn_ports, [0]);
        assert_eq!(state.spawn_configs, [None]);
        assert_eq!(state.stdin_writes, 1);
        assert_eq!(state.child_drops, 0);
        assert_eq!(
            state.trace,
            [
                "spawn:0",
                "stdin:init",
                "health",
                "cookie:set",
                "navigate:sidecar",
            ]
        );
    }

    drop(harness.supervisor);
    assert_eq!(harness.fake.lock().expect("fake state").child_drops, 1);
}

#[test]
fn saved_port_and_valid_test_config_are_passed_to_sidecar() {
    let temp = tempdir().expect("config temp directory");
    let raw_config = temp.path().join("empty.yaml");
    fs::write(&raw_config, b"kafka:\n  clusters: []\n").expect("write config");
    let config = resolve_test_config(Some(raw_config.clone().into_os_string()))
        .expect("resolve config")
        .expect("configured path");
    let mut harness = harness(
        vec![plan(1234, [ready(43127)])],
        Some(43127),
        Some(config.clone()),
    );

    start_and_poll_once(&mut harness);

    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.spawn_ports, [43127]);
    assert_eq!(state.spawn_configs, [Some(config)]);
}

#[test]
fn health_failure_does_not_save_port_or_set_cookie() {
    let mut harness = harness(vec![plan(1234, [ready(43127)])], None, None);
    harness.fake.lock().expect("fake state").health_fails = true;

    start_and_poll_once(&mut harness);

    assert_eq!(
        harness.supervisor.state(),
        &LifecycleState::Failed {
            code: FailureCode::StartFailed,
        }
    );
    assert_eq!(
        load_settings(&harness.settings_path).expect("settings"),
        DesktopSettings { port: None }
    );
    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.cookie_sets, 0);
    assert_eq!(state.kills, 1);
    assert_eq!(state.rendered_failures, [FailureCode::StartFailed]);
    assert!(!state.trace.iter().any(|entry| entry == "navigate:sidecar"));
}

#[test]
fn port_unavailable_can_clear_saved_port_and_retry_dynamically() {
    let mut harness = harness(
        vec![
            plan(1234, [sidecar_error("PORT_UNAVAILABLE")]),
            plan(1235, [ready(43128)]),
        ],
        Some(43127),
        None,
    );

    start_and_poll_once(&mut harness);
    assert_eq!(
        harness.supervisor.state(),
        &LifecycleState::Failed {
            code: FailureCode::PortUnavailable,
        }
    );

    harness
        .supervisor
        .handle_command(SupervisorCommand::ReassignPort)
        .expect("reassign port");
    poll_once(&mut harness);

    assert_eq!(
        harness.fake.lock().expect("fake state").spawn_ports,
        [43127, 0]
    );
    assert_eq!(
        load_settings(&harness.settings_path).expect("settings"),
        DesktopSettings { port: Some(43128) }
    );
    assert_eq!(
        harness
            .fake
            .lock()
            .expect("fake state")
            .trace
            .iter()
            .filter(|entry| entry.as_str() == "navigate:app")
            .count(),
        2,
        "failure and retry should each navigate to the bootstrap page"
    );
}

#[test]
fn reassign_port_is_ignored_while_sidecar_is_ready() {
    let mut harness = harness(vec![plan(1234, [ready(43127)])], Some(43127), None);
    start_and_poll_once(&mut harness);

    harness
        .supervisor
        .handle_command(SupervisorCommand::ReassignPort)
        .expect("ignore invalid action");

    assert_eq!(
        load_settings(&harness.settings_path).expect("settings"),
        DesktopSettings { port: Some(43127) }
    );
    assert_eq!(
        harness.fake.lock().expect("fake state").spawn_ports,
        [43127]
    );
}

#[test]
fn startup_timeout_kills_child_and_renders_fixed_failure() {
    let mut harness = harness(vec![plan(1234, [])], None, None);

    harness
        .supervisor
        .handle_command(SupervisorCommand::Start)
        .expect("begin starting");
    thread::sleep(SupervisorTimings::for_tests().startup + Duration::from_millis(1));
    poll_once(&mut harness);

    assert_eq!(
        harness.supervisor.state(),
        &LifecycleState::Failed {
            code: FailureCode::StartupTimeout,
        }
    );
    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.kills, 1);
    assert_eq!(state.rendered_failures, [FailureCode::StartupTimeout]);
}

#[test]
fn starting_sidecar_can_be_stopped_before_startup_timeout() {
    let mut harness = harness(vec![plan(1234, [])], None, None);

    harness
        .supervisor
        .handle_command(SupervisorCommand::Start)
        .expect("begin starting");

    assert_eq!(harness.supervisor.state(), &LifecycleState::Starting);
    assert!(
        harness
            .supervisor
            .handle_command(SupervisorCommand::StopAndExit)
            .expect("stop starting sidecar")
    );

    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.kills, 1);
    assert_eq!(state.exits, [0]);
}

#[test]
fn supervisor_loop_processes_close_while_sidecar_is_hung_starting() {
    let temp = tempdir().expect("temp directory");
    let settings_path = temp.path().join("desktop.json");
    let fake = Arc::new(Mutex::new(FakeState {
        plans: [hanging_plan(1234)].into(),
        settings_path: Some(settings_path.clone()),
        ..FakeState::default()
    }));
    let io = FakeIo {
        state: Arc::clone(&fake),
    };
    let (command_sender, command_receiver) = mpsc::channel();
    let (finished_sender, finished_receiver) = mpsc::channel();
    let timings = SupervisorTimings {
        startup: Duration::from_secs(1),
        ..SupervisorTimings::for_tests()
    };
    let log_directory = temp.path().join("logs");
    let thread = thread::spawn(move || {
        run_supervisor_loop(
            io,
            command_receiver,
            settings_path,
            log_directory,
            None,
            timings,
            Duration::from_millis(5),
        );
        finished_sender.send(()).expect("signal loop completion");
    });

    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    loop {
        if fake
            .lock()
            .expect("fake state")
            .trace
            .iter()
            .any(|entry| entry == "stdin:init")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sidecar did not enter starting state"
        );
        thread::yield_now();
    }
    command_sender
        .send(SupervisorCommand::StopAndExit)
        .expect("send close request");
    finished_receiver
        .recv_timeout(Duration::from_millis(200))
        .expect("close should not wait for the one-second startup timeout");
    thread.join().expect("join supervisor loop");

    let state = fake.lock().expect("fake state");
    assert_eq!(state.kills, 1);
    assert_eq!(state.exits, [0]);
}

#[test]
fn corrupt_settings_render_fixed_failure_and_window_close_still_exits() {
    let temp = tempdir().expect("temp directory");
    let settings_path = temp.path().join("desktop.json");
    fs::write(&settings_path, b"{ definitely not json").expect("write corrupt settings");
    let (io, fake, receiver) = fallback_fixture(settings_path.clone());

    run_supervisor_loop(
        io,
        receiver,
        settings_path,
        temp.path().join("logs"),
        None,
        SupervisorTimings::for_tests(),
        Duration::ZERO,
    );

    let state = fake.lock().expect("fake state");
    assert!(state.spawn_ports.is_empty());
    assert_eq!(state.rendered_failures, [FailureCode::StartFailed]);
    assert_eq!(state.exits, [0]);
    assert_eq!(
        state.trace,
        ["navigate:app", "failure:StartFailed", "exit:0"]
    );
}

#[test]
fn logger_initialization_failure_allows_window_close_to_exit() {
    let temp = tempdir().expect("temp directory");
    let settings_path = temp.path().join("desktop.json");
    let log_directory = temp.path().join("logs");
    fs::write(&log_directory, b"not a directory").expect("block log directory");
    let (io, fake, receiver) = fallback_fixture(settings_path.clone());

    run_supervisor_loop(
        io,
        receiver,
        settings_path,
        log_directory,
        None,
        SupervisorTimings::for_tests(),
        Duration::ZERO,
    );

    let state = fake.lock().expect("fake state");
    assert!(state.spawn_ports.is_empty());
    assert_eq!(state.rendered_failures, [FailureCode::StartFailed]);
    assert_eq!(state.exits, [0]);
}

#[test]
fn ready_child_exit_returns_to_failure_page_without_auto_restart() {
    let mut harness = harness(
        vec![plan(1234, [ready(43127), ChildEvent::Terminated(Some(1))])],
        None,
        None,
    );
    start_and_poll_once(&mut harness);

    poll_once(&mut harness);

    assert_eq!(
        harness.supervisor.state(),
        &LifecycleState::Failed {
            code: FailureCode::Exited,
        }
    );
    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.spawn_ports.len(), 1);
    assert_eq!(state.cookie_clears, 1);
    assert_eq!(state.rendered_failures.last(), Some(&FailureCode::Exited));
}

#[test]
fn graceful_stop_waits_for_exit_clears_cookie_checks_port_then_exits() {
    let mut harness = harness(
        vec![plan(1234, [ready(43127), ChildEvent::Terminated(Some(0))])],
        None,
        None,
    );
    harness.fake.lock().expect("fake state").release_results = [false, true].into();
    start_and_poll_once(&mut harness);

    assert!(
        harness
            .supervisor
            .handle_command(SupervisorCommand::StopAndExit)
            .expect("stop")
    );

    assert_eq!(harness.supervisor.state(), &LifecycleState::Stopped);
    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.kills, 0);
    assert_eq!(state.cookie_clears, 1);
    assert_eq!(state.exits, [0]);
    let shutdown = state
        .trace
        .iter()
        .position(|entry| entry == "shutdown")
        .expect("shutdown trace");
    let clear = state
        .trace
        .iter()
        .position(|entry| entry == "cookie:clear")
        .expect("clear trace");
    let exit = state
        .trace
        .iter()
        .position(|entry| entry == "exit:0")
        .expect("exit trace");
    assert!(shutdown < clear && clear < exit);
}

#[test]
fn shutdown_timeout_forces_kill_but_still_clears_cookie_and_checks_port() {
    let mut harness = harness(vec![plan(1234, [ready(43127)])], None, None);
    start_and_poll_once(&mut harness);

    assert!(
        harness
            .supervisor
            .handle_command(SupervisorCommand::StopAndExit)
            .expect("forced stop")
    );

    let state = harness.fake.lock().expect("fake state");
    assert_eq!(state.kills, 1);
    assert_eq!(state.cookie_clears, 1);
    assert_eq!(state.exits, [0]);
    assert!(state.trace.iter().any(|entry| entry == "port:check"));
}

#[test]
fn shutdown_request_and_exit_wait_share_one_grace_period() {
    let mut harness = harness(
        vec![plan(1234, [ready(43127), ChildEvent::Terminated(Some(0))])],
        None,
        None,
    );
    harness.fake.lock().expect("fake state").shutdown_delay = Duration::from_millis(20);
    start_and_poll_once(&mut harness);

    harness
        .supervisor
        .handle_command(SupervisorCommand::StopAndExit)
        .expect("stop");

    assert_eq!(harness.fake.lock().expect("fake state").kills, 1);
}

#[test]
fn test_config_must_be_an_absolute_regular_file() {
    assert_eq!(resolve_test_config(None).expect("unset"), None);

    assert!(resolve_test_config(Some(OsString::from("relative.yaml"))).is_err());

    let temp = tempdir().expect("temp directory");
    assert!(resolve_test_config(Some(temp.path().as_os_str().to_owned())).is_err());

    let file = temp.path().join("config.yaml");
    fs::write(&file, b"").expect("write config");
    assert_eq!(
        resolve_test_config(Some(file.clone().into_os_string())).expect("valid file"),
        Some(file.canonicalize().expect("canonical path"))
    );
}

#[test]
fn test_config_uses_an_isolated_sibling_runtime_directory() {
    let config_root = Path::new("/normal/config");
    let test_config = Path::new("/tmp/desktop-smoke/config.yaml");

    assert_eq!(
        desktop_data_directory(config_root, None),
        config_root.join("cy-kaf-client")
    );
    assert_eq!(
        desktop_data_directory(config_root, Some(test_config)),
        PathBuf::from("/tmp/desktop-smoke/config.yaml.runtime")
    );
}
