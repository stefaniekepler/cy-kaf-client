use super::state::{Gate, Phase};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub enum Action {
    Check,
    Install,
    Schedule,
    CancelSchedule,
}

#[derive(Clone, Debug)]
pub struct Offer {
    pub version: String,
    pub notes: String,
}

pub struct ReadyOffer {
    pub offer: Offer,
    pub bytes: u64,
    pub scheduled: bool,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub available: bool,
    pub current_version: String,
    pub phase: Phase,
    pub version: Option<String>,
    pub notes: Option<String>,
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub scheduled: bool,
    pub message: Option<String>,
}

pub trait Backend {
    fn restore(&mut self) -> Result<Option<ReadyOffer>, &'static str>;
    fn check(&mut self) -> Result<Option<Offer>, &'static str>;
    fn download(
        &mut self,
        offer: &Offer,
        progress: &mut dyn FnMut(u64, Option<u64>),
    ) -> Result<u64, &'static str>;
    fn schedule(&mut self, value: bool) -> Result<(), &'static str>;
    fn prepare_install(&mut self) -> Result<(), &'static str>;
}

pub struct Worker<B> {
    pub backend: B,
    gate: Gate,
    pub shared: Arc<Mutex<Snapshot>>,
    notify: Box<dyn Fn(&Snapshot) + Send>,
}

impl<B: Backend> Worker<B> {
    pub fn new(
        backend: B,
        current_version: String,
        available: bool,
        notify: impl Fn(&Snapshot) + Send + 'static,
    ) -> Self {
        Self {
            backend,
            gate: Gate {
                phase: if available {
                    Phase::Idle
                } else {
                    Phase::Unavailable
                },
                scheduled: false,
            },
            shared: Arc::new(Mutex::new(Snapshot {
                available,
                current_version,
                phase: if available {
                    Phase::Idle
                } else {
                    Phase::Unavailable
                },
                version: None,
                notes: None,
                downloaded_bytes: 0,
                total_bytes: None,
                scheduled: false,
                message: None,
            })),
            notify: Box::new(notify),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn change(&self, update: impl FnOnce(&mut Snapshot)) {
        let snapshot = {
            let mut state = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            state.phase = self.gate.phase;
            state.scheduled = self.gate.scheduled;
            update(&mut state);
            state.clone()
        };
        (self.notify)(&snapshot);
    }

    pub fn restore(&mut self) {
        if !self.snapshot().available {
            return;
        }
        match self.backend.restore() {
            Ok(Some(ready)) => {
                self.gate.ready();
                self.gate.scheduled = ready.scheduled;
                self.change(|state| {
                    state.version = Some(ready.offer.version);
                    state.notes = Some(ready.offer.notes);
                    state.downloaded_bytes = ready.bytes;
                    state.total_bytes = Some(ready.bytes);
                });
            }
            Ok(None) => {}
            Err(message) => self.error(message),
        }
    }

    pub fn handle(&mut self, action: Action) {
        if !self.snapshot().available {
            return;
        }
        match action {
            Action::Check => self.check(),
            Action::Install => {
                if !self.gate.install() {
                    return;
                }
                self.change(|s| s.message = None);
                if let Err(message) = self.backend.prepare_install() {
                    self.error(message);
                }
            }
            Action::Schedule | Action::CancelSchedule => {
                let value = matches!(action, Action::Schedule);
                let previous = self.gate.scheduled;
                if !self.gate.schedule(value) {
                    return;
                }
                match self.backend.schedule(value) {
                    Ok(()) => self.change(|s| s.message = None),
                    Err(message) => {
                        self.gate.scheduled = previous;
                        self.change(|s| s.message = Some(message.into()));
                    }
                }
            }
        }
    }

    fn error(&mut self, message: &'static str) {
        self.gate.fail();
        self.change(|s| s.message = Some(message.into()));
    }

    fn check(&mut self) {
        if !self.gate.begin_check() {
            return;
        }
        self.change(|s| {
            s.message = None;
            s.downloaded_bytes = 0;
            s.total_bytes = None;
        });
        let offer = match self.backend.check() {
            Ok(Some(offer)) => offer,
            Ok(None) => {
                self.gate.up_to_date();
                self.change(|s| {
                    s.version = None;
                    s.notes = None;
                });
                return;
            }
            Err(message) => {
                self.error(message);
                return;
            }
        };
        self.gate.downloading();
        self.change(|s| {
            s.version = Some(offer.version.clone());
            s.notes = Some(offer.notes.clone());
        });
        let shared = Arc::clone(&self.shared);
        let notify = &self.notify;
        let mut last_progress = Instant::now();
        let result = self.backend.download(&offer, &mut |bytes, total| {
            let snapshot = {
                let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
                state.downloaded_bytes = bytes;
                state.total_bytes = total;
                state.clone()
            };
            if last_progress.elapsed() >= Duration::from_secs(1) {
                notify(&snapshot);
                last_progress = Instant::now();
            }
        });
        match result {
            Ok(bytes) => {
                self.gate.ready();
                self.change(|s| {
                    s.downloaded_bytes = bytes;
                    s.total_bytes = Some(bytes);
                });
            }
            Err(message) => self.error(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct BackendStub {
        installs: usize,
        downloads: usize,
        checks: usize,
        scheduled: bool,
        failure: bool,
        install_failure: bool,
    }
    impl Backend for BackendStub {
        fn restore(&mut self) -> Result<Option<ReadyOffer>, &'static str> {
            Ok(None)
        }
        fn check(&mut self) -> Result<Option<Offer>, &'static str> {
            self.checks += 1;
            if self.failure {
                return Err("check failed");
            }
            Ok(Some(Offer {
                version: "0.1.4".into(),
                notes: "Update".into(),
            }))
        }
        fn download(
            &mut self,
            _: &Offer,
            progress: &mut dyn FnMut(u64, Option<u64>),
        ) -> Result<u64, &'static str> {
            self.downloads += 1;
            progress(12, Some(24));
            progress(24, Some(24));
            Ok(24)
        }
        fn schedule(&mut self, value: bool) -> Result<(), &'static str> {
            self.scheduled = value;
            Ok(())
        }
        fn prepare_install(&mut self) -> Result<(), &'static str> {
            self.installs += 1;
            if self.install_failure {
                return Err("installation deferred");
            }
            Ok(())
        }
    }

    #[test]
    fn a_background_check_downloads_without_installing_or_scheduling() {
        let mut worker = Worker::new(BackendStub::default(), "0.1.3".into(), true, |_| {});
        worker.handle(Action::Check);
        let status = worker.snapshot();
        assert_eq!(status.phase, Phase::Ready);
        assert_eq!(status.downloaded_bytes, 24);
        assert_eq!(worker.backend.installs, 0);
        assert!(!worker.backend.scheduled);
        worker.handle(Action::Check);
        assert_eq!(worker.backend.checks, 1);
        assert_eq!(worker.backend.downloads, 1);
    }

    #[test]
    fn status_queries_and_schedule_changes_cannot_trigger_installation() {
        let mut worker = Worker::new(BackendStub::default(), "0.1.3".into(), true, |_| {});
        worker.handle(Action::Install);
        assert_eq!(worker.backend.installs, 0);
        worker.handle(Action::Check);
        worker.handle(Action::Schedule);
        assert!(worker.snapshot().scheduled);
        assert_eq!(worker.backend.installs, 0);
        worker.handle(Action::CancelSchedule);
        assert!(!worker.snapshot().scheduled);
        worker.handle(Action::Install);
        assert_eq!(worker.backend.installs, 1);
        assert_eq!(worker.snapshot().phase, Phase::Installing);
    }

    #[test]
    fn unavailable_and_failed_background_checks_leave_installation_untouched() {
        let mut disabled = Worker::new(BackendStub::default(), "0.1.3".into(), false, |_| {});
        disabled.handle(Action::Check);
        assert_eq!(disabled.backend.checks, 0);
        let mut worker = Worker::new(
            BackendStub {
                failure: true,
                ..Default::default()
            },
            "0.1.3".into(),
            true,
            |_| {},
        );
        worker.handle(Action::Check);
        assert_eq!(worker.snapshot().phase, Phase::Error);
        assert_eq!(worker.backend.installs, 0);
        worker.backend.failure = false;
        worker.handle(Action::Check);
        assert_eq!(worker.snapshot().phase, Phase::Ready);
    }

    #[test]
    fn a_deferred_install_can_check_again_instead_of_getting_stuck_ready() {
        let mut worker = Worker::new(
            BackendStub {
                install_failure: true,
                ..Default::default()
            },
            "0.1.3".into(),
            true,
            |_| {},
        );
        worker.handle(Action::Check);
        worker.handle(Action::Install);
        assert_eq!(worker.snapshot().phase, Phase::Error);
        worker.handle(Action::Check);
        assert_eq!(worker.backend.checks, 2);
        assert_eq!(worker.snapshot().phase, Phase::Ready);
        assert_eq!(worker.backend.installs, 1);
    }

    #[test]
    fn a_stalled_download_never_holds_the_status_lock_needed_by_the_window() {
        use std::sync::mpsc;
        struct SlowBackend {
            started: mpsc::Sender<()>,
            resume: mpsc::Receiver<()>,
        }
        impl Backend for SlowBackend {
            fn restore(&mut self) -> Result<Option<ReadyOffer>, &'static str> {
                Ok(None)
            }
            fn check(&mut self) -> Result<Option<Offer>, &'static str> {
                Ok(Some(Offer {
                    version: "0.1.4".into(),
                    notes: String::new(),
                }))
            }
            fn download(
                &mut self,
                _: &Offer,
                progress: &mut dyn FnMut(u64, Option<u64>),
            ) -> Result<u64, &'static str> {
                progress(1, Some(2));
                self.started.send(()).unwrap();
                self.resume.recv_timeout(Duration::from_secs(5)).unwrap();
                progress(2, Some(2));
                Ok(2)
            }
            fn schedule(&mut self, _: bool) -> Result<(), &'static str> {
                panic!("download must not schedule")
            }
            fn prepare_install(&mut self) -> Result<(), &'static str> {
                panic!("download must not install")
            }
        }
        let (started, waiting) = mpsc::channel();
        let (resume, paused) = mpsc::channel();
        let mut worker = Worker::new(
            SlowBackend {
                started,
                resume: paused,
            },
            "0.1.3".into(),
            true,
            |_| {},
        );
        let shared = Arc::clone(&worker.shared);
        let thread = std::thread::spawn(move || worker.handle(Action::Check));
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        let visible = shared
            .try_lock()
            .map(|s| (s.phase, s.downloaded_bytes))
            .ok();
        resume.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(visible, Some((Phase::Downloading, 1)));
        assert_eq!(shared.lock().unwrap().phase, Phase::Ready);
    }
}
