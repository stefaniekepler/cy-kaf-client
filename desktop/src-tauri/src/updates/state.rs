use std::time::Duration;

pub const DOWNLOAD_BYTES_PER_SECOND: u64 = 512 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Phase {
    #[default]
    Idle,
    Checking,
    Downloading,
    Ready,
    UpToDate,
    Error,
    Installing,
    Unavailable,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Checking => "checking",
            Self::Downloading => "downloading",
            Self::Ready => "ready",
            Self::UpToDate => "up_to_date",
            Self::Error => "error",
            Self::Installing => "installing",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Default)]
pub struct Gate {
    pub phase: Phase,
    pub scheduled: bool,
}

impl Gate {
    pub fn begin_check(&mut self) -> bool {
        if matches!(self.phase, Phase::Idle | Phase::UpToDate | Phase::Error) {
            self.phase = Phase::Checking;
            true
        } else {
            false
        }
    }

    pub fn downloading(&mut self) {
        if self.phase == Phase::Checking {
            self.phase = Phase::Downloading;
        }
    }

    pub fn ready(&mut self) {
        self.phase = Phase::Ready;
    }

    pub fn up_to_date(&mut self) {
        self.phase = Phase::UpToDate;
    }

    pub fn fail(&mut self) {
        self.phase = Phase::Error;
    }

    pub fn install(&mut self) -> bool {
        if self.phase != Phase::Ready {
            return false;
        }
        self.phase = Phase::Installing;
        true
    }

    pub fn schedule(&mut self, scheduled: bool) -> bool {
        if self.phase == Phase::Installing || (scheduled && self.phase != Phase::Ready) {
            return false;
        }
        self.scheduled = scheduled;
        true
    }
}

pub fn transfer_delay(bytes: u64, elapsed: Duration) -> Duration {
    Duration::from_micros(bytes.saturating_mul(1_000_000) / DOWNLOAD_BYTES_PER_SECOND)
        .saturating_sub(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn background_completion_stops_at_ready_and_never_authorizes_installation() {
        let mut gate = Gate::default();
        assert!(gate.begin_check());
        assert!(!gate.begin_check());
        gate.downloading();
        assert!(!gate.install());
        gate.ready();
        assert_eq!(gate.phase, Phase::Ready);
        assert!(!gate.scheduled);
        assert!(!gate.begin_check());
    }

    #[test]
    fn only_a_ready_package_can_be_explicitly_installed_or_scheduled() {
        let mut gate = Gate::default();
        assert!(!gate.install());
        assert!(!gate.schedule(true));
        gate.begin_check();
        gate.downloading();
        gate.ready();
        assert!(gate.schedule(true));
        assert_eq!(gate.phase, Phase::Ready);
        assert!(gate.schedule(false));
        assert!(!gate.scheduled);
        assert!(gate.install());
        assert_eq!(gate.phase, Phase::Installing);
        assert!(!gate.install());
    }

    #[test]
    fn failure_does_not_restart_or_block_a_later_explicit_retry() {
        let mut gate = Gate::default();
        gate.begin_check();
        gate.fail();
        assert_eq!(gate.phase, Phase::Error);
        assert!(gate.begin_check());
        gate.up_to_date();
        assert_eq!(gate.phase, Phase::UpToDate);
        assert!(gate.begin_check());
    }

    #[test]
    fn download_budget_limits_rate_without_sleeping_on_the_ui_thread() {
        assert_eq!(
            transfer_delay(512 * 1024, Duration::ZERO),
            Duration::from_secs(1)
        );
        assert_eq!(
            transfer_delay(1024 * 1024, Duration::from_millis(1500)),
            Duration::from_millis(500)
        );
        assert_eq!(
            transfer_delay(1024, Duration::from_secs(10)),
            Duration::ZERO
        );
    }
}
