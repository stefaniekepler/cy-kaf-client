use thiserror::Error;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureCode {
    InitInvalid,
    ConfigInvalid,
    PortUnavailable,
    StartFailed,
    Exited,
    StartupTimeout,
}

impl FailureCode {
    pub fn user_message(self) -> &'static str {
        match self {
            Self::InitInvalid => "客户端初始化失败，请重试。",
            Self::ConfigInvalid => "配置无效，请检查 Kafka 配置后重试。",
            Self::PortUnavailable => "本地端口被占用，请重试或重新分配端口。",
            Self::StartFailed => "本地服务启动失败，请重试或查看日志。",
            Self::Exited => "本地服务已意外退出，请重试或查看日志。",
            Self::StartupTimeout => "本地服务启动超时，请重试或查看日志。",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleState {
    Starting,
    Ready { origin: Url, pid: u32 },
    Stopping { pid: u32 },
    Failed { code: FailureCode },
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisorCommand {
    Start,
    Retry,
    ReassignPort,
    StopAndExit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleEvent {
    StartRequested,
    Ready { origin: Url, pid: u32 },
    StartFailed { code: FailureCode },
    StopRequested,
    ChildExited,
}

#[derive(Debug, Error)]
#[error("invalid desktop lifecycle transition")]
pub struct LifecycleError;

pub fn transition(state: &mut LifecycleState, event: LifecycleEvent) -> Result<(), LifecycleError> {
    let next = match (&*state, event) {
        (
            LifecycleState::Stopped | LifecycleState::Failed { .. },
            LifecycleEvent::StartRequested,
        ) => LifecycleState::Starting,
        (LifecycleState::Starting, LifecycleEvent::Ready { origin, pid }) => {
            LifecycleState::Ready { origin, pid }
        }
        (
            LifecycleState::Starting | LifecycleState::Ready { .. },
            LifecycleEvent::StartFailed { code },
        ) => LifecycleState::Failed { code },
        (LifecycleState::Starting | LifecycleState::Ready { .. }, LifecycleEvent::ChildExited) => {
            LifecycleState::Failed {
                code: FailureCode::Exited,
            }
        }
        (LifecycleState::Ready { pid, .. }, LifecycleEvent::StopRequested) => {
            LifecycleState::Stopping { pid: *pid }
        }
        (LifecycleState::Stopping { .. }, LifecycleEvent::ChildExited) => LifecycleState::Stopped,
        _ => return Err(LifecycleError),
    };

    *state = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    fn origin() -> Url {
        Url::parse("http://127.0.0.1:43127").expect("valid origin")
    }

    #[test]
    fn transitions_through_start_ready_stop_and_stopped() {
        let mut state = LifecycleState::Stopped;

        transition(&mut state, LifecycleEvent::StartRequested).expect("start");
        assert_eq!(state, LifecycleState::Starting);

        transition(
            &mut state,
            LifecycleEvent::Ready {
                origin: origin(),
                pid: 1234,
            },
        )
        .expect("ready");
        assert_eq!(
            state,
            LifecycleState::Ready {
                origin: origin(),
                pid: 1234,
            }
        );

        transition(&mut state, LifecycleEvent::StopRequested).expect("stop");
        assert_eq!(state, LifecycleState::Stopping { pid: 1234 });

        transition(&mut state, LifecycleEvent::ChildExited).expect("child exit");
        assert_eq!(state, LifecycleState::Stopped);
    }

    #[test]
    fn failed_start_can_be_retried() {
        let mut state = LifecycleState::Stopped;
        transition(&mut state, LifecycleEvent::StartRequested).expect("start");
        transition(
            &mut state,
            LifecycleEvent::StartFailed {
                code: FailureCode::ConfigInvalid,
            },
        )
        .expect("fail");
        assert_eq!(
            state,
            LifecycleState::Failed {
                code: FailureCode::ConfigInvalid,
            }
        );

        transition(&mut state, LifecycleEvent::StartRequested).expect("retry");
        assert_eq!(state, LifecycleState::Starting);
    }

    #[test]
    fn ready_child_exit_becomes_failed_and_can_be_restarted() {
        let mut state = LifecycleState::Ready {
            origin: origin(),
            pid: 1234,
        };

        transition(&mut state, LifecycleEvent::ChildExited).expect("unexpected exit");
        assert_eq!(
            state,
            LifecycleState::Failed {
                code: FailureCode::Exited,
            }
        );
        transition(&mut state, LifecycleEvent::StartRequested).expect("restart");
        assert_eq!(state, LifecycleState::Starting);
    }

    #[test]
    fn rejects_invalid_transitions_without_mutating_state() {
        let cases = [
            (
                LifecycleState::Stopped,
                LifecycleEvent::Ready {
                    origin: origin(),
                    pid: 1234,
                },
            ),
            (LifecycleState::Starting, LifecycleEvent::StartRequested),
            (
                LifecycleState::Failed {
                    code: FailureCode::StartFailed,
                },
                LifecycleEvent::Ready {
                    origin: origin(),
                    pid: 1234,
                },
            ),
        ];

        for (mut state, event) in cases {
            let before = state.clone();
            assert!(transition(&mut state, event).is_err());
            assert_eq!(state, before);
        }
    }

    #[test]
    fn supports_every_fixed_failure_code() {
        let codes = [
            FailureCode::InitInvalid,
            FailureCode::ConfigInvalid,
            FailureCode::PortUnavailable,
            FailureCode::StartFailed,
            FailureCode::Exited,
            FailureCode::StartupTimeout,
        ];

        for code in codes {
            let mut state = LifecycleState::Starting;
            transition(&mut state, LifecycleEvent::StartFailed { code }).expect("failure");
            assert_eq!(state, LifecycleState::Failed { code });
        }
    }

    #[test]
    fn failure_messages_are_fixed_and_do_not_include_child_details() {
        let cases = [
            (FailureCode::InitInvalid, "客户端初始化失败，请重试。"),
            (
                FailureCode::ConfigInvalid,
                "配置无效，请检查 Kafka 配置后重试。",
            ),
            (
                FailureCode::PortUnavailable,
                "本地端口被占用，请重试或重新分配端口。",
            ),
            (
                FailureCode::StartFailed,
                "本地服务启动失败，请重试或查看日志。",
            ),
            (
                FailureCode::Exited,
                "本地服务已意外退出，请重试或查看日志。",
            ),
            (
                FailureCode::StartupTimeout,
                "本地服务启动超时，请重试或查看日志。",
            ),
        ];

        for (code, expected) in cases {
            assert_eq!(code.user_message(), expected);
        }
    }
}
