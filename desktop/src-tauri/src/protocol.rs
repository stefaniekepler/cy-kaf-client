use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

pub const SESSION_COOKIE: &str = "cy_kaf_desktop_session";

const PROTOCOL_VERSION: u8 = 1;
const INIT_PREFIX: &[u8] = b"CY_KAF_INIT ";
const READY_PREFIX: &[u8] = b"CY_KAF_READY ";
const ERROR_PREFIX: &[u8] = b"CY_KAF_ERROR ";
const CONTROL_PREFIX: &[u8] = b"CY_KAF_";

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("could not generate a desktop session token")]
    Random,
    #[error("desktop session token must encode exactly 32 bytes")]
    InvalidToken,
    #[error("desktop protocol payload is invalid")]
    InvalidPayload,
    #[error("desktop protocol version is unsupported")]
    UnsupportedVersion,
    #[error("sidecar origin is invalid")]
    InvalidOrigin,
    #[error("sidecar control prefix is unknown")]
    UnknownControlPrefix,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SidecarErrorCode {
    InitInvalid,
    ConfigInvalid,
    PortUnavailable,
    StartFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SidecarEvent {
    Ready { origin: Url },
    Error { code: SidecarErrorCode },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InitPayload<'a> {
    protocol: u8,
    session_token: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyPayload {
    protocol: u8,
    origin: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorPayload {
    protocol: u8,
    code: SidecarErrorCode,
}

pub fn new_session_token() -> Result<String, ProtocolError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| ProtocolError::Random)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn init_line(token: &str) -> Result<Vec<u8>, ProtocolError> {
    validate_token(token)?;

    let payload = serde_json::to_vec(&InitPayload {
        protocol: PROTOCOL_VERSION,
        session_token: token,
    })
    .map_err(|_| ProtocolError::InvalidPayload)?;
    let mut line = Vec::with_capacity(INIT_PREFIX.len() + payload.len() + 1);
    line.extend_from_slice(INIT_PREFIX);
    line.extend_from_slice(&payload);
    line.push(b'\n');
    Ok(line)
}

pub fn parse_sidecar_line(line: &[u8]) -> Result<Option<SidecarEvent>, ProtocolError> {
    let line = trim_line_ending(line);

    if let Some(payload) = line.strip_prefix(READY_PREFIX) {
        let payload: ReadyPayload =
            serde_json::from_slice(payload).map_err(|_| ProtocolError::InvalidPayload)?;
        require_version(payload.protocol)?;
        return Ok(Some(SidecarEvent::Ready {
            origin: parse_sidecar_origin(&payload.origin)?,
        }));
    }

    if let Some(payload) = line.strip_prefix(ERROR_PREFIX) {
        let payload: ErrorPayload =
            serde_json::from_slice(payload).map_err(|_| ProtocolError::InvalidPayload)?;
        require_version(payload.protocol)?;
        return Ok(Some(SidecarEvent::Error { code: payload.code }));
    }

    if line.starts_with(CONTROL_PREFIX) {
        return Err(ProtocolError::UnknownControlPrefix);
    }

    Ok(None)
}

fn validate_token(token: &str) -> Result<(), ProtocolError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| ProtocolError::InvalidToken)?;
    if decoded.len() != 32 || URL_SAFE_NO_PAD.encode(decoded) != token {
        return Err(ProtocolError::InvalidToken);
    }
    Ok(())
}

fn require_version(version: u8) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion)
    }
}

fn parse_sidecar_origin(raw: &str) -> Result<Url, ProtocolError> {
    let url = Url::parse(raw).map_err(|_| ProtocolError::InvalidOrigin)?;
    let port = url.port().filter(|port| *port != 0);
    let canonical = port.map(|port| format!("http://127.0.0.1:{port}"));
    let valid = url.scheme() == "http"
        && url.username().is_empty()
        && url.password().is_none()
        && url.host_str() == Some("127.0.0.1")
        && port.is_some()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
        && canonical
            .as_ref()
            .is_some_and(|expected| raw == expected || raw == format!("{expected}/"));
    if valid {
        Ok(url)
    } else {
        Err(ProtocolError::InvalidOrigin)
    }
}

fn trim_line_ending(mut line: &[u8]) -> &[u8] {
    if let Some(stripped) = line.strip_suffix(b"\n") {
        line = stripped;
    }
    if let Some(stripped) = line.strip_suffix(b"\r") {
        line = stripped;
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::Value;
    use url::Url;

    fn valid_token() -> String {
        URL_SAFE_NO_PAD.encode([7_u8; 32])
    }

    #[test]
    fn new_session_token_contains_32_random_bytes() {
        let token = new_session_token().expect("token generation should succeed");
        let decoded = URL_SAFE_NO_PAD
            .decode(token)
            .expect("token should be URL-safe base64 without padding");

        assert_eq!(decoded.len(), 32);
    }

    #[test]
    fn init_line_contains_only_version_and_valid_session_token() {
        let token = valid_token();
        let line = init_line(&token).expect("valid token should produce an init line");
        assert_eq!(line.last(), Some(&b'\n'));

        let payload: Value = serde_json::from_slice(&line[b"CY_KAF_INIT ".len()..line.len() - 1])
            .expect("init payload should be JSON");
        assert_eq!(payload["protocol"], 1);
        assert_eq!(payload["sessionToken"], token);
        assert_eq!(payload.as_object().expect("object").len(), 2);
    }

    #[test]
    fn init_line_rejects_token_that_is_not_exactly_32_bytes() {
        let short_token = URL_SAFE_NO_PAD.encode([7_u8; 31]);
        assert!(init_line(&short_token).is_err());
        assert!(init_line("not-base64").is_err());
    }

    #[test]
    fn parse_ready_accepts_only_exact_loopback_origin() {
        let event =
            parse_sidecar_line(br#"CY_KAF_READY {"protocol":1,"origin":"http://127.0.0.1:43127"}"#)
                .expect("valid ready line");

        assert_eq!(
            event,
            Some(SidecarEvent::Ready {
                origin: Url::parse("http://127.0.0.1:43127").expect("valid expected URL"),
            })
        );
    }

    #[test]
    fn parse_ready_rejects_noncanonical_origin_variants() {
        for origin in [
            "https://127.0.0.1:43127",
            "http://localhost:43127",
            "http://127.0.0.2:43127",
            "http://2130706433:43127",
            "HTTP://127.0.0.1:43127",
            "http://127.0.0.1:043127",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "http://user@127.0.0.1:43127",
            "http://user:password@127.0.0.1:43127",
            "http://127.0.0.1:43127/path",
            "http://127.0.0.1:43127/?query=1",
            "http://127.0.0.1:43127/#fragment",
        ] {
            let line = format!(r#"CY_KAF_READY {{"protocol":1,"origin":"{origin}"}}"#);
            assert!(
                parse_sidecar_line(line.as_bytes()).is_err(),
                "origin should be rejected: {origin}"
            );
        }
    }

    #[test]
    fn parse_sidecar_error_accepts_only_known_code_and_version() {
        for (raw_code, expected) in [
            ("INIT_INVALID", SidecarErrorCode::InitInvalid),
            ("CONFIG_INVALID", SidecarErrorCode::ConfigInvalid),
            ("PORT_UNAVAILABLE", SidecarErrorCode::PortUnavailable),
            ("START_FAILED", SidecarErrorCode::StartFailed),
        ] {
            let line = format!(r#"CY_KAF_ERROR {{"protocol":1,"code":"{raw_code}"}}"#);
            assert_eq!(
                parse_sidecar_line(line.as_bytes()).expect("known error"),
                Some(SidecarEvent::Error { code: expected })
            );
        }

        assert!(
            parse_sidecar_line(br#"CY_KAF_ERROR {"protocol":1,"code":"DETAILS_FROM_CHILD"}"#)
                .is_err()
        );
        assert!(
            parse_sidecar_line(br#"CY_KAF_ERROR {"protocol":2,"code":"START_FAILED"}"#).is_err()
        );
    }

    #[test]
    fn parse_sidecar_line_rejects_unknown_control_prefix_but_ignores_normal_output() {
        assert!(
            parse_sidecar_line(b"ordinary stdout line")
                .unwrap()
                .is_none()
        );
        assert!(parse_sidecar_line(b"CY_KAF_UNKNOWN {}").is_err());
    }
}
