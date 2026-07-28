use crate::protocol::{SESSION_COOKIE, init_line};
use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream},
    time::{Duration, Instant},
};
use thiserror::Error;
use url::Url;

const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;

#[derive(Debug, Error)]
pub enum ShutdownError {
    #[error("shutdown origin is invalid")]
    InvalidOrigin,
    #[error("shutdown session token is invalid")]
    InvalidToken,
    #[error("shutdown connection failed")]
    Connect,
    #[error("shutdown request timed out")]
    Timeout,
    #[error("shutdown request could not be written")]
    Write,
    #[error("shutdown response could not be read")]
    Read,
    #[error("shutdown response header is too large")]
    ResponseTooLarge,
    #[error("shutdown response is invalid")]
    InvalidResponse,
    #[error("shutdown request was rejected with HTTP status {0}")]
    Rejected(u16),
}

pub fn send_shutdown(origin: &Url, token: &str, timeout: Duration) -> Result<(), ShutdownError> {
    let port = validate_origin(origin)?;
    init_line(token).map_err(|_| ShutdownError::InvalidToken)?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(ShutdownError::Timeout)?;
    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&address, remaining(deadline)?)
        .map_err(classify_connect_error)?;

    stream
        .set_write_timeout(Some(remaining(deadline)?))
        .map_err(|_| ShutdownError::Write)?;
    let request = format!(
        "POST /__desktop/shutdown HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Origin: http://127.0.0.1:{port}\r\n\
         Cookie: {SESSION_COOKIE}={token}\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(classify_write_error)?;

    let response = read_response_header(&mut stream, deadline)?;
    let status = parse_status(&response)?;
    if status == 204 {
        Ok(())
    } else {
        Err(ShutdownError::Rejected(status))
    }
}

fn validate_origin(origin: &Url) -> Result<u16, ShutdownError> {
    let port = origin.port().filter(|port| *port != 0);
    let valid = origin.scheme() == "http"
        && origin.username().is_empty()
        && origin.password().is_none()
        && origin.host_str() == Some("127.0.0.1")
        && port.is_some()
        && origin.path() == "/"
        && origin.query().is_none()
        && origin.fragment().is_none();
    if valid {
        Ok(port.expect("validated port"))
    } else {
        Err(ShutdownError::InvalidOrigin)
    }
}

fn read_response_header(
    stream: &mut TcpStream,
    deadline: Instant,
) -> Result<Vec<u8>, ShutdownError> {
    let mut response = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 512];
    loop {
        stream
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|_| ShutdownError::Read)?;
        match stream.read(&mut chunk) {
            Ok(0) => return Err(ShutdownError::InvalidResponse),
            Ok(count) => response.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(classify_read_error(error)),
        }

        if response.len() > MAX_RESPONSE_HEADER_BYTES {
            return Err(ShutdownError::ResponseTooLarge);
        }
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(response);
        }
    }
}

fn parse_status(response: &[u8]) -> Result<u16, ShutdownError> {
    let line_end = response
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or(ShutdownError::InvalidResponse)?;
    let line =
        std::str::from_utf8(&response[..line_end]).map_err(|_| ShutdownError::InvalidResponse)?;
    let mut parts = line.split_ascii_whitespace();
    if parts.next() != Some("HTTP/1.1") {
        return Err(ShutdownError::InvalidResponse);
    }
    let status = parts
        .next()
        .ok_or(ShutdownError::InvalidResponse)?
        .parse::<u16>()
        .map_err(|_| ShutdownError::InvalidResponse)?;
    if !(100..=599).contains(&status) {
        return Err(ShutdownError::InvalidResponse);
    }
    Ok(status)
}

fn remaining(deadline: Instant) -> Result<Duration, ShutdownError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(ShutdownError::Timeout)
}

fn classify_connect_error(error: io::Error) -> ShutdownError {
    if is_timeout(&error) {
        ShutdownError::Timeout
    } else {
        ShutdownError::Connect
    }
}

fn classify_write_error(error: io::Error) -> ShutdownError {
    if is_timeout(&error) {
        ShutdownError::Timeout
    } else {
        ShutdownError::Write
    }
}

fn classify_read_error(error: io::Error) -> ShutdownError {
    if is_timeout(&error) {
        ShutdownError::Timeout
    } else {
        ShutdownError::Read
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };
    use url::Url;

    fn valid_token() -> String {
        URL_SAFE_NO_PAD.encode([9_u8; 32])
    }

    fn read_request(stream: &mut TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut chunk = [0_u8; 512];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).expect("read request");
            assert!(count > 0, "client closed before completing request");
            request.extend_from_slice(&chunk[..count]);
            assert!(
                request.len() < 16 * 1024,
                "request header is unexpectedly large"
            );
        }
        request
    }

    fn serve_once(response: Vec<u8>) -> (Url, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("local address");
        let origin = Url::parse(&format!("http://{address}")).expect("origin");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let request = read_request(&mut stream);
            stream.write_all(&response).expect("write response");
            request
        });
        (origin, handle)
    }

    #[test]
    fn sends_exact_protected_shutdown_request_and_accepts_204() {
        let token = valid_token();
        let response = b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".to_vec();
        let (origin, server) = serve_once(response);

        send_shutdown(&origin, &token, Duration::from_secs(2)).expect("shutdown succeeds");

        let request = server.join().expect("server thread");
        let port = origin.port().expect("explicit port");
        assert_eq!(
            request,
            format!(
                "POST /__desktop/shutdown HTTP/1.1\r\n\
                 Host: 127.0.0.1:{port}\r\n\
                 Origin: http://127.0.0.1:{port}\r\n\
                 Cookie: cy_kaf_desktop_session={token}\r\n\
                 Content-Length: 0\r\n\
                 Connection: close\r\n\
                 \r\n"
            )
            .into_bytes()
        );
    }

    #[test]
    fn returns_typed_error_for_rejected_statuses() {
        for status in [401_u16, 403, 500] {
            let response =
                format!("HTTP/1.1 {status} Rejected\r\nConnection: close\r\n\r\n").into_bytes();
            let (origin, server) = serve_once(response);

            let error = send_shutdown(&origin, &valid_token(), Duration::from_secs(2))
                .expect_err("status should fail");
            assert!(matches!(error, ShutdownError::Rejected(code) if code == status));
            server.join().expect("server thread");
        }
    }

    #[test]
    fn rejects_non_loopback_origins_and_invalid_tokens_before_connecting() {
        for raw in [
            "https://127.0.0.1:43127",
            "http://localhost:43127",
            "http://127.0.0.2:43127",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "http://user@127.0.0.1:43127",
            "http://127.0.0.1:43127/path",
        ] {
            let error = send_shutdown(
                &Url::parse(raw).expect("test URL"),
                &valid_token(),
                Duration::from_millis(50),
            )
            .expect_err("origin should fail");
            assert!(matches!(error, ShutdownError::InvalidOrigin));
        }

        let error = send_shutdown(
            &Url::parse("http://127.0.0.1:43127").expect("test URL"),
            "not-a-token",
            Duration::from_millis(50),
        )
        .expect_err("token should fail");
        assert!(matches!(error, ShutdownError::InvalidToken));
    }

    #[test]
    fn classifies_connection_failure_and_timeout_without_exposing_token() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let address = listener.local_addr().expect("local address");
        drop(listener);
        let origin = Url::parse(&format!("http://{address}")).expect("origin");
        let token = valid_token();

        let connection_error =
            send_shutdown(&origin, &token, Duration::from_millis(100)).expect_err("connect fails");
        assert!(matches!(connection_error, ShutdownError::Connect));
        assert!(!connection_error.to_string().contains(&token));

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind timeout server");
        let address = listener.local_addr().expect("local address");
        let origin = Url::parse(&format!("http://{address}")).expect("origin");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let _ = read_request(&mut stream);
            thread::sleep(Duration::from_millis(150));
        });

        let timeout_error =
            send_shutdown(&origin, &token, Duration::from_millis(30)).expect_err("read times out");
        assert!(matches!(timeout_error, ShutdownError::Timeout));
        assert!(!timeout_error.to_string().contains(&token));
        server.join().expect("server thread");
    }

    #[test]
    fn rejects_response_headers_larger_than_eight_kibibytes() {
        let mut response = b"HTTP/1.1 204 No Content\r\nX-Fill: ".to_vec();
        response.extend(vec![b'x'; 9 * 1024]);
        response.extend_from_slice(b"\r\n\r\n");
        let (origin, server) = serve_once(response);

        let error = send_shutdown(&origin, &valid_token(), Duration::from_secs(2))
            .expect_err("oversized response should fail");
        assert!(matches!(error, ShutdownError::ResponseTooLarge));
        server.join().expect("server thread");
    }
}
