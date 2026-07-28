use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    panic::{AssertUnwindSafe, catch_unwind},
    thread,
    time::Duration,
};
use url::Url;

fn valid_token() -> String {
    URL_SAFE_NO_PAD.encode([11_u8; 32])
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout");
    let mut request = Vec::new();
    let mut chunk = [0_u8; 512];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut chunk).expect("read request");
        assert!(count > 0);
        request.extend_from_slice(&chunk[..count]);
    }
    request
}

fn serve_health(response: Vec<u8>) -> (Url, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind health server");
    let address = listener.local_addr().expect("local address");
    let origin = Url::parse(&format!("http://{address}")).expect("origin");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept health request");
        let request = read_request(&mut stream);
        stream.write_all(&response).expect("write response");
        request
    });
    (origin, server)
}

#[test]
fn health_probe_sends_fixed_request_and_requires_up_json() {
    let body = br#"{"status":"UP"}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body.iter().copied())
    .collect();
    let (origin, server) = serve_health(response);

    probe_health_http(&origin, Duration::from_secs(2)).expect("health succeeds");

    let port = origin.port().expect("port");
    assert_eq!(
        server.join().expect("server"),
        format!(
            "GET /actuator/health HTTP/1.1\r\n\
             Host: 127.0.0.1:{port}\r\n\
             Accept: application/json\r\n\
             Connection: close\r\n\
             \r\n"
        )
        .into_bytes()
    );
}

#[test]
fn health_probe_rejects_bad_status_body_origin_and_timeout() {
    for response in [
        b"HTTP/1.1 500 Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\n{\"status\":\"DOWN\"}"
            .to_vec(),
    ] {
        let (origin, server) = serve_health(response);
        assert!(probe_health_http(&origin, Duration::from_secs(2)).is_err());
        server.join().expect("server");
    }

    assert!(
        probe_health_http(
            &Url::parse("http://example.com:43127").expect("URL"),
            Duration::from_millis(20),
        )
        .is_err()
    );

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind timeout server");
    let address = listener.local_addr().expect("address");
    let origin = Url::parse(&format!("http://{address}")).expect("origin");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let _ = read_request(&mut stream);
        thread::sleep(Duration::from_millis(150));
    });
    assert!(probe_health_http(&origin, Duration::from_millis(30)).is_err());
    server.join().expect("server");
}

#[test]
fn session_cookie_is_http_only_strict_and_nonpersistent() {
    let token = valid_token();
    let cookie = build_session_cookie(&token).expect("cookie");

    assert_eq!(cookie.name(), "cy_kaf_desktop_session");
    assert_eq!(cookie.value(), token);
    assert_eq!(cookie.domain(), Some("127.0.0.1"));
    assert_eq!(cookie.path(), Some("/"));
    assert_eq!(cookie.http_only(), Some(true));
    assert_eq!(format!("{:?}", cookie.same_site()), "Some(Strict)");
    assert_eq!(cookie.max_age(), None);
    assert_eq!(cookie.expires(), None);
}

#[test]
fn port_release_probe_distinguishes_listening_and_released() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let address = listener.local_addr().expect("address");
    let origin = Url::parse(&format!("http://{address}")).expect("origin");

    assert!(!is_port_released(&origin).expect("listening"));
    drop(listener);
    assert!(is_port_released(&origin).expect("released"));
}

#[test]
fn command_event_timeout_is_created_inside_tauri_runtime() {
    let (_sender, mut receiver) = tauri::async_runtime::channel::<CommandEvent>(1);

    let observed = catch_unwind(AssertUnwindSafe(|| {
        recv_command_event(&mut receiver, Duration::from_millis(1))
    }));

    assert!(
        observed.is_ok(),
        "waiting must not panic outside Tokio context"
    );
    assert!(
        observed
            .expect("no panic")
            .expect("receive result")
            .is_none()
    );
}
