use super::cache::{Cache, MAX_PACKAGE_BYTES};
use std::time::{Duration, Instant};
use tauri::AppHandle;
use tauri_plugin_updater::{Update, UpdaterExt};
use url::Url;

pub const CHECK_ERROR: &str = "暂时无法检查更新，可以稍后重试。";
pub const DOWNLOAD_ERROR: &str = "更新下载未完成，当前版本可以继续使用。";
pub const CACHE_ERROR: &str = "更新文件校验失败，请重新检查更新。";
const FIXED_MANIFEST_URL: &str =
    "https://github.com/stefaniekepler/cy-kaf-client/releases/latest/download/latest.json";
const DOWNLOAD_BYTES_PER_SECOND: u64 = 512 << 10;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(20 * 60);

#[derive(Clone, Copy)]
struct TransferPolicy {
    max_bytes: u64,
    bytes_per_second: u64,
    timeout: Duration,
}

pub fn valid_download_url(url: &Url, version: &str) -> bool {
    semver::Version::parse(version).is_ok()
        && url.scheme() == "https"
        && url.host_str() == Some("github.com")
        && url.port().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.path().starts_with(&format!(
            "/stefaniekepler/cy-kaf-client/releases/download/v{version}/"
        ))
        && url
            .path_segments()
            .and_then(|mut s| s.next_back())
            .is_some_and(|s| !s.is_empty())
}

pub fn same_release(
    cached_version: &str,
    cached_signature: &str,
    version: &str,
    signature: &str,
) -> bool {
    cached_version == version && !signature.is_empty() && cached_signature == signature
}

pub async fn check(app: &AppHandle, timeout: Duration) -> Result<Option<Update>, &'static str> {
    let endpoint = Url::parse(FIXED_MANIFEST_URL).map_err(|_| CHECK_ERROR)?;
    let updater = app
        .updater_builder()
        .endpoints(vec![endpoint])
        .map_err(|_| CHECK_ERROR)?
        .timeout(timeout)
        .build()
        .map_err(|_| CHECK_ERROR)?;
    let update = tokio::time::timeout(timeout, updater.check())
        .await
        .map_err(|_| CHECK_ERROR)?
        .map_err(|_| CHECK_ERROR)?;
    if let Some(update) = &update
        && (!valid_download_url(&update.download_url, &update.version)
            || update.signature.len() > 16 * 1024
            || update
                .body
                .as_ref()
                .is_some_and(|body| body.len() > 32 * 1024))
    {
        return Err(CHECK_ERROR);
    }
    Ok(update)
}

// This future runs only on the dedicated updater thread/runtime. Rate limiting
// yields that runtime rather than blocking the webview, supervisor or Go API.
pub async fn download(
    update: &Update,
    cache: &Cache,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<u64, &'static str> {
    if !valid_download_url(&update.download_url, &update.version) {
        return Err(DOWNLOAD_ERROR);
    }
    let client = http_client()?;
    download_package(
        &client,
        update.download_url.clone(),
        &update.version,
        &update.signature,
        cache,
        progress,
        TransferPolicy {
            max_bytes: MAX_PACKAGE_BYTES,
            bytes_per_second: DOWNLOAD_BYTES_PER_SECOND,
            timeout: DOWNLOAD_TIMEOUT,
        },
    )
    .await
}

fn http_client() -> Result<reqwest::Client, &'static str> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 || attempt.url().scheme() != "https" {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(|_| DOWNLOAD_ERROR)
}

async fn download_package(
    client: &reqwest::Client,
    url: Url,
    version: &str,
    signature: &str,
    cache: &Cache,
    progress: &mut dyn FnMut(u64, Option<u64>),
    policy: TransferPolicy,
) -> Result<u64, &'static str> {
    let download = async {
        let mut response = client
            .get(url)
            .send()
            .await
            .map_err(|_| DOWNLOAD_ERROR)?
            .error_for_status()
            .map_err(|_| DOWNLOAD_ERROR)?;
        let total = response.content_length();
        if total.is_some_and(|size| size > policy.max_bytes || size == 0) {
            return Err(DOWNLOAD_ERROR);
        }
        let mut bytes = Vec::new();
        let started = Instant::now();
        while let Some(chunk) = response.chunk().await.map_err(|_| DOWNLOAD_ERROR)? {
            let downloaded = (bytes.len() as u64)
                .checked_add(chunk.len() as u64)
                .ok_or(DOWNLOAD_ERROR)?;
            if downloaded > policy.max_bytes {
                return Err(DOWNLOAD_ERROR);
            }
            bytes.extend_from_slice(&chunk);
            tokio::time::sleep(rate_delay(
                downloaded,
                started.elapsed(),
                policy.bytes_per_second,
            ))
            .await;
            progress(downloaded, total);
        }
        if total.is_some_and(|size| size != bytes.len() as u64) {
            return Err(DOWNLOAD_ERROR);
        }
        cache
            .save(version, signature, &bytes)
            .map_err(|_| CACHE_ERROR)?;
        Ok(bytes.len() as u64)
    };
    tokio::time::timeout(policy.timeout, download)
        .await
        .map_err(|_| DOWNLOAD_ERROR)?
}

fn rate_delay(downloaded: u64, elapsed: Duration, bytes_per_second: u64) -> Duration {
    if bytes_per_second == 0 {
        return Duration::MAX;
    }
    Duration::from_secs_f64(downloaded as f64 / bytes_per_second as f64).saturating_sub(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const PACKAGE: &[u8] = b"Cy KafClient signed update cache fixture\n";
    const PUBLIC_KEY: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDMwMzY0NjU1MDkyMUVCNTIKUldSUzZ5RUpWVVkyTVBmVWtDSWRVL3d5WTg5WUU3bS9tMWtrL3ZTQkRTNjh4TUNOYm5qc2RTWjEK";
    const SIGNATURE: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IHNpZ25hdHVyZSBmcm9tIHRhdXJpIHNlY3JldCBrZXkKUlVSUzZ5RUpWVVkyTVAwVFNPQ1hFcmFEUlZqaUhmVms0WWQ0dkZmSEx2Yi9USXlySUgrRTBCcDRQdXh2eDJPSGlvT0wrVmdCc0d2TXNVQjB5MkpnVTVDamV0c3FxQUtKRVFZPQp0cnVzdGVkIGNvbW1lbnQ6IHRpbWVzdGFtcDoxNzg4NzYxNTEyCWZpbGU6cGFja2FnZS5iaW4KS1BCYUNaNXVVc25CSlZVRlcrRkVidjFuNXhRaEo2SDFOSFNRVmFtdnhhbzBEYkc1WGdBRDJVWXVlTEdyaW01NUZ5VVEvY2RrMGJDZHF6cUdvUmZlQkE9PQo=";
    #[test]
    fn only_the_release_repository_can_supply_update_packages() {
        assert!(valid_download_url(&Url::parse("https://github.com/stefaniekepler/cy-kaf-client/releases/download/v0.1.4/Cy-KafClient_0.1.4_macos-x86_64.app.tar.gz").unwrap(), "0.1.4"));
        for raw in [
            "http://github.com/stefaniekepler/cy-kaf-client/releases/download/v0.1.4/app",
            "https://github.com/other/repo/releases/download/v0.1.4/app",
            "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v0.1.2/app",
            "https://user:secret@github.com/stefaniekepler/cy-kaf-client/releases/download/v0.1.4/app",
            "https://example.com/app",
        ] {
            assert!(!valid_download_url(&Url::parse(raw).unwrap(), "0.1.4"));
        }
    }

    #[test]
    fn prepared_packages_must_match_the_fresh_release_metadata() {
        assert!(same_release("0.1.4", "signature", "0.1.4", "signature"));
        assert!(!same_release("0.1.4", "signature", "0.1.5", "signature"));
        assert!(!same_release("0.1.4", "signature", "0.1.4", "changed"));
    }

    #[test]
    fn production_rate_delay_enforces_512_kib_per_second() {
        assert_eq!(
            rate_delay(512 << 10, Duration::ZERO, DOWNLOAD_BYTES_PER_SECOND),
            Duration::from_secs(1)
        );
        assert_eq!(
            rate_delay(
                1024 << 10,
                Duration::from_millis(1500),
                DOWNLOAD_BYTES_PER_SECOND
            ),
            Duration::from_millis(500)
        );
    }

    #[tokio::test]
    async fn bounded_http_reader_rejects_declared_and_streamed_oversize_metadata() {
        let body = vec![b'x'; (128 << 10) + 1];
        let mut declared =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        declared.extend_from_slice(&body);
        let mut streamed = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
            body.len()
        )
        .into_bytes();
        streamed.extend_from_slice(&body);
        streamed.extend_from_slice(b"\r\n0\r\n\r\n");
        for response in [declared, streamed] {
            let url = serve_once(response).await;
            let client = test_client();
            let response = client.get(url).send().await.unwrap();
            assert!(
                tauri_plugin_updater::read_update_json(response)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn truncated_download_fails_without_committing_a_cache() {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\npartial",
            PACKAGE.len()
        )
        .into_bytes();
        assert_download_failure_leaves_no_cache(response, 1024).await;
    }

    #[tokio::test]
    async fn streamed_oversize_download_fails_without_committing_a_cache() {
        let response =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\n123456\r\n0\r\n\r\n"
                .to_vec();
        assert_download_failure_leaves_no_cache(response, 5).await;
    }

    #[tokio::test]
    async fn failed_http_status_leaves_no_cache() {
        assert_download_failure_leaves_no_cache(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\n\r\ndown".to_vec(),
            1024,
        )
        .await;
    }

    #[tokio::test]
    async fn complete_signed_http_download_is_the_only_path_that_commits() {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            PACKAGE.len()
        )
        .into_bytes()
        .into_iter()
        .chain(PACKAGE.iter().copied())
        .collect();
        let url = serve_once(response).await;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap().join("updates");
        let cache = Cache::new(root, PUBLIC_KEY.into());
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let bytes = download_package(
            &test_client(),
            url,
            "1.2.0",
            SIGNATURE,
            &cache,
            &mut move |_, _| {
                observed.fetch_add(1, Ordering::SeqCst);
            },
            TransferPolicy {
                max_bytes: 1024,
                bytes_per_second: u64::MAX,
                timeout: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
        assert_eq!(bytes, PACKAGE.len() as u64);
        assert!(count.load(Ordering::SeqCst) > 0);
        assert_eq!(cache.load("1.0.0").unwrap().unwrap().bytes, PACKAGE);
    }

    async fn assert_download_failure_leaves_no_cache(response: Vec<u8>, max_bytes: u64) {
        let url = serve_once(response).await;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap().join("updates");
        let cache = Cache::new(root.clone(), PUBLIC_KEY.into());
        let result = download_package(
            &test_client(),
            url,
            "1.2.0",
            SIGNATURE,
            &cache,
            &mut |_, _| {},
            TransferPolicy {
                max_bytes,
                bytes_per_second: u64::MAX,
                timeout: Duration::from_secs(2),
            },
        )
        .await;
        assert!(result.is_err());
        assert!(!root.exists(), "failed transfer created a cache");
    }

    fn test_client() -> reqwest::Client {
        http_client().unwrap()
    }

    async fn serve_once(response: Vec<u8>) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            let _ = stream.read(&mut request).await;
            stream.write_all(&response).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        Url::parse(&format!("http://{address}/fixture")).unwrap()
    }
}
