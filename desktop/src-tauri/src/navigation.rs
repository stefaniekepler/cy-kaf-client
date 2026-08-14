use url::{Origin, Url};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DesktopAction {
    Retry,
    ReassignPort,
    OpenLogs,
    RevealConfig,
    Quit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Navigation {
    Allow,
    ExternalHttps(Url),
    Action(DesktopAction),
    Deny,
}

pub fn classify_navigation(url: &Url, app_origin: &Origin, sidecar: Option<&Origin>) -> Navigation {
    if is_exact_action(url) {
        return Navigation::Action(action_from_host(
            url.host_str().expect("checked action host"),
        ));
    }

    if is_supported_app_url(url) && matches_captured_app_origin(url, app_origin) {
        return Navigation::Allow;
    }

    if is_loopback_sidecar_url(url) && sidecar.is_some_and(|expected| url.origin() == *expected) {
        return Navigation::Allow;
    }

    if url.scheme() == "https" && has_no_credentials(url) && url.host_str().is_some() {
        return Navigation::ExternalHttps(url.clone());
    }

    Navigation::Deny
}

fn matches_captured_app_origin(url: &Url, app_origin: &Origin) -> bool {
    match url.scheme() {
        "tauri" => matches!(app_origin, Origin::Opaque(_)),
        "http" => url.origin() == *app_origin,
        _ => false,
    }
}

fn is_supported_app_url(url: &Url) -> bool {
    if !has_no_credentials(url) || url.port().is_some() {
        return false;
    }

    matches!(
        (url.scheme(), url.host_str()),
        ("tauri", Some("localhost")) | ("http", Some("tauri.localhost"))
    )
}

fn is_loopback_sidecar_url(url: &Url) -> bool {
    url.scheme() == "http"
        && has_no_credentials(url)
        && url.host_str() == Some("127.0.0.1")
        && url.port().is_some_and(|port| port != 0)
}

fn has_no_credentials(url: &Url) -> bool {
    url.username().is_empty() && url.password().is_none()
}

fn is_exact_action(url: &Url) -> bool {
    url.scheme() == "cy-kaf-action"
        && has_no_credentials(url)
        && url.port().is_none()
        && url.path().is_empty()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.host_str().is_some_and(is_known_action)
}

fn is_known_action(host: &str) -> bool {
    matches!(host, "retry" | "reassign-port" | "open-logs" | "reveal-config" | "quit")
}

fn action_from_host(host: &str) -> DesktopAction {
    match host {
        "retry" => DesktopAction::Retry,
        "reassign-port" => DesktopAction::ReassignPort,
        "open-logs" => DesktopAction::OpenLogs,
        "reveal-config" => DesktopAction::RevealConfig,
        "quit" => DesktopAction::Quit,
        _ => unreachable!("action host was checked before conversion"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    fn parsed(raw: &str) -> Url {
        Url::parse(raw).expect("test URL should parse")
    }

    #[test]
    fn allows_only_the_captured_app_origin_and_ready_sidecar_origin() {
        let app_url = parsed("tauri://localhost/index.html");
        let app_origin = app_url.origin();
        let sidecar_url = parsed("http://127.0.0.1:43127/");
        let sidecar_origin = sidecar_url.origin();

        assert_eq!(
            classify_navigation(
                &parsed("tauri://localhost/assets/app.js"),
                &app_origin,
                Some(&sidecar_origin),
            ),
            Navigation::Allow
        );
        assert_eq!(
            classify_navigation(
                &parsed("http://127.0.0.1:43127/topics/orders?page=2"),
                &app_origin,
                Some(&sidecar_origin),
            ),
            Navigation::Allow
        );
        assert_eq!(
            classify_navigation(
                &parsed("http://127.0.0.1:43128/"),
                &app_origin,
                Some(&sidecar_origin),
            ),
            Navigation::Deny
        );
        assert_eq!(
            classify_navigation(
                &parsed("tauri://localhost.attacker.invalid/"),
                &app_origin,
                Some(&sidecar_origin),
            ),
            Navigation::Deny
        );
    }

    #[test]
    fn supports_the_windows_tauri_app_origin() {
        let initial = parsed("http://tauri.localhost/index.html");
        assert_eq!(
            classify_navigation(
                &parsed("http://tauri.localhost/styles.css"),
                &initial.origin(),
                None,
            ),
            Navigation::Allow
        );
    }

    #[test]
    fn classifies_only_https_as_external_navigation() {
        let initial = parsed("tauri://localhost/index.html");

        assert_eq!(
            classify_navigation(&parsed("https://example.com/help"), &initial.origin(), None,),
            Navigation::ExternalHttps(parsed("https://example.com/help"))
        );
        for raw in [
            "http://example.com/help",
            "file:///tmp/help.html",
            "data:text/plain,help",
            "javascript:alert(1)",
            "https://user:password@example.com/help",
        ] {
            assert_eq!(
                classify_navigation(&parsed(raw), &initial.origin(), None),
                Navigation::Deny,
                "URL should be denied: {raw}"
            );
        }
    }

    #[test]
    fn recognizes_only_exact_desktop_actions() {
        let initial = parsed("tauri://localhost/index.html");

        for (raw, expected) in [
            ("cy-kaf-action://retry", DesktopAction::Retry),
            ("cy-kaf-action://reassign-port", DesktopAction::ReassignPort),
            ("cy-kaf-action://open-logs", DesktopAction::OpenLogs),
            ("cy-kaf-action://reveal-config", DesktopAction::RevealConfig),
            ("cy-kaf-action://quit", DesktopAction::Quit),
        ] {
            assert_eq!(
                classify_navigation(&parsed(raw), &initial.origin(), None),
                Navigation::Action(expected)
            );
        }

        for raw in [
            "cy-kaf-action://unknown",
            "cy-kaf-action://retry/extra",
            "cy-kaf-action://retry?next=quit",
            "cy-kaf-action://user@retry",
        ] {
            assert_eq!(
                classify_navigation(&parsed(raw), &initial.origin(), None),
                Navigation::Deny,
                "action should be denied: {raw}"
            );
        }
    }
}
