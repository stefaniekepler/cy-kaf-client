# Vendored Tauri updater

Source: `tauri-plugin-updater` 2.11.0 from the Tauri plugins workspace,
commit `6aa2854f314481a459be1189b02c65a2450789ab` (`plugins/updater`). The pristine
upstream `src/updater.rs` SHA-256 is
`e95d7d44c3e9bfbaaf269702c0abf406b59cfbf6d5524cdc40c72d8224037243`.

The source retains its upstream MIT/Apache-2.0 license files. Registry metadata,
upstream tests, examples and documentation that are not required to compile the
crate were omitted.

Local behavior difference: the updater metadata response is streamed into a
buffer capped at 128 KiB before `serde_json` parsing. Both an oversized declared
`Content-Length` and a chunked response that crosses the limit fail the check.
No endpoint, TLS, signature, version-selection, download or installation logic
was changed.
