use super::*;

#[test]
fn upgrades_http_when_tls() {
    assert_eq!(with_scheme("http://a:1", true), "https://a:1");
    assert_eq!(with_scheme("https://a:1", true), "https://a:1");
}

#[test]
fn downgrades_https_without_tls() {
    assert_eq!(with_scheme("https://a:1", false), "http://a:1");
    assert_eq!(with_scheme("http://a:1", false), "http://a:1");
}

#[test]
fn adds_a_missing_scheme() {
    assert_eq!(with_scheme("a:1", true), "https://a:1");
    assert_eq!(with_scheme("a:1", false), "http://a:1");
}

#[test]
fn keeps_path_and_leaves_foreign_schemes_alone() {
    assert_eq!(with_scheme("http://a:1/x", true), "https://a:1/x");
    assert_eq!(with_scheme("unix:///tmp/s", true), "unix:///tmp/s");
}

#[test]
fn missing_file_names_the_path() {
    let err = load_ca(Path::new("/nonexistent/ca.pem")).unwrap_err();
    assert!(err.contains("/nonexistent/ca.pem"), "{err}");
}
