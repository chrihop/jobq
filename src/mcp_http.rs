//! Authenticated Streamable HTTP adapter. The daemon remains a local Unix service.
use anyhow::{ensure, Context, Result};
use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::Read,
    net::SocketAddr,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use subtle::ConstantTimeEq;

pub struct Options {
    pub listen: SocketAddr,
    pub token_file: PathBuf,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allow_remote_http: bool,
}

// Never derive Debug: credentials must not enter diagnostics.
struct Tokens(Vec<[u8; 32]>);
impl Tokens {
    fn load(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .context("open MCP token file")?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.mode() & 0o077 == 0,
            "MCP token file must be a regular owner-only file owned by this user"
        );
        let mut raw = String::new();
        file.take(16385)
            .read_to_string(&mut raw)
            .context("read MCP token file")?;
        ensure!(raw.len() <= 16384, "MCP token file exceeds 16KiB");
        Self::parse(&raw)
    }
    fn parse(raw: &str) -> Result<Self> {
        let mut keys = Vec::new();
        for line in raw.lines().filter(|s| !s.is_empty()) {
            ensure!((32..=256).contains(&line.len()) && line.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-'),
                "each MCP token must be 32..256 URL-safe characters; generate random tokens, one per line");
            keys.push(Sha256::digest(line.as_bytes()).into());
        }
        ensure!(
            !keys.is_empty() && keys.len() <= 64,
            "MCP token file requires 1..64 tokens"
        );
        Ok(Self(keys))
    }
    fn verify(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let Some(value) = values.next() else {
            return false;
        };
        if values.next().is_some() {
            return false;
        }
        let Ok(value) = value.to_str() else {
            return false;
        };
        let Some((scheme, token)) = value.split_once(' ') else {
            return false;
        };
        if !scheme.eq_ignore_ascii_case("Bearer") || !(32..=256).contains(&token.len()) {
            return false;
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matches = subtle::Choice::from(0);
        for key in &self.0 {
            matches |= key.ct_eq(&digest);
        }
        bool::from(matches)
    }
}

type Verifier = Arc<RwLock<Option<Tokens>>>;
#[derive(PartialEq, Eq)]
struct Origin(String, String, u16);
fn origin(raw: &str) -> Option<Origin> {
    let uri: axum::http::Uri = raw.parse().ok()?;
    let scheme = uri.scheme_str()?;
    let authority = uri.authority()?.as_str();
    let explicit_port = if authority.starts_with('[') {
        !authority.ends_with(']')
    } else {
        authority.contains(':')
    };
    if !matches!(scheme, "http" | "https")
        || uri.path() != "/"
        || uri.query().is_some()
        || authority.contains('@')
        || raw.contains('#')
        || (explicit_port && uri.port_u16().is_none())
    {
        return None;
    }
    Some(Origin(
        scheme.into(),
        uri.host()?.to_ascii_lowercase(),
        uri.port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 }),
    ))
}
struct Gate {
    tokens: Verifier,
    origins: Vec<Origin>,
}
async fn authenticate(State(gate): State<Arc<Gate>>, request: Request, next: Next) -> Response {
    let mut origins = request.headers().get_all(header::ORIGIN).iter();
    if let Some(value) = origins.next() {
        let parsed = value.to_str().ok().and_then(origin);
        if origins.next().is_some() || !parsed.is_some_and(|o| gate.origins.contains(&o)) {
            return (
                StatusCode::FORBIDDEN,
                [(header::CACHE_CONTROL, "no-store")],
                "Forbidden origin\n",
            )
                .into_response();
        }
    }
    let authorized = gate
        .tokens
        .read()
        .ok()
        .and_then(|keys| keys.as_ref().map(|k| k.verify(request.headers())))
        .unwrap_or(false);
    let mut response = if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer realm=\"jobq\"")],
            "Unauthorized\n",
        )
            .into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub fn run(socket: PathBuf, umask: u32, options: Options) -> Result<()> {
    ensure!(options.listen.ip().is_loopback() || options.allow_remote_http,
        "non-loopback plaintext HTTP requires --allow-remote-http; prefer loopback with SSH or an HTTPS reverse proxy");
    let tokens: Verifier = Arc::new(RwLock::new(Some(Tokens::load(&options.token_file)?)));
    for host in &options.allowed_hosts {
        ensure!(
            !host.contains('*') && host.parse::<axum::http::uri::Authority>().is_ok(),
            "invalid --allowed-host authority"
        );
    }
    for allowed_origin in &options.allowed_origins {
        let uri: axum::http::Uri = allowed_origin.parse().context("invalid --allowed-origin")?;
        ensure!(
            origin(allowed_origin).is_some()
                && matches!(uri.scheme_str(), Some("https" | "http"))
                && uri.host().is_some()
                && uri.port_u16().is_some()
                && uri.path() == "/"
                && uri.query().is_none(),
            "--allowed-origin requires scheme, host and explicit port, without path/query"
        );
    }
    let adapter = crate::mcp::Adapter::new(socket, crate::client::inherited_env()?, umask);
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async move {
        let gate = Arc::new(Gate { tokens: tokens.clone(), origins: options.allowed_origins.iter().filter_map(|s| origin(s)).collect() });
        let mut config = StreamableHttpServerConfig::default().enforce_origin_validation();
        config.legacy_session_mode = false; // No bearer/session cross-client ownership ambiguity.
        config.json_response = true;
        config.allowed_origins = options.allowed_origins;
        config.allowed_hosts.extend(options.allowed_hosts);
        config.max_request_body_bytes = 1024 * 1024;
        let service = StreamableHttpService::new(move || Ok(adapter.clone()), Arc::new(LocalSessionManager::default()), config);
        let app = Router::new().route_service("/mcp", service)
            .layer(middleware::from_fn_with_state(gate, authenticate));
        let listener = tokio::net::TcpListener::bind(options.listen).await?;
        eprintln!("jobq MCP HTTP listening on http://{}/mcp (bearer token required)", listener.local_addr()?);
        if !options.listen.ip().is_loopback() {
            eprintln!("warning: this listener uses plaintext HTTP; protect network traffic with a trusted encrypted transport");
        }
        let mut reload = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        tokio::spawn(async move {
            while reload.recv().await.is_some() {
                let replacement = match Tokens::load(&options.token_file) {
                    Ok(value) => { eprintln!("MCP bearer tokens reloaded"); Some(value) }
                    Err(e) => { eprintln!("MCP token reload failed; all new requests denied: {e:#}"); None }
                };
                if let Ok(mut keys) = tokens.write() { *keys = replacement; }
            }
        });
        tokio::select! {
            result = axum::serve(listener, app) => { result?; }
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn origin_validation_is_strict() {
        assert!(origin("https://trusted.example") == origin("https://trusted.example:443"));
        for invalid in [
            "null",
            "https://user@trusted.example",
            "https://trusted.example:bad",
            "https://trusted.example/path",
            "https://trusted.example#fragment",
        ] {
            assert!(origin(invalid).is_none(), "{invalid}");
        }
    }
    #[test]
    fn token_validation_and_private_file() {
        let key = "a".repeat(43);
        let keys = Tokens::parse(&format!("{key}\n{}\n", "b".repeat(43))).unwrap();
        let mut headers = HeaderMap::new();
        assert!(!keys.verify(&headers));
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {key}").parse().unwrap(),
        );
        assert!(keys.verify(&headers));
        headers.append(
            header::AUTHORIZATION,
            format!("Bearer {key}").parse().unwrap(),
        );
        assert!(!keys.verify(&headers));
        for value in ["", "short", &"x ".repeat(32)] {
            assert!(Tokens::parse(value).is_err());
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens");
        std::fs::write(&path, key).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Tokens::load(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(Tokens::load(&path).is_ok());
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Tokens::load(&link).is_err());
    }
}
