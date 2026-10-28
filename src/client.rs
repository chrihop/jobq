use crate::model::{Envelope, Request, Response};
use anyhow::{ensure, Context as _, Result};
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    time::Duration,
};

pub const RESPONSE_LIMIT: u64 = 64 * 1024 * 1024;

/// Shared wire helpers keep CLI and MCP validation/error decoding identical.
pub fn encode_request(request: Request) -> Result<Vec<u8>> {
    let mut data = serde_json::to_vec(&Envelope {
        version: 1,
        request,
    })?;
    ensure!(data.len() <= 4 * 1024 * 1024, "request exceeds 4MiB");
    data.push(b'\n');
    Ok(data)
}

#[derive(Debug)]
pub enum RpcError {
    Api(crate::model::ApiError),
    Protocol(String),
}
impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api(e) => write!(f, "{}: {}", e.code, e.message),
            Self::Protocol(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for RpcError {}

pub fn decode_response(line: &[u8]) -> std::result::Result<serde_json::Value, RpcError> {
    let protocol = |e: &str| RpcError::Protocol(e.into());
    if line.last() != Some(&b'\n') {
        return Err(protocol(
            "incomplete server response; outcome may be unknown",
        ));
    }
    let r: Response = serde_json::from_slice(line)
        .map_err(|e| protocol(&format!("invalid server response: {e}")))?;
    if r.version != 1 {
        return Err(protocol("unsupported response version"));
    }
    if !r.ok {
        return Err(match r.error {
            Some(e) => RpcError::Api(e),
            None => protocol("missing API error"),
        });
    }
    match r.data {
        Some(v) if v.is_object() => Ok(v),
        Some(_) => Err(protocol("API response data must be an object")),
        None => Err(protocol("missing API response data")),
    }
}

pub fn inherited_env() -> Result<std::collections::BTreeMap<String, String>> {
    std::env::vars_os()
        .map(|(k, v)| {
            Ok((
                k.into_string()
                    .map_err(|_| anyhow::anyhow!("non-UTF8 environment key"))?,
                v.into_string()
                    .map_err(|_| anyhow::anyhow!("non-UTF8 environment value"))?,
            ))
        })
        .collect()
}

pub fn rpc(socket: &Path, request: Request) -> Result<serde_json::Value> {
    let data = encode_request(request)?;
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connect {}; start jobq server first", socket.display()))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    stream.write_all(&data)?;
    let mut line = Vec::new();
    BufReader::new(stream)
        .take(RESPONSE_LIMIT)
        .read_until(b'\n', &mut line)?;
    Ok(decode_response(&line)?)
}
pub fn wait(socket: &Path, id: &str) -> Result<serde_json::Value> {
    loop {
        let value = rpc(socket, Request::Status { id: id.into() })?;
        if !matches!(
            value["state"].as_str(),
            Some("queued" | "starting" | "running" | "cancelling")
        ) {
            return Ok(value);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}
pub fn outcome_exit(value: &serde_json::Value) -> i32 {
    match value["state"].as_str().unwrap_or("") {
        "succeeded" => 0,
        "failed" => value["outcome"]["exit_code"]
            .as_i64()
            .map(|n| n.clamp(1, 255) as i32)
            .unwrap_or(1),
        "timed_out" => 124,
        "memory_exceeded" => 125,
        "spawn_failed" => 127,
        "cancelled" => 130,
        "interrupted" => 131,
        _ => 126,
    }
}
