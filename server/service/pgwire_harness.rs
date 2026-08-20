/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Shared harness for the pgwire integration tests.
//!
//! Every test built on this harness talks to a **real** TypeDB server: schema
//! and data go in as TypeQL over the HTTP API, and results come back over a
//! real Postgres wire protocol TCP connection. Nothing is ever seeded directly
//! into [`projection::catalog::MaterializedCatalog`], so an assertion can only
//! pass if the facade actually opened a TypeDB read transaction and ran the
//! projection's TypeQL source query.

#![allow(dead_code)]

use std::{
    collections::BTreeSet,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use projection::pgwire::messages::{
    AUTH_CLEARTEXT_PASSWORD, AUTH_OK, MSG_AUTHENTICATION, MSG_COMMAND_COMPLETE, MSG_DATA_ROW, MSG_ERROR_RESPONSE,
    MSG_PASSWORD, MSG_QUERY, MSG_READY_FOR_QUERY, MSG_ROW_DESCRIPTION, MSG_TERMINATE, PROTOCOL_VERSION_30,
};
use resource::{
    constants::server::{DEFAULT_USER_NAME, DEFAULT_USER_PASSWORD},
    distribution_info::DistributionInfo,
};
use server::{
    ServerBuilder,
    parameters::config::{ConfigBuilder, DiagnosticsConfig},
};
use test_utils::create_tmp_storage_dir;

const DISTRIBUTION_INFO: DistributionInfo =
    DistributionInfo { logo: "logo", distribution: "TypeDB CE TEST", version: "0.0.0-test" };

/// Poll interval for the projection refresher. Kept short so tests observe
/// post-commit data without waiting on the production default.
const REFRESH_INTERVAL: Duration = Duration::from_millis(200);

// ── Harness ────────────────────────────────────────────────────────

/// A live TypeDB server plus the coordinates a client needs to reach it.
pub struct Harness {
    pub http: String,
    pub pgwire_port: u16,
    pub database: String,
    pub token: String,
}

impl Harness {
    pub fn pgwire_addr(&self) -> String {
        format!("127.0.0.1:{}", self.pgwire_port)
    }

    /// Run a TypeQL query through the HTTP API and commit it.
    pub fn run_typeql(&self, transaction_type: &str, query: &str) {
        let payload = serde_json::json!({
            "databaseName": self.database,
            "transactionType": transaction_type,
            "query": query,
            "commit": true,
        })
        .to_string();
        let (status, response) = http_post(&self.http, "/v1/query", Some(&self.token), &payload);
        assert!(status < 300, "{transaction_type} query failed with {status}: {response}\n{query}");
    }

    /// Open an authenticated pgwire connection to this server.
    pub fn connect(&self) -> TcpStream {
        pgwire_connect(self.pgwire_port, &self.database)
    }
}

/// Boot a real TypeDB server with the pgwire facade enabled, create `database`,
/// then hand the coordinates to `runner` on a blocking thread.
///
/// The server is shut down once `runner` returns; a panic inside `runner` is
/// re-raised on the caller's thread after shutdown so failures are not masked
/// by a leaked server task.
pub async fn with_pgwire_server<T, F>(database: &str, runner: F) -> T
where
    F: FnOnce(&Harness) -> T + Send + 'static,
    T: Send + 'static,
{
    let data_directory = create_tmp_storage_dir();
    let (grpc_port, http_port, pgwire_port) = (free_port(), free_port(), free_port());

    let config = ConfigBuilder::from_file(config_path())
        .expect("config file should load")
        .server_listen_address(format!("127.0.0.1:{grpc_port}"))
        .server_http_enabled(true)
        .server_http_listen_address(format!("127.0.0.1:{http_port}"))
        .admin_enabled(false)
        .server_pgwire_enabled(true)
        .server_pgwire_address(format!("127.0.0.1:{pgwire_port}"))
        .server_pgwire_refresh_interval(REFRESH_INTERVAL)
        .data_directory(data_directory.as_ref())
        .diagnostics(DiagnosticsConfig::disabled())
        .development_mode(true)
        .build()
        .expect("config should validate");

    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(());
    let server = ServerBuilder::new()
        .distribution_info(DISTRIBUTION_INFO)
        .shutdown_channel((shutdown_sender.clone(), shutdown_receiver))
        .build(config)
        .await
        .expect("server should build");

    // Without this the reason a server failed to come up is discarded, and the
    // only symptom is a bare connection-refused timeout.
    let serve_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let serve_error_sink = Arc::clone(&serve_error);
    let serving = tokio::spawn(async move {
        if let Err(err) = server.serve().await {
            *serve_error_sink.lock().unwrap() = Some(format!("{err:?}"));
        }
    });

    let database = database.to_string();
    let outcome = tokio::task::spawn_blocking(move || {
        let http = format!("127.0.0.1:{http_port}");
        if let Err(reason) = await_http_ready(&http) {
            panic!("{reason}\nserver error: {:?}", serve_error.lock().unwrap());
        }

        let token = signin(&http);
        let (status, body) = http_post(&http, &format!("/v1/databases/{database}"), Some(&token), "");
        assert!(status < 300, "creating database failed with {status}: {body}");

        let harness = Harness { http, pgwire_port, database, token };
        runner(&harness)
    })
    .await;

    let _ = shutdown_sender.send(());
    let _ = tokio::time::timeout(Duration::from_secs(30), serving).await;

    match outcome {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic.into_panic()),
    }
}

// ── Server harness helpers ─────────────────────────────────────────

fn config_path() -> PathBuf {
    let cwd = std::env::current_dir().expect("current directory should be readable");
    let bazel_layout = cwd.join("server/config.yml");
    if bazel_layout.exists() { bazel_layout } else { cwd.join("config.yml") }
}

/// Reserve a port by binding and immediately releasing it.
///
/// The release opens a window in which the OS may offer the same port to
/// another caller, so every port handed out in this process is recorded and
/// never issued twice. Rejected listeners are held until a fresh port is found,
/// which forces the OS to offer a different one on each retry.
pub fn free_port() -> u16 {
    static ISSUED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    const ATTEMPTS: usize = 256;

    let mut held = Vec::new();
    for _ in 0..ATTEMPTS {
        let listener = TcpListener::bind("127.0.0.1:0").expect("an ephemeral port should be available");
        let port = listener.local_addr().unwrap().port();
        if ISSUED.lock().unwrap().insert(port) {
            return port;
        }
        held.push(listener);
    }
    panic!("no unused ephemeral port after {ATTEMPTS} attempts")
}

pub fn deadline(seconds: u64) -> Instant {
    Instant::now() + Duration::from_secs(seconds)
}

/// Re-run `sql` until it yields exactly `expected`, or fail at the deadline.
///
/// Projections are re-materialized by the background refresher, so rows
/// committed after a projection was defined become visible one poll cycle
/// later rather than instantly.
pub fn await_rows(stream: &mut TcpStream, sql: &str, expected: &[Vec<Option<String>>], context: &str) {
    let until = deadline(30);
    let mut last = QueryReply::default();
    while Instant::now() < until {
        last = simple_query(stream, sql);
        if last.errors.is_empty() && last.rows == expected {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{context}\nexpected rows: {:?}\nlast rows:     {:?}\nlast errors:   {:?}",
        expected, last.rows, last.errors
    );
}

/// Retry `probe` until it returns `true`, or fail at the deadline.
pub fn await_condition<F: FnMut() -> bool>(seconds: u64, context: &str, mut probe: F) {
    let until = deadline(seconds);
    while Instant::now() < until {
        if probe() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("{context}");
}

// ── HTTP client (hand-rolled: the server exposes no test client) ────

pub fn await_http_ready(addr: &str) -> Result<(), String> {
    let until = deadline(60);
    let mut last = String::new();
    while Instant::now() < until {
        match std::panic::catch_unwind(|| http_get(addr, "/v1/health")) {
            Ok((status, _)) if status < 300 => return Ok(()),
            Ok((status, body)) => last = format!("status {status}: {body}"),
            Err(_) => last = "connection refused".to_string(),
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!("HTTP endpoint {addr} never became ready ({last})"))
}

pub fn signin(addr: &str) -> String {
    let body = format!(r#"{{"username":"{DEFAULT_USER_NAME}","password":"{DEFAULT_USER_PASSWORD}"}}"#);
    let (status, response) = http_post(addr, "/v1/signin", None, &body);
    assert!(status < 300, "signin failed with {status}: {response}");
    let parsed: serde_json::Value = serde_json::from_str(&response).expect("signin response should be JSON");
    parsed["token"].as_str().expect("signin response should carry a token").to_string()
}

pub fn http_get(addr: &str, path: &str) -> (u16, String) {
    http_request(addr, "GET", path, None, "")
}

pub fn http_post(addr: &str, path: &str, token: Option<&str>, body: &str) -> (u16, String) {
    http_request(addr, "POST", path, token, body)
}

fn http_request(addr: &str, method: &str, path: &str, token: Option<&str>, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("HTTP endpoint should accept connections");
    stream.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(30))).unwrap();

    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(token) = token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);

    stream.write_all(request.as_bytes()).expect("HTTP request should send");
    stream.flush().unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("HTTP response should read to EOF");

    let separator = find_subslice(&raw, b"\r\n\r\n").expect("HTTP response should terminate its headers");
    let head = String::from_utf8_lossy(&raw[..separator]).into_owned();
    let payload = &raw[separator + 4..];

    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("HTTP response should start with a status line, got: {head}"));

    let chunked = head.to_ascii_lowercase().contains("transfer-encoding: chunked");
    let decoded = if chunked { dechunk(payload) } else { payload.to_vec() };
    (status, String::from_utf8_lossy(&decoded).into_owned())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn dechunk(body: &[u8]) -> Vec<u8> {
    let mut decoded = Vec::new();
    let mut rest = body;
    while let Some(end_of_size) = find_subslice(rest, b"\r\n") {
        let header = String::from_utf8_lossy(&rest[..end_of_size]).into_owned();
        let size_text = header.split(';').next().unwrap_or("").trim();
        let Ok(size) = usize::from_str_radix(size_text, 16) else { break };
        if size == 0 {
            break;
        }
        let start = end_of_size + 2;
        let end = (start + size).min(rest.len());
        decoded.extend_from_slice(&rest[start..end]);
        rest = &rest[(end + 2).min(rest.len())..];
    }
    decoded
}

// ── Postgres wire protocol client (hand-rolled) ────────────────────

#[derive(Debug, Default)]
pub struct QueryReply {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tags: Vec<String>,
    pub errors: Vec<String>,
}

pub fn pgwire_connect(port: u16, database: &str) -> TcpStream {
    let until = deadline(60);
    let mut last = String::new();
    while Instant::now() < until {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
                stream.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
                pgwire_handshake(&mut stream, database);
                return stream;
            }
            Err(err) => last = err.to_string(),
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("pgwire endpoint 127.0.0.1:{port} never accepted a connection ({last})");
}

fn pgwire_handshake(stream: &mut TcpStream, database: &str) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
    for (key, value) in [("user", DEFAULT_USER_NAME), ("database", database)] {
        payload.extend_from_slice(key.as_bytes());
        payload.push(0);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
    }
    payload.push(0);

    let mut startup = Vec::new();
    startup.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
    startup.extend_from_slice(&payload);
    stream.write_all(&startup).expect("startup message should send");
    stream.flush().unwrap();

    loop {
        let (message_type, payload) = read_message(stream);
        match message_type {
            MSG_AUTHENTICATION => {
                let auth = i32::from_be_bytes(payload[..4].try_into().unwrap());
                match auth {
                    AUTH_OK => {}
                    AUTH_CLEARTEXT_PASSWORD => {
                        let mut password = Vec::new();
                        password.push(MSG_PASSWORD);
                        let secret = format!("{DEFAULT_USER_PASSWORD}\0");
                        password.extend_from_slice(&((4 + secret.len()) as i32).to_be_bytes());
                        password.extend_from_slice(secret.as_bytes());
                        stream.write_all(&password).expect("password message should send");
                        stream.flush().unwrap();
                    }
                    other => panic!("unexpected authentication request {other}"),
                }
            }
            MSG_ERROR_RESPONSE => panic!("authentication rejected: {}", decode_error(&payload)),
            MSG_READY_FOR_QUERY => return,
            _ => {}
        }
    }
}

pub fn simple_query(stream: &mut TcpStream, sql: &str) -> QueryReply {
    let mut packet = Vec::new();
    packet.push(MSG_QUERY);
    packet.extend_from_slice(&((4 + sql.len() + 1) as i32).to_be_bytes());
    packet.extend_from_slice(sql.as_bytes());
    packet.push(0);
    stream.write_all(&packet).expect("query message should send");
    stream.flush().unwrap();

    let mut reply = QueryReply::default();
    loop {
        let (message_type, payload) = read_message(stream);
        match message_type {
            MSG_ROW_DESCRIPTION => reply.columns = decode_row_description(&payload),
            MSG_DATA_ROW => reply.rows.push(decode_data_row(&payload)),
            MSG_COMMAND_COMPLETE => reply.command_tags.push(decode_cstring(&payload, 0).0),
            MSG_ERROR_RESPONSE => reply.errors.push(decode_error(&payload)),
            MSG_READY_FOR_QUERY => return reply,
            _ => {}
        }
    }
}

pub fn terminate(stream: &mut TcpStream) {
    let _ = stream.write_all(&[MSG_TERMINATE, 0, 0, 0, 4]);
}

fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).expect("a backend message should arrive");
    let length = i32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; length - 4];
    if !payload.is_empty() {
        stream.read_exact(&mut payload).expect("a backend message body should arrive");
    }
    (header[0], payload)
}

fn decode_row_description(payload: &[u8]) -> Vec<String> {
    let count = i16::from_be_bytes(payload[..2].try_into().unwrap()) as usize;
    let mut columns = Vec::with_capacity(count);
    let mut cursor = 2;
    for _ in 0..count {
        let (name, next) = decode_cstring(payload, cursor);
        columns.push(name);
        cursor = next + 18; // table oid, attnum, type oid, typlen, typmod, format
    }
    columns
}

fn decode_data_row(payload: &[u8]) -> Vec<Option<String>> {
    let count = i16::from_be_bytes(payload[..2].try_into().unwrap()) as usize;
    let mut cells = Vec::with_capacity(count);
    let mut cursor = 2;
    for _ in 0..count {
        let length = i32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        if length < 0 {
            cells.push(None);
        } else {
            let end = cursor + length as usize;
            cells.push(Some(String::from_utf8_lossy(&payload[cursor..end]).into_owned()));
            cursor = end;
        }
    }
    cells
}

fn decode_error(payload: &[u8]) -> String {
    let mut parts = Vec::new();
    let mut cursor = 0;
    while cursor < payload.len() && payload[cursor] != 0 {
        let field = payload[cursor] as char;
        let (value, next) = decode_cstring(payload, cursor + 1);
        parts.push(format!("{field}={value}"));
        cursor = next;
    }
    parts.join(" ")
}

fn decode_cstring(payload: &[u8], start: usize) -> (String, usize) {
    let end = payload[start..].iter().position(|byte| *byte == 0).map(|offset| start + offset).unwrap_or(payload.len());
    (String::from_utf8_lossy(&payload[start..end]).into_owned(), end + 1)
}

// ── Recording proxy ────────────────────────────────────────────────

/// A transparent TCP proxy in front of the pgwire listener that records every
/// simple-query (`Q`) message a client sends.
///
/// Capturing on the wire rather than inside the server keeps the production
/// query handler free of test-only instrumentation, and records exactly the
/// bytes the client emitted rather than a re-serialization of them.
pub struct RecordingProxy {
    port: u16,
    queries: Arc<Mutex<Vec<String>>>,
    errors: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
}

impl RecordingProxy {
    pub fn start(target_port: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("proxy should bind an ephemeral port");
        let port = listener.local_addr().unwrap().port();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let accept_queries = Arc::clone(&queries);
        let accept_errors = Arc::clone(&errors);
        let accept_stop = Arc::clone(&stop);
        thread::spawn(move || {
            for incoming in listener.incoming() {
                if accept_stop.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(client) = incoming else { return };
                let Ok(upstream) = TcpStream::connect(("127.0.0.1", target_port)) else { return };
                let queries = Arc::clone(&accept_queries);
                let errors = Arc::clone(&accept_errors);
                thread::spawn(move || proxy_connection(client, upstream, queries, errors));
            }
        });

        Self { port, queries, errors, stop }
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Every simple-query statement the client sent, in order.
    pub fn captured_queries(&self) -> Vec<String> {
        self.queries.lock().unwrap().clone()
    }

    /// Every `ErrorResponse` the server sent back, in order.
    pub fn captured_errors(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Unblock the accept loop so the thread can observe the stop flag.
        let _ = TcpStream::connect(("127.0.0.1", self.port));
    }
}

fn proxy_connection(
    client: TcpStream,
    upstream: TcpStream,
    queries: Arc<Mutex<Vec<String>>>,
    errors: Arc<Mutex<Vec<String>>>,
) {
    let Ok(client_read) = client.try_clone() else { return };
    let Ok(upstream_read) = upstream.try_clone() else { return };

    let backward = thread::spawn(move || backward_and_record(upstream_read, client, errors));
    forward_and_record(client_read, upstream, queries);
    let _ = backward.join();
}

/// Relay server → client, decoding the backend message stream to pull out every
/// `ErrorResponse`.
///
/// Backend messages are uniformly typed, so no startup-phase handling is needed.
fn backward_and_record(mut from: TcpStream, mut to: TcpStream, errors: Arc<Mutex<Vec<String>>>) {
    let mut pending: Vec<u8> = Vec::new();
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
        let _ = to.flush();
        pending.extend_from_slice(&buffer[..read]);

        while pending.len() >= 5 {
            let length = i32::from_be_bytes(pending[1..5].try_into().unwrap()) as usize;
            if length < 4 || pending.len() < length + 1 {
                break;
            }
            if pending[0] == MSG_ERROR_RESPONSE {
                errors.lock().unwrap().push(decode_error(&pending[5..length + 1]));
            }
            pending.drain(..length + 1);
        }
    }
    let _ = to.shutdown(std::net::Shutdown::Write);
}

/// Relay client → server, decoding the frontend message stream well enough to
/// pull out every `Q` payload.
///
/// The frontend stream starts with untyped startup packets (`SSLRequest`,
/// `GSSENCRequest`, or `StartupMessage`, each a bare `int32` length followed by
/// its body); after the real `StartupMessage` every message carries a leading
/// type byte.
fn forward_and_record(mut from: TcpStream, mut to: TcpStream, queries: Arc<Mutex<Vec<String>>>) {
    const SSL_REQUEST_CODE: i32 = 80877103;
    const GSSENC_REQUEST_CODE: i32 = 80877104;

    let mut pending: Vec<u8> = Vec::new();
    let mut in_startup = true;
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
        let _ = to.flush();
        pending.extend_from_slice(&buffer[..read]);

        loop {
            if in_startup {
                if pending.len() < 4 {
                    break;
                }
                let length = i32::from_be_bytes(pending[..4].try_into().unwrap()) as usize;
                if length < 4 || pending.len() < length {
                    break;
                }
                let code =
                    (length == 8 && pending.len() >= 8).then(|| i32::from_be_bytes(pending[4..8].try_into().unwrap()));
                // Cancel/SSL/GSS negotiation packets are followed by another
                // untyped packet; only a real StartupMessage ends the phase.
                if !matches!(code, Some(SSL_REQUEST_CODE) | Some(GSSENC_REQUEST_CODE)) {
                    in_startup = false;
                }
                pending.drain(..length);
            } else {
                if pending.len() < 5 {
                    break;
                }
                let length = i32::from_be_bytes(pending[1..5].try_into().unwrap()) as usize;
                if length < 4 || pending.len() < length + 1 {
                    break;
                }
                if pending[0] == MSG_QUERY {
                    let body = &pending[5..length + 1];
                    let text = String::from_utf8_lossy(body.strip_suffix(b"\0").unwrap_or(body)).into_owned();
                    queries.lock().unwrap().push(text);
                }
                pending.drain(..length + 1);
            }
        }
    }
    let _ = to.shutdown(std::net::Shutdown::Write);
}
