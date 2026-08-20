/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Real end-to-end coverage for the PostgreSQL wire facade.
//!
//! This test deliberately shares nothing with the production code path except
//! the two sockets a real user would use:
//!
//! * every byte of schema and data goes in as TypeQL over the HTTP API, and
//! * every byte of the result comes back over a real Postgres wire protocol
//!   TCP connection, framed by hand.
//!
//! Nothing is seeded into [`projection::catalog::MaterializedCatalog`] by the
//! test. If the facade were not actually opening TypeDB read transactions and
//! running the projection's TypeQL source query, these assertions would fail.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
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

const DATABASE: &str = "pgwire-e2e";

const SCHEMA_QUERY: &str = r#"
define
  attribute id value integer;
  attribute name value string;
  entity person owns id, owns name;
"#;

const INSERT_TWO: &str = r#"
insert
  $a isa person, has id 1, has name "Alice";
  $b isa person, has id 2, has name "Bob";
"#;

const INSERT_THIRD: &str = r#"
insert
  $c isa person, has id 3, has name "Carol";
"#;

/// The projection is declared over a TypeQL `fetch` query whose document keys
/// are exactly the projected column names.
const DEFINE_PROJECTION: &str = concat!(
    "define projection people(id: integer, name: string) as ",
    r#"'match $p isa person; fetch { "id": $p.id, "name": $p.name };';"#
);

// ── Test ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeql_data_is_served_over_real_pgwire() {
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
        .server_pgwire_refresh_interval(Duration::from_millis(200))
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
    let serving = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    let outcome = tokio::task::spawn_blocking(move || exercise_facade(http_port, pgwire_port)).await;

    let _ = shutdown_sender.send(());
    let _ = tokio::time::timeout(Duration::from_secs(30), serving).await;

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic.into_panic());
    }
}

/// The whole user-visible flow, on blocking sockets.
fn exercise_facade(http_port: u16, pgwire_port: u16) {
    let http = format!("127.0.0.1:{http_port}");
    await_http_ready(&http);

    let token = signin(&http);
    let (status, body) = http_post(&http, &format!("/v1/databases/{DATABASE}"), Some(&token), "");
    assert!(status < 300, "creating database failed with {status}: {body}");

    run_typeql(&http, &token, "schema", SCHEMA_QUERY);
    run_typeql(&http, &token, "write", INSERT_TWO);

    let mut pg = pgwire_connect(pgwire_port);

    // Before any projection exists the table must not resolve. This is the
    // tripwire: a facade serving pre-seeded rows would answer this happily.
    let missing = simple_query(&mut pg, "SELECT id, name FROM people");
    assert!(
        !missing.errors.is_empty(),
        "selecting an undefined projection must error, got columns {:?} rows {:?}",
        missing.columns,
        missing.rows
    );

    let defined = simple_query(&mut pg, DEFINE_PROJECTION);
    assert!(defined.errors.is_empty(), "defining the projection failed: {:?}", defined.errors);

    let selected = simple_query(&mut pg, "SELECT id, name FROM people ORDER BY id");
    assert!(selected.errors.is_empty(), "selecting the projection failed: {:?}", selected.errors);
    assert_eq!(selected.columns, vec!["id".to_string(), "name".to_string()], "row description column names");
    assert_eq!(
        selected.rows,
        vec![
            vec![Some("1".to_string()), Some("Alice".to_string())],
            vec![Some("2".to_string()), Some("Bob".to_string())],
        ],
        "pgwire must return exactly the entities inserted through TypeQL"
    );
    assert_eq!(selected.command_tags, vec!["SELECT 2".to_string()], "command tag must report the real row count");

    // Data committed after the projection was defined must also be visible:
    // proves the catalog tracks TypeDB rather than being frozen at DDL time.
    run_typeql(&http, &token, "write", INSERT_THIRD);
    await_rows(
        &mut pg,
        "SELECT id, name FROM people ORDER BY id",
        &[
            vec![Some("1".to_string()), Some("Alice".to_string())],
            vec![Some("2".to_string()), Some("Bob".to_string())],
            vec![Some("3".to_string()), Some("Carol".to_string())],
        ],
        "a row committed through TypeQL after DDL must appear over pgwire",
    );

    // Pushdown still runs against live data rather than the whole table.
    let filtered = simple_query(&mut pg, "SELECT name FROM people WHERE id > 1 ORDER BY name DESC");
    assert!(filtered.errors.is_empty(), "filtered select failed: {:?}", filtered.errors);
    assert_eq!(filtered.columns, vec!["name".to_string()]);
    assert_eq!(
        filtered.rows,
        vec![vec![Some("Carol".to_string())], vec![Some("Bob".to_string())]],
        "WHERE and ORDER BY must apply to TypeDB-sourced rows"
    );

    let undefined = simple_query(&mut pg, "undefine projection people;");
    assert!(undefined.errors.is_empty(), "undefining the projection failed: {:?}", undefined.errors);
    let gone = simple_query(&mut pg, "SELECT id, name FROM people");
    assert!(!gone.errors.is_empty(), "an undefined projection must stop resolving, got rows {:?}", gone.rows);

    let _ = pg.write_all(&[MSG_TERMINATE, 0, 0, 0, 4]);
}

// ── Server harness helpers ─────────────────────────────────────────

fn config_path() -> PathBuf {
    let cwd = std::env::current_dir().expect("current directory should be readable");
    let bazel_layout = cwd.join("server/config.yml");
    if bazel_layout.exists() { bazel_layout } else { cwd.join("config.yml") }
}

/// Reserve a port by binding and immediately releasing it.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").expect("an ephemeral port should be available").local_addr().unwrap().port()
}

fn deadline(seconds: u64) -> Instant {
    Instant::now() + Duration::from_secs(seconds)
}

/// Re-run `sql` until it yields exactly `expected`, or fail at the deadline.
///
/// Projections are re-materialized by the background refresher, so rows
/// committed after a projection was defined become visible one poll cycle
/// later rather than instantly.
fn await_rows(stream: &mut TcpStream, sql: &str, expected: &[Vec<Option<String>>], context: &str) {
    let until = deadline(30);
    let mut last = QueryReply::default();
    while Instant::now() < until {
        last = simple_query(stream, sql);
        if last.errors.is_empty() && last.rows == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{context}\nexpected rows: {:?}\nlast rows:     {:?}\nlast errors:   {:?}",
        expected, last.rows, last.errors
    );
}

// ── HTTP client (hand-rolled: the server exposes no test client) ────

fn await_http_ready(addr: &str) {
    let until = deadline(60);
    let mut last = String::new();
    while Instant::now() < until {
        match std::panic::catch_unwind(|| http_get(addr, "/v1/health")) {
            Ok((status, _)) if status < 300 => return,
            Ok((status, body)) => last = format!("status {status}: {body}"),
            Err(_) => last = "connection refused".to_string(),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("HTTP endpoint {addr} never became ready ({last})");
}

fn signin(addr: &str) -> String {
    let body = format!(r#"{{"username":"{DEFAULT_USER_NAME}","password":"{DEFAULT_USER_PASSWORD}"}}"#);
    let (status, response) = http_post(addr, "/v1/signin", None, &body);
    assert!(status < 300, "signin failed with {status}: {response}");
    let parsed: serde_json::Value = serde_json::from_str(&response).expect("signin response should be JSON");
    parsed["token"].as_str().expect("signin response should carry a token").to_string()
}

fn run_typeql(addr: &str, token: &str, transaction_type: &str, query: &str) {
    let payload = serde_json::json!({
        "databaseName": DATABASE,
        "transactionType": transaction_type,
        "query": query,
        "commit": true,
    })
    .to_string();
    let (status, response) = http_post(addr, "/v1/query", Some(token), &payload);
    assert!(status < 300, "{transaction_type} query failed with {status}: {response}\n{query}");
}

fn http_get(addr: &str, path: &str) -> (u16, String) {
    http_request(addr, "GET", path, None, "")
}

fn http_post(addr: &str, path: &str, token: Option<&str>, body: &str) -> (u16, String) {
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
struct QueryReply {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    command_tags: Vec<String>,
    errors: Vec<String>,
}

fn pgwire_connect(port: u16) -> TcpStream {
    let until = deadline(60);
    let mut last = String::new();
    while Instant::now() < until {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
                stream.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
                pgwire_handshake(&mut stream);
                return stream;
            }
            Err(err) => last = err.to_string(),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("pgwire endpoint 127.0.0.1:{port} never accepted a connection ({last})");
}

fn pgwire_handshake(stream: &mut TcpStream) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&PROTOCOL_VERSION_30.to_be_bytes());
    for (key, value) in [("user", DEFAULT_USER_NAME), ("database", DATABASE)] {
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

fn simple_query(stream: &mut TcpStream, sql: &str) -> QueryReply {
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
