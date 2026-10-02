/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Live-client coverage for the pgwire facade.
//!
//! Every row asserted here was written into TypeDB as TypeQL and is read back
//! by a real third-party client — the DuckDB CLI's `postgres` extension, the
//! dbt TypeDB adapter, or a hand-framed Postgres wire connection. Nothing is
//! seeded into the materialized catalog by the test, so these assertions are
//! only satisfiable if the facade actually resolves the projection against a
//! TypeDB read transaction.

pub mod pgwire_harness;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use pgwire_harness::{Harness, RecordingProxy, await_condition, simple_query, terminate, with_pgwire_server};

/// DuckDB's `ATTACH ... (TYPE postgres)` names the catalog after the Postgres
/// database, and the dbt profile below connects to the same name.
const DATABASE: &str = "typedb";

const SCHEMA_QUERY: &str = r#"
define
  attribute id value integer;
  attribute name value string;
  entity person owns id, owns name;
"#;

const INSERT_PEOPLE: &str = r#"
insert
  $a isa person, has id 1, has name "Alice";
  $b isa person, has id 2, has name "Bob";
"#;

const DEFINE_PROJECTION: &str = concat!(
    "define projection people(id: integer, name: string) as ",
    r#"'match $p isa person; fetch { "id": $p.id, "name": $p.name };';"#
);

/// Write the schema and rows as TypeQL, then declare the projection over
/// pgwire, exactly as a user would.
fn seed_people(harness: &Harness) {
    harness.run_typeql("schema", SCHEMA_QUERY);
    harness.run_typeql("write", INSERT_PEOPLE);

    let mut pg = harness.connect();
    let defined = simple_query(&mut pg, DEFINE_PROJECTION);
    assert!(defined.errors.is_empty(), "defining the projection failed: {:?}", defined.errors);
    terminate(&mut pg);
}

// ── DuckDB ─────────────────────────────────────────────────────────

struct DuckdbRun {
    status_code: Option<i32>,
    stdout: String,
    stderr: String,
    database_file_created: bool,
    temp_dir_removed: bool,
}

impl DuckdbRun {
    fn rows(&self) -> Vec<&str> {
        self.stdout.lines().collect()
    }

    fn context(&self) -> String {
        format!("status: {:?}\nstdout:\n{}\nstderr:\n{}", self.status_code, self.stdout, self.stderr)
    }
}

static DUCKDB_PROCESS_LOCK: Mutex<()> = Mutex::new(());

fn run_duckdb_at(database_path: &Path, sql: &str) -> Output {
    let _guard = DUCKDB_PROCESS_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Command::new("duckdb")
        .args(["-unsigned", "-batch", "-csv", "-noheader", "-no-stdin", "-c", sql])
        .arg(database_path)
        .output()
        .expect("duckdb CLI must be available on PATH for this integration test")
}

#[test]
fn concurrent_duckdb_file_access_waits_for_current_process() {
    let duckdb_dir = TempDir::new("typedb-duckdb-lock");
    let duckdb_path = duckdb_dir.path().join("integration.duckdb");
    let first_path = duckdb_path.clone();
    let first = std::thread::spawn(move || {
        run_duckdb_at(
            &first_path,
            "CREATE TABLE lock_holder(value INTEGER); \
             SELECT pg_sleep(2);",
        )
    });

    await_condition(10, "first DuckDB process should create its database file", || duckdb_path.is_file());
    let second = run_duckdb_at(&duckdb_path, "SELECT COUNT(*) FROM lock_holder;");
    let first = first.join().expect("first DuckDB process should not panic");

    assert!(first.status.success(), "first DuckDB process should succeed.\n{}", String::from_utf8_lossy(&first.stderr));
    assert!(
        second.status.success(),
        "concurrent DuckDB access should wait rather than fail on the file lock.\n{}",
        String::from_utf8_lossy(&second.stderr)
    );
}

fn duckdb_sql(addr: &str, tail_sql: &str) -> String {
    let (_host, port) = addr.rsplit_once(':').expect("listener address should include a port");
    format!(
        "INSTALL postgres; \
         LOAD postgres; \
         ATTACH 'host=127.0.0.1 port={port} dbname={DATABASE} user=admin password=password' AS typedb (TYPE postgres); \
         {tail_sql}"
    )
}

fn run_duckdb(addr: &str, tail_sql: &str) -> DuckdbRun {
    let duckdb_dir = TempDir::new("typedb-duckdb");
    let duckdb_path = duckdb_dir.path().join("integration.duckdb");
    assert!(!duckdb_path.exists(), "temporary DuckDB database {duckdb_path:?} was handed out twice");

    let output = run_duckdb_at(&duckdb_path, &duckdb_sql(addr, tail_sql));
    let database_file_created = duckdb_path.is_file();
    drop(duckdb_dir);
    let temp_dir_removed = duckdb_path.parent().is_some_and(|path| !path.exists());

    DuckdbRun {
        status_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        database_file_created,
        temp_dir_removed,
    }
}

/// Seed TypeDB, then run `tail_sql` through the DuckDB CLI against the facade.
async fn duckdb_against_typedb(tail_sql: &'static str) -> DuckdbRun {
    with_pgwire_server(DATABASE, move |harness| {
        seed_people(harness);
        run_duckdb(&harness.pgwire_addr(), tail_sql)
    })
    .await
}

/// Assert that `tail_sql` succeeds and prints exactly `expected` lines.
async fn assert_duckdb_output(tail_sql: &'static str, expected: &[&str]) {
    let run = duckdb_against_typedb(tail_sql).await;
    assert!(run.status_code == Some(0), "DuckDB query should succeed.\n{}", run.context());
    assert_eq!(run.rows(), expected, "DuckDB output mismatch.\n{}", run.context());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_scans_rows_written_as_typeql() {
    let run = duckdb_against_typedb("SELECT id, name FROM typedb.public.people ORDER BY id;").await;

    assert!(run.database_file_created, "DuckDB should run against a file-backed temporary database");
    assert!(run.temp_dir_removed, "DuckDB should remove its temporary database directory after the run");
    assert!(run.status_code == Some(0), "DuckDB should attach and scan the projection.\n{}", run.context());
    assert_eq!(
        run.rows(),
        vec!["1,Alice", "2,Bob"],
        "DuckDB must read back exactly the entities inserted through TypeQL.\n{}",
        run.context()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_discovers_the_projection_as_a_table() {
    let run = duckdb_against_typedb("SHOW ALL TABLES;").await;

    assert!(run.status_code == Some(0), "DuckDB should attach and list tables.\n{}", run.context());
    assert!(
        run.stdout.lines().any(|line| line.contains("people")),
        "DuckDB table discovery should include the declared projection.\n{}",
        run.context()
    );
}

/// The catalog-emulation contract, asserted the way it actually matters: every
/// statement DuckDB issues while attaching must be answered without an
/// `ErrorResponse`.
///
/// Comparing the introspection SQL verbatim would only detect that DuckDB
/// changed its queries; it would not detect that this facade answered one of
/// them wrongly. Recording on the wire keeps the production query handler free
/// of test-only instrumentation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_attach_is_answered_without_any_backend_error() {
    let (run, queries, errors) = with_pgwire_server(DATABASE, |harness| {
        seed_people(harness);
        let proxy = RecordingProxy::start(harness.pgwire_port);
        let run = run_duckdb(&proxy.addr(), "SHOW ALL TABLES;");
        (run, proxy.captured_queries(), proxy.captured_errors())
    })
    .await;

    assert!(run.status_code == Some(0), "DuckDB attach should succeed.\n{}", run.context());
    assert!(
        errors.is_empty(),
        "the facade must answer every statement DuckDB sends without an error.\nerrors:\n{:#?}\ncaptured queries:\n{:#?}\n{}",
        errors,
        queries,
        run.context()
    );

    // Prove the catalog emulation was genuinely exercised rather than skipped.
    let all = queries.join("\n").to_ascii_lowercase();
    for probe in ["pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_indexes"] {
        assert!(all.contains(probe), "DuckDB attach should have probed {probe}.\ncaptured queries:\n{:#?}", queries);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_applies_filter_order_and_limit() {
    assert_duckdb_output("SELECT name FROM typedb.public.people WHERE id = 2 ORDER BY name LIMIT 1;", &["Bob"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_applies_ordering_and_limit_without_filter() {
    assert_duckdb_output("SELECT name FROM typedb.public.people ORDER BY name DESC LIMIT 1;", &["Bob"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_scans_every_row() {
    assert_duckdb_output("SELECT name FROM typedb.public.people;", &["Alice", "Bob"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_casts_integer_columns_to_text() {
    assert_duckdb_output("SELECT CAST(id AS VARCHAR) AS id_text FROM typedb.public.people ORDER BY id;", &["1", "2"])
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_counts_rows() {
    assert_duckdb_output("SELECT COUNT(*) FROM typedb.public.people;", &["2"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_returns_no_rows_for_a_constant_false_filter() {
    assert_duckdb_output("SELECT * FROM typedb.public.people WHERE 1 = 0;", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_counts_zero_for_a_constant_false_filter() {
    assert_duckdb_output("SELECT COUNT(*) FROM typedb.public.people WHERE 1 = 0;", &["0"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_counts_rows_with_an_is_not_null_filter() {
    assert_duckdb_output("SELECT COUNT(*) FROM typedb.public.people WHERE name IS NOT NULL;", &["2"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_accepts_quoted_identifiers() {
    assert_duckdb_output("SELECT \"name\" FROM typedb.public.people ORDER BY \"name\" DESC LIMIT 1;", &["Bob"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_orders_by_an_alias() {
    assert_duckdb_output(
        "SELECT name AS full_name FROM typedb.public.people ORDER BY full_name DESC LIMIT 1;",
        &["Bob"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_returns_no_rows_for_limit_zero() {
    assert_duckdb_output("SELECT * FROM typedb.public.people LIMIT 0;", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_scans_filters_and_detaches_in_one_session() {
    assert_duckdb_output(
        "SELECT name FROM typedb.public.people ORDER BY id; \
         SELECT name FROM typedb.public.people WHERE id = 2; \
         DETACH typedb;",
        &["Alice", "Bob", "Bob"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_reads_session_metadata_from_pg_stat_activity() {
    let run =
        duckdb_against_typedb("SELECT datname, usename, pid FROM typedb.pg_catalog.pg_stat_activity LIMIT 1;").await;

    assert!(run.status_code == Some(0), "DuckDB should read pg_stat_activity.\n{}", run.context());
    let row = run.stdout.trim().to_string();
    let fields: Vec<&str> = row.split(',').collect();
    assert_eq!(fields.len(), 3, "pg_stat_activity row should have three fields, got {row}.\n{}", run.context());
    assert_eq!(
        &fields[..2],
        &[DATABASE, "admin"],
        "pg_stat_activity should report the live session.\n{}",
        run.context()
    );
    // The listener allocates backend PIDs per connection, so the value depends
    // on how many connections DuckDB opened; only its shape is guaranteed.
    assert!(
        fields[2].parse::<i32>().is_ok_and(|pid| pid > 0),
        "pg_stat_activity should report a positive backend PID, got {row}.\n{}",
        run.context()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duckdb_reads_the_live_listener_port_from_pg_settings() {
    let (run, pgwire_port) = with_pgwire_server(DATABASE, |harness| {
        seed_people(harness);
        let run = run_duckdb(
            &harness.pgwire_addr(),
            "SELECT setting FROM typedb.pg_catalog.pg_settings WHERE name = 'port';",
        );
        (run, harness.pgwire_port)
    })
    .await;

    assert!(run.status_code == Some(0), "DuckDB should read pg_settings.\n{}", run.context());
    assert_eq!(
        run.stdout.trim(),
        pgwire_port.to_string(),
        "pg_settings should expose the port this server is actually listening on.\n{}",
        run.context()
    );
}

// ── Raw Postgres wire protocol ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_pgwire_session_context_is_truthful() {
    with_pgwire_server(DATABASE, |harness| {
        seed_people(harness);
        let mut pg = harness.connect();

        let backend_pid = simple_query(&mut pg, "SELECT pg_backend_pid()");
        assert!(backend_pid.errors.is_empty(), "pg_backend_pid() failed: {:?}", backend_pid.errors);
        let pid = backend_pid.rows[0][0].clone().expect("pg_backend_pid() should not be null");

        let reply = simple_query(
            &mut pg,
            "SELECT current_database(), current_schema(), current_user, pg_backend_pid(), inet_server_port()",
        );
        assert!(reply.errors.is_empty(), "session function query failed: {:?}", reply.errors);
        assert_eq!(
            reply.columns,
            vec!["current_database", "current_schema", "current_user", "pg_backend_pid", "inet_server_port"],
            "session function query should expose the expected columns"
        );
        assert_eq!(reply.command_tags, vec!["SELECT 1".to_string()], "simple query should complete with SELECT 1");
        assert_eq!(
            reply.rows,
            vec![vec![
                Some(DATABASE.to_string()),
                Some("public".to_string()),
                Some("admin".to_string()),
                Some(pid.clone()),
                Some(harness.pgwire_port.to_string()),
            ]],
            "session functions must reflect the real connection, not fixed values"
        );

        // The same PID must be what pg_stat_activity reports for this session.
        let activity = simple_query(&mut pg, "SELECT datname, usename, pid FROM pg_catalog.pg_stat_activity");
        assert!(activity.errors.is_empty(), "pg_stat_activity query failed: {:?}", activity.errors);
        assert_eq!(
            activity.rows,
            vec![vec![Some(DATABASE.to_string()), Some("admin".to_string()), Some(pid)]],
            "pg_stat_activity must describe the connection issuing the query"
        );

        terminate(&mut pg);
    })
    .await;
}

// ── dbt ────────────────────────────────────────────────────────────

struct DbtRun {
    status_code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl DbtRun {
    fn context(&self) -> String {
        format!("status: {:?}\nstdout:\n{}\nstderr:\n{}", self.status_code, self.stdout, self.stderr)
    }
}

#[derive(Debug)]
enum DbtInvoker {
    Executable(PathBuf),
    Uv(PathBuf),
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

fn dbt_tool_dir() -> PathBuf {
    repo_root().join("tools").join("dbt-typedb")
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(name);
            candidate.is_file().then_some(candidate)
        })
    })
}

fn find_dbt_invoker() -> Option<DbtInvoker> {
    let venv = dbt_tool_dir().join(".venv");
    for candidate in [venv.join("Scripts").join("dbt.exe"), venv.join("bin").join("dbt")] {
        if candidate.is_file() {
            return Some(DbtInvoker::Executable(candidate));
        }
    }

    if let Some(dbt) = find_on_path(if cfg!(windows) { "dbt.exe" } else { "dbt" }) {
        return Some(DbtInvoker::Executable(dbt));
    }

    find_on_path(if cfg!(windows) { "uv.exe" } else { "uv" }).map(DbtInvoker::Uv)
}

fn write_dbt_project(project_dir: &Path) {
    fs::write(
        project_dir.join("dbt_project.yml"),
        "name: typedb_pgwire_smoke\nversion: '1.0'\nconfig-version: 2\nprofile: typedb\nmodel-paths: ['models']\n",
    )
    .unwrap();
    fs::create_dir_all(project_dir.join("models")).unwrap();
}

fn write_dbt_profiles(profiles_dir: &Path, host: &str, port: u16) {
    fs::write(
        profiles_dir.join("profiles.yml"),
        format!(
            "typedb:\n  target: dev\n  outputs:\n    dev:\n      type: typedb\n      host: {host}\n      port: {port}\n      user: admin\n      pass: password\n      dbname: {DATABASE}\n      schema: public\n      threads: 1\n"
        ),
    )
    .unwrap();
}

fn run_dbt(addr: &str, subcommand: &str, subcommand_args: &[&str]) -> DbtRun {
    let invoker = find_dbt_invoker().expect("dbt must be available via tools/dbt-typedb/.venv, PATH, or uv");
    let project_dir = TempDir::new("typedb-dbt-project");
    let profiles_dir = TempDir::new("typedb-dbt-profiles");
    write_dbt_project(project_dir.path());

    let (_host, port) = addr.rsplit_once(':').expect("listener address should include a port");
    let port = port.parse::<u16>().expect("listener address should include a numeric port");
    write_dbt_profiles(profiles_dir.path(), "127.0.0.1", port);

    let mut command = match &invoker {
        DbtInvoker::Executable(path) => Command::new(path),
        DbtInvoker::Uv(path) => {
            let mut command = Command::new(path);
            command.arg("run").arg("--directory").arg(dbt_tool_dir()).arg("dbt");
            command
        }
    };
    let output = command
        .arg("--no-use-colors")
        .arg(subcommand)
        .arg("--project-dir")
        .arg(project_dir.path())
        .arg("--profiles-dir")
        .arg(profiles_dir.path())
        .args(subcommand_args)
        .current_dir(repo_root())
        .output()
        .expect("dbt command must start successfully for this integration test");

    DbtRun {
        status_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

async fn dbt_against_typedb(subcommand: &'static str, args: &'static [&'static str]) -> DbtRun {
    with_pgwire_server(DATABASE, move |harness| {
        seed_people(harness);
        run_dbt(&harness.pgwire_addr(), subcommand, args)
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dbt_debug_connects_to_the_facade() {
    let run = dbt_against_typedb("debug", &[]).await;
    assert!(run.status_code == Some(0), "dbt debug should connect to the pgwire endpoint.\n{}", run.context());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dbt_show_reads_rows_written_as_typeql() {
    let run = dbt_against_typedb("show", &["--inline", "select name from public.people order by id"]).await;

    assert!(run.status_code == Some(0), "dbt show should query the projection.\n{}", run.context());
    assert!(
        run.stdout.contains("Alice") && run.stdout.contains("Bob"),
        "dbt show must return the entities inserted through TypeQL.\n{}",
        run.context()
    );
}

// ── Process helpers ────────────────────────────────────────────────

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
