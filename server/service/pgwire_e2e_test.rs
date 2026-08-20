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

mod pgwire_harness;

use pgwire_harness::{Harness, await_rows, free_port, simple_query, terminate, with_pgwire_server};

/// `free_port` reserves a port by binding an ephemeral listener and closing it,
/// so the port is genuinely free when the server binds it. That leaves a window
/// in which the OS can hand the very same port to a concurrent caller: two
/// harnesses in one test binary then configure the same port and the second
/// server dies with `AddrInUse`.
///
/// Every port issued in this process must therefore be distinct, even when
/// callers race.
#[test]
fn free_port_never_issues_one_port_twice() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 64;

    let handles: Vec<_> = (0..THREADS)
        .map(|_| std::thread::spawn(|| (0..PER_THREAD).map(|_| free_port()).collect::<Vec<u16>>()))
        .collect();

    let issued: Vec<u16> =
        handles.into_iter().flat_map(|handle| handle.join().expect("thread should not panic")).collect();

    let unique: std::collections::BTreeSet<u16> = issued.iter().copied().collect();
    assert_eq!(
        unique.len(),
        issued.len(),
        "free_port handed out {} duplicate port(s) across {THREADS} threads",
        issued.len() - unique.len()
    );
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeql_data_is_served_over_real_pgwire() {
    with_pgwire_server(DATABASE, exercise_facade).await;
}

/// The whole user-visible flow, on blocking sockets.
fn exercise_facade(harness: &Harness) {
    harness.run_typeql("schema", SCHEMA_QUERY);
    harness.run_typeql("write", INSERT_TWO);

    let mut pg = harness.connect();

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
    harness.run_typeql("write", INSERT_THIRD);
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

    terminate(&mut pg);
}
