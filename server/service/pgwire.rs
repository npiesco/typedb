/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Postgres wire protocol server integration.
//!
//! Bridges the `projection` crate's pgwire listener into the TypeDB server.
//! This module owns the three seams that make the facade serve live data:
//!
//! * [`ServerStateAuthenticator`] — verifies pgwire startup credentials
//!   against TypeDB's own user store.
//! * [`TypeDBSourceQueryExecutor`] — runs a projection's TypeQL `fetch`
//!   source query inside a real read transaction and flattens each answer
//!   document into positional Postgres text cells.
//! * [`DatabaseChangeDetector`] — reports how far a database's write-ahead log
//!   has advanced, so the refresher only re-materializes after real commits.
//! * [`CatalogQueryHandler`] — keeps one projection store, materialized catalog
//!   and background refresher per TypeDB database, and applies projection DDL.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use database::transaction::TransactionRead;
use executor::ExecutionInterrupt;
use options::TransactionOptions;
use projection::{
    background_refresher::{BackgroundRefresher, RefreshDecision},
    catalog::MaterializedCatalog,
    change_detector::{ChangeDetector, ChangeEvent},
    materializer::{Materializer, SourceQueryExecutor},
    pgwire::{
        authenticator::PgAuthenticator,
        connection::{QueryHandler, QueryOutcome, QueryResult, SessionContext},
        query_executor::{execute_raw_query_batch_with_session, execute_raw_query_with_session},
    },
    projection_store::ProjectionStore,
    refresh_policy::{ProjectionRefreshConfig, RefreshPolicy},
    schema_parser::{ProjectionStatement, parse_projection_ddl},
};
use query::query_cache::ParsedQuery;
use resource::profile::StorageCounters;
use storage::{durability_client::WALClient, snapshot::ReadableSnapshot};
use tracing::{Level, event};

use crate::{
    service::http::message::query::{GivenRowsHttp, document::encode_document},
    state::ServerState,
};

// ── Async bridge ───────────────────────────────────────────────────

/// Run an async operation to completion from a synchronous pgwire callback.
///
/// Each pgwire connection is its own `tokio::spawn`ed task on the server's
/// multi-threaded runtime, so `block_in_place` correctly hands any other
/// tasks on this worker to a sibling thread instead of stalling them.
fn block_on_runtime<F: std::future::Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

// ── ServerStateAuthenticator ───────────────────────────────────────

/// Bridges [`PgAuthenticator`] to TypeDB's user store.
#[derive(Debug)]
pub struct ServerStateAuthenticator {
    server_state: Arc<ServerState>,
}

impl ServerStateAuthenticator {
    pub fn new(server_state: Arc<ServerState>) -> Self {
        Self { server_state }
    }
}

impl PgAuthenticator for ServerStateAuthenticator {
    fn verify_password(&self, username: &str, password: &str) -> Result<(), String> {
        let server_state = self.server_state.clone();
        let username = username.to_string();
        let password = password.to_string();
        block_on_runtime(async move { server_state.users().verify_password(&username, &password).await })
            .map_err(|err| format!("{err:?}"))
    }
}

// ── TypeDBSourceQueryExecutor ──────────────────────────────────────

/// Runs projection source queries against live TypeDB databases.
#[derive(Debug)]
pub struct TypeDBSourceQueryExecutor {
    server_state: Arc<ServerState>,
}

impl TypeDBSourceQueryExecutor {
    pub fn new(server_state: Arc<ServerState>) -> Self {
        Self { server_state }
    }

    /// Flatten one encoded answer document into positional cells.
    fn document_to_row(document: &serde_json::Value, columns: &[&str]) -> Vec<Option<String>> {
        columns.iter().map(|column| Self::cell_to_text(document.get(*column))).collect()
    }

    /// Convert a single JSON cell into its Postgres text representation.
    fn cell_to_text(value: Option<&serde_json::Value>) -> Option<String> {
        match value {
            None | Some(serde_json::Value::Null) => None,
            // Attributes encode as `{"value": <scalar>, ..}` when instance
            // types are included; unwrap so the cell stays flat either way.
            Some(serde_json::Value::Object(map)) => match map.get("value") {
                Some(inner) => Self::cell_to_text(Some(inner)),
                None => Some(serde_json::Value::Object(map.clone()).to_string()),
            },
            Some(serde_json::Value::Bool(flag)) => Some(if *flag { "t".to_string() } else { "f".to_string() }),
            Some(serde_json::Value::String(text)) => Some(text.clone()),
            Some(other) => Some(other.to_string()),
        }
    }

    /// Parse, prepare and drain a read-only `fetch` pipeline.
    fn run_fetch(
        transaction: &TransactionRead<WALClient>,
        source_query: &str,
        columns: &[&str],
    ) -> Result<Vec<Vec<Option<String>>>, String> {
        let query_manager = transaction.query_manager.clone();
        let parsed = query_manager
            .parse(source_query)
            .map_err(|err| format!("could not parse projection source query: {err:?}"))?;
        let ParsedQuery::Pipeline(pipeline) = parsed else {
            return Err("projection source query must be a read pipeline, not a schema query".to_string());
        };

        let snapshot = transaction.snapshot.clone();
        let type_manager = transaction.type_manager.clone();
        let thing_manager = transaction.thing_manager.clone();
        let function_manager = transaction.function_manager.clone();

        let prepared = query_manager
            .prepare_read_pipeline(
                snapshot.clone(),
                &type_manager,
                thing_manager.clone(),
                function_manager.clone(),
                &pipeline,
                None::<GivenRowsHttp>,
                source_query,
            )
            .map_err(|err| format!("could not prepare projection source query: {err:?}"))?;

        if !prepared.has_fetch() {
            return Err("projection source query must be a 'fetch' query".to_string());
        }

        let (documents, context) = prepared
            .into_documents_iterator(ExecutionInterrupt::new_uninterruptible())
            .map_err(|(err, _)| format!("could not execute projection source query: {err:?}"))?;

        let parameters = context.parameters;
        let mut rows = Vec::new();
        for document in documents {
            let document = document.map_err(|err| format!("could not read projection source answer: {err:?}"))?;
            let encoded = encode_document(
                document,
                snapshot.as_ref(),
                &type_manager,
                &thing_manager,
                &parameters,
                StorageCounters::DISABLED,
            )
            .map_err(|err| format!("could not encode projection source answer: {err:?}"))?;
            rows.push(Self::document_to_row(&encoded, columns));
        }
        Ok(rows)
    }
}

impl SourceQueryExecutor for TypeDBSourceQueryExecutor {
    fn execute(
        &self,
        database_name: &str,
        source_query: &str,
        columns: &[&str],
    ) -> Result<Vec<Vec<Option<String>>>, String> {
        let server_state = self.server_state.clone();
        let lookup_name = database_name.to_string();
        let database = block_on_runtime(async move { server_state.databases().get(&lookup_name).await })
            .map_err(|err| format!("{err:?}"))?
            .ok_or_else(|| format!("database '{database_name}' not found"))?;

        let transaction = TransactionRead::<WALClient>::open(database, TransactionOptions::default())
            .map_err(|err| format!("could not open read transaction: {err:?}"))?;
        let result = Self::run_fetch(&transaction, source_query, columns);
        transaction.close();
        result
    }
}

// ── DatabaseChangeDetector ─────────────────────────────────────────

/// Reports write-ahead-log progress for a single database.
///
/// The WAL itself is private to the `database` crate, so progress is observed
/// through the sequence number a freshly opened read snapshot is pinned to.
/// That yields watermark granularity rather than one event per commit record:
/// a cycle that sees the watermark advance reports one change event carrying
/// the new watermark. The detector therefore never invents a commit that did
/// not happen and never misses one that did.
///
/// Schema commits advance the same counter as data commits, so every advance is
/// reported as a data change. That is the conservative direction — it can only
/// cause an extra refresh, never a stale projection.
#[derive(Debug)]
pub struct DatabaseChangeDetector {
    server_state: Arc<ServerState>,
    database_name: String,
}

impl DatabaseChangeDetector {
    pub fn new(server_state: Arc<ServerState>, database_name: impl Into<String>) -> Self {
        Self { server_state, database_name: database_name.into() }
    }
}

impl ChangeDetector for DatabaseChangeDetector {
    fn poll_changes(&self, after_sequence_number: u64) -> Result<Vec<ChangeEvent>, String> {
        let watermark = self.current_watermark()?;
        Ok(if watermark > after_sequence_number { vec![ChangeEvent::data(watermark)] } else { Vec::new() })
    }

    fn current_watermark(&self) -> Result<u64, String> {
        let server_state = self.server_state.clone();
        let lookup_name = self.database_name.clone();
        let database = block_on_runtime(async move { server_state.databases().get(&lookup_name).await })
            .map_err(|err| format!("{err:?}"))?
            .ok_or_else(|| format!("database '{}' not found", self.database_name))?;
        let transaction = TransactionRead::<WALClient>::open(database, TransactionOptions::default())
            .map_err(|err| format!("could not open read transaction: {err:?}"))?;
        let watermark = transaction.snapshot.open_sequence_number().number();
        transaction.close();
        Ok(watermark)
    }
}

// ── CatalogQueryHandler ────────────────────────────────────────────

/// Per-database projection definitions, their materialized rows, and the
/// refresher that keeps those rows current.
#[derive(Debug)]
struct DatabaseCatalog {
    store: ProjectionStore,
    catalog: MaterializedCatalog,
    refresher: Arc<Mutex<BackgroundRefresher>>,
}

impl DatabaseCatalog {
    fn new(database_name: &str) -> Self {
        let catalog = MaterializedCatalog::new();
        let refresher = BackgroundRefresher::new(Materializer::new(catalog.clone(), database_name));
        Self { store: ProjectionStore::new(), catalog, refresher: Arc::new(Mutex::new(refresher)) }
    }
}

/// Serves pgwire queries from projections materialized out of TypeDB.
#[derive(Debug)]
pub struct CatalogQueryHandler {
    databases: Mutex<HashMap<String, DatabaseCatalog>>,
    server_state: Arc<ServerState>,
    executor: TypeDBSourceQueryExecutor,
    refresh_policy: RefreshPolicy,
}

impl CatalogQueryHandler {
    /// Projections are refreshed whenever the database's WAL has advanced,
    /// which the pgwire refresh loop checks on its configured interval.
    pub fn new(server_state: Arc<ServerState>) -> Self {
        Self {
            databases: Mutex::new(HashMap::new()),
            server_state: server_state.clone(),
            executor: TypeDBSourceQueryExecutor::new(server_state),
            refresh_policy: RefreshPolicy::Eager,
        }
    }

    /// Run one poll-and-refresh cycle across every database holding projections.
    ///
    /// Databases are collected under a short-lived lock so a slow refresh of one
    /// database cannot block sessions reading another. A projection that fails
    /// to refresh keeps serving its last good rows and is reported, so a broken
    /// source query cannot silently freeze a table.
    pub fn run_refresh_cycle(&self) {
        let refreshers: Vec<(String, Arc<Mutex<BackgroundRefresher>>)> = {
            let databases = self.databases.lock().unwrap();
            databases.iter().map(|(name, entry)| (name.clone(), entry.refresher.clone())).collect()
        };
        for (database_name, refresher) in refreshers {
            let detector = DatabaseChangeDetector::new(self.server_state.clone(), database_name.clone());
            let outcome = refresher.lock().unwrap().run_cycle(&detector, &self.executor);
            match outcome {
                Ok(cycle) => {
                    for (projection, decision) in cycle.decisions {
                        if let RefreshDecision::Failed(err) = decision {
                            event!(
                                Level::WARN,
                                "Could not refresh projection '{projection}' of database '{database_name}': {err}"
                            );
                        }
                    }
                }
                Err(err) => {
                    event!(Level::WARN, "Could not poll database '{database_name}' for projection changes: {err}")
                }
            }
        }
    }

    fn error(message: impl Into<String>) -> QueryOutcome {
        QueryOutcome::Error { severity: "ERROR".to_string(), code: "42000".to_string(), message: message.into() }
    }

    /// An empty result set carrying a DDL command tag.
    fn ddl_ok(command_tag: &str) -> QueryOutcome {
        QueryOutcome::Result(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: Some(command_tag.to_string()),
        })
    }

    /// Apply projection DDL, materializing immediately on `define` so the
    /// projection is queryable as soon as the statement returns, and enrolling
    /// it with the refresher so it tracks the database from then on.
    fn apply_ddl(&self, statement: ProjectionStatement, database_name: &str) -> QueryOutcome {
        let mut databases = self.databases.lock().unwrap();
        let entry = databases.entry(database_name.to_string()).or_insert_with(|| DatabaseCatalog::new(database_name));

        match statement {
            ProjectionStatement::Define(definition) => {
                let name = definition.name().to_string();
                if let Err(err) = entry.store.define(definition.clone()) {
                    return Self::error(err.to_string());
                }
                entry.catalog.register(definition);

                let detector = DatabaseChangeDetector::new(self.server_state.clone(), database_name);
                let watermark = match detector.current_watermark() {
                    Ok(watermark) => watermark,
                    Err(err) => {
                        let _ = entry.store.undefine(&name);
                        entry.catalog.deregister(&name);
                        return Self::error(err);
                    }
                };

                let materialized = {
                    let mut refresher = entry.refresher.lock().unwrap();
                    refresher.register(ProjectionRefreshConfig::new(&name, self.refresh_policy.clone()));
                    let outcome = refresher.force_refresh(&name, &self.executor, watermark);
                    if outcome.is_err() {
                        refresher.unregister(&name);
                    }
                    outcome
                };

                match materialized {
                    Ok(_) => Self::ddl_ok("DEFINE PROJECTION"),
                    Err(err) => {
                        let _ = entry.store.undefine(&name);
                        entry.catalog.deregister(&name);
                        Self::error(err.to_string())
                    }
                }
            }
            ProjectionStatement::Undefine { name } => {
                if let Err(err) = entry.store.undefine(&name) {
                    return Self::error(err.to_string());
                }
                entry.catalog.deregister(&name);
                entry.refresher.lock().unwrap().unregister(&name);
                Self::ddl_ok("UNDEFINE PROJECTION")
            }
        }
    }

    /// Resolve the catalog a session reads from.
    ///
    /// Rows are kept current by the background refresher, so a read is a plain
    /// lookup and never pays for re-materializing every projection.
    fn session_catalog(&self, session: &SessionContext) -> MaterializedCatalog {
        let databases = self.databases.lock().unwrap();
        match databases.get(&session.current_database) {
            Some(entry) => entry.catalog.clone(),
            None => MaterializedCatalog::new(),
        }
    }
}

impl QueryHandler for CatalogQueryHandler {
    fn handle_query(&self, sql: &str) -> QueryOutcome {
        self.handle_query_with_session(sql, &SessionContext::default())
    }

    fn handle_query_with_session(&self, sql: &str, session: &SessionContext) -> QueryOutcome {
        if let Ok(statement) = parse_projection_ddl(sql) {
            return self.apply_ddl(statement, &session.current_database);
        }
        execute_raw_query_with_session(&self.session_catalog(session), sql, session)
    }

    fn handle_query_batch_with_session(&self, sql: &str, session: &SessionContext) -> Vec<QueryOutcome> {
        if let Ok(statement) = parse_projection_ddl(sql) {
            return vec![self.apply_ddl(statement, &session.current_database)];
        }
        execute_raw_query_batch_with_session(&self.session_catalog(session), sql, session)
    }
}
