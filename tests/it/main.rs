//! Integration tests share one binary so the runtime links once; add new test files as modules here.

#[path = "../support/local_project.rs"]
mod local_project;
#[path = "../fixtures/sqlite.rs"]
mod sqlite;

mod actor_calls;
mod bucket_authority;
mod immutable_state;
mod local_contract;
mod reentrant;
mod runtime_environment;
mod runtime_logging;
mod runtime_shutdown;
