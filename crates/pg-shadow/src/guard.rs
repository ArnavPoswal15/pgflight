//! `pg_query` AST guard: SELECT-only statements for shadow EXPLAIN/replay (ADR-003).

use pg_query::protobuf::{CommonTableExpr, SelectStmt};
use pg_query::{parse, NodeEnum};
use thiserror::Error;

/// Blocked function names (case-insensitive). Prefix entries end with `*`.
const DENYLIST: &[&str] = &[
    "nextval",
    "setval",
    "pg_sleep",
    "pg_advisory_lock*",
    "pg_terminate_backend",
    "dblink*",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GuardError {
    #[error("parse failed: {0}")]
    Parse(String),
    #[error("only a single statement is allowed")]
    MultiStatement,
    #[error("statement is not SELECT-only")]
    NotSelect,
    #[error("SELECT INTO is not allowed on shadow")]
    SelectInto,
    #[error("locking clause (e.g. FOR UPDATE) is not allowed on shadow")]
    LockingClause,
    #[error("writable CTE is not allowed on shadow")]
    WritableCte,
    #[error("denylisted function: {0}")]
    DenylistedFunction(String),
}

/// Validate that `sql` is safe to run on the shadow DB (SELECT-only, ADR-003).
pub fn assert_select_only(sql: &str) -> Result<(), GuardError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(GuardError::NotSelect);
    }
    let parsed = parse(trimmed).map_err(|e| GuardError::Parse(e.to_string()))?;
    if parsed.protobuf.stmts.len() != 1 {
        return Err(GuardError::MultiStatement);
    }
    for (name, _) in &parsed.functions {
        if function_denied(name) {
            return Err(GuardError::DenylistedFunction(name.clone()));
        }
    }
    let raw = parsed
        .protobuf
        .stmts
        .first()
        .and_then(|r| r.stmt.as_ref())
        .ok_or(GuardError::NotSelect)?;
    let node = raw.node.as_ref().ok_or(GuardError::NotSelect)?;
    match node {
        NodeEnum::SelectStmt(stmt) => validate_select(stmt),
        NodeEnum::ExplainStmt(explain) => {
            let inner = explain
                .query
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or(GuardError::NotSelect)?;
            match inner {
                NodeEnum::SelectStmt(stmt) => validate_select(stmt),
                _ => Err(GuardError::NotSelect),
            }
        }
        _ => Err(GuardError::NotSelect),
    }
}

fn validate_select(stmt: &SelectStmt) -> Result<(), GuardError> {
    if stmt.into_clause.is_some() {
        return Err(GuardError::SelectInto);
    }
    if !stmt.locking_clause.is_empty() {
        return Err(GuardError::LockingClause);
    }
    if let Some(with) = &stmt.with_clause {
        for cte_node in &with.ctes {
            if let Some(NodeEnum::CommonTableExpr(cte)) = cte_node.node.as_ref() {
                validate_cte(cte)?;
            }
        }
    }
    if let Some(larg) = stmt.larg.as_deref() {
        validate_select(larg)?;
    }
    if let Some(rarg) = stmt.rarg.as_deref() {
        validate_select(rarg)?;
    }
    Ok(())
}

fn validate_cte(cte: &CommonTableExpr) -> Result<(), GuardError> {
    let q = cte
        .ctequery
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or(GuardError::WritableCte)?;
    match q {
        NodeEnum::SelectStmt(stmt) => validate_select(stmt),
        _ => Err(GuardError::WritableCte),
    }
}

fn function_denied(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    DENYLIST.iter().any(|entry| {
        if let Some(prefix) = entry.strip_suffix('*') {
            lower.starts_with(prefix)
        } else {
            lower == *entry
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_simple_select() {
        assert_select_only("SELECT 1").unwrap();
        assert_select_only("SELECT * FROM users WHERE id = $1").unwrap();
    }

    #[test]
    fn rejects_dml() {
        assert_eq!(
            assert_select_only("INSERT INTO users VALUES (1)"),
            Err(GuardError::NotSelect)
        );
        assert_eq!(
            assert_select_only("UPDATE users SET id = 1"),
            Err(GuardError::NotSelect)
        );
    }

    #[test]
    fn rejects_select_into() {
        assert_eq!(
            assert_select_only("SELECT * INTO t FROM users"),
            Err(GuardError::SelectInto)
        );
    }

    #[test]
    fn rejects_for_update() {
        assert_eq!(
            assert_select_only("SELECT * FROM users FOR UPDATE"),
            Err(GuardError::LockingClause)
        );
    }

    #[test]
    fn rejects_denylisted_function() {
        assert_eq!(
            assert_select_only("SELECT nextval('users_id_seq')"),
            Err(GuardError::DenylistedFunction("nextval".into()))
        );
        assert_eq!(
            assert_select_only("SELECT pg_sleep(1)"),
            Err(GuardError::DenylistedFunction("pg_sleep".into()))
        );
    }

    #[test]
    fn rejects_writable_cte() {
        assert_eq!(
            assert_select_only(
                "WITH ins AS (INSERT INTO users(id) VALUES (1) RETURNING *) SELECT * FROM ins"
            ),
            Err(GuardError::WritableCte)
        );
    }

    #[test]
    fn rejects_multi_statement() {
        assert_eq!(
            assert_select_only("SELECT 1; SELECT 2"),
            Err(GuardError::MultiStatement)
        );
    }
}
