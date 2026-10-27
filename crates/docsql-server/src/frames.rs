use super::*;

/// MVCC stage B read path: classified read-only SELECTs run on a guardless
/// [`docsql_core::engine::ReadView`] — the database lock is held only for the
/// microseconds it takes to clone the catalog and take the pager snapshot,
/// then the statement executes with no lock held, so reads never block
/// writes (and vice versa). Statement deadline and audit apply identically
/// to the write path; the view owns its deadline, so concurrent readers
/// never clobber each other's arming.
/// Render an engine outcome as the client-facing response frame: rows as a
/// JSON object, affected counts as a LE u64, errors as RESP_ERROR. Shared by
/// the write path and the guardless read tier so the wire shape cannot drift
/// between them.
/// The interpreter's executor: one statement through the full client
/// pipeline (authorization, query log, replication fan-out, deadline) —
/// interpreted batches behave exactly like hand-split statements.
pub(super) struct BatchPipeExec<'a> {
    pub(super) state: &'a Arc<ServerState>,
    pub(super) user: Option<&'a UserAuth>,
    pub(super) conn: Option<u64>,
    pub(super) deadline: Option<std::time::Instant>,
    /// Audit attribution for every interpreted statement — without it the
    /// whole batch (writes included) bypassed the query log while plain
    /// statements were recorded.
    pub(super) peer: String,
    /// The last statement's AUTOINCREMENT id, snapshotted inside the engine
    /// write lock by the executor pipeline. Never re-read the engine-global
    /// counter here: another connection's INSERT between this statement's
    /// unlock and a late read would leak its id into this session.
    pub(super) stmt_identity: Option<docsql_core::Value>,
}

impl docsql_core::tsql_batch::BatchExecutor for BatchPipeExec<'_> {
    fn last_identity(&mut self) -> Option<docsql_core::Value> {
        self.stmt_identity.clone()
    }

    fn execute(&mut self, sql: &str) -> docsql_core::tsql_batch::ExecFuture<'_> {
        let state = self.state;
        let user = self.user;
        let conn = self.conn;
        let deadline = self.deadline;
        let peer = self.peer.clone();
        let sql = sql.to_string();
        Box::pin(async move {
            let started = std::time::Instant::now();
            // The pubsub-view rewrite only covers single statements — batch
            // statements land here directly, so the same rewrite (with the
            // same user-table guard) must run per interpreted statement or
            // `SELECT ... FROM docsql_pubsub` inside a batch fails with
            // "table does not exist".
            let user_table_wins = {
                state
                    .db
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .table_exists_ci("docsql_pubsub")
            };
            let (effective, allow_system) = if user_table_wins {
                (sql.clone(), false)
            } else {
                match pubsub::try_rewrite_pubsub_view(&sql) {
                    Some(rewritten) => (rewritten, true),
                    None => (sql.clone(), false),
                }
            };
            let mut exec_logged_sql: Option<String> = None;
            let (frame, identity) = execute_sql_with_identity(
                state,
                &effective,
                allow_system,
                false, // never the replication path
                conn,
                false,
                None,
                user,
                deadline,
                &mut exec_logged_sql,
            )
            .await;
            self.stmt_identity = identity.map(docsql_core::Value::Int);
            querylog::record(
                state,
                &peer,
                exec_logged_sql.as_deref().unwrap_or(&sql),
                started.elapsed().as_secs_f64() * 1000.0,
                &frame,
                false,
            );
            frame_to_exec(frame)
        })
    }
}

/// Decode a response frame back into the interpreter's result shape.
pub(super) fn frame_to_exec(
    frame: Frame,
) -> docsql_core::engine::Result<docsql_core::tsql_batch::ExecResult> {
    use docsql_core::tsql_batch::ExecResult;
    if frame.frame_type == proto::RESP_ERROR {
        return Err(docsql_core::engine::SqlError::Message(
            String::from_utf8_lossy(&frame.payload).to_string(),
        ));
    }
    if frame.frame_type == proto::RESP_AFFECTED {
        let mut bytes = [0u8; 8];
        let n = frame.payload.len().min(8);
        bytes[..n].copy_from_slice(&frame.payload[..n]);
        return Ok(ExecResult::Affected(u64::from_le_bytes(bytes)));
    }
    // RESP_ROWS: {"columns": [...], "rows": [[...]]} — the wire JSON is
    // the engine's own value serialization, so it round-trips exactly
    // ($dec/$ts/$bytes markers included).
    let payload = String::from_utf8_lossy(&frame.payload).to_string();
    match docsql_core::json::from_str(&payload) {
        Ok(docsql_core::Value::Object(obj)) => {
            let columns: Vec<String> = match obj.get("columns") {
                Some(docsql_core::Value::Array(cols)) => cols
                    .iter()
                    .map(|c| match c {
                        docsql_core::Value::Str(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .collect(),
                _ => Vec::new(),
            };
            let rows: Vec<Vec<docsql_core::Value>> = match obj.get("rows") {
                Some(docsql_core::Value::Array(rows)) => rows
                    .iter()
                    .map(|r| match r {
                        docsql_core::Value::Array(vals) => vals.clone(),
                        other => vec![other.clone()],
                    })
                    .collect(),
                _ => Vec::new(),
            };
            Ok(ExecResult::Rows(docsql_core::engine::QueryResult {
                columns,
                rows,
            }))
        }
        _ => Err(docsql_core::engine::SqlError::Message(
            "interpreter: malformed rows payload".into(),
        )),
    }
}

/// The batch's reply frame: the LAST statement's outcome, rendered exactly
/// like a single-statement response.
pub(super) fn exec_result_frame(out: Option<docsql_core::tsql_batch::ExecResult>) -> Frame {
    match out {
        Some(docsql_core::tsql_batch::ExecResult::Affected(n)) => {
            Frame::new(proto::RESP_AFFECTED, n.to_le_bytes().to_vec())
        }
        Some(docsql_core::tsql_batch::ExecResult::Rows(r)) => {
            outcome_frame(Ok::<ExecOutcome, std::convert::Infallible>(
                ExecOutcome::Rows(r),
            ))
        }
        // A batch of only control flow/assignments affected nothing.
        None => Frame::new(proto::RESP_AFFECTED, 0u64.to_le_bytes().to_vec()),
    }
}

/// Rows affected/returned by a response frame, for @@ROWCOUNT bookkeeping
/// on the fast paths (RESP_AFFECTED carries a LE u64; RESP_ROWS carries the
/// JSON result whose `rows` array length is the count; anything else is 0).
pub(super) fn resp_row_count(f: &Frame) -> u64 {
    if f.frame_type == proto::RESP_AFFECTED {
        let mut bytes = [0u8; 8];
        let n = f.payload.len().min(8);
        bytes[..n].copy_from_slice(&f.payload[..n]);
        return u64::from_le_bytes(bytes);
    }
    if f.frame_type == proto::RESP_ROWS {
        if let Ok(docsql_core::Value::Object(obj)) =
            docsql_core::json::from_str(&String::from_utf8_lossy(&f.payload))
        {
            if let Some(docsql_core::Value::Array(rows)) = obj.get("rows") {
                return rows.len() as u64;
            }
        }
    }
    0
}

pub(super) fn outcome_frame<E: std::fmt::Display>(
    outcome: std::result::Result<ExecOutcome, E>,
) -> Frame {
    match outcome {
        Ok(ExecOutcome::Rows(r)) => {
            let mut obj = docsql_core::value::Object::new();
            obj.insert(
                "columns".into(),
                Value::Array(r.columns.into_iter().map(Value::Str).collect()),
            );
            obj.insert(
                "rows".into(),
                Value::Array(r.rows.into_iter().map(Value::Array).collect()),
            );
            let payload = docsql_core::json::to_string(&Value::Object(obj)).into_bytes();
            // The protocol and every client cap a frame at 64 MiB. Serializing
            // past it used to emit a frame the client rejects as malformed —
            // the connection then sat with an unread payload. Refuse with an
            // actionable error instead of a protocol failure.
            if payload.len() > 64 * 1024 * 1024 {
                return Frame::new(
                    proto::RESP_ERROR,
                    err_payload(
                        "result set too large for one response (>64 MiB); \
                         add LIMIT/OFFSET or select fewer columns",
                    ),
                );
            }
            Frame::new(proto::RESP_ROWS, payload)
        }
        Ok(ExecOutcome::Affected(n)) => Frame::new(proto::RESP_AFFECTED, n.to_le_bytes().to_vec()),
        Err(e) => Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
    }
}
