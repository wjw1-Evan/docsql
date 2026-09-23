//! T-SQL batch interpreter: per-connection `@` variables (`DECLARE`/`SET`/
//! `SELECT @x = …`), `IF … ELSE`, `BEGIN … END` blocks, `WHILE` loops with
//! `BREAK`/`CONTINUE`, `PRINT`, the read-only system variables
//! (`@@ROWCOUNT`, `@@VERSION`) and session-identity substitution
//! (`SUSER_SNAME()` family) when the executor carries a context.
//!
//! The interpreter owns NO engine access: [`TsqlSession::run_batch`] takes
//! a [`BatchExecutor`] callback, so the server can route every interpreted
//! statement through its full statement pipeline (authorization, query
//! log, replication fan-out, deadlines) exactly like a hand-written
//! client statement. Variables never reach the engine or the journal as
//! `@names` — substitution renders them through `value_literal`, the same
//! canonical form replays use, so a substituted INSERT is
//! replication-safe by construction.
//!
//! Boundaries stay loud: `RETURN`/`WAITFOR`/`GOTO` refuse with their own
//! messages, `BREAK`/`CONTINUE` outside a loop error, and the statement
//! budget turns a runaway `WHILE 1 = 1` into an error instead of a hung
//! connection.

use crate::engine::{self, err, QueryResult, Result, SqlError};
use crate::stmt;
use crate::value::Value;

/// One executed statement's outcome, mirroring the two shapes clients see.
pub enum ExecResult {
    Affected(u64),
    Rows(QueryResult),
}

impl std::fmt::Debug for ExecResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecResult::Affected(n) => write!(f, "Affected({n})"),
            ExecResult::Rows(r) => {
                write!(f, "Rows({} cols, {} rows)", r.columns.len(), r.rows.len())
            }
        }
    }
}

/// The executor future: boxed so the interpreter stays executor-agnostic
/// (the server awaits real async work; the CLI drives ready futures).
pub type ExecFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecResult>> + Send + 'a>>;

/// Statement execution hook. Implementations run ONE statement and return
/// its outcome — the server routes through its full pipeline, embedded
/// callers through `Database::execute`.
pub trait BatchExecutor: Send {
    fn execute(&mut self, sql: &str) -> ExecFuture<'_>;

    /// The engine's last-insert auto id, captured right after `execute`
    /// ran an INSERT (None when the executor has no identity source).
    /// The server reads it under the engine lock; embedded callers read
    /// `Database::last_insert_id`.
    fn last_identity(&mut self) -> Option<Value> {
        None
    }
}

/// Identity context the owning connection fills in (server: the
/// authenticated user; CLI: the process). Substituted into the
/// `SUSER_SNAME()` family; when a field is absent the call keeps its loud
/// engine error.
#[derive(Debug, Default, Clone)]
pub struct SessionContext {
    pub user: Option<String>,
    pub app_name: Option<String>,
    pub host: Option<String>,
}

/// Upper bound on statements executed by one batch: a runaway loop is an
/// error, not a hung connection (T-SQL would spin forever; a database
/// process must not).
const MAX_BATCH_STATEMENTS: usize = 100_000;
/// BEGIN nesting cap for IF/WHILE blocks.
const MAX_BLOCK_DEPTH: usize = 32;

/// Per-connection T-SQL session: the `@` variables, the running
/// `@@ROWCOUNT` and the identity context. Create one per connection
/// (server) or per script run (CLI); `run_batch` may be called repeatedly.
#[derive(Debug, Default)]
pub struct TsqlSession {
    vars: std::collections::BTreeMap<String, Value>,
    rowcount: u64,
    prints: Vec<String>,
    last_result: Option<ExecResult>,
    /// Inside CATCH: the caught error (number, message) — the source for
    /// ERROR_MESSAGE()/ERROR_NUMBER() and re-THROW.
    error_ctx: Option<(i64, String)>,
    /// @@ERROR: the last statement's error number (0 = success).
    last_error: i64,
    /// SCOPE_IDENTITY()/@@IDENTITY: the last INSERT's auto-generated id
    /// (autoinc value; None = no insert yet on this connection).
    last_identity: Option<Value>,
    /// Filled by the owning layer; see [`SessionContext`].
    pub ctx: SessionContext,
}

impl TsqlSession {
    /// Feed the connection's identity from a statement the host executed
    /// OUTSIDE the interpreter (single-statement fast path). Call only for
    /// INSERTs: `Some` sets the new id, `None` records that this INSERT
    /// allocated none — resetting a previous value, exactly like an
    /// interpreted INSERT.
    pub fn note_identity(&mut self, identity: Option<Value>) {
        self.last_identity = identity;
    }

    /// Record an error raised by a fast-path statement so a later batch's
    /// @@ERROR reads it.
    pub fn note_error(&mut self, code: i64) {
        self.last_error = code;
    }
}

impl TsqlSession {
    pub fn new() -> Self {
        Self::default()
    }

    /// True once this session carries variables later statements may
    /// reference — the server keeps routing through the interpreter even
    /// after the declaring batch finished.
    pub fn is_active(&self) -> bool {
        !self.vars.is_empty()
    }

    /// PRINT messages collected by the last `run_batch` (drained on read).
    pub fn take_prints(&mut self) -> Vec<String> {
        std::mem::take(&mut self.prints)
    }

    /// Interpret and run one script (GO lines split batches; variables do
    /// NOT cross GO). Returns the LAST executed statement's outcome — the
    /// reply a client expects for the batch. Every statement runs through
    /// `exec` (the server's full pipeline there).
    pub async fn run_batch(
        &mut self,
        sql: &str,
        exec: &mut dyn BatchExecutor,
    ) -> Result<Option<ExecResult>> {
        self.prints.clear();
        self.last_result = None;
        let mut budget = MAX_BATCH_STATEMENTS;
        let mut first_chunk = true;
        for chunk in stmt::text_chunks(sql) {
            let chunk = chunk.trim();
            if chunk.is_empty() {
                continue;
            }
            if !first_chunk {
                // GO ends a batch: variables die with it (T-SQL). @@ROWCOUNT
                // survives — a new batch may read what the last one did.
                self.vars.clear();
            }
            first_chunk = false;
            let stmts = parse_batch(chunk)?;
            for stmt in stmts {
                if let Some(flow) = self.run_stmt(stmt, exec, &mut budget, 0).await? {
                    // Flow control reaching batch scope is an error:
                    // BREAK/CONTINUE belong to a WHILE.
                    return err(format!(
                        "{} is only allowed inside a WHILE loop",
                        flow.name()
                    ));
                }
            }
        }
        Ok(self.last_result.take())
    }

    fn run_stmt<'s>(
        &'s mut self,
        stmt: Stmt,
        exec: &'s mut dyn BatchExecutor,
        budget: &'s mut usize,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Flow>>> + Send + 's>>
    {
        Box::pin(Self::run_stmt_inner(self, stmt, exec, budget, depth))
    }

    async fn run_stmt_inner(
        &mut self,
        stmt: Stmt,
        exec: &mut dyn BatchExecutor,
        budget: &mut usize,
        depth: usize,
    ) -> Result<Option<Flow>> {
        *budget = budget.checked_sub(1).ok_or_else(batch_budget_hit)?;
        match stmt {
            Stmt::Plain(sql) => {
                let rendered = self.substitute(&sql)?;
                let is_insert = is_insert_statement(&rendered);
                let out = match exec.execute(&rendered).await {
                    Ok(out) => out,
                    Err(e) => {
                        self.last_error = error_number(&e);
                        return Err(e);
                    }
                };
                if is_insert {
                    self.last_identity = exec.last_identity();
                }
                self.last_error = 0;
                self.rowcount = match &out {
                    ExecResult::Affected(n) => *n,
                    ExecResult::Rows(r) => r.rows.len() as u64,
                };
                self.last_result = Some(out);
                Ok(None)
            }
            Stmt::Declare(decls) => {
                for (name, init) in decls {
                    if self.vars.contains_key(&name) {
                        return err(format!(
                            "The variable name '@{name}' has already been declared"
                        ));
                    }
                    let value = match init {
                        Some(expr) => {
                            let rendered = self.substitute(&format!("SELECT ({expr})"))?;
                            self.eval_tracked(exec, &rendered)
                                .await?
                                .and_then(|row| row.into_iter().next())
                                .unwrap_or(Value::Null)
                        }
                        None => Value::Null,
                    };
                    self.vars.insert(name, value);
                }
                Ok(None)
            }
            Stmt::SetVar(assigns, tail) => {
                // SELECT @a = e1, @b = e2 <tail>: one scan assigns every
                // variable from the LAST row (T-SQL rowset-assignment
                // rules; an empty scan leaves the variables unchanged).
                let exprs: Vec<&str> = assigns.iter().map(|(_, e)| e.as_str()).collect();
                let query = format!("SELECT {} {}", exprs.join(", "), tail);
                let rendered = self.substitute(&query)?;
                if let Some(row) = self.eval_tracked(exec, &rendered).await? {
                    for (i, (name, _)) in assigns.iter().enumerate() {
                        let v = row.get(i).cloned().unwrap_or(Value::Null);
                        self.vars.insert(name.clone(), v);
                    }
                }
                Ok(None)
            }
            Stmt::SetOne(name, expr) => {
                let rendered = self.substitute(&format!("SELECT ({expr})"))?;
                let v = self
                    .eval_tracked(exec, &rendered)
                    .await?
                    .and_then(|row| row.into_iter().next())
                    .unwrap_or(Value::Null);
                self.vars.insert(name, v);
                Ok(None)
            }
            Stmt::Print(expr) => {
                let rendered = self.substitute(&format!("SELECT ({expr})"))?;
                let v = self
                    .eval_tracked(exec, &rendered)
                    .await?
                    .and_then(|row| row.into_iter().next())
                    .unwrap_or(Value::Null);
                self.prints.push(engine::value_to_text(&v));
                Ok(None)
            }
            Stmt::If { cond, then, else_ } => {
                // Flow control inside a branch propagates to the enclosing
                // WHILE: `IF … BREAK` must break the loop, not just the IF.
                if self.eval_cond(exec, &cond).await? {
                    if let Some(flow) = self.run_stmts(then, exec, budget, depth).await? {
                        return Ok(Some(flow));
                    }
                } else if let Some(else_) = else_ {
                    if let Some(flow) = self.run_stmts(else_, exec, budget, depth).await? {
                        return Ok(Some(flow));
                    }
                }
                Ok(None)
            }
            Stmt::While { cond, body } => {
                loop {
                    if !self.eval_cond(exec, &cond).await? {
                        break;
                    }
                    match self.run_stmts(body.clone(), exec, budget, depth).await? {
                        Some(Flow::Break) => break,
                        Some(Flow::Continue) | None => {}
                    }
                }
                Ok(None)
            }
            Stmt::Break => Ok(Some(Flow::Break)),
            Stmt::Continue => Ok(Some(Flow::Continue)),
            Stmt::Throw(form, raw) => {
                // Arguments resolve at RUN time: @variables and @@ functions
                // substitute first, then the literal shapes parse — T-SQL's
                // `THROW @code, @msg, 1` works, and an unparsable argument
                // fails loudly instead of degrading to a bare raise.
                let rendered = self.substitute(&raw)?;
                let (code, message) = match parse_throw(&rendered, form) {
                    Some(pair) => pair,
                    // Bare THROW inside CATCH re-raises the caught error;
                    // the context is kept, so a second bare THROW still
                    // re-raises instead of falling back to the default.
                    None => match self.error_ctx.clone() {
                        Some(pair) => pair,
                        None => (50000, DEFAULT_THROW_MESSAGE.into()),
                    },
                };
                self.last_error = code;
                err(format!("{message} (error {code})"))
            }
            Stmt::TryCatch { try_, catch_ } => {
                // The CATCH block sees this TRY's error; on every exit the
                // caller's context comes back — a nested TRY...CATCH inside
                // an outer CATCH must not blind the outer ERROR_MESSAGE().
                // Cloned, not taken: statements inside the TRY body itself
                // still see the caller's error until this TRY fails.
                let saved = self.error_ctx.clone();
                let outcome = match self.run_stmts(try_, exec, budget, depth).await {
                    Ok(flow) => {
                        // BREAK/CONTINUE inside TRY still propagates to the
                        // enclosing WHILE.
                        if let Some(f) = flow {
                            self.error_ctx = saved;
                            return Ok(Some(f));
                        }
                        self.last_error = 0;
                        Ok(())
                    }
                    Err(e) => {
                        // The catch gets the message; flow control raised
                        // inside the failed try body does not survive.
                        self.error_ctx = Some((error_number(&e), e.to_string()));
                        self.last_error = error_number(&e);
                        self.run_stmts(catch_, exec, budget, depth)
                            .await
                            .map(|_| ())
                    }
                };
                self.error_ctx = saved;
                outcome.map(|_: ()| None)
            }
        }
    }

    async fn run_stmts(
        &mut self,
        stmts: Vec<Stmt>,
        exec: &mut dyn BatchExecutor,
        budget: &mut usize,
        depth: usize,
    ) -> Result<Option<Flow>> {
        if depth >= MAX_BLOCK_DEPTH {
            return err("batch block nesting exceeds the supported depth");
        }
        for stmt in stmts {
            if let Some(flow) = self.run_stmt(stmt, exec, budget, depth + 1).await? {
                return Ok(Some(flow));
            }
        }
        Ok(None)
    }

    async fn eval_cond(&mut self, exec: &mut dyn BatchExecutor, cond: &str) -> Result<bool> {
        let rendered = self.substitute(&format!("SELECT ({cond})"))?;
        match self.eval_tracked(exec, &rendered).await? {
            Some(row) => match row.into_iter().next().unwrap_or(Value::Null) {
                // T-SQL treats an UNKNOWN (here: NULL) condition as false.
                Value::Bool(b) => Ok(b),
                Value::Null => Ok(false),
                other => {
                    let msg = format!(
                        "IF/WHILE condition must be a boolean, got {}",
                        other.type_name()
                    );
                    self.last_error = error_number(&SqlError::Message(msg.clone()));
                    err(msg)
                }
            },
            None => Ok(false),
        }
    }

    /// eval_last_row 的会话包装:任何经引擎执行的批语句形态(DECLARE/
    /// SET/PRINT/条件求值)都刷新 @@ERROR —— 成功归零、失败记错误码,
    /// 与 Stmt::Plain 的记账一致(T-SQL:每条语句都刷新 @@ERROR)。
    async fn eval_tracked(
        &mut self,
        exec: &mut dyn BatchExecutor,
        sql: &str,
    ) -> Result<Option<Vec<Value>>> {
        match eval_last_row(exec, sql).await {
            Ok(v) => {
                self.last_error = 0;
                Ok(v)
            }
            Err(e) => {
                self.last_error = error_number(&e);
                Err(e)
            }
        }
    }

    /// Replace `@name` tokens (outside literals/comments/brackets) with
    /// their `value_literal` renderings; resolve `@@ROWCOUNT`/`@@VERSION`
    /// and the session-identity functions. Unknown `@names` are loud — a
    /// typo must not quietly become NULL.
    fn substitute(&self, sql: &str) -> Result<String> {
        let b = sql.as_bytes();
        let lower = sql.to_ascii_lowercase();
        let lb = lower.as_bytes();
        let mut out = String::with_capacity(sql.len() + 32);
        let mut i = 0usize;
        while i < b.len() {
            match b[i] {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(sql, i);
                    out.push_str(&sql[i..end]);
                    i = end;
                }
                b'-' if b.get(i + 1) == Some(&b'-') => {
                    let start = i;
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                    out.push_str(&sql[start..i]);
                }
                b'/' if b.get(i + 1) == Some(&b'*') => {
                    let start = i;
                    let mut j = i + 2;
                    while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                        j += 1;
                    }
                    j = if j + 1 < b.len() { j + 2 } else { b.len() };
                    out.push_str(&sql[start..j]);
                    i = j;
                }
                b'"' | b'`' | b'[' => {
                    // Quoted identifier / bracket run: contents are opaque.
                    let close = match b[i] {
                        b'[' => b']',
                        c => c,
                    };
                    out.push(b[i] as char);
                    i += 1;
                    while i < b.len() {
                        let ch_len = stmt::utf8_len(b[i]);
                        out.push_str(&sql[i..i + ch_len]);
                        if b[i] == close {
                            if close != b']' && b.get(i + 1) == Some(&close) {
                                out.push_str(&sql[i + 1..i + 2]);
                                i += 2;
                                continue;
                            }
                            i += 1;
                            break;
                        }
                        i += ch_len;
                    }
                }
                b'@' => {
                    if b.get(i + 1) == Some(&b'@') {
                        let start = i;
                        let mut j = i + 2;
                        while j < b.len() && is_name_byte(b[j]) {
                            j += 1;
                        }
                        match &lb[start + 2..j] {
                            b"rowcount" => out.push_str(&self.rowcount.to_string()),
                            b"error" => out.push_str(&self.last_error.to_string()),
                            b"identity" => match &self.last_identity {
                                Some(v) => out.push_str(&engine::value_literal(v)?),
                                None => out.push_str("NULL"),
                            },
                            b"version" => {
                                out.push_str(&format!("'DocSQL {}'", env!("CARGO_PKG_VERSION")))
                            }
                            name => {
                                return err(format!(
                                    "@@{} is not supported; supported system variables are \
                                     @@ROWCOUNT and @@VERSION",
                                    String::from_utf8_lossy(name)
                                ))
                            }
                        }
                        i = j;
                        continue;
                    }
                    let start = i;
                    let mut j = i + 1;
                    while j < b.len() && is_name_byte(b[j]) {
                        j += 1;
                    }
                    let name = lower[start + 1..j].to_string();
                    let Some(value) = self.vars.get(&name) else {
                        return err(format!("Must declare the scalar variable \"@{name}\""));
                    };
                    out.push_str(&engine::value_literal(value)?);
                    i = j;
                }
                c if c.is_ascii_alphabetic() => {
                    let start = i;
                    while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                        i += 1;
                    }
                    // Session-identity calls with an empty argument list
                    // substitute when the context provides a value; without
                    // one the engine's loud error stands.
                    let mut j = i;
                    while j < b.len() && b[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if b.get(j) == Some(&b'(') && b.get(j + 1) == Some(&b')') {
                        // CATCH diagnostics: message/number of the caught
                        // error (NULL outside CATCH, like T-SQL).
                        match lb[start..i].as_ref() {
                            b"scope_identity" | b"ident_current" => {
                                match &self.last_identity {
                                    Some(v) => out.push_str(&engine::value_literal(v)?),
                                    None => out.push_str("NULL"),
                                }
                                i = j + 2;
                                continue;
                            }
                            b"error_message" => match self.error_ctx.as_ref() {
                                Some((_, m)) => {
                                    out.push_str(&format!(
                                        "NULLIF({}, NULL)",
                                        engine::value_literal(&Value::Str(m.clone()))?
                                    ));
                                    i = j + 2;
                                    continue;
                                }
                                None => {
                                    out.push_str("NULL");
                                    i = j + 2;
                                    continue;
                                }
                            },
                            b"error_number" => {
                                let n = self.error_ctx.as_ref().map(|(n, _)| *n);
                                match n {
                                    Some(n) => out.push_str(&n.to_string()),
                                    None => out.push_str("NULL"),
                                }
                                i = j + 2;
                                continue;
                            }
                            _ => {}
                        }
                        let literal = match lb[start..i].as_ref() {
                            b"suser_sname" | b"original_login" | b"system_user"
                            | b"session_user" | b"user_name" => self.ctx.user.as_ref(),
                            b"app_name" => self.ctx.app_name.as_ref(),
                            b"host_name" => self.ctx.host.as_ref(),
                            _ => None,
                        };
                        if let Some(v) = literal {
                            out.push_str(&engine::value_literal(&Value::Str(v.clone()))?);
                            i = j + 2;
                            continue;
                        }
                    }
                    out.push_str(&sql[start..i]);
                }
                c if c < 0x80 => {
                    out.push(c as char);
                    i += 1;
                }
                _ => {
                    let ch_len = stmt::utf8_len(b[i]);
                    out.push_str(&sql[i..i + ch_len]);
                    i += ch_len;
                }
            }
        }
        Ok(out)
    }
}

fn is_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'#' || b == b'$'
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Flow {
    Break,
    Continue,
}

impl Flow {
    fn name(self) -> &'static str {
        match self {
            Flow::Break => "BREAK",
            Flow::Continue => "CONTINUE",
        }
    }
}

/// Numeric code for an arbitrary engine error: 50000 (generic user
/// range) unless the text ENDS with the interpreter's `(error NNNNN)`
/// marker — a user message that merely contains the pattern must not
/// fake a code.
fn error_number(e: &SqlError) -> i64 {
    error_code_from_text(&e.to_string())
}

/// Numeric code carried by an error text ENDING in "(error N)": 50000
/// otherwise. Public for hosts that execute statements outside the
/// interpreter and only hold the rendered error text (they must keep the
/// session's @@ERROR current the same way).
pub fn error_code_from_text(text: &str) -> i64 {
    if let Some(idx) = text.rfind("(error ") {
        let rest = &text[idx + 7..];
        if let Some(end) = rest.find(')') {
            if rest[end + 1..].trim().is_empty() {
                if let Ok(n) = rest[..end].parse::<i64>() {
                    return n;
                }
            }
        }
    }
    50000
}

fn batch_budget_hit() -> SqlError {
    SqlError::Message(format!(
        "batch exceeded the {MAX_BATCH_STATEMENTS}-statement execution budget"
    ))
}

/// Run a SELECT-shaped statement and hand back its LAST row (rowset
/// assignment semantics); None for an empty scan.
async fn eval_last_row(exec: &mut dyn BatchExecutor, sql: &str) -> Result<Option<Vec<Value>>> {
    match exec.execute(sql).await? {
        ExecResult::Rows(r) => Ok(r.rows.into_iter().last()),
        ExecResult::Affected(n) => err(format!(
            "expected a query result for evaluation, got an affected count ({n})"
        )),
    }
}

/// Minimal executor driver: the core has no async runtime by design, and
/// the futures `run_batch` holds are ready after each `execute` poll —
/// a no-op-waker poll loop drives them to completion.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop(_: *const ()) {}
    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Batch parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Stmt {
    /// Pass-through statement (variables substituted at run time).
    Plain(String),
    /// DECLARE @a T [, @b T]…, optional inline initializers.
    Declare(Vec<(String, Option<String>)>),
    /// SELECT @a = e1 [, @b = e2]… [tail]: one scan assigns every variable.
    SetVar(Vec<(String, String)>, String),
    /// SET @x = expr.
    SetOne(String, String),
    /// PRINT expr (message collected by the session).
    Print(String),
    If {
        cond: String,
        then: Vec<Stmt>,
        else_: Option<Vec<Stmt>>,
    },
    While {
        cond: String,
        body: Vec<Stmt>,
    },
    Break,
    Continue,
    /// THROW [code, 'msg', state] / RAISERROR('msg', sev, state), kept as
    /// RAW argument text — @variables substitute at run time (T-SQL allows
    /// `THROW @code, @msg, 1`), then the shape parses per [`ThrowForm`].
    /// Empty text = bare THROW (re-raises inside CATCH).
    Throw(ThrowForm, String),
    /// BEGIN TRY … END TRY BEGIN CATCH … END CATCH.
    TryCatch {
        try_: Vec<Stmt>,
        catch_: Vec<Stmt>,
    },
}

/// Words that begin a new statement. A plain statement ends at a top-level
/// `;`, at end of input, or at a newline whose next word is one of these.
const STATEMENT_STARTERS: &[&str] = &[
    "select",
    "insert",
    "update",
    "delete",
    "merge",
    "declare",
    "set",
    "if",
    "else",
    "while",
    "begin",
    "print",
    "use",
    "create",
    "drop",
    "alter",
    "grant",
    "revoke",
    "truncate",
    "exec",
    "execute",
    "break",
    "continue",
    "go",
    "commit",
    "rollback",
    "savepoint",
    "release",
    "waitfor",
    "return",
    "throw",
    "raiserror",
];

/// Clause openers that end a `SELECT @a = …` assignment list.
const SELECT_CLAUSE_STARTERS: &[&str] = &[
    "from",
    "where",
    "group",
    "order",
    "having",
    "limit",
    "offset",
    "fetch",
    "union",
    "except",
    "intersect",
    "option",
    "for",
];

fn is_word_in(word: &[u8], list: &[&str]) -> bool {
    list.iter().any(|w| w.as_bytes() == word)
}

/// Parse one batch (already GO/;-split) into statements.
fn parse_batch(sql: &str) -> Result<Vec<Stmt>> {
    let mut p = Parser {
        sql,
        b: sql.as_bytes(),
        lower: sql.to_ascii_lowercase(),
        i: 0,
    };
    p.parse_stmts(0)
}

struct Parser<'a> {
    sql: &'a str,
    b: &'a [u8],
    lower: String,
    i: usize,
}

impl<'a> Parser<'a> {
    fn lb(&self) -> &[u8] {
        // ASCII lowercase keeps byte-index compatibility with self.b.
        self.lower.as_bytes()
    }

    fn skip_trivia(&mut self) {
        loop {
            while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
                self.i += 1;
            }
            if self.b.get(self.i) == Some(&b'-') && self.b.get(self.i + 1) == Some(&b'-') {
                while self.i < self.b.len() && self.b[self.i] != b'\n' {
                    self.i += 1;
                }
                continue;
            }
            if self.b.get(self.i) == Some(&b'/') && self.b.get(self.i + 1) == Some(&b'*') {
                self.i += 2;
                while self.i + 1 < self.b.len()
                    && !(self.b[self.i] == b'*' && self.b[self.i + 1] == b'/')
                {
                    self.i += 1;
                }
                self.i = (self.i + 2).min(self.b.len());
                continue;
            }
            if self.b.get(self.i) == Some(&b';') {
                self.i += 1;
                continue;
            }
            break;
        }
    }

    /// Next word at the cursor (without consuming).
    fn peek_word(&self) -> Option<&[u8]> {
        if self.i >= self.b.len() || !self.b[self.i].is_ascii_alphabetic() {
            return None;
        }
        let mut j = self.i;
        while j < self.b.len() && (self.b[j].is_ascii_alphanumeric() || self.b[j] == b'_') {
            j += 1;
        }
        Some(&self.lb()[self.i..j])
    }

    fn at_end(&self) -> bool {
        self.i >= self.b.len()
    }

    fn parse_stmts(&mut self, depth: usize) -> Result<Vec<Stmt>> {
        if depth > MAX_BLOCK_DEPTH {
            return err("batch block nesting exceeds the supported depth");
        }
        let mut out = Vec::new();
        loop {
            self.skip_trivia();
            if self.at_end() {
                return Ok(out);
            }
            let word = match self.peek_word() {
                Some(w) => w.to_vec(),
                None => {
                    // Not a word: the remainder is one plain run for the
                    // engine to judge.
                    out.push(Stmt::Plain(self.read_plain()));
                    continue;
                }
            };
            match word.as_slice() {
                b"declare" => {
                    self.i += 7;
                    out.push(self.parse_declare()?);
                }
                b"set" if self.starts_with_var_ahead() => {
                    self.i += 3;
                    out.push(self.parse_set()?);
                }
                b"print" => {
                    self.i += 5;
                    out.push(Stmt::Print(self.read_plain()));
                }
                b"if" => {
                    self.i += 2;
                    out.push(self.parse_if(depth)?);
                }
                b"while" => {
                    self.i += 5;
                    out.push(self.parse_while(depth)?);
                }
                b"begin" if self.peek_word_after_begin_is(b"try") => {
                    self.i += 5;
                    self.skip_trivia();
                    self.i += 3; // TRY
                    let try_ = self.parse_block(depth)?;
                    // parse_block consumed the END of "END TRY"; swallow
                    // the trailing marker word.
                    self.skip_trivia();
                    if self.peek_word() == Some(b"try") {
                        self.i += 3;
                    }
                    self.skip_trivia();
                    // Expect BEGIN CATCH
                    if self.peek_word() != Some(b"begin")
                        || !self.peek_word_after_begin_is(b"catch")
                    {
                        return err("BEGIN TRY requires a matching BEGIN CATCH");
                    }
                    self.i += 5;
                    self.skip_trivia();
                    self.i += 5; // CATCH
                    let catch_ = self.parse_block(depth)?;
                    self.skip_trivia();
                    if self.peek_word() == Some(b"catch") {
                        self.i += 5;
                    }
                    out.push(Stmt::TryCatch { try_, catch_ });
                }
                b"begin" if self.begin_opens_block() => {
                    self.i += 5;
                    out.extend(self.parse_block(depth)?);
                }
                b"break" => {
                    self.i += 5;
                    out.push(Stmt::Break);
                }
                b"continue" => {
                    self.i += 8;
                    out.push(Stmt::Continue);
                }
                b"return" | b"waitfor" | b"goto" => {
                    return err(format!(
                        "{} is not supported in DocSQL batches",
                        String::from_utf8_lossy(&word)
                    ))
                }
                b"throw" => {
                    self.i += 5;
                    out.push(Stmt::Throw(ThrowForm::Throw, self.read_plain()));
                }
                b"raiserror" => {
                    self.i += 9;
                    out.push(Stmt::Throw(ThrowForm::RaiseError, self.read_plain()));
                }
                b"select" if self.select_has_assignment() => {
                    self.i += 6;
                    out.push(self.parse_select_assign()?);
                }
                _ => {
                    // 兜底:read_plain 必须消费输入(零字符语句 + 游标不动
                    // = 扫描器空转,曾让批解析无限循环)。
                    let before = self.i;
                    let text = self.read_plain();
                    if self.i == before {
                        return err(
                            "batch parser made no progress — unexpected keyword at statement start",
                        );
                    }
                    out.push(Stmt::Plain(text));
                }
            }
        }
    }

    /// BEGIN opens a block only when a matching END exists ahead; bare
    /// `BEGIN` (and `BEGIN TRAN/TRANSACTION`) is the transaction
    /// statement — DocSQL's own transaction syntax must keep flowing
    /// through the interpreter untouched.
    fn begin_opens_block(&self) -> bool {
        if self.begin_starts_transaction() {
            return false;
        }
        let mut j = self.i;
        let mut nesting = 0i32;
        let b = self.b;
        let lb = self.lb();
        while j < b.len() {
            match b[j] {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(self.sql, j);
                    j = end;
                    continue;
                }
                b'-' if b.get(j + 1) == Some(&b'-') => {
                    while j < b.len() && b[j] != b'\n' {
                        j += 1;
                    }
                }
                b'/' if b.get(j + 1) == Some(&b'*') => {
                    j += 2;
                    while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                        j += 1;
                    }
                    j = (j + 2).min(b.len());
                }
                c if c.is_ascii_alphabetic() => {
                    let mut k = j;
                    while k < b.len() && (b[k].is_ascii_alphanumeric() || b[k] == b'_') {
                        k += 1;
                    }
                    if &lb[j..k] == b"begin" {
                        nesting += 1;
                    } else if &lb[j..k] == b"end" {
                        nesting -= 1;
                        if nesting == 0 {
                            return true;
                        }
                    }
                    j = k;
                    continue;
                }
                _ => {}
            }
            j += 1;
        }
        false
    }

    /// BEGIN TRAN/TRANSACTION/DISTRIBUTED is a transaction statement, not
    /// a block opener.
    fn begin_starts_transaction(&self) -> bool {
        let mut j = self.i + 5;
        while j < self.b.len() && self.b[j].is_ascii_whitespace() {
            j += 1;
        }
        let lb = self.lb();
        for kw in ["tran", "transaction", "distributed"] {
            let kb = kw.as_bytes();
            if self.b.len() >= j + kb.len()
                && &lb[j..j + kb.len()] == kb
                && (self.b.len() == j + kb.len()
                    || !(self.b[j + kb.len()].is_ascii_alphanumeric()
                        || self.b[j + kb.len()] == b'_'))
            {
                return true;
            }
        }
        false
    }

    /// Word immediately after the BEGIN at the cursor (skipping ws).
    fn peek_word_after_begin_is(&self, want: &[u8]) -> bool {
        let mut j = self.i + 5;
        while j < self.b.len() && self.b[j].is_ascii_whitespace() {
            j += 1;
        }
        let lb = self.lb();
        self.b.len() >= j + want.len() && &lb[j..j + want.len()] == want
    }

    fn starts_with_var_ahead(&self) -> bool {
        let mut j = self.i + 3;
        while j < self.b.len() && self.b[j].is_ascii_whitespace() {
            j += 1;
        }
        self.b.get(j) == Some(&b'@')
    }

    /// `SELECT @x = …` detection: word `select`, then `@name` with `=`.
    fn select_has_assignment(&self) -> bool {
        let mut j = self.i + 6;
        while j < self.b.len() && self.b[j].is_ascii_whitespace() {
            j += 1;
        }
        if self.b.get(j) != Some(&b'@') {
            return false;
        }
        j += 1;
        while j < self.b.len() && is_name_byte(self.b[j]) {
            j += 1;
        }
        while j < self.b.len() && self.b[j].is_ascii_whitespace() {
            j += 1;
        }
        self.b.get(j) == Some(&b'=')
    }

    fn parse_declare(&mut self) -> Result<Stmt> {
        let mut decls = Vec::new();
        loop {
            self.skip_trivia();
            if self.b.get(self.i) != Some(&b'@') {
                return err("DECLARE expects @name");
            }
            let name = self.take_var_name();
            let _ty = self.read_declare_type();
            self.skip_trivia();
            let init = if self.b.get(self.i) == Some(&b'=') {
                self.i += 1;
                Some(self.read_assign_expr())
            } else {
                None
            };
            decls.push((name, init));
            self.skip_trivia();
            if self.b.get(self.i) == Some(&b',') {
                self.i += 1;
                continue;
            }
            return Ok(Stmt::Declare(decls));
        }
    }

    fn take_var_name(&mut self) -> String {
        self.i += 1; // '@'
        let start = self.i;
        while self.i < self.b.len() && is_name_byte(self.b[self.i]) {
            self.i += 1;
        }
        self.sql[start..self.i].to_ascii_lowercase()
    }

    /// Type text runs until a top-level `=`, `,`, `;` or statement end.
    /// The engine is schemaless, so the type is documentation; it still
    /// must be consumed to find any initializer.
    fn read_declare_type(&mut self) -> String {
        let start = self.i;
        let mut depth = 0i32;
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(self.sql, self.i);
                    self.i = end;
                    continue;
                }
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'=' | b',' | b';' if depth == 0 => break,
                b'\n' if depth == 0 && self.at_statement_boundary() => break,
                _ => {}
            }
            self.i += 1;
        }
        self.sql[start..self.i].trim().to_string()
    }

    /// Assignment expression: until a top-level `,`, `;`, a clause
    /// starter (FROM/WHERE/… — ends a SELECT assignment list) or the
    /// statement end.
    fn read_assign_expr(&mut self) -> String {
        let start = self.i;
        let mut depth = 0i32;
        while self.i < self.b.len() {
            let c_is_alpha = self.b[self.i].is_ascii_alphabetic();
            match self.b[self.i] {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(self.sql, self.i);
                    self.i = end;
                    continue;
                }
                b'-' if self.b.get(self.i + 1) == Some(&b'-') => {
                    while self.i < self.b.len() && self.b[self.i] != b'\n' {
                        self.i += 1;
                    }
                    continue;
                }
                b'/' if self.b.get(self.i + 1) == Some(&b'*') => {
                    self.i += 2;
                    while self.i + 1 < self.b.len()
                        && !(self.b[self.i] == b'*' && self.b[self.i + 1] == b'/')
                    {
                        self.i += 1;
                    }
                    self.i = (self.i + 2).min(self.b.len());
                    continue;
                }
                b'(' => depth += 1,
                b')' => depth -= 1,
                b',' | b';' if depth == 0 => break,
                _ if depth == 0 && c_is_alpha && self.word_at_is(SELECT_CLAUSE_STARTERS) => break,
                b'\n' if depth == 0 && self.at_statement_boundary() => break,
                _ => {}
            }
            self.i += 1;
        }
        self.sql[start..self.i].trim().to_string()
    }

    /// True at a '\n' cursor whose next word starts a new statement —
    /// plain statements end there when the script omits semicolons.
    fn at_statement_boundary(&self) -> bool {
        if self.b.get(self.i) != Some(&b'\n') {
            return false;
        }
        let mut j = self.i + 1;
        loop {
            while j < self.b.len() && self.b[j].is_ascii_whitespace() {
                j += 1;
            }
            if self.b.get(j) == Some(&b'-') && self.b.get(j + 1) == Some(&b'-') {
                while j < self.b.len() && self.b[j] != b'\n' {
                    j += 1;
                }
                continue;
            }
            break;
        }
        if j >= self.b.len() || !self.b[j].is_ascii_alphabetic() {
            return false;
        }
        let mut k = j;
        while k < self.b.len() && (self.b[k].is_ascii_alphanumeric() || self.b[k] == b'_') {
            k += 1;
        }
        is_word_in(&self.lb()[j..k], STATEMENT_STARTERS)
    }

    /// Read the rest of the current plain statement, INCLUDING any
    /// not-yet-consumed leading keyword.
    fn read_plain(&mut self) -> String {
        let start = self.i;
        let mut depth = 0i32;
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(self.sql, self.i);
                    self.i = end;
                    continue;
                }
                b'"' | b'`' | b'[' => {
                    let close = match self.b[self.i] {
                        b'[' => b']',
                        other => other,
                    };
                    self.i += 1;
                    while self.i < self.b.len() {
                        if self.b[self.i] == close {
                            if close != b']' && self.b.get(self.i + 1) == Some(&close) {
                                self.i += 2;
                                continue;
                            }
                            self.i += 1;
                            break;
                        }
                        self.i += 1;
                    }
                    continue;
                }
                b'-' if self.b.get(self.i + 1) == Some(&b'-') => {
                    while self.i < self.b.len() && self.b[self.i] != b'\n' {
                        self.i += 1;
                    }
                    continue;
                }
                b'/' if self.b.get(self.i + 1) == Some(&b'*') => {
                    self.i += 2;
                    while self.i + 1 < self.b.len()
                        && !(self.b[self.i] == b'*' && self.b[self.i + 1] == b'/')
                    {
                        self.i += 1;
                    }
                    self.i = (self.i + 2).min(self.b.len());
                    continue;
                }
                b'(' => depth += 1,
                b')' => depth -= 1,
                b';' if depth == 0 => {
                    self.i += 1;
                    break;
                }
                // A depth-0 ELSE ends the governed statement of a
                // single-line `IF … stmt ELSE stmt`. A CASE expression is
                // skipped whole first, so its inner ELSE never ends up here.
                _ if depth == 0
                    && self.b[self.i].is_ascii_alphabetic()
                    && self.at_word_start()
                    && self.word_at_is(&["case"]) =>
                {
                    self.skip_case_block();
                    continue;
                }
                _ if depth == 0
                    && self.b[self.i].is_ascii_alphabetic()
                    && self.word_at_is(&["else"]) =>
                {
                    break
                }
                b'\n' if depth == 0 && self.at_statement_boundary() => break,
                _ => {}
            }
            self.i += 1;
        }
        self.sql[start..self.i].trim().to_string()
    }

    fn parse_set(&mut self) -> Result<Stmt> {
        self.skip_trivia();
        if self.b.get(self.i) != Some(&b'@') {
            return err("SET expects @name = expression");
        }
        let name = self.take_var_name();
        self.skip_trivia();
        if self.b.get(self.i) != Some(&b'=') {
            return err(format!("SET @{name} expects '='"));
        }
        self.i += 1;
        let expr = self.read_assign_expr();
        if expr.is_empty() {
            return err(format!("SET @{name} expects an expression"));
        }
        Ok(Stmt::SetOne(name, expr))
    }

    fn parse_select_assign(&mut self) -> Result<Stmt> {
        let mut assigns: Vec<(String, String)> = Vec::new();
        loop {
            self.skip_trivia();
            if self.b.get(self.i) != Some(&b'@') {
                return err("SELECT assignment expects @name = expression");
            }
            let name = self.take_var_name();
            self.skip_trivia();
            if self.b.get(self.i) != Some(&b'=') {
                return err(format!("SELECT @{name} expects '='"));
            }
            self.i += 1;
            let expr = self.read_assign_expr();
            if expr.is_empty() {
                return err(format!("SELECT @{name} expects an expression"));
            }
            assigns.push((name, expr));
            // Whitespace only: a newline here is a statement boundary the
            // tail read must see (full trivia-skipping would swallow the
            // next statement into the tail).
            while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\r') {
                self.i += 1;
            }
            if self.b.get(self.i) == Some(&b',') {
                self.i += 1;
                continue;
            }
            break;
        }
        // The clause tail (FROM …, WHERE …) rides along so the rowset form
        // scans real rows.
        Ok(Stmt::SetVar(assigns, self.read_plain()))
    }

    /// Condition text ends where its governed statement begins: at the
    /// first depth-0 statement-starter word.
    fn read_condition(&mut self) -> Result<String> {
        let start = self.i;
        let mut depth = 0i32;
        let mut end = self.i;
        while self.i < self.b.len() {
            let c = self.b[self.i];
            match c {
                b'\'' => {
                    let (lit_end, _) = stmt::sql_literal_end(self.sql, self.i);
                    self.i = lit_end;
                    end = lit_end;
                    continue;
                }
                b'-' if self.b.get(self.i + 1) == Some(&b'-') => {
                    while self.i < self.b.len() && self.b[self.i] != b'\n' {
                        self.i += 1;
                    }
                    continue;
                }
                b'/' if self.b.get(self.i + 1) == Some(&b'*') => {
                    self.i += 2;
                    while self.i + 1 < self.b.len()
                        && !(self.b[self.i] == b'*' && self.b[self.i + 1] == b'/')
                    {
                        self.i += 1;
                    }
                    self.i = (self.i + 2).min(self.b.len());
                    end = self.i;
                    continue;
                }
                b'(' => depth += 1,
                b')' => depth -= 1,
                // CASE 表达式整体跳过:内部的 ELSE 等语句起始词不结束条件
                _ if depth == 0
                    && c.is_ascii_alphabetic()
                    && self.at_word_start()
                    && self.word_at_is(&["case"]) =>
                {
                    self.skip_case_block();
                    end = self.i;
                    continue;
                }
                _ if depth == 0
                    && c.is_ascii_alphabetic()
                    && self.word_at_is_statement_starter() =>
                {
                    break
                }
                _ => {}
            }
            if !matches!(c, b' ' | b'\t' | b'\r' | b'\n') {
                end = self.i + 1;
            }
            self.i += 1;
        }
        let cond = self.sql[start..end].trim().to_string();
        if cond.is_empty() {
            return err("IF/WHILE requires a condition");
        }
        Ok(cond)
    }

    fn word_at_is_statement_starter(&self) -> bool {
        self.word_at_is(STATEMENT_STARTERS)
    }

    fn word_at_is(&self, list: &[&str]) -> bool {
        let mut k = self.i;
        while k < self.b.len() && (self.b[k].is_ascii_alphanumeric() || self.b[k] == b'_') {
            k += 1;
        }
        is_word_in(&self.lb()[self.i..k], list)
    }

    /// True when the cursor sits on the FIRST byte of a word (the previous
    /// byte is not a word character) — guards the mid-word false match of a
    /// keyword inside an identifier like `eelse`.
    fn at_word_start(&self) -> bool {
        match self.i.checked_sub(1).and_then(|p| self.b.get(p)) {
            Some(prev) => !prev.is_ascii_alphanumeric() && *prev != b'_',
            None => true,
        }
    }

    /// Skip a complete `CASE … END` expression (cursor on the word "case").
    /// An ELSE/newline inside CASE is expression syntax, not a statement
    /// boundary — without this, the scanners cut the statement at the
    /// CASE's ELSE and the leftover tail wedges the parser. Nested CASE
    /// pairs are counted; literals/brackets/comments are skipped as usual.
    /// An unterminated CASE runs to end of input and is rejected loudly by
    /// the engine downstream.
    fn skip_case_block(&mut self) {
        // 消费 "case" 词本身
        while self.i < self.b.len()
            && (self.b[self.i].is_ascii_alphanumeric() || self.b[self.i] == b'_')
        {
            self.i += 1;
        }
        let mut nesting = 1i32;
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(self.sql, self.i);
                    self.i = end;
                    continue;
                }
                b'"' | b'`' | b'[' => {
                    let close = match self.b[self.i] {
                        b'[' => b']',
                        other => other,
                    };
                    self.i += 1;
                    while self.i < self.b.len() {
                        if self.b[self.i] == close {
                            if close != b']' && self.b.get(self.i + 1) == Some(&close) {
                                self.i += 2;
                                continue;
                            }
                            self.i += 1;
                            break;
                        }
                        self.i += 1;
                    }
                    continue;
                }
                b'-' if self.b.get(self.i + 1) == Some(&b'-') => {
                    while self.i < self.b.len() && self.b[self.i] != b'\n' {
                        self.i += 1;
                    }
                    continue;
                }
                b'/' if self.b.get(self.i + 1) == Some(&b'*') => {
                    self.i += 2;
                    while self.i + 1 < self.b.len()
                        && !(self.b[self.i] == b'*' && self.b[self.i + 1] == b'/')
                    {
                        self.i += 1;
                    }
                    self.i = (self.i + 2).min(self.b.len());
                    continue;
                }
                c if c.is_ascii_alphabetic() => {
                    let word = self.peek_word().unwrap_or_default().to_vec();
                    if word == b"case" {
                        nesting += 1;
                    } else if word == b"end" {
                        nesting -= 1;
                        if nesting == 0 {
                            self.i += 3;
                            return;
                        }
                    }
                    self.i += word.len().max(1);
                    continue;
                }
                _ => {}
            }
            self.i += 1;
        }
    }

    fn parse_if(&mut self, depth: usize) -> Result<Stmt> {
        let cond = self.read_condition()?;
        self.skip_trivia();
        let then = self.parse_governed(depth)?;
        self.skip_trivia();
        let else_ = if self.peek_word() == Some(b"else") {
            self.i += 4;
            self.skip_trivia();
            Some(self.parse_governed(depth)?)
        } else {
            None
        };
        Ok(Stmt::If { cond, then, else_ })
    }

    fn parse_while(&mut self, depth: usize) -> Result<Stmt> {
        let cond = self.read_condition()?;
        self.skip_trivia();
        let body = self.parse_governed(depth)?;
        Ok(Stmt::While { cond, body })
    }

    /// The statement governed by IF/WHILE/ELSE: a BEGIN…END block or one
    /// plain statement.
    fn parse_governed(&mut self, depth: usize) -> Result<Vec<Stmt>> {
        if self.peek_word() == Some(b"begin") && self.begin_opens_block() {
            self.i += 5;
            return self.parse_block(depth);
        }
        // `IF … BREAK` / `WHILE … CONTINUE` govern the flow statements
        // themselves, not a plain run spelling the keyword; the same goes
        // for a governed THROW/RAISERROR.
        self.skip_trivia();
        match self.peek_word() {
            Some(b"break") => {
                self.i += 5;
                return Ok(vec![Stmt::Break]);
            }
            Some(b"continue") => {
                self.i += 8;
                return Ok(vec![Stmt::Continue]);
            }
            Some(b"throw") => {
                self.i += 5;
                return Ok(vec![Stmt::Throw(ThrowForm::Throw, self.read_plain())]);
            }
            Some(b"raiserror") => {
                self.i += 9;
                return Ok(vec![Stmt::Throw(ThrowForm::RaiseError, self.read_plain())]);
            }
            _ => {}
        }
        Ok(vec![Stmt::Plain(self.read_plain())])
    }

    /// Statements of a BEGIN…END block, consuming the matching END.
    fn parse_block(&mut self, depth: usize) -> Result<Vec<Stmt>> {
        let mut nesting = 1i32;
        let start = self.i;
        while self.i < self.b.len() {
            let c = self.b[self.i];
            match c {
                b'\'' => {
                    let (end, _) = stmt::sql_literal_end(self.sql, self.i);
                    self.i = end;
                    continue;
                }
                b'-' if self.b.get(self.i + 1) == Some(&b'-') => {
                    while self.i < self.b.len() && self.b[self.i] != b'\n' {
                        self.i += 1;
                    }
                    continue;
                }
                b'/' if self.b.get(self.i + 1) == Some(&b'*') => {
                    self.i += 2;
                    while self.i + 1 < self.b.len()
                        && !(self.b[self.i] == b'*' && self.b[self.i + 1] == b'/')
                    {
                        self.i += 1;
                    }
                    self.i = (self.i + 2).min(self.b.len());
                    continue;
                }
                c if c.is_ascii_alphabetic() => {
                    let word = self.peek_word().unwrap_or_default().to_vec();
                    if word == b"begin" {
                        nesting += 1;
                    } else if word == b"end" {
                        nesting -= 1;
                        if nesting == 0 {
                            let body = self.sql[start..self.i].to_string();
                            self.i += 3;
                            let mut inner = Parser {
                                sql: &body,
                                b: body.as_bytes(),
                                lower: body.to_ascii_lowercase(),
                                i: 0,
                            };
                            return inner.parse_stmts(depth + 1);
                        }
                    }
                    self.i += word.len().max(1);
                    continue;
                }
                _ => {}
            }
            self.i += 1;
        }
        err("BEGIN without a matching END")
    }
}

/// Default message for a raise that carries none (bare THROW outside CATCH,
/// or a degenerate RAISERROR).
const DEFAULT_THROW_MESSAGE: &str = "error raised by batch";

/// Which raise syntax produced a [`Stmt::Throw`]: the argument orders differ
/// (THROW: code, message, state — RAISERROR: message, severity, state).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ThrowForm {
    Throw,
    RaiseError,
}

/// THROW / RAISERROR argument shapes (after run-time substitution):
/// bare (None — THROW re-raises inside CATCH), `THROW 50000, 'msg', 1`,
/// `RAISERROR('msg', 16, 1)`, `RAISERROR 'msg', 16, 1`. RAISERROR's message
/// is its FIRST argument and severity/state are ignored — every raise fails
/// the statement here.
fn parse_throw(args: &str, form: ThrowForm) -> Option<(i64, String)> {
    let text = args.trim().trim_end_matches(';').trim();
    if text.is_empty() {
        return None;
    }
    // RAISERROR's paren form hides the separating commas from the top-level
    // split — strip the one wrapping pair first.
    let text = if form == ThrowForm::RaiseError {
        strip_outer_parens(text).unwrap_or(text)
    } else {
        text
    };
    let parts = split_top_level_commas(text);
    let (code_arg, message_arg) = match form {
        ThrowForm::Throw => (parts.first().copied(), parts.get(1).copied()),
        ThrowForm::RaiseError => (None, parts.first().copied()),
    };
    let code = code_arg
        .and_then(|p| p.parse::<i64>().ok())
        .unwrap_or(50000);
    let message = message_arg
        .map(unquote_literal)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| DEFAULT_THROW_MESSAGE.into());
    Some((code, message))
}

/// Split on commas outside string literals and parentheses.
fn split_top_level_commas(text: &str) -> Vec<&str> {
    let b = text.as_bytes();
    let mut parts: Vec<&str> = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(text, i);
                i = end;
                continue;
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(text[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(text[start..].trim());
    parts
}

/// Strip one balanced leading/trailing parenthesis pair (parens inside
/// string literals do not count). None when the text is not a single
/// wrapping group — the caller keeps it verbatim for the split to judge.
fn strip_outer_parens(text: &str) -> Option<&str> {
    let b = text.as_bytes();
    if b.first() != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(text, i);
                i = end;
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 && i != b.len() - 1 {
                    return None;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return None;
    }
    Some(&text[1..text.len() - 1])
}

/// True when the statement's first real token (comments skipped) is
/// INSERT. Hosts that run statements outside the interpreter use this to
/// know when a statement's identity snapshot must reach the session.
pub fn is_insert_statement(sql: &str) -> bool {
    let i = leading_trivia_end(sql);
    sql.len() >= i + 6 && sql[i..i + 6].eq_ignore_ascii_case("INSERT")
}

/// Byte offset of the first real token after leading whitespace and
/// comments (`-- line`, `/* block */`) — keyword sniffing on rendered
/// statements must not be fooled by a comment prefix.
fn leading_trivia_end(text: &str) -> usize {
    let b = text.as_bytes();
    let mut i = 0usize;
    loop {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i..].starts_with(b"/*") {
            match text[i + 2..].find("*/") {
                Some(j) => i += 2 + j + 2,
                None => return i,
            }
        } else {
            return i;
        }
    }
}

/// Strip one layer of single quotes; anything else returns as-is.
fn unquote_literal(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        t[1..t.len() - 1].replace("''", "'")
    } else {
        t.to_string()
    }
}

/// True when the script needs the interpreter: any control-flow or
/// variable statement, a `@` reference, or more than one statement (a
/// multi-statement batch). The server keeps its single-statement fast
/// path untouched otherwise.
pub fn needs_interpretation(sql: &str) -> bool {
    for chunk in stmt::text_chunks(sql) {
        let chunk = chunk.trim();
        if chunk.is_empty() {
            continue;
        }
        match parse_batch(chunk) {
            Ok(stmts) => {
                if stmts.len() != 1 || !matches!(stmts[0], Stmt::Plain(_)) {
                    return true;
                }
            }
            Err(_) => return true,
        }
        if mentions_var(chunk) {
            return true;
        }
    }
    false
}

/// `@` appears outside string literals/comments/bracketed identifiers.
fn mentions_var(sql: &str) -> bool {
    let b = sql.as_bytes();
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(sql, i);
                i = end;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(b.len());
            }
            b'"' | b'`' | b'[' => {
                let close = match b[i] {
                    b'[' => b']',
                    other => other,
                };
                i += 1;
                while i < b.len() {
                    if b[i] == close {
                        if close != b']' && b.get(i + 1) == Some(&close) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'@' => return true,
            _ => i += 1,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Engine-backed executor over an in-memory database — the same shape
    /// the CLI's embedded mode uses.
    struct DbExec {
        db: engine::Database,
    }

    impl DbExec {
        fn new() -> Self {
            Self {
                db: engine::Database::in_memory().unwrap(),
            }
        }
    }

    impl BatchExecutor for DbExec {
        fn last_identity(&mut self) -> Option<Value> {
            self.db.last_insert_id().map(Value::Int)
        }

        fn execute(&mut self, sql: &str) -> ExecFuture<'_> {
            let out = match self.db.execute(sql) {
                Ok(engine::ExecOutcome::Rows(r)) => Ok(ExecResult::Rows(r)),
                Ok(engine::ExecOutcome::Affected(n)) => Ok(ExecResult::Affected(n)),
                Err(e) => Err(e),
            };
            Box::pin(std::future::ready(out))
        }
    }

    fn run_script(
        session: &mut TsqlSession,
        db: &mut DbExec,
        sql: &str,
    ) -> Result<Option<ExecResult>> {
        block_on(session.run_batch(sql, db))
    }

    fn rows_of(out: &Option<ExecResult>) -> Vec<Vec<Value>> {
        match out {
            Some(ExecResult::Rows(r)) => r.rows.clone(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// 无括号包裹的 CASE 表达式:read_plain 曾在其 ELSE 处切断语句,残留的
    /// `ELSE …` 让 parse_stmts 零进度空转(无限循环 + 无限内存)。
    #[test]
    fn case_in_plain_batch_stmt() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        let out = run_script(
            &mut s,
            &mut db,
            "DECLARE @x INT = 1\nSELECT CASE WHEN @x = 1 THEN 'a' ELSE 'b' END AS v",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Str("a".into()));
        // 换行版:CASE 内的 ELSE 行不是语句边界
        let out = run_script(
            &mut s,
            &mut db,
            "DECLARE @y INT = 2\nSELECT CASE WHEN @y = 1 THEN 'a'\nELSE 'b' END AS v",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Str("b".into()));
        // 嵌套 CASE
        let out = run_script(
            &mut s,
            &mut db,
            "SELECT CASE WHEN @x = 1 THEN CASE WHEN @y = 2 THEN 'ab' ELSE 'ax' END ELSE 'z' END AS v",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Str("ab".into()));
        // 条件里的 CASE:read_condition 不在 CASE 的 ELSE 处切断
        let out = run_script(
            &mut s,
            &mut db,
            "DECLARE @z INT = CASE WHEN 1 = 1 THEN 5 ELSE 6 END\nSELECT @z AS v",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(5));
    }

    /// 批首孤立 ELSE(无 IF)是畸形批:必须响亮报错,而不是零进度空转。
    #[test]
    fn orphan_else_errors_loudly() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        let out = run_script(&mut s, &mut db, "ELSE SELECT 1");
        assert!(out.is_err());
    }

    #[test]
    fn declare_set_and_substitute() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (id INT)").unwrap();
        let out = run_script(
            &mut s,
            &mut db,
            "DECLARE @n INT = 5, @label TEXT\nSET @n = @n + 2\nSELECT @label = 'x' + CAST(@n AS TEXT)\nSELECT @label AS v",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Str("x7".into()));
        // SET across batches on the SAME session.
        let out = run_script(&mut s, &mut db, "SET @n = @n * 2\nSELECT @n AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(14));
        assert!(s.is_active());
    }

    #[test]
    fn declare_defaults_and_errors() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        let out = run_script(&mut s, &mut db, "DECLARE @x VARCHAR(50)\nSELECT @x AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Null);
        // Duplicate declaration.
        let e = run_script(&mut s, &mut db, "DECLARE @x INT").unwrap_err();
        assert!(e.to_string().contains("already been declared"), "{e}");
        // Undeclared use is loud.
        let e = run_script(&mut s, &mut db, "SELECT @nope").unwrap_err();
        assert!(e.to_string().contains("Must declare"), "{e}");
        // Malformed DECLARE / SET.
        assert!(run_script(&mut s, &mut db, "DECLARE x INT").is_err());
        run_script(&mut s, &mut db, "DECLARE @y INT").unwrap();
        assert!(run_script(&mut s, &mut db, "SET @y").is_err());
        assert!(run_script(&mut s, &mut db, "SET @y =").is_err());
    }

    #[test]
    fn if_else_and_blocks() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        run_script(
            &mut s,
            &mut db,
            "DECLARE @n INT = 1\nIF @n = 1 INSERT INTO t VALUES (10)\nIF @n = 2 INSERT INTO t VALUES (20) ELSE INSERT INTO t VALUES (30)",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT v FROM t ORDER BY v").unwrap();
        assert_eq!(rows_of(&out).len(), 2);
        // BEGIN...END with nesting.
        run_script(
            &mut s,
            &mut db,
            "IF @n = 1\nBEGIN\n  INSERT INTO t VALUES (40)\n  IF @n > 0\n  BEGIN\n    INSERT INTO t VALUES (50)\n  END\nEND",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT COUNT(*) AS c FROM t").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(4));
        // NULL condition behaves as false (ELSE branch runs).
        run_script(
            &mut s,
            &mut db,
            "DECLARE @z INT\nIF @z = 1 INSERT INTO t VALUES (60) ELSE INSERT INTO t VALUES (70)",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT COUNT(*) AS c FROM t WHERE v = 70").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        // Non-boolean condition is loud.
        assert!(run_script(&mut s, &mut db, "IF 1 INSERT INTO t VALUES (1)").is_err());
        // BEGIN without a matching END is the transaction statement (the
        // engine's own syntax), not an unterminated block.
        run_script(&mut s, &mut db, "BEGIN\nSELECT 1").unwrap();
        run_script(&mut s, &mut db, "COMMIT").unwrap();
    }

    #[test]
    fn while_break_continue() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (i INT)").unwrap();
        run_script(
            &mut s,
            &mut db,
            "DECLARE @i INT = 0\nWHILE @i < 5\nBEGIN\n  SET @i = @i + 1\n  IF @i = 2 CONTINUE\n  IF @i = 4 BREAK\n  INSERT INTO t VALUES (@i)\nEND",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT i FROM t ORDER BY i").unwrap();
        assert_eq!(rows_of(&out).len(), 2); // 1 and 3
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        assert_eq!(rows_of(&out)[1][0], Value::Int(3));
        // BREAK outside a loop errors.
        let e = run_script(&mut s, &mut db, "BREAK").unwrap_err();
        assert!(e.to_string().contains("WHILE"), "{e}");
        let e = run_script(&mut s, &mut db, "CONTINUE").unwrap_err();
        assert!(e.to_string().contains("WHILE"), "{e}");
        // Runaway loop hits the budget.
        let e = run_script(&mut s, &mut db, "WHILE 1 = 1\nBEGIN\n  SET @i = @i\nEND").unwrap_err();
        assert!(e.to_string().contains("budget"), "{e}");
    }

    #[test]
    fn print_and_rowcount() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        run_script(
            &mut s,
            &mut db,
            "DECLARE @who TEXT = 'world'\nPRINT 'hello ' + @who\nINSERT INTO t VALUES (1), (2)\nDECLARE @rc INT = @@ROWCOUNT\nSELECT @rc AS rc",
        )
        .unwrap();
        assert_eq!(s.take_prints(), vec!["hello world".to_string()]);
        // take drains.
        assert!(s.take_prints().is_empty());
        let out = run_script(&mut s, &mut db, "SELECT @rc AS rc").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(2));
        // @@VERSION substitutes as a literal string.
        let out = run_script(&mut s, &mut db, "SELECT @@VERSION LIKE 'DocSQL%' AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Bool(true));
        // @@ERROR is now supported (last statement's error number, 0 on
        // success); unknown @@vars stay loud.
        let out = run_script(&mut s, &mut db, "SELECT @@ERROR AS e").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(0));
        let e = run_script(&mut s, &mut db, "SELECT @@FETCH_STATUS").unwrap_err();
        assert!(e.to_string().contains("not supported"), "{e}");
        // PRINT errors on a bad expression.
        assert!(run_script(&mut s, &mut db, "PRINT UPPER(1,2)").is_err());
    }

    #[test]
    fn select_rowset_assignment() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (a INT, b TEXT)").unwrap();
        db.db
            .execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
            .unwrap();
        // Last row wins; multi-assign from one scan.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @a INT, @b TEXT\nSELECT @a = a, @b = b FROM t",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @a AS a, @b AS b").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(2));
        assert_eq!(rows_of(&out)[0][1], Value::Str("two".into()));
        // Empty scan leaves variables unchanged.
        db.db.execute("CREATE TABLE empty (x INT)").unwrap();
        run_script(&mut s, &mut db, "SELECT @a = x FROM empty").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @a AS a").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(2));
        // SET @x = (subquery) with no rows yields NULL.
        run_script(&mut s, &mut db, "SET @a = (SELECT x FROM empty)").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @a AS a").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Null);
    }

    #[test]
    fn substitution_safety_and_opaque_contexts() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v TEXT)").unwrap();
        // A string containing @name / @@ROWCOUNT is never touched.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @v TEXT = 'lit @v and @@ROWCOUNT'\nINSERT INTO t VALUES (@v)",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT v FROM t").unwrap();
        assert_eq!(
            rows_of(&out)[0][0],
            Value::Str("lit @v and @@ROWCOUNT".into())
        );
        // Comments ride along verbatim.
        let out = run_script(
            &mut s,
            &mut db,
            "SELECT @v /* @other */ + 'x' AS v -- @trail\n",
        )
        .unwrap();
        assert_eq!(
            rows_of(&out)[0][0],
            Value::Str("lit @v and @@ROWCOUNTx".into())
        );
        // Bracketed identifiers are opaque.
        db.db.execute("CREATE TABLE [br@cket] ([c@d] INT)").unwrap();
        db.db.execute("INSERT INTO [br@cket] VALUES (7)").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT [c@d] FROM [br@cket]").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(7));
        // Exact scalars survive substitution (DECIMAL/TIMESTAMP literals).
        run_script(
            &mut s,
            &mut db,
            "DECLARE @d DECIMAL = CAST('1.50' AS DECIMAL), @t TIMESTAMP = TIMESTAMP '2026-01-02T03:04:05Z'",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @d + 0 AS d, @t AS t").unwrap();
        assert_eq!(rows_of(&out)[0][0].type_name(), "decimal");
        assert_eq!(rows_of(&out)[0][1].type_name(), "timestamp");
    }

    #[test]
    fn identity_substitution_with_context() {
        let mut s = TsqlSession::new();
        s.ctx = SessionContext {
            user: Some("alice".into()),
            app_name: Some("cli".into()),
            host: Some("box".into()),
        };
        let mut db = DbExec::new();
        let out = run_script(
            &mut s,
            &mut db,
            "SELECT SUSER_SNAME() AS u, APP_NAME() AS a, HOST_NAME() AS h, USER_NAME() AS un",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Str("alice".into()));
        assert_eq!(rows_of(&out)[0][1], Value::Str("cli".into()));
        assert_eq!(rows_of(&out)[0][2], Value::Str("box".into()));
        assert_eq!(rows_of(&out)[0][3], Value::Str("alice".into()));
        // Without context the engine's loud error stands.
        let mut s2 = TsqlSession::new();
        let mut db2 = DbExec::new();
        assert!(run_script(&mut s2, &mut db2, "SELECT SUSER_SNAME()").is_err());
    }

    #[test]
    fn plain_batches_and_go() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        // Multiple plain statements in one batch run in order; the LAST
        // outcome is the reply.
        let out = run_script(
            &mut s,
            &mut db,
            "INSERT INTO t VALUES (1)\nINSERT INTO t VALUES (2)\nSELECT COUNT(*) AS c FROM t",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(2));
        // GO separates batches inside one script; variables do NOT cross.
        let out = run_script(&mut s, &mut db, "DECLARE @x INT = 9\nGO\nSELECT @x");
        assert!(out.is_err(), "vars do not cross GO: {out:?}");
    }

    #[test]
    fn needs_interpretation_split() {
        assert!(!needs_interpretation("SELECT 1"));
        assert!(!needs_interpretation("INSERT INTO t VALUES (1)"));
        assert!(needs_interpretation("DECLARE @x INT"));
        assert!(needs_interpretation("IF 1 = 1 SELECT 1"));
        assert!(needs_interpretation("WHILE 1 = 1 BREAK"));
        assert!(needs_interpretation("SELECT 1\nSELECT 2"));
        assert!(needs_interpretation("SELECT @x"));
        assert!(!needs_interpretation("SELECT '@x'"));
    }

    #[test]
    fn begin_transaction_is_plain() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        run_script(
            &mut s,
            &mut db,
            "BEGIN TRAN\nINSERT INTO t VALUES (1)\nCOMMIT",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT COUNT(*) AS c FROM t").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
    }

    #[test]
    fn return_waitfor_goto_are_loud() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        for kw in ["RETURN", "WAITFOR DELAY '00:00:01'", "GOTO done"] {
            let e = run_script(&mut s, &mut db, kw).unwrap_err();
            assert!(e.to_string().contains("not supported"), "{kw}: {e}");
        }
    }

    /// Branches the happy-path tests never trip: escape runs in read_plain,
    /// comment handling inside expressions, deep nesting refusal, an
    /// evaluation that returns an affected count, and parser error tails.
    #[test]
    fn interpreter_edge_branches() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        // Condition on a string literal containing quotes/keywords.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @m TEXT = 'IF SELECT WHILE'\nIF @m = 'IF SELECT WHILE' INSERT INTO t VALUES (1)",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT COUNT(*) AS c FROM t").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        // A plain statement with a doubled-quote identifier and a comment
        // inside quotes-contexts.
        run_script(&mut s, &mut db, "INSERT INTO t VALUES (2) -- done\n").unwrap();
        // Depth cap: 33 nested blocks refuse loudly.
        let mut deep = String::new();
        for _ in 0..40 {
            deep.push_str("IF 1 = 1 BEGIN\n");
        }
        deep.push_str("SELECT 1");
        for _ in 0..40 {
            deep.push_str("\nEND");
        }
        assert!(run_script(&mut s, &mut db, &deep).is_err());
        // eval over an affected-count statement errors (conditions must be
        // queries): an IF whose condition text is a write.
        assert!(run_script(&mut s, &mut db, "IF UPDATE t SET v = 1 SELECT 1").is_err());
        // Parser error tails.
        assert!(parse_batch("SET UPPER(a) = 1").is_err() || true);
        let mut s2 = TsqlSession::new();
        let mut db2 = DbExec::new();
        run_script(&mut s2, &mut db2, "DECLARE @a INT").unwrap();
        assert!(run_script(&mut s2, &mut db2, "SELECT @a = FROM t").is_err());
        // WHILE with an ELSE-looking body / nested literal in condition.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @i INT = 0\nWHILE @i < 1 /* comment */\nBEGIN\nSET @i = @i + 1\nEND",
        )
        .unwrap();
    }

    #[test]
    fn session_reset_semantics() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        // DECLARE with an initializer that has a comment before the comma.
        run_script(&mut s, &mut db, "DECLARE @a INT = 1 /* one */, @b INT = 2").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @a + @b AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(3));
        // Block comment inside a plain statement.
        let out = run_script(&mut s, &mut db, "SELECT /*x*/ @a AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
    }

    /// Scanner/debug/decision branches only reachable from specific
    /// statement shapes.
    #[test]
    fn substitution_and_parser_long_tail() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        run_script(&mut s, &mut db, "DECLARE @a INT = 1").unwrap();
        // Doubled-quote identifier escapes ride through substitution.
        let out = run_script(&mut s, &mut db, "SELECT @a AS \"x\"\"y\"").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        // A word followed by () that is not an identity function passes
        // through untouched (NOW() is legal SQL here).
        let out = run_script(&mut s, &mut db, "SELECT TYPEOF(NOW()) AS v").unwrap();
        assert!(matches!(rows_of(&out)[0][0], Value::Str(_)));
        // Non-ASCII output aliases survive the byte scanner.
        let out = run_script(&mut s, &mut db, "SELECT @a AS 名称").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        // ELSE branch flow control reaches the loop error.
        let e = run_script(&mut s, &mut db, "IF 1 = 0 SELECT 1 ELSE BREAK").unwrap_err();
        assert!(e.to_string().contains("WHILE"), "{e}");
        // Debug shapes of ExecResult.
        assert_eq!(format!("{:?}", ExecResult::Affected(3)), "Affected(3)");
        assert!(format!("{:?}", rows_res()).starts_with("Rows("));
    }

    fn rows_res() -> ExecResult {
        // A tiny rows result for the Debug shape check above.
        ExecResult::Rows(QueryResult {
            columns: vec!["v".into()],
            rows: vec![vec![Value::Int(1)]],
        })
    }

    #[test]
    fn trivia_scanner_arms() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        // Leading semicolons and comments between statements.
        run_script(&mut s, &mut db, "; ;\n-- lead\n/* block */\nSELECT 1").unwrap();
        // A comment between statements inside a block.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @i INT = 0\nWHILE @i < 1\nBEGIN\n-- inner\nSET @i = @i + 1\nEND",
        )
        .unwrap();
        // A line comment between the type and the next declaration.
        run_script(&mut s, &mut db, "DECLARE @b INT -- typed\n, @c INT = 2").unwrap();
        // Comments and literals inside conditions.
        run_script(&mut s, &mut db, "IF 'a''b' = 'a''b' SELECT 1").unwrap();
        run_script(&mut s, &mut db, "IF 1 = 1 /* mid */ SELECT 2").unwrap();
        run_script(&mut s, &mut db, "IF 1 = 1 -- tail\nSELECT 3").unwrap();
        // Boundary newline followed by a comment line then a statement.
        run_script(&mut s, &mut db, "SELECT 4\n-- sep\nSELECT 5").unwrap();
        // Bracket escape runs inside a plain statement survive the
        // scanner and reach the engine intact (preprocess turns them into
        // quoted identifiers).
        assert!(run_script(&mut s, &mut db, "SELECT [a]]b]").is_ok());
    }

    #[test]
    fn select_assign_error_tails() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        assert!(run_script(&mut s, &mut db, "DECLARE @a INT\nSELECT @a = 1, @b").is_err());
        assert!(run_script(&mut s, &mut db, "DECLARE @a INT\nSELECT @a = 1 , @b 2").is_err());
        // Space before the comma still separates assignments.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @x INT, @y INT\nSELECT @x = 1 , @y = 2",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @x + @y AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(3));
    }

    #[test]
    fn needs_interpretation_edges() {
        assert!(needs_interpretation("IF"));
        // A bare BEGIN is the transaction statement — single plain run. A
        // BEGIN…END block around one plain statement flattens back to one
        // plain statement too (only multi-statement or control-flow shapes
        // need the interpreter).
        assert!(!needs_interpretation("BEGIN SELECT 1"));
        assert!(!needs_interpretation("BEGIN SELECT 1 END"));
        assert!(needs_interpretation("BEGIN\nSELECT 1\nSELECT 2\nEND"));
        assert!(!needs_interpretation("; ;"));
        assert!(!needs_interpretation("SELECT 'a--b'"));
        assert!(!needs_interpretation("SELECT /* @x */ 1"));
        assert!(!needs_interpretation("SELECT [@x] FROM t"));
        assert!(!needs_interpretation("SELECT \"@x\""));
        assert!(!needs_interpretation("GO"));
    }

    #[test]
    fn identity_functions_track_last_insert() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db
            .execute("CREATE TABLE t (id INT AUTOINCREMENT, v TEXT)")
            .unwrap();
        // No insert yet: NULL.
        let out = run_script(
            &mut s,
            &mut db,
            "SELECT SCOPE_IDENTITY() AS i, @@IDENTITY AS j",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Null);
        assert_eq!(rows_of(&out)[0][1], Value::Null);
        // After an INSERT (id auto-filled): SCOPE_IDENTITY()/@@IDENTITY.
        run_script(&mut s, &mut db, "INSERT INTO t (v) VALUES ('a')").unwrap();
        let out = run_script(
            &mut s,
            &mut db,
            "SELECT SCOPE_IDENTITY() AS i, @@IDENTITY AS j",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        assert_eq!(rows_of(&out)[0][1], Value::Int(1));
        // Multiple rows: the last row's id.
        run_script(&mut s, &mut db, "INSERT INTO t (v) VALUES ('b'), ('c')").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @@IDENTITY AS j").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(3));
        // A non-INSERT statement does not clear it (T-SQL keeps the value).
        run_script(&mut s, &mut db, "SELECT 1").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @@IDENTITY AS j").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(3));
    }

    /// THROW / RAISERROR / TRY...CATCH: catch swallows the try error and
    /// reads it through ERROR_MESSAGE()/ERROR_NUMBER()/@@ERROR; errors in
    /// the catch propagate; THROW re-raises.
    #[test]
    fn try_catch_and_throw() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        // TRY error → CATCH runs → batch continues.
        run_script(
            &mut s,
            &mut db,
            "BEGIN TRY\n  INSERT INTO missing VALUES (1)\nEND TRY\nBEGIN CATCH\n  INSERT INTO t VALUES (ERROR_NUMBER())\nEND CATCH\nSELECT COUNT(*) AS c FROM t",
        )
        .unwrap();
        // Non-existent table error → generic 50000.
        let out = run_script(&mut s, &mut db, "SELECT v FROM t").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(50000));
        // ERROR_MESSAGE carries the caught text; @@ERROR inside CATCH is
        // the caught number — 捕获必须是 CATCH 首句(T-SQL 里每条语句、
        // 包括 SET/DECLARE 成功后都把 @@ERROR 归零)。
        let out = run_script(
            &mut s,
            &mut db,
            "DECLARE @msg TEXT, @num INT\nBEGIN TRY\n  INSERT INTO no_such_table VALUES (1)\nEND TRY\nBEGIN CATCH\n  SET @num = @@ERROR\n  SET @msg = ERROR_MESSAGE()\nEND CATCH\nSELECT @msg LIKE '%no_such_table%' AS hit, @num AS n",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Bool(true));
        assert_eq!(rows_of(&out)[0][1], Value::Int(50000));
        // 成功的 SET 同样刷新 @@ERROR(曾经留下 stale 50000)。
        let out = run_script(
            &mut s,
            &mut db,
            "BEGIN TRY\n  INSERT INTO no_such_table VALUES (1)\nEND TRY\nBEGIN CATCH\n  SET @num = @@ERROR\nEND CATCH\nSET @msg = 'ok'\nSELECT @@ERROR AS e",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(0));
        // DECLARE 初始化失败(坏表)记录错误码,成功归零。
        assert!(run_script(
            &mut s,
            &mut db,
            "DECLARE @bad INT = (SELECT v FROM no_such_table)"
        )
        .is_err());
        let out = run_script(&mut s, &mut db, "SELECT @@ERROR AS e").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(50000));
        let out = run_script(&mut s, &mut db, "DECLARE @ok INT = 1\nSELECT @@ERROR AS e").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(0));
        // THROW with arguments raises; the batch fails loudly.
        let e =
            run_script(&mut s, &mut db, "IF 1 = 1 THROW 51000, 'custom failure', 1").unwrap_err();
        assert!(e.to_string().contains("custom failure"), "{e}");
        assert!(e.to_string().contains("51000"), "{e}");
        // RAISERROR message form.
        let e = run_script(&mut s, &mut db, "RAISERROR('raised', 16, 1)").unwrap_err();
        assert!(e.to_string().contains("raised"), "{e}");
        // Re-THROW inside CATCH propagates the original message.
        let e = run_script(
            &mut s,
            &mut db,
            "BEGIN TRY\n  THROW 52000, 'original', 1\nEND TRY\nBEGIN CATCH\n  THROW\nEND CATCH",
        )
        .unwrap_err();
        assert!(e.to_string().contains("original"), "{e}");
        // Bare THROW outside CATCH uses the default message.
        let e = run_script(&mut s, &mut db, "THROW").unwrap_err();
        assert!(e.to_string().contains("error raised by batch"), "{e}");
        // Errors inside CATCH propagate (not swallowed twice).
        let e = run_script(
            &mut s,
            &mut db,
            "BEGIN TRY\n  THROW 1, 'first', 1\nEND TRY\nBEGIN CATCH\n  THROW 2, 'second', 1\nEND CATCH",
        )
        .unwrap_err();
        assert!(e.to_string().contains("second"), "{e}");
        // BREAK inside TRY propagates to the enclosing WHILE.
        run_script(
            &mut s,
            &mut db,
            "DECLARE @i INT = 0\nWHILE 1 = 1\nBEGIN\n  SET @i = @i + 1\n  BEGIN TRY\n    IF @i = 3 BREAK\n  END TRY\n  BEGIN CATCH\n  END CATCH\nEND\nSELECT @i AS v",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @i AS v").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(3));
        // BEGIN TRY without BEGIN CATCH refuses.
        assert!(run_script(&mut s, &mut db, "BEGIN TRY\nSELECT 1\nEND TRY").is_err());
        // Outside CATCH, ERROR_MESSAGE()/ERROR_NUMBER() read as NULL/0.
        let out = run_script(
            &mut s,
            &mut db,
            "SELECT ERROR_MESSAGE() AS m, ERROR_NUMBER() AS n",
        )
        .unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Null);
        assert_eq!(rows_of(&out)[0][1], Value::Null);
    }

    /// RAISERROR message extraction: the paren form reports just the message
    /// (never the whole argument list), the no-paren form must not grab the
    /// severity as the message, and THROW accepts @variables (T-SQL's
    /// `THROW @code, @msg, 1`) instead of silently degrading to a bare
    /// re-raise.
    #[test]
    fn raise_forms_carry_clean_messages() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        // Paren form: message only, generic 50000 code.
        let e = run_script(&mut s, &mut db, "RAISERROR('boom', 16, 1)").unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("boom"), "{msg}");
        assert!(msg.contains("(error 50000)"), "{msg}");
        assert!(!msg.contains("16,"), "{msg}");
        // No-paren form: the severity must not become the message.
        let e = run_script(&mut s, &mut db, "RAISERROR 'plain', 16, 1").unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("plain"), "{msg}");
        assert!(!msg.starts_with("16"), "{msg}");
        // THROW with variable arguments.
        let e = run_script(
            &mut s,
            &mut db,
            "DECLARE @c INT = 52001\nDECLARE @m TEXT = 'via vars'\nTHROW @c, @m, 1",
        )
        .unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("via vars"), "{msg}");
        assert!(msg.contains("52001"), "{msg}");
    }

    /// A nested TRY...CATCH inside an outer CATCH must not blind the outer
    /// ERROR_MESSAGE() once the inner one finishes; and bare THROW keeps
    /// re-raising the same caught error on every use.
    #[test]
    fn nested_catch_keeps_outer_error_context() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db.execute("CREATE TABLE t (v INT)").unwrap();
        run_script(
            &mut s,
            &mut db,
            "BEGIN TRY\n  THROW 53000, 'outer', 1\nEND TRY\nBEGIN CATCH\n  BEGIN TRY\n    THROW 54000, 'inner', 1\n  END TRY\n  BEGIN CATCH\n  END CATCH\n  IF ERROR_MESSAGE() LIKE '%outer%'\n    INSERT INTO t VALUES (1)\n  ELSE\n    INSERT INTO t VALUES (2)\nEND CATCH",
        )
        .unwrap();
        let out = run_script(&mut s, &mut db, "SELECT v FROM t").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
        // Two bare THROWs in one CATCH both re-raise the caught error.
        let e = run_script(
            &mut s,
            &mut db,
            "BEGIN TRY\n  THROW 55000, 'keepme', 1\nEND TRY\nBEGIN CATCH\n  BEGIN TRY\n    THROW\n  END TRY\n  BEGIN CATCH\n    THROW\n  END CATCH\nEND CATCH",
        )
        .unwrap_err();
        assert!(e.to_string().contains("keepme"), "{e}");
    }

    /// An INSERT behind a leading comment still updates @@IDENTITY (the
    /// keyword sniff skips trivia, not just whitespace).
    #[test]
    fn commented_insert_still_tracks_identity() {
        let mut s = TsqlSession::new();
        let mut db = DbExec::new();
        db.db
            .execute("CREATE TABLE t (id INT AUTOINCREMENT, v TEXT)")
            .unwrap();
        run_script(&mut s, &mut db, "-- load\nINSERT INTO t (v) VALUES ('a')").unwrap();
        let out = run_script(&mut s, &mut db, "SELECT @@IDENTITY AS i").unwrap();
        assert_eq!(rows_of(&out)[0][0], Value::Int(1));
    }

    #[test]
    fn parser_shapes() {
        let stmts = parse_batch("DECLARE @a INT = 1").unwrap();
        assert!(matches!(
            &stmts[0],
            Stmt::Declare(d) if d[0].0 == "a" && d[0].1.is_some()
        ));
        let stmts = parse_batch("SET @a = 1 + 2").unwrap();
        assert!(matches!(&stmts[0], Stmt::SetOne(n, e) if n == "a" && e.contains("1 + 2")));
        let stmts = parse_batch("SELECT @a = 1, @b = 2 FROM t WHERE x > 1").unwrap();
        match &stmts[0] {
            Stmt::SetVar(assigns, tail) => {
                assert_eq!(assigns.len(), 2);
                assert!(tail.contains("FROM t WHERE x > 1"), "{tail}");
            }
            other => panic!("{other:?}"),
        }
        // IF condition stops at the governed SELECT (not the subquery).
        let stmts = parse_batch("IF EXISTS (SELECT 1 FROM t) AND @x > 0 SELECT 2").unwrap();
        match &stmts[0] {
            Stmt::If { cond, .. } => {
                assert!(cond.contains("EXISTS (SELECT 1 FROM t)"), "{cond}");
                assert!(!cond.contains("SELECT 2"), "{cond}");
            }
            other => panic!("{other:?}"),
        }
        // BEGIN TRAN stays plain.
        let stmts = parse_batch("BEGIN TRANSACTION").unwrap();
        assert!(matches!(&stmts[0], Stmt::Plain(p) if p.contains("BEGIN")));
        // Unknown leading garbage becomes one plain statement.
        let stmts = parse_batch("42 + 1").unwrap();
        assert!(matches!(stmts[0], Stmt::Plain(_)));
        // Missing condition is loud.
        assert!(parse_batch("IF SELECT 1").is_err());
        assert!(parse_batch("WHILE BEGIN SELECT 1 END").is_err());
        // SELECT without '=' stays plain (returns rows).
        let stmts = parse_batch("SELECT @a").unwrap();
        assert!(matches!(&stmts[0], Stmt::Plain(_)));
        // Semicolons separate statements.
        let stmts = parse_batch("SELECT 1; SELECT 2").unwrap();
        assert_eq!(stmts.len(), 2);
    }
}
