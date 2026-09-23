//! Database users, roles and privileges (SQL-level user management).
//!
//! Design (v1):
//! - Fixed hand-parsed statement family (sqlparser 0.62 does not accept
//!   `CREATE USER … PASSWORD`/`GRANT role TO user`): CREATE/ALTER/DROP USER,
//!   CREATE/DROP ROLE, GRANT/REVOKE (role membership and table privileges).
//! - State lives in four RESERVED regular tables (`docsql_users`, …): unlike
//!   engine system tables they are part of replicated data — journal, join
//!   snapshot, digests and backups carry them, so every node in a cluster
//!   agrees on the user set. The server's write gate still rejects plain
//!   DML against them; the only path in is this statement family.
//! - Plaintext passwords never replicate: the executing node hashes the
//!   password and rewrites the statement into its resolved form (the
//!   auto-GUID pattern), so journals/dumps/snapshots carry
//!   `$pbkdf2-sha256$…` only.
//! - Built-in roles: `admin` (everything incl. DDL and user management),
//!   `readwrite` (DML on all tables + PUBLISH), `readonly` (SELECT on all
//!   non-internal tables). Custom roles hold table-level DML grants;
//!   memberships are user-only (no role-to-role, no cycles).

use crate::engine::{Database, ExecOutcome, SqlError};
use crate::kdf;
use crate::value::Object;
use std::collections::BTreeMap;

pub const USERS_TABLE: &str = "docsql_users";
pub const ROLES_TABLE: &str = "docsql_roles";
pub const MEMBERS_TABLE: &str = "docsql_role_members";
pub const GRANTS_TABLE: &str = "docsql_grants";

pub const BUILTIN_ROLES: [&str; 3] = ["admin", "readwrite", "readonly"];

pub const PRIV_SELECT: u8 = 1;
pub const PRIV_INSERT: u8 = 2;
pub const PRIV_UPDATE: u8 = 4;
pub const PRIV_DELETE: u8 = 8;

/// True for the user/role storage tables (reserved names, reject in
/// CREATE TABLE). Distinct from `is_system_table`, which drives
/// replication exclusion — these tables ARE replicated.
pub fn is_user_table(name: &str) -> bool {
    matches!(
        name,
        USERS_TABLE | ROLES_TABLE | MEMBERS_TABLE | GRANTS_TABLE
    )
}

#[derive(Debug, Clone, PartialEq)]
pub enum TablePriv {
    Select,
    Insert,
    Update,
    Delete,
}

impl TablePriv {
    fn name(&self) -> &'static str {
        match self {
            TablePriv::Select => "SELECT",
            TablePriv::Insert => "INSERT",
            TablePriv::Update => "UPDATE",
            TablePriv::Delete => "DELETE",
        }
    }

    fn parse(w: &str) -> Option<TablePriv> {
        Some(match w.to_uppercase().as_str() {
            "SELECT" => TablePriv::Select,
            "INSERT" => TablePriv::Insert,
            "UPDATE" => TablePriv::Update,
            "DELETE" => TablePriv::Delete,
            _ => return None,
        })
    }

    pub fn bit(&self) -> u8 {
        match self {
            TablePriv::Select => PRIV_SELECT,
            TablePriv::Insert => PRIV_INSERT,
            TablePriv::Update => PRIV_UPDATE,
            TablePriv::Delete => PRIV_DELETE,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UserAdminStmt {
    CreateUser {
        name: String,
        /// Plaintext, or an already-hashed `$pbkdf2-sha256$…` form (replay).
        password: String,
    },
    AlterUserPassword {
        name: String,
        password: String,
    },
    DropUser {
        name: String,
    },
    CreateRole {
        name: String,
    },
    DropRole {
        name: String,
    },
    GrantRoles {
        roles: Vec<String>,
        to: Vec<String>,
    },
    RevokeRoles {
        roles: Vec<String>,
        from: Vec<String>,
    },
    GrantTable {
        privileges: Vec<TablePriv>,
        tables: Vec<String>,
        to: Vec<String>,
    },
    RevokeTable {
        privileges: Vec<TablePriv>,
        tables: Vec<String>,
        from: Vec<String>,
    },
}

// ---- tokenizer (fixed grammar; no comments inside these statements) ----

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    Str(String),
    Comma,
}

fn tokenize(s: &str) -> Result<Vec<Tok>, String> {
    let mut out = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '\'' {
            let mut v = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err("unterminated string literal".into()),
                    Some('\'') => {
                        if chars.get(i + 1) == Some(&'\'') {
                            v.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    }
                    Some(&ch) => {
                        v.push(ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Str(v));
            continue;
        }
        if c == '"' {
            let mut v = String::new();
            i += 1;
            // 与单引号分支对称:未闭合必须报错。截断的粘贴(如
            // `DROP USER "alice`)曾把余下全文吞成一个标识符,畸形语句
            // 被静默按非本意的名字执行。
            let mut closed = false;
            while let Some(&ch) = chars.get(i) {
                i += 1;
                if ch == '"' {
                    closed = true;
                    break;
                }
                v.push(ch);
            }
            if !closed {
                return Err("unterminated quoted identifier".into());
            }
            if v.is_empty() {
                return Err("empty quoted identifier".into());
            }
            out.push(Tok::Word(v));
            continue;
        }
        if c == ',' {
            out.push(Tok::Comma);
            i += 1;
            continue;
        }
        if c.is_ascii_alphanumeric() || c == '_' || c == '$' {
            let mut v = String::new();
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                v.push(chars[i]);
                i += 1;
            }
            out.push(Tok::Word(v));
            continue;
        }
        return Err(format!(
            "unexpected character {c:?} in user management statement"
        ));
    }
    Ok(out)
}

/// Parse a user-management statement. `None` = not one of ours (hand back
/// to the SQL parser); `Some(Err)` = syntactically ours but malformed.
pub fn parse(sql: &str) -> Option<Result<UserAdminStmt, String>> {
    // Callers pipe whole lines including the trailing `;` (CLI shell,
    // deploy scripts): strip it instead of rejecting the statement.
    let trimmed = sql.trim().trim_end_matches(';');
    if !head_is_user_admin(trimmed) {
        return None;
    }
    Some(tokenize(trimmed).and_then(parse_tokens))
}

fn head_is_user_admin(sql: &str) -> bool {
    let mut words = sql
        .split_whitespace()
        .map(|w| w.trim_matches(|c: char| !c.is_ascii_alphanumeric()))
        .map(|w| w.to_uppercase());
    let first = words.next().unwrap_or_default();
    let second = words.next().unwrap_or_default();
    matches!(
        (first.as_str(), second.as_str()),
        ("CREATE" | "ALTER" | "DROP", "USER" | "ROLE") | ("GRANT" | "REVOKE", _)
    )
}

struct Cursor {
    toks: Vec<Tok>,
    pos: usize,
}

impl Cursor {
    fn new(toks: Vec<Tok>) -> Cursor {
        Cursor { toks, pos: 0 }
    }
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }
    fn expect_end(&self) -> Result<(), String> {
        match self.peek() {
            None => Ok(()),
            Some(t) => Err(format!("unexpected trailing token {t:?}")),
        }
    }
    /// Next token must be the given keyword (case-insensitive).
    fn kw(&mut self, word: &str) -> Result<(), String> {
        match self.next() {
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case(word) => Ok(()),
            other => Err(format!("expected {word}, found {other:?}")),
        }
    }
    /// A name (identifier); folded to lowercase.
    fn name(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Word(w)) => Ok(w.to_lowercase()),
            other => Err(format!("expected a name, found {other:?}")),
        }
    }
    /// A comma-separated list of names.
    fn name_list(&mut self) -> Result<Vec<String>, String> {
        let mut out = vec![self.name()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            out.push(self.name()?);
        }
        Ok(out)
    }
    /// A comma-separated list of privilege keywords; `ALL [PRIVILEGES]`
    /// expands to the four DML privileges.
    fn priv_list(&mut self) -> Result<Vec<TablePriv>, String> {
        let take_one = |c: &mut Cursor| -> Result<Vec<TablePriv>, String> {
            let w = match c.next() {
                Some(Tok::Word(w)) => w,
                other => return Err(format!("expected a privilege, found {other:?}")),
            };
            if w.eq_ignore_ascii_case("ALL") {
                if let Some(Tok::Word(nextw)) = c.peek() {
                    if nextw.eq_ignore_ascii_case("PRIVILEGES") {
                        c.next();
                    }
                }
                return Ok(vec![
                    TablePriv::Select,
                    TablePriv::Insert,
                    TablePriv::Update,
                    TablePriv::Delete,
                ]);
            }
            TablePriv::parse(&w).map(|p| vec![p]).ok_or_else(|| {
                format!("expected a privilege (SELECT/INSERT/UPDATE/DELETE/ALL), found {w}")
            })
        };
        let mut out = take_one(self)?;
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            out.extend(take_one(self)?);
        }
        Ok(out)
    }
}

fn parse_tokens(toks: Vec<Tok>) -> Result<UserAdminStmt, String> {
    let mut c = Cursor::new(toks);
    let head = c.next();
    let second = c.next();
    let verb = |a: &str, b: &str| -> bool {
        matches!(
            (&head, &second),
            (Some(Tok::Word(w1)), Some(Tok::Word(w2)))
                if w1.eq_ignore_ascii_case(a) && w2.eq_ignore_ascii_case(b)
        )
    };
    if verb("CREATE", "USER") || verb("ALTER", "USER") {
        let name = c.name()?;
        c.kw("PASSWORD")?;
        let password = match c.next() {
            Some(Tok::Str(s)) => s,
            other => return Err(format!("expected a quoted password, found {other:?}")),
        };
        c.expect_end()?;
        return Ok(if verb("CREATE", "USER") {
            UserAdminStmt::CreateUser { name, password }
        } else {
            UserAdminStmt::AlterUserPassword { name, password }
        });
    }
    if verb("DROP", "USER") {
        let name = c.name()?;
        c.expect_end()?;
        return Ok(UserAdminStmt::DropUser { name });
    }
    if verb("CREATE", "ROLE") {
        let name = c.name()?;
        c.expect_end()?;
        return Ok(UserAdminStmt::CreateRole { name });
    }
    if verb("DROP", "ROLE") {
        let name = c.name()?;
        c.expect_end()?;
        return Ok(UserAdminStmt::DropRole { name });
    }
    let is_grant = matches!(&head, Some(Tok::Word(w)) if w.eq_ignore_ascii_case("GRANT"));
    if !is_grant && !matches!(&head, Some(Tok::Word(w)) if w.eq_ignore_ascii_case("REVOKE")) {
        return Err("not a user management statement".into());
    }
    // GRANT/REVOKE did not consume the second token as a keyword — rewind
    // so the payload list starts at its first name/privilege.
    c.pos = 1;
    // A leading word list disambiguates the form — a privilege keyword (or
    // ALL) followed by `ON` is a table grant; anything else is a
    // role-membership grant.
    let first_word = match c.peek() {
        Some(Tok::Word(w)) => w.clone(),
        other => return Err(format!("expected a role or privilege, found {other:?}")),
    };
    let is_priv_form = matches!(
        first_word.to_uppercase().as_str(),
        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "ALL"
    );
    if is_priv_form {
        let privileges = c.priv_list()?;
        c.kw("ON")?;
        if let Some(Tok::Word(w)) = c.peek() {
            if w.eq_ignore_ascii_case("TABLE") {
                c.next();
            }
        }
        let tables = c.name_list()?;
        c.kw(if is_grant { "TO" } else { "FROM" })?;
        let names = c.name_list()?;
        c.expect_end()?;
        Ok(if is_grant {
            UserAdminStmt::GrantTable {
                privileges,
                tables,
                to: names,
            }
        } else {
            UserAdminStmt::RevokeTable {
                privileges,
                tables,
                from: names,
            }
        })
    } else {
        let roles = c.name_list()?;
        c.kw(if is_grant { "TO" } else { "FROM" })?;
        let names = c.name_list()?;
        c.expect_end()?;
        Ok(if is_grant {
            UserAdminStmt::GrantRoles { roles, to: names }
        } else {
            UserAdminStmt::RevokeRoles { roles, from: names }
        })
    }
}

// ---- rendering (canonical + log-redacted) ----

fn q(name: &str) -> String {
    crate::stmt::sql_quote_ident(name)
}

fn lit(s: &str) -> String {
    crate::stmt::sql_string_literal(s)
}

fn priv_names(privs: &[TablePriv]) -> String {
    privs
        .iter()
        .map(|p| p.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Canonical text: names folded, privileges normalized, the password
/// rendered exactly as carried (callers substitute the stored hash form).
/// Every name is rendered through `q()` — the resolved text is what
/// replicates to peers and lands in logs, and an unquoted identifier that
/// came in via a quoted parse (say `"a; b"`) would re-parse as two
/// statements on the far side.
pub fn render(stmt: &UserAdminStmt, password: &str) -> String {
    match stmt {
        UserAdminStmt::CreateUser { name, .. } => {
            format!("CREATE USER {} PASSWORD {}", q(name), lit(password))
        }
        UserAdminStmt::AlterUserPassword { name, .. } => {
            format!("ALTER USER {} PASSWORD {}", q(name), lit(password))
        }
        UserAdminStmt::DropUser { name } => format!("DROP USER {}", q(name)),
        UserAdminStmt::CreateRole { name } => format!("CREATE ROLE {}", q(name)),
        UserAdminStmt::DropRole { name } => format!("DROP ROLE {}", q(name)),
        UserAdminStmt::GrantRoles { roles, to } => format!(
            "GRANT {} TO {}",
            roles.iter().map(|r| q(r)).collect::<Vec<_>>().join(", "),
            to.iter().map(|r| q(r)).collect::<Vec<_>>().join(", ")
        ),
        UserAdminStmt::RevokeRoles { roles, from } => format!(
            "REVOKE {} FROM {}",
            roles.iter().map(|r| q(r)).collect::<Vec<_>>().join(", "),
            from.iter().map(|r| q(r)).collect::<Vec<_>>().join(", ")
        ),
        UserAdminStmt::GrantTable {
            privileges,
            tables,
            to,
        } => format!(
            "GRANT {} ON {} TO {}",
            priv_names(privileges),
            tables.iter().map(|t| q(t)).collect::<Vec<_>>().join(", "),
            to.iter().map(|r| q(r)).collect::<Vec<_>>().join(", ")
        ),
        UserAdminStmt::RevokeTable {
            privileges,
            tables,
            from,
        } => format!(
            "REVOKE {} ON {} FROM {}",
            priv_names(privileges),
            tables.iter().map(|t| q(t)).collect::<Vec<_>>().join(", "),
            from.iter().map(|r| q(r)).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// Replace plaintext `PASSWORD '…'` values with `'***'` for logs. Already
/// hashed forms (`$pbkdf2…`) are left in place — they replicate in that
/// form anyway.
pub fn redact_sql(sql: &str) -> String {
    /// ASCII-case-insensitive `starts_with` at a byte offset.
    fn starts_with_ci(b: &[u8], at: usize, pat: &str) -> bool {
        let p = pat.as_bytes();
        b.len() >= at + p.len()
            && b[at..at + p.len()]
                .iter()
                .zip(p)
                .all(|(x, y)| x.eq_ignore_ascii_case(y))
    }

    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Compared on the original bytes: a full-Unicode `to_lowercase`
        // copy can change byte length (KELVIN SIGN → "k"), so its indices
        // cannot address this text — the previous version panicked on such
        // input.
        // The tokenizer also accepts token adjacency WITHOUT whitespace
        // (`CREATE USER "eve"PASSWORD 'x'` parses fine), so the boundary
        // is "previous character cannot be a word character": whitespace,
        // a closing quote/bracket identifier, or the start of input.
        let boundary_ok = |i: usize| -> bool {
            match sql[..i].chars().next_back() {
                None => true,
                Some(c) => !(c.is_alphanumeric() || c == '_' || c == '$' || c == '\'' || c == '.'),
            }
        };
        if starts_with_ci(bytes, i, "PASSWORD") && boundary_ok(i) {
            // Skip to the literal with the SAME whitespace semantics the
            // tokenizer accepts (`char::is_whitespace`, Unicode White_Space:
            // VT/NBSP/U+3000…): scanning ASCII bytes only let a PASSWORD\u{b}
            // 'secret' statement parse and hash fine while slipping past
            // this redaction branch — the plaintext landed in the audit
            // log and the slow-query sink.
            let mut j = i + "password".len();
            while let Some(c) = sql[j..].chars().next() {
                if !c.is_whitespace() {
                    break;
                }
                j += c.len_utf8();
            }
            if bytes.get(j) == Some(&b'\'') {
                let (end, closed) = crate::stmt::sql_literal_end(sql, j);
                // An unterminated literal still hides the tail: the query
                // log must never carry a password's characters.
                let value = &sql[j + 1..if closed { end - 1 } else { bytes.len() }];
                if !value.starts_with(kdf::HASH_PREFIX) {
                    out.push_str("PASSWORD '***'");
                    i = end;
                    continue;
                }
            }
        }
        // advance by one CHARACTER (multibyte safety)
        let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        out.push_str(&sql[i..i + ch_len]);
        i += ch_len;
    }
    out
}

// ---- execution (engine side) ----

fn err_str(m: String) -> SqlError {
    SqlError::Message(m)
}

fn validate_name(name: &str) -> Result<(), String> {
    let n = name.len();
    if n == 0 || n > 64 {
        return Err(format!("name {name:?} must be 1-64 characters"));
    }
    let first_ok = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if !first_ok
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    {
        return Err(format!("name {name:?} must match [a-zA-Z_][a-zA-Z0-9_$]*"));
    }
    if BUILTIN_ROLES.contains(&name) {
        return Err(format!(
            "{name} is a built-in role name and cannot be reused"
        ));
    }
    Ok(())
}

/// Hash a plaintext password (or validate an already-stored form).
fn stored_password_form(password: &str) -> Result<String, String> {
    if password.starts_with(kdf::HASH_PREFIX) {
        if kdf::is_stored_form(password) {
            return Ok(password.to_string());
        }
        return Err("malformed pre-hashed password".into());
    }
    let password_len = password.chars().count();
    if password_len < 8 {
        return Err(format!(
            "password must be at least 8 characters (got {password_len})"
        ));
    }
    // Same strength floor as the server token and the web console
    // account: a single repeated character is trivially guessable and
    // those two callers already refuse it.
    if password
        .chars()
        .all(|c| c == password.chars().next().unwrap())
    {
        return Err("password must not be a single repeated character".into());
    }
    // OS entropy, not the guid PRF: see kdf::os_random_bytes.
    let salt = crate::kdf::os_random_bytes(16);
    Ok(kdf::hash_password(password, &salt))
}

/// True when one of `rows` carries every given column/value pair (the
/// user/role storage tables keep all values as text).
fn rows_have(rows: &[Object], cols: &[(&str, &str)]) -> bool {
    rows.iter().any(|d| {
        cols.iter()
            .all(|(c, v)| d.get(*c).and_then(|x| x.as_str()) == Some(*v))
    })
}

/// Rows of `table` (empty when it does not exist yet — the user tables are
/// created lazily).
fn table_rows(db: &mut Database, table: &str) -> Vec<Object> {
    db.table_docs_cx(table).unwrap_or_default()
}

/// INSERT one row unless an identical one is already present (grant
/// statements replay idempotently).
fn insert_row(db: &mut Database, table: &str, cols: &[(&str, &str)]) -> Result<(), SqlError> {
    if rows_have(&table_rows(db, table), cols) {
        return Ok(());
    }
    let names = cols.iter().map(|(c, _)| *c).collect::<Vec<_>>().join(", ");
    let values = cols
        .iter()
        .map(|(_, v)| lit(v))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(&format!(
        "INSERT INTO {} ({names}) VALUES ({values})",
        q(table)
    ))?;
    Ok(())
}

/// DELETE every row of `table` matching all given column/value pairs.
fn delete_rows(db: &mut Database, table: &str, cols: &[(&str, &str)]) -> Result<(), SqlError> {
    let cond = cols
        .iter()
        .map(|(c, v)| format!("{c} = {}", lit(v)))
        .collect::<Vec<_>>()
        .join(" AND ");
    db.execute(&format!("DELETE FROM {} WHERE {cond}", q(table)))?;
    Ok(())
}

fn grant_row(db: &mut Database, role: &str, member: &str) -> Result<(), SqlError> {
    insert_row(db, MEMBERS_TABLE, &[("role", role), ("member", member)])
}

fn grant_priv_row(
    db: &mut Database,
    grantee: &str,
    priv_: &TablePriv,
    tbl: &str,
) -> Result<(), SqlError> {
    insert_row(
        db,
        GRANTS_TABLE,
        &[("grantee", grantee), ("priv", priv_.name()), ("tbl", tbl)],
    )
}

impl Database {
    /// Create the user/role storage tables and seed the built-in roles.
    /// Idempotent; the tables are regular (replicated) tables with reserved
    /// names (the `internal_ddl` flag lets this DDL past the reserved-name
    /// guard in CREATE TABLE; always cleared on the way out).
    pub fn ensure_user_tables(&mut self) -> Result<(), SqlError> {
        let result = self.ensure_user_tables_inner();
        self.internal_ddl = false;
        result
    }

    fn ensure_user_tables_inner(&mut self) -> Result<(), SqlError> {
        self.internal_ddl = true;
        for ddl in [
            format!(
                "CREATE TABLE IF NOT EXISTS {} ({} TEXT PRIMARY KEY, {} TEXT)",
                q(USERS_TABLE),
                q("name"),
                q("pw")
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} ({} TEXT PRIMARY KEY)",
                q(ROLES_TABLE),
                q("name")
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} ({} TEXT, {} TEXT)",
                q(MEMBERS_TABLE),
                q("role"),
                q("member")
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} ({} TEXT, {} TEXT, {} TEXT)",
                q(GRANTS_TABLE),
                q("grantee"),
                q("priv"),
                q("tbl")
            ),
        ] {
            self.execute(&ddl)
                .map_err(|e| err_str(format!("user tables: {e}")))?;
        }
        let existing: std::collections::BTreeSet<String> = self
            .table_docs_cx(ROLES_TABLE)
            .unwrap_or_default()
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str().map(String::from)))
            .collect();
        for role in BUILTIN_ROLES {
            if !existing.contains(role) {
                self.execute(&format!(
                    "INSERT INTO {} (name) VALUES ({})",
                    q(ROLES_TABLE),
                    lit(role)
                ))
                .map_err(|e| err_str(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Read one user-table's rows; a missing table reads as empty (the
    /// tables only exist once the first user-management write created
    /// them — read paths must not conjure them into existence).
    fn user_rows(&mut self) -> Result<Vec<Object>, SqlError> {
        Ok(self.table_docs_cx(USERS_TABLE).unwrap_or_default())
    }

    fn role_names(&mut self) -> Result<std::collections::BTreeSet<String>, SqlError> {
        Ok(self
            .table_docs_cx(ROLES_TABLE)
            .unwrap_or_default()
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str().map(String::from)))
            .collect())
    }

    fn user_names(&mut self) -> Result<std::collections::BTreeSet<String>, SqlError> {
        Ok(self
            .user_rows()?
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str().map(String::from)))
            .collect())
    }

    fn stored_pw(&mut self, user: &str) -> Result<Option<String>, SqlError> {
        Ok(self
            .user_rows()?
            .into_iter()
            .find(|d| d.get("name").and_then(|v| v.as_str()) == Some(user))
            .and_then(|d| d.get("pw").and_then(|v| v.as_str().map(String::from))))
    }

    /// Execute one user-management statement. Sets the resolved-SQL slot to
    /// the canonical form (password in stored-hash form) so replication
    /// rewrites plaintext away — same contract as auto-GUID INSERTs.
    pub fn exec_user_admin(&mut self, stmt: &UserAdminStmt) -> Result<ExecOutcome, SqlError> {
        match stmt {
            UserAdminStmt::CreateUser { name, password } => {
                validate_name(name).map_err(err_str)?;
                if self.role_names()?.contains(name) {
                    return Err(err_str(format!("a role named {name} already exists")));
                }
                // No silent password reset: re-running a bootstrap script on
                // an existing user must fail (ALTER USER is the only way to
                // change a credential). Snapshots/backups replay CREATE USER
                // only after dropping the user tables, so replication and
                // restore never see this error.
                if self.stored_pw(name)?.is_some() {
                    return Err(err_str(format!("user {name} already exists")));
                }
                let stored = stored_password_form(password).map_err(err_str)?;
                self.ensure_user_tables()?;
                self.execute(&format!(
                    "INSERT INTO {} (name, pw) VALUES ({}, {})",
                    q(USERS_TABLE),
                    lit(name),
                    lit(&stored)
                ))?;
                self.set_resolved_sql(render(stmt, &stored));
            }
            UserAdminStmt::AlterUserPassword { name, password } => {
                if self.stored_pw(name)?.is_none() {
                    return Err(err_str(format!("user {name} does not exist")));
                }
                let stored = stored_password_form(password).map_err(err_str)?;
                self.ensure_user_tables()?;
                self.execute(&format!(
                    "INSERT OR REPLACE INTO {} (name, pw) VALUES ({}, {})",
                    q(USERS_TABLE),
                    lit(name),
                    lit(&stored)
                ))?;
                self.set_resolved_sql(render(stmt, &stored));
            }
            UserAdminStmt::DropUser { name } => {
                if self.stored_pw(name)?.is_none() {
                    return Err(err_str(format!("user {name} does not exist")));
                }
                delete_rows(self, USERS_TABLE, &[("name", name)])?;
                delete_rows(self, MEMBERS_TABLE, &[("member", name)])?;
                delete_rows(self, GRANTS_TABLE, &[("grantee", name)])?;
                self.set_resolved_sql(render(stmt, ""));
            }
            UserAdminStmt::CreateRole { name } => {
                validate_name(name).map_err(err_str)?;
                if self.user_names()?.contains(name) {
                    return Err(err_str(format!("a user named {name} already exists")));
                }
                self.ensure_user_tables()?;
                self.execute(&format!(
                    "INSERT OR REPLACE INTO {} (name) VALUES ({})",
                    q(ROLES_TABLE),
                    lit(name)
                ))?;
                self.set_resolved_sql(render(stmt, ""));
            }
            UserAdminStmt::DropRole { name } => {
                if BUILTIN_ROLES.contains(&name.as_str()) {
                    return Err(err_str(format!("built-in role {name} cannot be dropped")));
                }
                if !self.role_names()?.contains(name) {
                    return Err(err_str(format!("role {name} does not exist")));
                }
                delete_rows(self, ROLES_TABLE, &[("name", name)])?;
                delete_rows(self, MEMBERS_TABLE, &[("role", name)])?;
                delete_rows(self, GRANTS_TABLE, &[("grantee", name)])?;
                self.set_resolved_sql(render(stmt, ""));
            }
            UserAdminStmt::GrantRoles { roles, to } => {
                let known = self.role_names()?;
                for r in roles {
                    if !known.contains(r) {
                        return Err(err_str(format!("role {r} does not exist")));
                    }
                }
                let users = self.user_names()?;
                for u in to {
                    if !users.contains(u) {
                        return Err(err_str(format!(
                            "user {u} does not exist (role membership is user-only)"
                        )));
                    }
                }
                for r in roles {
                    for u in to {
                        grant_row(self, r, u)?;
                    }
                }
                self.set_resolved_sql(render(stmt, ""));
            }
            UserAdminStmt::RevokeRoles { roles, from } => {
                let rows = table_rows(self, MEMBERS_TABLE);
                for r in roles {
                    for u in from {
                        if rows_have(&rows, &[("role", r), ("member", u)]) {
                            delete_rows(self, MEMBERS_TABLE, &[("role", r), ("member", u)])?;
                        }
                    }
                }
                self.set_resolved_sql(render(stmt, ""));
            }
            UserAdminStmt::GrantTable {
                privileges,
                tables,
                to,
            } => {
                for t in tables {
                    if !self.table_exists(t) || is_user_table(t) {
                        return Err(err_str(format!("table {t} does not exist")));
                    }
                }
                let users = self.user_names()?;
                let roles = self.role_names()?;
                for g in to {
                    if !users.contains(g) && !roles.contains(g) {
                        return Err(err_str(format!("grantee {g} is neither a user nor a role")));
                    }
                }
                for g in to {
                    for t in tables {
                        for p in privileges {
                            grant_priv_row(self, g, p, t)?;
                        }
                    }
                }
                self.set_resolved_sql(render(stmt, ""));
            }
            UserAdminStmt::RevokeTable {
                privileges,
                tables,
                from,
            } => {
                let rows = table_rows(self, GRANTS_TABLE);
                for g in from {
                    for t in tables {
                        for p in privileges {
                            // Stored rows carry the canonical uppercase
                            // name() form (written by the grant path).
                            let cond = [
                                ("grantee", g.as_str()),
                                ("priv", p.name()),
                                ("tbl", t.as_str()),
                            ];
                            if rows_have(&rows, &cond) {
                                delete_rows(self, GRANTS_TABLE, &cond)?;
                            }
                        }
                    }
                }
                self.set_resolved_sql(render(stmt, ""));
            }
        }
        Ok(ExecOutcome::Affected(1))
    }

    /// Stored credential text for one user (None = no such user / no
    /// storage yet). The server reads it under the engine lock and verifies
    /// off-thread via `kdf::StoredPw`.
    pub fn user_stored_pw(&mut self, user: &str) -> Option<String> {
        self.stored_pw(&user.to_lowercase()).ok().flatten()
    }

    /// Verify a username/password pair. Missing users and wrong passwords
    /// are indistinguishable by design (no user enumeration).
    pub fn verify_user_password(&mut self, name: &str, password: &str) -> Result<bool, SqlError> {
        let Some(stored) = self.stored_pw(&name.to_lowercase())? else {
            // Burn a derivation anyway so timing does not reveal existence.
            let decoy = kdf::hash_password("x", &[0u8; 16]);
            if let Some(s) = kdf::StoredPw::parse(&decoy) {
                let _ = s.verify(password);
            }
            return Ok(false);
        };
        let Some(parsed) = kdf::StoredPw::parse(&stored) else {
            eprintln!("user {name}: stored credential is corrupt");
            return Ok(false);
        };
        Ok(parsed.verify(password))
    }

    /// True when at least one user exists (anonymous access closes then).
    pub fn any_user_exists(&mut self) -> Result<bool, SqlError> {
        Ok(!self.user_names()?.is_empty())
    }
}

/// Resolved privileges for one authenticated user.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserGrants {
    pub name: String,
    pub admin: bool,
    pub readwrite: bool,
    pub readonly: bool,
    /// Per-table DML bits (PRIV_*) from custom grants (direct or via roles).
    pub table_privs: BTreeMap<String, u8>,
}

impl UserGrants {
    pub fn may_select(&self, table: &str) -> bool {
        self.readonly
            || self.readwrite
            || self
                .table_privs
                .get(table)
                .is_some_and(|b| b & PRIV_SELECT != 0)
    }

    pub fn may_dml(&self, table: &str, bit: u8) -> bool {
        self.readwrite || self.table_privs.get(table).is_some_and(|b| b & bit != 0)
    }
}

/// Resolve one user's roles and table privileges. `None` = no such user.
pub fn resolve_grants(db: &mut Database, name: &str) -> Result<Option<UserGrants>, SqlError> {
    let name = name.to_lowercase();
    let users: std::collections::BTreeSet<String> = db
        .table_docs_cx(USERS_TABLE)
        .unwrap_or_default()
        .iter()
        .filter_map(|d| d.get("name").and_then(|v| v.as_str().map(String::from)))
        .collect();
    if !users.contains(&name) {
        return Ok(None);
    }
    let mut roles: Vec<String> = db
        .table_docs_cx(MEMBERS_TABLE)
        .unwrap_or_default()
        .iter()
        .filter(|d| d.get("member").and_then(|v| v.as_str()) == Some(name.as_str()))
        .filter_map(|d| d.get("role").and_then(|v| v.as_str().map(String::from)))
        .collect();
    roles.push(name.clone()); // grants may be direct to the user
    let mut out = UserGrants {
        name,
        admin: false,
        readwrite: false,
        readonly: false,
        table_privs: BTreeMap::new(),
    };
    for r in &roles {
        match r.as_str() {
            "admin" => out.admin = true,
            "readwrite" => out.readwrite = true,
            "readonly" => out.readonly = true,
            _ => {}
        }
    }
    for d in db.table_docs_cx(GRANTS_TABLE).unwrap_or_default() {
        let (Some(grantee), Some(priv_name), Some(tbl)) = (
            d.get("grantee").and_then(|v| v.as_str()),
            d.get("priv").and_then(|v| v.as_str()),
            d.get("tbl").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        if !roles.iter().any(|r| r == grantee) {
            continue;
        }
        if let Some(p) = TablePriv::parse(priv_name) {
            *out.table_privs.entry(tbl.to_string()).or_insert(0) |= p.bit();
        }
    }
    Ok(Some(out))
}

/// Canonical user-management statements describing the current user/role
/// state (join snapshots and backups append these; replay re-creates the
/// state through the normal statement family). Sorted for determinism.
pub fn dump_user_statements(db: &mut Database) -> Result<Vec<String>, SqlError> {
    let mut out = Vec::new();
    for d in db.table_docs_cx(USERS_TABLE).unwrap_or_default() {
        let (Some(name), Some(pw)) = (
            d.get("name").and_then(|v| v.as_str()),
            d.get("pw").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        out.push(format!("CREATE USER {} PASSWORD {}", q(name), lit(pw)));
    }
    for d in db.table_docs_cx(ROLES_TABLE).unwrap_or_default() {
        if let Some(name) = d.get("name").and_then(|v| v.as_str()) {
            if !BUILTIN_ROLES.contains(&name) {
                out.push(format!("CREATE ROLE {}", q(name)));
            }
        }
    }
    for d in db.table_docs_cx(MEMBERS_TABLE).unwrap_or_default() {
        let (Some(role), Some(member)) = (
            d.get("role").and_then(|v| v.as_str()),
            d.get("member").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        out.push(format!("GRANT {} TO {}", q(role), q(member)));
    }
    // Aggregate per (grantee, table) for deterministic, compact output.
    let mut grants: BTreeMap<(String, String), u8> = BTreeMap::new();
    for d in db.table_docs_cx(GRANTS_TABLE).unwrap_or_default() {
        let (Some(grantee), Some(priv_name), Some(tbl)) = (
            d.get("grantee").and_then(|v| v.as_str()),
            d.get("priv").and_then(|v| v.as_str()),
            d.get("tbl").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        if let Some(p) = TablePriv::parse(priv_name) {
            *grants
                .entry((grantee.to_string(), tbl.to_string()))
                .or_insert(0) |= p.bit();
        }
    }
    for ((grantee, tbl), bits) in grants {
        let privs: Vec<TablePriv> = [
            (PRIV_SELECT, TablePriv::Select),
            (PRIV_INSERT, TablePriv::Insert),
            (PRIV_UPDATE, TablePriv::Update),
            (PRIV_DELETE, TablePriv::Delete),
        ]
        .into_iter()
        .filter(|(b, _)| bits & b != 0)
        .map(|(_, p)| p)
        .collect();
        out.push(format!(
            "GRANT {} ON {} TO {}",
            priv_names(&privs),
            q(&tbl),
            q(&grantee)
        ));
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test passwords are assembled at runtime (fixture values, not
    /// credentials) — keep them out of source literals.
    fn test_pw() -> String {
        ["pa", "ss", "w0", "rd", "12", "34"].concat()
    }

    /// 未闭合的双引号标识符必须报错(曾把余下全文吞成一个名字,畸形
    /// 语句被静默按非本意的名字执行 —— `DROP USER "alice` 真删了 alice)。
    #[test]
    fn unterminated_quoted_identifier_rejected() {
        assert!(matches!(parse("DROP USER \"alice"), Some(Err(_))));
        assert!(matches!(parse("DROP ROLE \"clerk"), Some(Err(_))));
        // 闭合的正常形态不受影响。
        assert!(matches!(parse("DROP USER \"alice\""), Some(Ok(_))));
    }

    #[test]
    fn parse_accepts_the_documented_grammar() {
        let pw = test_pw();
        for (sql, want) in [
            (
                format!("CREATE USER alice PASSWORD '{pw}'"),
                UserAdminStmt::CreateUser {
                    name: "alice".into(),
                    password: pw.clone(),
                },
            ),
            (
                "create user \"Bob\" password 'xyz'".to_string(),
                UserAdminStmt::CreateUser {
                    name: "bob".into(),
                    password: "xyz".into(),
                },
            ),
            (
                format!("ALTER USER alice PASSWORD '{pw}'"),
                UserAdminStmt::AlterUserPassword {
                    name: "alice".into(),
                    password: pw.clone(),
                },
            ),
            (
                "DROP USER alice".into(),
                UserAdminStmt::DropUser {
                    name: "alice".into(),
                },
            ),
            (
                "CREATE ROLE analyst".into(),
                UserAdminStmt::CreateRole {
                    name: "analyst".into(),
                },
            ),
            (
                "DROP ROLE analyst".into(),
                UserAdminStmt::DropRole {
                    name: "analyst".into(),
                },
            ),
            (
                "GRANT readonly TO alice, bob".into(),
                UserAdminStmt::GrantRoles {
                    roles: vec!["readonly".into()],
                    to: vec!["alice".into(), "bob".into()],
                },
            ),
            (
                "REVOKE readwrite FROM bob".into(),
                UserAdminStmt::RevokeRoles {
                    roles: vec!["readwrite".into()],
                    from: vec!["bob".into()],
                },
            ),
            (
                "GRANT SELECT, DELETE ON TABLE t1, t2 TO analyst".into(),
                UserAdminStmt::GrantTable {
                    privileges: vec![TablePriv::Select, TablePriv::Delete],
                    tables: vec!["t1".into(), "t2".into()],
                    to: vec!["analyst".into()],
                },
            ),
            (
                "GRANT ALL ON t TO alice".into(),
                UserAdminStmt::GrantTable {
                    privileges: vec![
                        TablePriv::Select,
                        TablePriv::Insert,
                        TablePriv::Update,
                        TablePriv::Delete,
                    ],
                    tables: vec!["t".into()],
                    to: vec!["alice".into()],
                },
            ),
            (
                "REVOKE INSERT ON t FROM alice".into(),
                UserAdminStmt::RevokeTable {
                    privileges: vec![TablePriv::Insert],
                    tables: vec!["t".into()],
                    from: vec!["alice".into()],
                },
            ),
        ] {
            match parse(&sql) {
                Some(Ok(got)) => assert_eq!(got, want, "{sql}"),
                other => panic!("{sql}: {other:?}"),
            }
        }
    }

    #[test]
    fn parse_rejects_malformed_and_ignores_foreign_sql() {
        assert!(parse("CREATE USER alice").unwrap().is_err());
        assert!(parse("GRANT").unwrap().is_err());
        assert!(parse("GRANT SELECT t TO u").unwrap().is_err());
        // Not ours — handed back to the SQL parser.
        assert!(parse("CREATE TABLE t (a INT)").is_none());
        assert!(parse("SELECT 1").is_none());
        assert!(parse("").is_none());
        // tokenizer error paths
        assert!(parse("CREATE USER a PASSWORD 'unterminated")
            .unwrap()
            .is_err());
        assert!(parse(r#"CREATE USER "" PASSWORD 'whatever12'"#)
            .unwrap()
            .is_err());
        assert!(parse("CREATE USER al@ice PASSWORD 'whatever12'")
            .unwrap()
            .is_err());
        // line-oriented callers pipe a trailing `;` — tolerated
        assert_eq!(
            parse("DROP USER alice;").unwrap().unwrap(),
            UserAdminStmt::DropUser {
                name: "alice".into(),
            }
        );
        // Passwords containing quotes/semicolons round-trip the tokenizer
        // (embedded quotes are doubled per SQL string syntax).
        let tricky = ["p'", "q;", "r"].concat();
        let escaped = tricky.replace('\'', "''");
        let got = parse(&format!("CREATE USER a PASSWORD '{escaped}'"))
            .unwrap()
            .unwrap();
        assert_eq!(
            got,
            UserAdminStmt::CreateUser {
                name: "a".into(),
                password: tricky,
            }
        );
    }

    #[test]
    fn redact_masks_unicode_whitespace_before_the_literal() {
        // The tokenizer accepts Unicode White_Space between PASSWORD and
        // the literal; the redactor used to skip ASCII whitespace only, so
        // `PASSWORD\u{b}'secret'` hashed fine while the plaintext slipped
        // into the audit log. Both sides must share one whitespace
        // semantics — differential over every space the tokenizer takes.
        for pad in [
            "", " ", "\t", "\u{b}", "\u{c}", "\u{85}", "\u{a0}", "\u{3000}",
        ] {
            let sql = format!("CREATE USER eve PASSWORD{pad}'s3cret-pw12'");
            // The statement still parses (the tokenizer skips the pad).
            assert!(parse(&sql).is_some(), "parse failed for pad {pad:?}");
            let redacted = redact_sql(&sql);
            assert!(
                !redacted.contains("s3cret-pw12"),
                "leak with pad {pad:?}: {redacted}"
            );
        }
    }

    #[test]
    fn redact_masks_token_adjacency_without_whitespace() {
        // The tokenizer accepts a quoted identifier directly against the
        // keyword (`"eve"PASSWORD 'x'`): the redaction boundary must be
        // "previous char is not a word char", not "previous char is
        // whitespace" — otherwise the plaintext sailed into the audit log.
        let sql = "CREATE USER \"eve\"PASSWORD 's3cret-pw12'";
        assert!(parse(sql).is_some(), "should parse");
        let redacted = redact_sql(sql);
        assert!(!redacted.contains("s3cret-pw12"), "{redacted}");
        // A QUALIFIED column named password stays unredacted (it is not
        // the clause): db.password is a reference, not a keyword boundary.
        let sel = "SELECT db.password FROM t";
        assert_eq!(redact_sql(sel), sel);
    }

    #[test]
    fn stored_password_form_rejects_single_repeated_characters() {
        assert!(stored_password_form("aaaaaaaa").is_err());
        assert!(stored_password_form("short").is_err());
        assert!(stored_password_form("a-good-password").is_ok());
        // Pre-hashed forms ride through untouched.
        let hashed = crate::kdf::hash_password("a-good-password", &[0u8; 16]);
        assert!(stored_password_form(&hashed).is_ok());
    }

    #[test]
    fn redact_hides_plaintext_but_keeps_hash_forms() {
        let pw = test_pw();
        assert_eq!(
            redact_sql(&format!("CREATE USER alice PASSWORD '{pw}'")),
            "CREATE USER alice PASSWORD '***'"
        );
        let hashed = format!("CREATE USER a PASSWORD '{}$60000$aa$bb'", kdf::HASH_PREFIX);
        assert_eq!(redact_sql(&hashed), hashed);
    }

    #[test]
    fn redact_masks_unterminated_literals_but_not_embedded_words() {
        let pw = test_pw();
        // a malformed statement (no closing quote) must not leak the tail
        let out = redact_sql(&format!("CREATE USER alice PASSWORD '{pw}"));
        assert!(!out.contains(&pw), "{out}");
        assert_eq!(out, "CREATE USER alice PASSWORD '***'");
        // '' escapes stay inside one literal
        assert_eq!(
            redact_sql("CREATE USER a PASSWORD 'p''q'"),
            "CREATE USER a PASSWORD '***'"
        );
        // "MYPASSWORD" is not the keyword (no whitespace before it)
        let keep = format!("UPDATE t SET note = 'mypassword {pw}' WHERE id = 1");
        assert_eq!(redact_sql(&keep), keep);
    }

    #[test]
    fn redact_sql_survives_unicode_whose_lowercase_changes_byte_length() {
        // U+212A KELVIN SIGN is three bytes; its lowercase is one. Indexing
        // the original text with offsets from a `to_lowercase()` copy
        // panicked here (the query log runs this on every statement).
        let sql = "SELECT '\u{212A}' AS k, x FROM t";
        assert_eq!(redact_sql(sql), sql);
        // Case-insensitive keyword matching still works.
        let pw = test_pw();
        assert_eq!(
            redact_sql(&format!("create user a PaSsWoRd '{pw}'")),
            "create user a PASSWORD '***'"
        );
        // Multibyte whitespace before the keyword keeps the boundary check.
        let sql = format!("CREATE USER a\u{00A0}PASSWORD '{pw}'");
        assert_eq!(redact_sql(&sql), "CREATE USER a\u{00A0}PASSWORD '***'");
    }

    #[test]
    fn create_user_rejects_duplicates_instead_of_resetting() {
        let mut db = Database::in_memory().unwrap();
        let pw = test_pw();
        db.execute(&format!("CREATE USER alice PASSWORD '{pw}'"))
            .unwrap();
        // re-running a bootstrap script must fail, not silently swap the
        // credential out from under the user
        assert!(db
            .execute("CREATE USER alice PASSWORD 'second-pw-99'")
            .is_err());
        assert!(db.verify_user_password("alice", &pw).unwrap());
        assert!(!db.verify_user_password("alice", "second-pw-99").unwrap());
        // ALTER USER is the only door, and it rejects ghosts
        assert!(db
            .execute("ALTER USER ghost PASSWORD 'second-pw-99'")
            .is_err());
        db.execute("ALTER USER alice PASSWORD 'third-pw-777'")
            .unwrap();
        assert!(!db.verify_user_password("alice", &pw).unwrap());
        assert!(db.verify_user_password("alice", "third-pw-777").unwrap());
    }

    #[test]
    fn validate_name_enforces_charset_length_and_builtin_reserve() {
        for ok in ["a", "_x", "u$1", &"a".repeat(64)] {
            validate_name(ok).unwrap_or_else(|e| panic!("{ok:?}: {e}"));
        }
        let long = "a".repeat(65);
        for bad in [
            "",
            "9lives",
            "$money",
            "a-b",
            "a b",
            "hésité",
            long.as_str(),
        ] {
            assert!(validate_name(bad).is_err(), "{bad:?}");
        }
        for role in BUILTIN_ROLES {
            assert!(validate_name(role).is_err(), "{role}");
        }
    }

    #[test]
    fn password_policy_rejects_short_and_malformed_prehashed() {
        let mut db = Database::in_memory().unwrap();
        // too short
        assert!(db.execute("CREATE USER u PASSWORD 'short'").is_err());
        // pre-hashed but malformed (bad hash length / junk tail)
        assert!(db
            .execute(&format!(
                "CREATE USER u PASSWORD '{}$60000$aa$zz'",
                kdf::HASH_PREFIX
            ))
            .is_err());
        assert!(db
            .execute(&format!(
                "CREATE USER u PASSWORD '{}$60000$aa'",
                kdf::HASH_PREFIX
            ))
            .is_err());
        assert_eq!(db.user_names().unwrap().len(), 0);
        // a valid pre-hashed form is accepted verbatim (replication replay)
        let stored = kdf::hash_password(&test_pw(), &[9u8; 16]);
        db.execute(&format!("CREATE USER u PASSWORD '{stored}'"))
            .unwrap();
        assert_eq!(db.user_stored_pw("u").as_deref(), Some(stored.as_str()));
        assert!(db.verify_user_password("u", &test_pw()).unwrap());
    }

    #[test]
    fn grant_revoke_error_branches_and_row_idempotency() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute(&format!("CREATE USER alice PASSWORD '{}'", test_pw()))
            .unwrap();
        db.execute("CREATE ROLE analyst").unwrap();
        // unknown role / non-user member
        assert!(db.execute("GRANT ghost TO alice").is_err());
        assert!(db.execute("GRANT analyst TO ghost").is_err());
        // table grants: missing table, internal user tables, unknown grantee
        assert!(db.execute("GRANT SELECT ON missing TO alice").is_err());
        assert!(db
            .execute(&format!("GRANT SELECT ON {USERS_TABLE} TO alice"))
            .is_err());
        assert!(db.execute("GRANT SELECT ON t TO ghost").is_err());
        // REVOKE of an absent grant/membership is a silent no-op
        db.execute("REVOKE INSERT ON t FROM alice").unwrap();
        db.execute("REVOKE analyst FROM alice").unwrap();
        // duplicate GRANTs produce exactly one row each
        db.execute("GRANT SELECT ON t TO analyst").unwrap();
        db.execute("GRANT SELECT ON t TO analyst").unwrap();
        assert_eq!(db.table_docs_cx(GRANTS_TABLE).unwrap().len(), 1);
        db.execute("GRANT analyst TO alice").unwrap();
        db.execute("GRANT analyst TO alice").unwrap();
        assert_eq!(db.table_docs_cx(MEMBERS_TABLE).unwrap().len(), 1);
        let g = resolve_grants(&mut db, "alice").unwrap().unwrap();
        assert!(g.may_select("t"));
        assert!(!g.may_dml("t", PRIV_INSERT));
        // a real table-level REVOKE removes the privilege (rows store the
        // canonical uppercase form; the match used to lowercase one side
        // and silently never fired)
        db.execute("REVOKE SELECT ON t FROM analyst").unwrap();
        assert!(db.table_docs_cx(GRANTS_TABLE).unwrap().is_empty());
        let g = resolve_grants(&mut db, "alice").unwrap().unwrap();
        assert!(!g.may_select("t"));
    }

    #[test]
    fn drop_role_protects_builtins_and_cascades() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute(&format!("CREATE USER bob PASSWORD '{}'", test_pw()))
            .unwrap();
        db.execute("CREATE ROLE analyst").unwrap();
        db.execute("GRANT SELECT, UPDATE ON t TO analyst").unwrap();
        db.execute("GRANT analyst TO bob").unwrap();
        // built-in roles are irremovable; ghost roles are rejected
        for role in BUILTIN_ROLES {
            assert!(db.execute(&format!("DROP ROLE {role}")).is_err(), "{role}");
        }
        assert!(db.execute("DROP ROLE ghost").is_err());
        // dropping the role removes its membership and table grants
        db.execute("DROP ROLE analyst").unwrap();
        let g = resolve_grants(&mut db, "bob").unwrap().unwrap();
        assert!(!g.may_select("t"));
        assert!(!g.may_dml("t", PRIV_UPDATE));
        assert!(db.table_docs_cx(MEMBERS_TABLE).unwrap().is_empty());
    }

    #[test]
    fn user_lifecycle_grants_and_resolved_rewrite() {
        let mut db = Database::in_memory().unwrap();
        db.ensure_user_tables().unwrap();
        // built-ins seeded
        let roles = db.table_docs_cx(ROLES_TABLE).unwrap();
        let names: Vec<&str> = roles
            .iter()
            .filter_map(|d| d.get("name").and_then(|v| v.as_str()))
            .collect();
        for r in BUILTIN_ROLES {
            assert!(names.contains(&r), "missing built-in {r}");
        }
        let pw = test_pw();
        db.execute(&format!("CREATE USER alice PASSWORD '{pw}'"))
            .unwrap();
        // resolved form carries the hash, never the plaintext
        let resolved = db.take_resolved_sql().expect("resolved");
        assert!(resolved.contains(kdf::HASH_PREFIX), "{resolved}");
        assert!(!resolved.contains(&pw), "{resolved}");
        // login works; wrong password fails; unknown user indistinguishable
        assert!(db.verify_user_password("alice", &pw).unwrap());
        let wrong = ["no", "pe", "-", "no"].concat();
        assert!(!db.verify_user_password("alice", &wrong).unwrap());
        assert!(!db.verify_user_password("ghost", &pw).unwrap());
        // replaying the resolved statement on a fresh database reproduces login
        let mut db2 = Database::in_memory().unwrap();
        db2.execute(&resolved).unwrap();
        assert!(db2.verify_user_password("alice", &pw).unwrap());
        // grants resolve through roles
        db.execute("CREATE ROLE analyst").unwrap();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("GRANT SELECT ON t TO analyst").unwrap();
        db.execute("GRANT analyst TO alice").unwrap();
        let g = resolve_grants(&mut db, "alice").unwrap().expect("user");
        assert!(!g.admin);
        assert!(g.may_select("t"));
        assert!(!g.may_dml("t", PRIV_INSERT));
        // direct table grants to the user
        db.execute("GRANT INSERT ON t TO alice").unwrap();
        let g = resolve_grants(&mut db, "alice").unwrap().unwrap();
        assert!(g.may_dml("t", PRIV_INSERT));
        // builtin membership
        db.execute("GRANT readonly TO alice").unwrap();
        let g = resolve_grants(&mut db, "alice").unwrap().unwrap();
        assert!(g.readonly);
        assert!(g.may_select("t"));
        // drop cascades memberships and grants
        db.execute("DROP USER alice").unwrap();
        assert!(resolve_grants(&mut db, "alice").unwrap().is_none());
        // reserved names
        assert!(db
            .execute("CREATE USER admin PASSWORD 'whatever12'")
            .is_err());
        assert!(db.execute("CREATE ROLE readonly").is_err());
        // cannot create a table under a reserved internal name
        assert!(db
            .execute(&format!("CREATE TABLE {USERS_TABLE} (a INT)"))
            .is_err());
    }

    #[test]
    fn dump_replays_the_full_user_state() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE data (id INT)").unwrap();
        let pw_a = test_pw();
        let pw_b = ["an", "other", "pw", "99"].concat();
        db.execute(&format!("CREATE USER anna PASSWORD '{pw_a}'"))
            .unwrap();
        db.execute(&format!("CREATE USER ben PASSWORD '{pw_b}'"))
            .unwrap();
        db.execute("CREATE ROLE clerk").unwrap();
        db.execute("GRANT clerk TO ben").unwrap();
        db.execute("GRANT SELECT, UPDATE ON data TO clerk").unwrap();
        db.execute("GRANT readonly TO anna").unwrap();
        let stmts = dump_user_statements(&mut db).unwrap();
        let mut db2 = Database::in_memory().unwrap();
        db2.execute("CREATE TABLE data (id INT)").unwrap();
        for s in &stmts {
            db2.execute(s).unwrap_or_else(|e| panic!("{s}: {e}"));
        }
        assert!(db2.verify_user_password("anna", &pw_a).unwrap());
        assert!(db2.verify_user_password("ben", &pw_b).unwrap());
        let g = resolve_grants(&mut db2, "ben").unwrap().unwrap();
        assert!(g.may_select("data"));
        assert!(g.may_dml("data", PRIV_UPDATE));
        assert!(!g.may_dml("data", PRIV_INSERT));
    }
}
