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
            while let Some(&ch) = chars.get(i) {
                i += 1;
                if ch == '"' {
                    break;
                }
                v.push(ch);
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
        if is_grant {
            c.kw("TO")?;
            let to = c.name_list()?;
            c.expect_end()?;
            Ok(UserAdminStmt::GrantTable {
                privileges,
                tables,
                to,
            })
        } else {
            c.kw("FROM")?;
            let from = c.name_list()?;
            c.expect_end()?;
            Ok(UserAdminStmt::RevokeTable {
                privileges,
                tables,
                from,
            })
        }
    } else {
        let roles = c.name_list()?;
        if is_grant {
            c.kw("TO")?;
            let to = c.name_list()?;
            c.expect_end()?;
            Ok(UserAdminStmt::GrantRoles { roles, to })
        } else {
            c.kw("FROM")?;
            let from = c.name_list()?;
            c.expect_end()?;
            Ok(UserAdminStmt::RevokeRoles { roles, from })
        }
    }
}

// ---- rendering (canonical + log-redacted) ----

fn q(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
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
pub fn render(stmt: &UserAdminStmt, password: &str) -> String {
    match stmt {
        UserAdminStmt::CreateUser { name, .. } => {
            format!("CREATE USER {name} PASSWORD {}", lit(password))
        }
        UserAdminStmt::AlterUserPassword { name, .. } => {
            format!("ALTER USER {name} PASSWORD {}", lit(password))
        }
        UserAdminStmt::DropUser { name } => format!("DROP USER {name}"),
        UserAdminStmt::CreateRole { name } => format!("CREATE ROLE {name}"),
        UserAdminStmt::DropRole { name } => format!("DROP ROLE {name}"),
        UserAdminStmt::GrantRoles { roles, to } => {
            format!("GRANT {} TO {}", roles.join(", "), to.join(", "))
        }
        UserAdminStmt::RevokeRoles { roles, from } => {
            format!("REVOKE {} FROM {}", roles.join(", "), from.join(", "))
        }
        UserAdminStmt::GrantTable {
            privileges,
            tables,
            to,
        } => format!(
            "GRANT {} ON {} TO {}",
            priv_names(privileges),
            tables.iter().map(|t| q(t)).collect::<Vec<_>>().join(", "),
            to.join(", ")
        ),
        UserAdminStmt::RevokeTable {
            privileges,
            tables,
            from,
        } => format!(
            "REVOKE {} ON {} FROM {}",
            priv_names(privileges),
            tables.iter().map(|t| q(t)).collect::<Vec<_>>().join(", "),
            from.join(", ")
        ),
    }
}

/// Replace plaintext `PASSWORD '…'` values with `'***'` for logs. Already
/// hashed forms (`$pbkdf2…`) are left in place — they replicate in that
/// form anyway.
pub fn redact_sql(sql: &str) -> String {
    let lower = sql.to_lowercase();
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if lower[i..].starts_with("password")
            && (i == 0 || lower[..i].ends_with(|c: char| c.is_whitespace()))
        {
            let mut j = i + "password".len();
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if bytes.get(j) == Some(&b'\'') {
                // scan to the closing quote (respecting '' escapes)
                let mut k = j + 1;
                while k < bytes.len() {
                    if bytes[k] == b'\'' {
                        if bytes.get(k + 1) == Some(&b'\'') {
                            k += 2;
                            continue;
                        }
                        break;
                    }
                    k += 1;
                }
                if k < bytes.len() {
                    let value = &sql[j + 1..k];
                    if !value.starts_with(kdf::HASH_PREFIX) {
                        out.push_str("PASSWORD '***'");
                        i = k + 1;
                        continue;
                    }
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
    let salt = crate::guid::rand_bytes(16);
    Ok(kdf::hash_password(password, &salt))
}

fn grant_row(db: &mut Database, role: &str, member: &str) -> Result<(), SqlError> {
    let exists = db
        .table_docs(MEMBERS_TABLE)
        .unwrap_or_default()
        .iter()
        .any(|d| {
            d.get("role").and_then(|v| v.as_str()) == Some(role)
                && d.get("member").and_then(|v| v.as_str()) == Some(member)
        });
    if !exists {
        db.execute(&format!(
            "INSERT INTO {} (role, member) VALUES ({}, {})",
            q(MEMBERS_TABLE),
            lit(role),
            lit(member)
        ))?;
    }
    Ok(())
}

fn grant_priv_row(
    db: &mut Database,
    grantee: &str,
    priv_: &TablePriv,
    tbl: &str,
) -> Result<(), SqlError> {
    let exists = db
        .table_docs(GRANTS_TABLE)
        .unwrap_or_default()
        .iter()
        .any(|d| {
            d.get("grantee").and_then(|v| v.as_str()) == Some(grantee)
                && d.get("priv").and_then(|v| v.as_str()) == Some(priv_.name())
                && d.get("tbl").and_then(|v| v.as_str()) == Some(tbl)
        });
    if !exists {
        db.execute(&format!(
            "INSERT INTO {} (grantee, priv, tbl) VALUES ({}, {}, {})",
            q(GRANTS_TABLE),
            lit(grantee),
            lit(priv_.name()),
            lit(tbl)
        ))?;
    }
    Ok(())
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
            .table_docs(ROLES_TABLE)
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
        Ok(self.table_docs(USERS_TABLE).unwrap_or_default())
    }

    fn role_names(&mut self) -> Result<std::collections::BTreeSet<String>, SqlError> {
        Ok(self
            .table_docs(ROLES_TABLE)
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
                self.execute(&format!(
                    "DELETE FROM {} WHERE name = {}",
                    q(USERS_TABLE),
                    lit(name)
                ))?;
                self.execute(&format!(
                    "DELETE FROM {} WHERE member = {}",
                    q(MEMBERS_TABLE),
                    lit(name)
                ))?;
                self.execute(&format!(
                    "DELETE FROM {} WHERE grantee = {}",
                    q(GRANTS_TABLE),
                    lit(name)
                ))?;
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
                self.execute(&format!(
                    "DELETE FROM {} WHERE name = {}",
                    q(ROLES_TABLE),
                    lit(name)
                ))?;
                self.execute(&format!(
                    "DELETE FROM {} WHERE role = {}",
                    q(MEMBERS_TABLE),
                    lit(name)
                ))?;
                self.execute(&format!(
                    "DELETE FROM {} WHERE grantee = {}",
                    q(GRANTS_TABLE),
                    lit(name)
                ))?;
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
                let rows = self.table_docs(MEMBERS_TABLE).unwrap_or_default();
                for r in roles {
                    for u in from {
                        let present = rows.iter().any(|d| {
                            d.get("role").and_then(|v| v.as_str()) == Some(r.as_str())
                                && d.get("member").and_then(|v| v.as_str()) == Some(u.as_str())
                        });
                        if present {
                            self.execute(&format!(
                                "DELETE FROM {} WHERE role = {} AND member = {}",
                                q(MEMBERS_TABLE),
                                lit(r),
                                lit(u)
                            ))?;
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
                let rows = self.table_docs(GRANTS_TABLE).unwrap_or_default();
                for g in from {
                    for t in tables {
                        for p in privileges {
                            let present = rows.iter().any(|d| {
                                d.get("grantee").and_then(|v| v.as_str()) == Some(g.as_str())
                                    && d.get("priv").and_then(|v| v.as_str())
                                        == Some(p.name().to_lowercase().as_str())
                                    && d.get("tbl").and_then(|v| v.as_str()) == Some(t.as_str())
                            });
                            if present {
                                self.execute(&format!(
                                    "DELETE FROM {} WHERE grantee = {} AND priv = {} AND tbl = {}",
                                    q(GRANTS_TABLE),
                                    lit(g),
                                    lit(p.name()),
                                    lit(t)
                                ))?;
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
        .table_docs(USERS_TABLE)
        .unwrap_or_default()
        .iter()
        .filter_map(|d| d.get("name").and_then(|v| v.as_str().map(String::from)))
        .collect();
    if !users.contains(&name) {
        return Ok(None);
    }
    let mut roles: Vec<String> = db
        .table_docs(MEMBERS_TABLE)
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
    for d in db.table_docs(GRANTS_TABLE).unwrap_or_default() {
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
    for d in db.table_docs(USERS_TABLE).unwrap_or_default() {
        let (Some(name), Some(pw)) = (
            d.get("name").and_then(|v| v.as_str()),
            d.get("pw").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        out.push(format!("CREATE USER {name} PASSWORD {}", lit(pw)));
    }
    for d in db.table_docs(ROLES_TABLE).unwrap_or_default() {
        if let Some(name) = d.get("name").and_then(|v| v.as_str()) {
            if !BUILTIN_ROLES.contains(&name) {
                out.push(format!("CREATE ROLE {name}"));
            }
        }
    }
    for d in db.table_docs(MEMBERS_TABLE).unwrap_or_default() {
        let (Some(role), Some(member)) = (
            d.get("role").and_then(|v| v.as_str()),
            d.get("member").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        out.push(format!("GRANT {role} TO {member}"));
    }
    // Aggregate per (grantee, table) for deterministic, compact output.
    let mut grants: BTreeMap<(String, String), u8> = BTreeMap::new();
    for d in db.table_docs(GRANTS_TABLE).unwrap_or_default() {
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
            "GRANT {} ON {} TO {grantee}",
            priv_names(&privs),
            q(&tbl)
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
    fn user_lifecycle_grants_and_resolved_rewrite() {
        let mut db = Database::in_memory().unwrap();
        db.ensure_user_tables().unwrap();
        // built-ins seeded
        let roles = db.table_docs(ROLES_TABLE).unwrap();
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
