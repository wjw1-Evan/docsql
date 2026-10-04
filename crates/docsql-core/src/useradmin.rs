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
use std::collections::{BTreeMap, BTreeSet};

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
        /// Column-restricted SELECT (`GRANT SELECT (a, b) ON t`). Empty =
        /// all columns. SELECT grants only (enforced at exec time).
        cols: Vec<String>,
        /// Row filter (`GRANT SELECT ON t WHERE <expr>`): predicate text
        /// stored verbatim, evaluated per row for non-admin connections.
        row_filter: Option<String>,
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
    LParen,
    RParen,
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
            // 被静默按非本意的名字执行。`""` 是 `"` 的转义(SQL 标准
            // 双写),与控制台 `sql_quote_ident` 的产出一致 —— 不实现
            // 双写时含引号的表名的 GRANT 永远解析失败。
            let mut closed = false;
            while let Some(&ch) = chars.get(i) {
                i += 1;
                if ch == '"' {
                    if chars.get(i) == Some(&'"') {
                        v.push('"');
                        i += 1;
                        continue;
                    }
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
        if c == '(' {
            out.push(Tok::LParen);
            i += 1;
            continue;
        }
        if c == ')' {
            out.push(Tok::RParen);
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
    // Row-filter grant form: `GRANT SELECT ON t WHERE <predicate> TO u`.
    // The predicate is arbitrary expression TEXT the fixed tokenizer
    // cannot carry (operators, parens, dotted names), so it is captured
    // verbatim before tokenization: the SPLIT POINT is the LAST top-level
    // `TO` (quote/paren aware) — grantees never contain TO, and a bare
    // column named `to` inside the predicate always precedes it.
    if starts_with_word(trimmed, "GRANT")
        && find_top_level_word(trimmed, "WHERE", 0, false).is_some()
        // Word-level ON detection, not a literal " ON " substring: scripts
        // freely use newlines/tabs between clauses, and the tokenizer
        // accepts any Unicode whitespace — a substring prefilter rejected
        // legal multi-line grants with a misleading tokenizer error. The
        // quote/paren-aware word scan still keeps a role literally named
        // WHERE/ON from misrouting.
        && find_top_level_word(trimmed, "ON", 0, true).is_some()
    {
        return Some(parse_grant_with_filter(trimmed));
    }
    Some(tokenize(trimmed).and_then(parse_tokens))
}

fn starts_with_word(sql: &str, word: &str) -> bool {
    sql.split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case(word))
}

/// Quote/paren-aware scan for a word token at top level; returns the byte
/// offset of the word's start.
fn find_top_level_word(sql: &str, word: &str, from: usize, last: bool) -> Option<usize> {
    let b = sql.as_bytes();
    let mut i = from;
    let mut depth = 0usize;
    let mut hit: Option<usize> = None;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                // '' is the escaped quote (SQL doubling).
                i += 1;
                while i < b.len() {
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'"' {
                        if b.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && is_word_start(b, i) => {
                let start = i;
                while i < b.len() && is_word_byte(b[i]) {
                    i += 1;
                }
                if b[start..i].eq_ignore_ascii_case(word.as_bytes()) {
                    hit = Some(start);
                    if !last {
                        return hit;
                    }
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    hit
}

fn is_word_start(b: &[u8], i: usize) -> bool {
    b[i].is_ascii_alphabetic() || b[i] == b'_' || b[i] == b'$'
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

fn parse_grant_with_filter(sql: &str) -> Result<UserAdminStmt, String> {
    let where_at = find_top_level_word(sql, "WHERE", 0, false)
        .ok_or_else(|| "WHERE without a position".to_string())?;
    // The clause marker is the first top-level TO that still has a grantee
    // after it. A user/role literally named `to` makes the LAST TO the
    // grantee itself (`… WHERE x > 0 TO to`): picking the last TO mistook
    // the grantee for the marker and the parse failed on a legal grant.
    let b = sql.as_bytes();
    let mut to_at: Option<usize> = None;
    let mut scan = where_at;
    while let Some(p) = find_top_level_word(sql, "TO", scan, false) {
        let mut k = p + 2;
        loop {
            while k < b.len() && b[k].is_ascii_whitespace() {
                k += 1;
            }
            if b.get(k) == Some(&b'-') && b.get(k + 1) == Some(&b'-') {
                k = (k..b.len())
                    .find(|&i| b[i] == b'\n')
                    .map(|i| i + 1)
                    .unwrap_or(b.len());
                continue;
            }
            if b.get(k) == Some(&b'/') && b.get(k + 1) == Some(&b'*') {
                k += 2;
                while k + 1 < b.len() && !(b[k] == b'*' && b[k + 1] == b'/') {
                    k += 1;
                }
                k = (k + 2).min(b.len());
                continue;
            }
            break;
        }
        if k < b.len() && (is_word_start(b, k) || b[k] == b'"') {
            to_at = Some(p);
            break;
        }
        scan = p + 2;
    }
    let to_at = to_at
        .ok_or_else(|| "GRANT ... WHERE requires TO <grantee> after the predicate".to_string())?;
    let predicate = sql[where_at + "WHERE".len()..to_at].trim();
    if predicate.is_empty() {
        return Err("row filter predicate is empty".into());
    }
    let prefix = &sql[..where_at];
    let suffix = &sql[to_at..]; // starts with TO
    let mut toks = tokenize(prefix)?;
    toks.extend(tokenize(suffix)?);
    match parse_tokens(toks)? {
        UserAdminStmt::GrantTable {
            privileges,
            tables,
            to,
            cols,
            ..
        } => Ok(UserAdminStmt::GrantTable {
            privileges,
            tables,
            to,
            cols,
            row_filter: Some(predicate.to_string()),
        }),
        other => Err(format!("row filters apply to table grants, not {other:?}")),
    }
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
    /// A table name for GRANT/REVOKE targets: case PRESERVED. The engine
    /// catalog is case-sensitive, so a grant stored lowercased could never
    /// match a mixed-case table (the grant failed loudly, but roles were
    /// useless against such tables). Resolve-side matching is
    /// case-insensitive, so rows written by older versions keep working.
    fn name_raw(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Word(w)) => Ok(w),
            other => Err(format!("expected a name, found {other:?}")),
        }
    }
    /// Comma-separated list of case-preserved table names.
    fn name_list_raw(&mut self) -> Result<Vec<String>, String> {
        let mut out = vec![self.name_raw()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.next();
            out.push(self.name_raw()?);
        }
        Ok(out)
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
        // Column-restricted form: `GRANT SELECT (a, b) ON t ...` — the
        // list attaches to the whole privilege list (SELECT-only is
        // enforced at exec time).
        let mut cols = Vec::new();
        if matches!(c.peek(), Some(Tok::LParen)) {
            c.next();
            cols = c.name_list_raw()?;
            match c.next() {
                Some(Tok::RParen) => {}
                other => return Err(format!("expected ')' after column list, found {other:?}")),
            }
        }
        c.kw("ON")?;
        if let Some(Tok::Word(w)) = c.peek() {
            if w.eq_ignore_ascii_case("TABLE") {
                c.next();
            }
        }
        let tables = c.name_list_raw()?;
        c.kw(if is_grant { "TO" } else { "FROM" })?;
        let names = c.name_list()?;
        c.expect_end()?;
        Ok(if is_grant {
            UserAdminStmt::GrantTable {
                privileges,
                tables,
                to: names,
                cols,
                row_filter: None,
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
            cols,
            row_filter,
        } => format!(
            "GRANT {}{} ON {}{} TO {}",
            priv_names(privileges),
            if cols.is_empty() {
                String::new()
            } else {
                format!(
                    " ({})",
                    cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ")
                )
            },
            tables.iter().map(|t| q(t)).collect::<Vec<_>>().join(", "),
            match row_filter {
                Some(pred) => format!(" WHERE {pred}"),
                None => String::new(),
            },
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
        // String literals are opaque: a PASSWORD inside quoted data is user
        // text, not the keyword — redacting there rewrote the audit entry
        // into a statement that never executed. Skip whole literals with
        // the same scanner the rest of the pipeline uses.
        if bytes[i] == b'\'' {
            let (end, _closed) = crate::stmt::sql_literal_end(sql, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        // Compared on the original bytes: a full-Unicode `to_lowercase`
        // copy can change byte length (KELVIN SIGN → "k"), so its indices
        // cannot address this text — the previous version panicked on such
        // input.
        // The tokenizer also accepts token adjacency WITHOUT whitespace
        // (`CREATE USER "eve"PASSWORD 'x'` parses fine — the closing dquote
        // does NOT end keyword matching), so the boundary stays "previous
        // character cannot be a word character or a closing single quote".
        let boundary_ok = |i: usize| -> bool {
            match sql[..i].chars().next_back() {
                None => true,
                // ASCII word characters only, mirroring the tokenizer's
                // charset: a non-ASCII letter before PASSWORD can only come
                // from a statement the tokenizer will reject, and the audit
                // log must mask the plaintext anyway (fail closed).
                Some(c) => {
                    !(c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '\'' || c == '.')
                }
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
                // log must never carry a password's characters. The hash
                // exemption needs the FULL stored shape (prefix alone would
                // let a hostile prefix smuggle a real secret through).
                let value = &sql[j + 1..if closed { end - 1 } else { bytes.len() }];
                if !kdf::is_stored_form(value) {
                    out.push_str("PASSWORD '***'");
                    i = end;
                    continue;
                }
            } else {
                // Fail-closed for malformed forms: PASSWORD followed by
                // something that is not a plain literal (PASSWORD='x',
                // PASSWORD /*c*/ 'x', PASSWORD N'x') will be REJECTED by the
                // tokenizer — but rejected statements still reach the audit
                // log, and the user's intended plaintext rides in a literal
                // shortly after the keyword. Scan a short assignment-ish
                // prefix (whitespace, '=', N/n, comments) for that literal
                // and mask it; any other character stops the scan so
                // ordinary queries mentioning a `password` COLUMN keep
                // their literals verbatim.
                let mut k = j;
                let mut mask_end: Option<usize> = None;
                // `(` opens a mistyped group (`PASSWORD ('pw')`): skip it so
                // the literal inside is found, and remember to swallow its
                // closing paren into the mask.
                let mut paren = false;
                loop {
                    let rest = &sql[k..];
                    let Some(c) = rest.chars().next() else { break };
                    if c.is_whitespace() || c == '=' || c == 'N' || c == 'n' || c == '(' {
                        if c == '(' {
                            paren = true;
                        }
                        k += c.len_utf8();
                    } else if rest.starts_with("/*") {
                        match rest.find("*/") {
                            Some(p) => k += p + 2,
                            None => break,
                        }
                    } else if rest.starts_with("--") {
                        match rest.find('\n') {
                            Some(p) => k += p + 1,
                            None => break,
                        }
                    } else if c == '\'' {
                        let (end, closed) = crate::stmt::sql_literal_end(sql, k);
                        let value = &sql[k + 1..if closed { end - 1 } else { bytes.len() }];
                        if !kdf::is_stored_form(value) {
                            let mut end = end;
                            if paren && sql.as_bytes().get(end) == Some(&b')') {
                                end += 1;
                            }
                            mask_end = Some(end);
                        }
                        break;
                    } else if c == '"' {
                        // Double-quoted mistyped literal (`PASSWORD "pw"`):
                        // the parser rejects it, so this can only be
                        // intended credential material — mask it. Over-
                        // masking a legal `SELECT password "alias"` costs
                        // audit-log fidelity; under-masking costs the
                        // secret. "" escapes are part of the identifier.
                        let b = sql.as_bytes();
                        let mut e = k + 1;
                        loop {
                            match b[e..].iter().position(|&x| x == b'"') {
                                Some(p) => {
                                    e += p;
                                    if b.get(e + 1) == Some(&b'"') {
                                        e += 2;
                                    } else {
                                        e += 1;
                                        break;
                                    }
                                }
                                None => {
                                    e = b.len();
                                    break;
                                }
                            }
                        }
                        mask_end = Some(e);
                        break;
                    } else if c.is_ascii_alphanumeric() && {
                        // Bare unquoted password (`PASSWORD pw123`): always
                        // rejected by the parser, still plaintext in the
                        // audit log — mask the single word run and stop.
                        // A SQL KEYWORD is not a password (`SELECT password
                        // FROM t` must stay verbatim): read the whole run
                        // and only treat it as credential material when it
                        // is not one of the common continuations of a
                        // column reference.
                        let end = sql[k..]
                            .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                            .map(|p| k + p)
                            .unwrap_or(sql.len());
                        let word = sql[k..end].to_ascii_lowercase();
                        !matches!(
                            word.as_str(),
                            "from"
                                | "where"
                                | "like"
                                | "in"
                                | "not"
                                | "is"
                                | "between"
                                | "and"
                                | "or"
                                | "as"
                                | "desc"
                                | "asc"
                                | "order"
                                | "group"
                                | "select"
                                | "when"
                                | "then"
                                | "else"
                                | "end"
                                | "null"
                                | "escape"
                        )
                    } {
                        let end = sql[k..]
                            .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                            .map(|p| k + p)
                            .unwrap_or(sql.len());
                        mask_end = Some(end);
                        // Keep scanning: a hex-bitmap mistype (`PASSWORD
                        // x'..'`) has its literal right after the bare word,
                        // and stopping here used to leave that literal for
                        // the main loop to copy verbatim.
                        k = end;
                        continue;
                    } else {
                        break;
                    }
                }
                if let Some(end) = mask_end {
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
    // Privilege keywords can never be granted as ROLE names: the GRANT
    // parser reads them as the privilege list, so such a role would be a
    // dead end (creatable, ungrantable).
    if matches!(name, "select" | "insert" | "update" | "delete" | "all") {
        return Err(format!(
            "{name} is a privilege keyword and cannot be used as a name"
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
    cols: &[String],
    row_filter: Option<&str>,
) -> Result<(), SqlError> {
    let cols_json = if cols.is_empty() {
        None
    } else {
        Some(serde_json::to_string(cols).unwrap_or_else(|_| "[]".to_string()))
    };
    let mut fields: Vec<(&str, &str)> =
        vec![("grantee", grantee), ("priv", priv_.name()), ("tbl", tbl)];
    if let Some(cj) = &cols_json {
        fields.push(("cols", cj.as_str()));
    }
    if let Some(pred) = row_filter {
        fields.push(("filter", pred));
    }
    // Dedup is EXACT-SHAPE, never the (grantee, priv, tbl) subset: a plain
    // SELECT grant and a SELECT(id)-with-filter grant are DIFFERENT
    // privilege sets (the plain one voids the restrictions at resolve
    // time), so re-granting one must not swallow the other. Plain rows
    // dedup against plain rows; qualified rows carry their full shape.
    let plain: Vec<(&str, &str)> = fields.iter().take(3).copied().collect();
    if cols.is_empty() && row_filter.is_none() {
        let rows = table_rows(db, GRANTS_TABLE);
        let duplicate = rows.iter().any(|d| {
            plain
                .iter()
                .all(|(c, v)| d.get(*c).and_then(|x| x.as_str()) == Some(*v))
                && d.get("cols").is_none()
                && d.get("filter").is_none()
        });
        if duplicate {
            return Ok(());
        }
        let names = fields
            .iter()
            .map(|(c, _)| *c)
            .collect::<Vec<_>>()
            .join(", ");
        let values = fields
            .iter()
            .map(|(_, v)| lit(v))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(&format!(
            "INSERT INTO {} ({names}) VALUES ({values})",
            q(GRANTS_TABLE)
        ))?;
        return Ok(());
    }
    let rows = table_rows(db, GRANTS_TABLE);
    let duplicate = rows.iter().any(|d| {
        plain
            .iter()
            .all(|(c, v)| d.get(*c).and_then(|x| x.as_str()) == Some(*v))
            && d.get("cols").and_then(|x| x.as_str()) == cols_json.as_deref()
            && d.get("filter").and_then(|x| x.as_str()) == row_filter
    });
    if duplicate {
        return Ok(());
    }
    let names = fields
        .iter()
        .map(|(c, _)| *c)
        .collect::<Vec<_>>()
        .join(", ");
    let values = fields
        .iter()
        .map(|(_, v)| lit(v))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(&format!(
        "INSERT INTO {} ({names}) VALUES ({values})",
        q(GRANTS_TABLE)
    ))?;
    Ok(())
}

impl Database {
    /// True when the named catalog object is a view.
    fn table_is_view(&self, name: &str) -> bool {
        self.object_is_view(name)
    }

    /// Validate a row-filter predicate at GRANT time: it must parse as a
    /// single expression, carry no subqueries (per-row evaluation happens
    /// inside the table scan — a subquery there is both a perf trap and a
    /// scope hazard) and no volatile functions (NEWID()/RAND() would
    /// diverge replay; wall clocks would freeze at grant time through the
    /// fold). Column names are NOT validated against the catalog: the
    /// model is schemaless, and an unknown column evaluates to NULL —
    /// the fail-closed direction (rows hide, never leak).
    pub(crate) fn validate_row_filter(&mut self, _table: &str, pred: &str) -> Result<(), SqlError> {
        let expr = crate::engine::parse_expr_text(pred)
            .map_err(|e| err_str(format!("row filter is not a valid expression: {e}")))?;
        let volatile = |e: &sqlparser::ast::Expr| {
            crate::engine::calls_newid(e) || crate::engine::expr_calls_wall_clock(e)
        };
        fn scan(
            e: &sqlparser::ast::Expr,
            volatile: &dyn Fn(&sqlparser::ast::Expr) -> bool,
        ) -> Option<String> {
            use sqlparser::ast::Expr as E;
            match e {
                E::Subquery(_) | E::Exists { .. } => {
                    Some("subqueries are not supported in row filters".into())
                }
                E::InSubquery { .. } => {
                    Some("IN (SELECT …) is not supported in row filters".into())
                }
                E::AnyOp { .. } | E::AllOp { .. } => {
                    Some("quantified comparisons are not supported in row filters".into())
                }
                other if volatile(other) => Some(
                    "volatile functions (NEWID/RAND/NOW family) are not supported in row filters"
                        .into(),
                ),
                E::Function(f) => {
                    if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                        for a in &list.args {
                            if matches!(
                                a,
                                sqlparser::ast::FunctionArg::Named { .. }
                                    | sqlparser::ast::FunctionArg::ExprNamed { .. }
                            ) {
                                return Some(
                                    "named function arguments are not supported in row filters"
                                        .into(),
                                );
                            }
                        }
                    }
                    // Function args + aggregate FILTER, via the shared
                    // traversal below.
                    crate::engine::child_exprs(e)
                        .into_iter()
                        .find_map(|c| scan(c, volatile))
                }
                // Shared traversal (engine::child_exprs): every container
                // the evaluator can descend into — FLOOR/IS NULL/
                // SUBSTRING/POSITION/… — must be scanned. A narrower
                // hand-rolled list let `FLOOR(RAND())` pass validation and
                // land as a per-row random visibility filter, and let
                // subqueries under those wrappers fail only at SELECT time.
                _ => crate::engine::child_exprs(e)
                    .into_iter()
                    .find_map(|c| scan(c, volatile)),
            }
        }
        if let Some(m) = scan(&expr, &volatile) {
            return Err(err_str(format!("row filter: {m}")));
        }
        Ok(())
    }

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
                cols,
                row_filter,
            } => {
                for t in tables {
                    if !self.table_exists(t) || is_user_table(t) {
                        return Err(err_str(format!("table {t} does not exist")));
                    }
                    // Column lists and row filters are SELECT-only and
                    // table-only: they are evaluated per referenced
                    // column / per scanned row, and a view is itself the
                    // permission boundary for its readers.
                    if (cols.is_empty() && row_filter.is_none())
                        || matches!(privileges.as_slice(), [TablePriv::Select])
                    {
                        // ok
                    } else {
                        return Err(err_str(
                            "column lists and row filters apply to single-privilege SELECT grants only".to_string(),
                        ));
                    }
                    if (cols.is_empty() && row_filter.is_none()) && self.table_is_view(t) {
                        continue;
                    }
                    if self.table_is_view(t) {
                        return Err(err_str(
                            "column lists and row filters apply to tables, not views".to_string(),
                        ));
                    }
                    if let Some(pred) = row_filter {
                        self.validate_row_filter(t, pred)?;
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
                            grant_priv_row(self, g, p, t, cols, row_filter.as_deref())?;
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
    /// Column-restricted SELECT grants: table -> UNION of the granted
    /// column lists (case as granted). Only meaningful when the user has
    /// NO unrestricted SELECT grant on that table (roles/blanket grants
    /// void the restriction — privilege union semantics).
    pub table_cols: BTreeMap<String, Vec<String>>,
    /// Row filters (`GRANT SELECT ON t WHERE <pred>`): table -> every
    /// matching predicate text. OR-joined at enforcement; empty = no
    /// filtering. Admin/readonly/readwrite roles never consult these for
    /// their blanket access (see `resolve_grants`).
    pub row_filters: BTreeMap<String, Vec<String>>,
}

impl UserGrants {
    pub fn may_select(&self, table: &str) -> bool {
        self.readonly
            || self.readwrite
            || self
                .table_privs
                .iter()
                .find(|(t, _)| t.eq_ignore_ascii_case(table))
                .is_some_and(|(_, b)| b & PRIV_SELECT != 0)
    }

    pub fn may_dml(&self, table: &str, bit: u8) -> bool {
        self.readwrite
            || self
                .table_privs
                .iter()
                .find(|(t, _)| t.eq_ignore_ascii_case(table))
                .is_some_and(|(_, b)| b & bit != 0)
    }

    /// True when the user's SELECT on this table is column-restricted
    /// (`resolve_grants` drops the entry when an unrestricted grant
    /// exists, so presence in the map IS the restriction).
    pub fn is_restricted_select(&self, table: &str) -> bool {
        self.table_cols
            .keys()
            .any(|t| t.eq_ignore_ascii_case(table))
    }

    /// Granted column list for a restricted table (case as granted).
    pub fn granted_cols(&self, table: &str) -> &[String] {
        self.table_cols
            .iter()
            .find(|(t, _)| t.eq_ignore_ascii_case(table))
            .map(|(_, v)| v.as_slice())
            .unwrap_or(&[])
    }

    /// Row filters for a table (OR-joined at enforcement); empty = none.
    pub fn filters_for(&self, table: &str) -> &[String] {
        self.row_filters
            .iter()
            .find(|(t, _)| t.eq_ignore_ascii_case(table))
            .map(|(_, v)| v.as_slice())
            .unwrap_or(&[])
    }

    /// The active row-filter map for enforcement: filtered table (lower-
    /// cased) -> predicates. Empty unless this user carries explicit
    /// filtered SELECT grants (blanket roles never land here).
    pub fn filter_map(&self) -> std::collections::BTreeMap<String, Vec<String>> {
        self.row_filters
            .iter()
            .map(|(t, v)| (t.to_ascii_lowercase(), v.clone()))
            .collect()
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
        table_cols: BTreeMap::new(),
        row_filters: BTreeMap::new(),
    };
    for r in &roles {
        match r.as_str() {
            "admin" => out.admin = true,
            "readwrite" => out.readwrite = true,
            "readonly" => out.readonly = true,
            _ => {}
        }
    }
    // Unrestricted SELECT grants (blanket roles included) void column
    // restrictions on that table — privileges are a union.
    let mut unrestricted: BTreeSet<String> = BTreeSet::new();
    if out.readonly || out.readwrite {
        // Blanket read access: no column restriction and no row filter can
        // narrow it (filters attach to explicit SELECT grants only).
        return Ok(Some(out));
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
        let Some(p) = TablePriv::parse(priv_name) else {
            continue;
        };
        *out.table_privs.entry(tbl.to_string()).or_insert(0) |= p.bit();
        let cols: Option<Vec<String>> = d
            .get("cols")
            .and_then(|v| v.as_str())
            .and_then(|text| serde_json::from_str(text).ok());
        let filter = d.get("filter").and_then(|v| v.as_str()).map(String::from);
        if p == TablePriv::Select {
            // A cols+filter grant restricts BOTH dimensions; a filter
            // without a column list leaves columns unrestricted, and a
            // plain grant (neither field) is the unrestricted form.
            if let Some(pred) = &filter {
                out.row_filters
                    .entry(tbl.to_string())
                    .or_default()
                    .push(pred.clone());
            }
            if let Some(list) = &cols {
                if !list.is_empty() {
                    let entry = out.table_cols.entry(tbl.to_string()).or_default();
                    for c in list {
                        if !entry.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                            entry.push(c.clone());
                        }
                    }
                }
            }
            if cols.is_none() && filter.is_none() {
                unrestricted.insert(tbl.to_string());
            }
        }
    }
    // Unrestricted SELECT voids both restriction dimensions (privileges
    // are a union — a narrower grant cannot narrow a wider one).
    for t in unrestricted {
        out.table_cols.remove(&t);
        out.row_filters.remove(&t);
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
    // Column lists and row filters do NOT aggregate: they qualify one
    // SELECT grant each and render as their own statements.
    let mut grants: BTreeMap<(String, String), u8> = BTreeMap::new();
    let mut qualified: Vec<(String, String, Vec<String>, Option<String>)> = Vec::new();
    for d in db.table_docs_cx(GRANTS_TABLE).unwrap_or_default() {
        let (Some(grantee), Some(priv_name), Some(tbl)) = (
            d.get("grantee").and_then(|v| v.as_str()),
            d.get("priv").and_then(|v| v.as_str()),
            d.get("tbl").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let cols: Vec<String> = d
            .get("cols")
            .and_then(|v| v.as_str())
            .and_then(|text| serde_json::from_str(text).ok())
            .unwrap_or_default();
        let filter = d.get("filter").and_then(|v| v.as_str()).map(String::from);
        if (cols.is_empty() && filter.is_none())
            || TablePriv::parse(priv_name) != Some(TablePriv::Select)
        {
            if let Some(p) = TablePriv::parse(priv_name) {
                *grants
                    .entry((grantee.to_string(), tbl.to_string()))
                    .or_insert(0) |= p.bit();
            }
            continue;
        }
        qualified.push((grantee.to_string(), tbl.to_string(), cols, filter));
    }
    qualified.sort();
    for (grantee, tbl, cols, pred) in qualified {
        let col_part = if cols.is_empty() {
            String::new()
        } else {
            format!(
                " ({})",
                cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ")
            )
        };
        let filter_part = match pred {
            Some(pred) => format!(" WHERE {pred}"),
            None => String::new(),
        };
        out.push(format!(
            "GRANT SELECT{col_part} ON {}{filter_part} TO {}",
            q(&tbl),
            q(&grantee)
        ));
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
    fn parse_column_and_filter_grants() {
        // Column-restricted SELECT.
        match parse("GRANT SELECT (id, Name) ON sales TO alice") {
            Some(Ok(UserAdminStmt::GrantTable {
                privileges,
                tables,
                to,
                cols,
                row_filter,
            })) => {
                assert_eq!(privileges, vec![TablePriv::Select]);
                assert_eq!(tables, vec!["sales"]);
                assert_eq!(to, vec!["alice"]);
                assert_eq!(cols, vec!["id", "Name"]);
                assert_eq!(row_filter, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Row filter: predicate captured verbatim (the fixed tokenizer
        // cannot carry operators/dotted names).
        match parse("GRANT SELECT ON sales WHERE region = 'east' AND sales.qty > 0 TO alice") {
            Some(Ok(UserAdminStmt::GrantTable {
                cols, row_filter, ..
            })) => {
                assert!(cols.is_empty());
                assert_eq!(
                    row_filter.as_deref(),
                    Some("region = 'east' AND sales.qty > 0")
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Cols + filter combine; a string literal containing " TO " does
        // not derail the split (quote-aware scan).
        match parse("GRANT SELECT (id) ON t WHERE note = 'send TO bob' TO carol") {
            Some(Ok(UserAdminStmt::GrantTable {
                cols, row_filter, ..
            })) => {
                assert_eq!(cols, vec!["id"]);
                assert_eq!(row_filter.as_deref(), Some("note = 'send TO bob'"));
            }
            other => panic!("unexpected: {other:?}"),
        }
        // REVOKE never takes the filter form (split only runs for GRANT).
        assert!(matches!(
            parse("REVOKE SELECT ON t FROM u"),
            Some(Ok(UserAdminStmt::RevokeTable { .. }))
        ));
        // Missing TO / empty predicate refuse loudly.
        assert!(matches!(
            parse("GRANT SELECT ON t WHERE x > 0"),
            Some(Err(_))
        ));
        assert!(matches!(
            parse("GRANT SELECT ON t WHERE TO u"),
            Some(Err(_))
        ));
    }

    #[test]
    fn render_round_trips_qualified_grants() {
        let stmt = match parse("GRANT SELECT (a, b) ON mytbl WHERE x > 0 TO alice") {
            Some(Ok(s)) => s,
            other => panic!("{other:?}"),
        };
        let text = render(&stmt, "");
        assert_eq!(
            text,
            "GRANT SELECT (\"a\", \"b\") ON \"mytbl\" WHERE x > 0 TO \"alice\""
        );
        // The rendered form re-parses to the same statement.
        match parse(&text) {
            Some(Ok(other)) => assert_eq!(other, stmt),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn row_filter_wall_clocks_fold_and_random_rejects() {
        // Wall-clock calls in a row filter freeze at grant time through the
        // fold — the accepted (pre-existing NOW()) semantics; the journal /
        // fan-out must carry the folded literal, never the raw call, or every
        // replay peer re-stamps its own clock and applies the filter to a
        // different row set. NOW_MS() used to escape the fold entirely (the
        // word `now_ms` never matched the fold's word list while the cheap
        // prefilter substring hit) and rode the grant raw. RAND() is not
        // foldable and must reject loudly.
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT, ts TIMESTAMP)").unwrap();
        db.execute("CREATE USER eve PASSWORD 'pw12345678'").unwrap();
        for sql in [
            "GRANT SELECT ON t WHERE ts >= NOW() TO eve",
            "GRANT SELECT ON t WHERE ts >= NOW_MS() TO eve",
        ] {
            db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let resolved = db.take_resolved_sql().unwrap();
            assert!(resolved.contains("AS TIMESTAMP"), "{resolved}");
            assert!(!resolved.to_uppercase().contains("NOW"), "{resolved}");
        }
        assert!(db
            .execute("GRANT SELECT ON t WHERE ts >= RAND() TO eve")
            .is_err());
    }

    /// 行过滤器的 GRANT 校验曾漏 Expr::Case:子查询藏进 WHEN/THEN/ELSE
    /// 分支即可绕过校验落库,并在受限用户的逐行求值里读取无权表。
    #[test]
    fn row_filter_rejects_subquery_hidden_in_case() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE sales (id INT, region TEXT)")
            .unwrap();
        db.execute("CREATE TABLE secret (x INT)").unwrap();
        db.execute("CREATE USER eve PASSWORD 'pw12345678'").unwrap();
        let sql = "GRANT SELECT ON sales TO eve WHERE \
             (CASE WHEN region = 'east' THEN 1 ELSE (SELECT COUNT(*) FROM secret) END) = 1";
        assert!(
            db.execute(sql).is_err(),
            "a subquery inside CASE must be rejected at GRANT time"
        );
        // 对照:直白形态同样被拒。
        assert!(db
            .execute("GRANT SELECT ON sales TO eve WHERE id IN (SELECT x FROM secret)")
            .is_err());
    }

    /// `PASSWORD=` / `PASSWORD /*c*/ 'x'` / `PASSWORD N'x'` 等畸形形态会被
    /// tokenizer 拒绝,但被拒语句同样进审计日志:掩码必须 fail-closed 地
    /// 盖住其后紧跟的字面量,而普通查询里提到 password 列不受误伤。
    #[test]
    fn redact_masks_malformed_password_assignment_forms() {
        for sql in [
            "CREATE USER eve PASSWORD='s3cret-pw12'",
            "CREATE USER eve PASSWORD /* why */ 's3cret-pw12'",
            "CREATE USER eve PASSWORD N's3cret-pw12'",
        ] {
            let redacted = redact_sql(sql);
            assert!(!redacted.contains("s3cret-pw12"), "{sql} -> {redacted}");
            assert!(redacted.contains("PASSWORD '***'"), "{sql} -> {redacted}");
        }
        // 不是赋值形态:password 列后的普通字面量原样保留。
        let sel = "SELECT password FROM t WHERE note = 'my PASSWORD is fine'";
        assert_eq!(redact_sql(sel), sel);

        // 十六进制位图误打(`x'..'`)与括号组形态(`('..')`):裸词/括号之后
        // 紧跟的字面量曾 fail-open 地整段进审计日志。
        for sql in [
            "CREATE USER eve PASSWORD x's3cret-pw12'",
            "CREATE USER eve PASSWORD ('s3cret-pw12')",
        ] {
            let redacted = redact_sql(sql);
            assert!(!redacted.contains("s3cret-pw12"), "{sql} -> {redacted}");
            assert!(redacted.contains("PASSWORD '***'"), "{sql} -> {redacted}");
        }
        // 关键词续接(合法列引用)不受新扫描影响。
        let sel2 = "SELECT password FROM t";
        assert_eq!(redact_sql(sel2), sel2);
    }

    /// 受让人恰名为 `to` 时,最后一个顶层 TO 是受让人自己:子句标记必须是
    /// 「其后还有名字」的那个 TO。
    #[test]
    fn grant_with_filter_to_grantee_named_to() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (x INT)").unwrap();
        db.execute("CREATE USER to PASSWORD 'pw12345678'").unwrap();
        db.execute("GRANT SELECT ON t WHERE x > 0 TO to")
            .unwrap_or_else(|e| panic!("grant to user named 'to': {e}"));
        // 渲染回放(带双引号受让人)同样成立。
        db.execute("GRANT SELECT ON t WHERE x > 1 TO \"to\"")
            .unwrap();
    }

    /// 过滤式 GRANT 的路由预筛曾是字面 " ON " 子串:换行/制表符分隔的合法
    /// 授权被误导进 tokenizer 路径、以误导性错误被拒。
    #[test]
    fn grant_with_filter_accepts_any_whitespace_between_clauses() {
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE sales (id INT, region TEXT)")
            .unwrap();
        db.execute("CREATE USER eve PASSWORD 'pw12345678'").unwrap();
        db.execute("GRANT SELECT\n\tON sales\nWHERE region = 'east'\nTO eve")
            .unwrap_or_else(|e| panic!("multi-line grant must parse: {e}"));
    }

    #[test]
    fn resolve_union_and_restriction_semantics() {
        let mut db = Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE sales (id INT, region TEXT, amount INT)",
            "CREATE USER alice PASSWORD 'pw12345678'",
            "CREATE USER bob PASSWORD 'pw12345678'",
            "CREATE USER carol PASSWORD 'pw12345678'",
            "GRANT SELECT (id, amount) ON sales TO alice",
            "GRANT SELECT ON sales WHERE region = 'east' TO bob",
            "GRANT SELECT (id) ON sales WHERE region = 'west' TO carol",
        ] {
            db.execute(sql).unwrap();
        }
        let g = resolve_grants(&mut db, "alice").unwrap().unwrap();
        assert!(g.is_restricted_select("sales"));
        assert_eq!(
            g.granted_cols("sales"),
            &["id".to_string(), "amount".to_string()]
        );
        assert!(g.filters_for("sales").is_empty());

        let g = resolve_grants(&mut db, "bob").unwrap().unwrap();
        assert!(!g.is_restricted_select("sales"));
        assert_eq!(g.filters_for("sales"), &["region = 'east'".to_string()]);
        assert_eq!(g.filter_map().get("sales").map(|v| v.len()), Some(1));

        // Cols + filter restrict BOTH dimensions.
        let g = resolve_grants(&mut db, "carol").unwrap().unwrap();
        assert!(g.is_restricted_select("sales"));
        assert_eq!(g.granted_cols("sales"), &["id".to_string()]);
        assert_eq!(g.filters_for("sales"), &["region = 'west'".to_string()]);

        // A later unrestricted grant voids both restrictions.
        db.execute("GRANT SELECT ON sales TO carol").unwrap();
        let g = resolve_grants(&mut db, "carol").unwrap().unwrap();
        assert!(!g.is_restricted_select("sales"));
        assert!(g.filters_for("sales").is_empty());

        // Blanket roles never carry restrictions.
        db.execute("GRANT readonly TO alice").unwrap();
        let g = resolve_grants(&mut db, "alice").unwrap().unwrap();
        assert!(g.readonly);
        assert!(!g.is_restricted_select("sales"));
        assert!(g.row_filters.is_empty());
    }

    #[test]
    fn dump_replays_qualified_grants() {
        let mut db = Database::in_memory().unwrap();
        for sql in [
            "CREATE TABLE sales (id INT, region TEXT)",
            "CREATE USER alice PASSWORD 'pw12345678'",
            "GRANT SELECT (id) ON sales TO alice",
            "GRANT SELECT ON sales WHERE region = 'east' TO alice",
        ] {
            db.execute(sql).unwrap();
        }
        let dumped = dump_user_statements(&mut db).unwrap();
        // Replay through the normal statement family: the state survives.
        let mut fresh = Database::in_memory().unwrap();
        fresh
            .execute("CREATE TABLE sales (id INT, region TEXT)")
            .unwrap();
        for sql in dumped {
            fresh.execute(&sql).unwrap();
        }
        let g = resolve_grants(&mut fresh, "alice").unwrap().unwrap();
        assert_eq!(g.granted_cols("sales"), &["id".to_string()]);
        assert_eq!(g.filters_for("sales"), &["region = 'east'".to_string()]);
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
                    cols: Vec::new(),
                    row_filter: None,
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
                    cols: Vec::new(),
                    row_filter: None,
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
    fn redact_masks_keyword_after_non_ascii_letter() {
        // Regression: the boundary predicate used Unicode
        // `is_alphanumeric` while the tokenizer's word charset is ASCII —
        // `xéPASSWORD '…'` (which the tokenizer rejects) logged its
        // plaintext verbatim. Rejected statements still reach the audit
        // log, so the mask must fail closed on the ASCII charset.
        let sql = "CREATE USER xéPASSWORD 's3cret-pw12'";
        let redacted = redact_sql(sql);
        assert!(!redacted.contains("s3cret-pw12"), "{redacted}");
        // The statement itself still refuses to parse.
        assert!(parse(sql).is_some_and(|r| r.is_err()));
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
        // A real stored form rides through (dump/replay text stays readable).
        let real = crate::kdf::hash_password("a-good-password", &[0u8; 16]);
        let stmt = format!("CREATE USER a PASSWORD '{real}'");
        assert_eq!(redact_sql(&stmt), stmt);
        // A prefix-forged value (fails the full stored-shape parse) is a
        // password like any other — never reach the log.
        let forged = format!(
            "CREATE USER a PASSWORD '{}my-real-secret'",
            kdf::HASH_PREFIX
        );
        assert_eq!(redact_sql(&forged), "CREATE USER a PASSWORD '***'");
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

    #[test]
    fn redact_skips_password_inside_string_literals() {
        // A PASSWORD inside quoted data is user text, not the keyword:
        // redacting it rewrote the audit entry into a statement that never
        // executed. The literal passes through verbatim.
        let sql = "SELECT 'a PASSWORD ''secret''' AS note";
        assert_eq!(redact_sql(sql), sql);
        // The real keyword form still redacts.
        assert_eq!(
            redact_sql("CREATE USER u PASSWORD 'pw'"),
            "CREATE USER u PASSWORD '***'"
        );
    }
    #[test]
    fn row_filter_rejects_wrapper_hidden_volatile_and_subquery() {
        // 第三轮:FLOOR/IS NULL/SUBSTRING 等包装节点曾不在校验扫描的
        // 遍历集里——RAND() 经 FLOOR 包装后落库成逐行随机可见性过滤器,
        // 子查询则推迟到受限用户的 SELECT 才报错。
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE sales (id INT)").unwrap();
        db.execute("CREATE TABLE secret (x INT)").unwrap();
        db.execute("CREATE USER eve PASSWORD 'pw12345678'").unwrap();
        assert!(db
            .execute("GRANT SELECT ON sales TO eve WHERE FLOOR(RAND()) = 0")
            .is_err());
        assert!(db
            .execute("GRANT SELECT ON sales TO eve WHERE CAST(RAND() AS INT) = 0")
            .is_err());
        assert!(db
            .execute("GRANT SELECT ON sales TO eve WHERE (SELECT COUNT(*) FROM secret) IS NOT NULL")
            .is_err());
        // 合法包装谓词照常通过。
        db.execute("GRANT SELECT ON sales WHERE FLOOR(id / 2.0) >= 0 TO eve")
            .unwrap();
    }

    #[test]
    fn row_filter_rejects_trailing_input_after_predicate() {
        // `region = 'east'; anything` used to parse as a prefix and store
        // the whole tail verbatim — silently different filter semantics at
        // execution, and a broken dump replay at the bare `;`.
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE sales (region TEXT)").unwrap();
        db.execute("CREATE USER eve PASSWORD 'pw12345678'").unwrap();
        assert!(db
            .execute("GRANT SELECT ON sales WHERE region = 'east'; DROP TABLE sales TO eve")
            .is_err());
        db.execute("GRANT SELECT ON sales WHERE region = 'east' TO eve")
            .unwrap();
    }

    #[test]
    fn redact_masks_bare_and_double_quoted_passwords() {
        // These forms are always REJECTED by the parser, but the audit log
        // still carried the intended secret verbatim (fail-open for
        // mistyped quoting).
        assert_eq!(
            redact_sql("CREATE USER eve PASSWORD 12345678"),
            "CREATE USER eve PASSWORD '***'"
        );
        assert_eq!(
            redact_sql("CREATE USER eve PASSWORD \"secret-pw\""),
            "CREATE USER eve PASSWORD '***'"
        );
        // A keyword continuation of a column reference stays verbatim.
        assert_eq!(
            redact_sql("SELECT password FROM t"),
            "SELECT password FROM t"
        );
    }
}
