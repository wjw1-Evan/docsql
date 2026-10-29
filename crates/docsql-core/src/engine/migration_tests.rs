//! Migration-ingestion tests: SQL Server (T-SQL) and MySQL dumps landing in
//! DocSQL.
//!
//! These pin the dialects' *supported* subset (the shapes migration scripts
//! actually use) and the *loud* failures for constructs the engine refuses —
//! per the repository rule that unsupported syntax must never be silently
//! dropped. They exercise the public `Database` API exactly like the CLI and
//! the FFI embedding path do (preprocessing included).

use super::*;

fn run(db: &mut Database, sql: &str) -> ExecOutcome {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
}

fn rows(db: &mut Database, sql: &str) -> QueryResult {
    match run(db, sql) {
        ExecOutcome::Rows(r) => r,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn err_of(db: &mut Database, sql: &str) -> String {
    match db.execute(sql) {
        Ok(other) => panic!("expected a loud rejection for: {sql}\n got {other:?}"),
        Err(e) => e.to_string(),
    }
}

/// SQL Server dump: schema-qualified bracketed names, IDENTITY(1,1),
/// NVARCHAR/DATETIME2/DECIMAL types, N'' literals, GETDATE(), multi-row
/// VALUES, TOP, ISNULL — the everyday migration surface.
#[test]
fn sql_server_dump_schema_and_data_migrate() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE [dbo].[Customers] (\
             [Id] INT IDENTITY(1,1) PRIMARY KEY, \
             [Name] NVARCHAR(100) NOT NULL, \
             [Email] NVARCHAR(255) NULL, \
             [Balance] DECIMAL(18,2) NULL, \
             [CreatedAt] DATETIME2 NOT NULL)",
    );
    // Dumps that carry explicit identity values bracket the inserts with
    // this session switch; it is a compatibility no-op (explicit ids are
    // legal on AUTOINCREMENT keys).
    run(&mut db, "SET IDENTITY_INSERT [dbo].[Customers] ON");
    run(
        &mut db,
        "INSERT INTO [dbo].[Customers] ([Name], [Email], [Balance], [CreatedAt]) VALUES \
             (N'O''Brien', N'a@b.c', CAST('1234.56' AS DECIMAL), GETDATE()), \
             (N'李雷', NULL, CAST('0.10' AS DECIMAL), GETDATE())",
    );
    run(
        &mut db,
        "INSERT INTO [dbo].[Customers] ([Id], [Name], [CreatedAt]) \
         VALUES (10, N'显式主键', GETDATE())",
    );
    run(&mut db, "SET IDENTITY_INSERT [dbo].[Customers] OFF");

    // TOP + qualified names resolve (schema flattens into the dotted name).
    let r = rows(
        &mut db,
        "SELECT TOP 2 [Id], [Name] FROM [dbo].[Customers] ORDER BY [Id]",
    );
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(1), Value::Str("O'Brien".into())],
            vec![Value::Int(2), Value::Str("李雷".into())],
        ]
    );

    // ISNULL update on a decimal column.
    run(
        &mut db,
        "UPDATE [dbo].[Customers] SET [Balance] = ISNULL([Balance], 0) + 1 WHERE [Id] = 2",
    );
    let r = rows(
        &mut db,
        "SELECT [Balance] FROM [dbo].[Customers] WHERE [Id] = 2",
    );
    assert_eq!(r.rows, vec![vec![Value::Decimal("1.10".parse().unwrap())]]);

    // Explicit id 10 is retained; the next auto id continues after the max.
    run(
        &mut db,
        "INSERT INTO [dbo].[Customers] ([Name], [CreatedAt]) VALUES (N'after', GETDATE())",
    );
    let r = rows(&mut db, "SELECT MAX([Id]) FROM [dbo].[Customers]");
    assert_eq!(r.rows, vec![vec![Value::Int(11)]]);
}

/// A migrated database must survive the backup/replication artifact too:
/// `dump_script()` replay into a fresh engine reproduces schema + data.
#[test]
fn sql_server_dump_round_trips_through_dump_script() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE [dbo].[Orders] (\
             [Id] INT IDENTITY(1,1) PRIMARY KEY, \
             [Customer] NVARCHAR(80) NOT NULL, \
             [Amount] DECIMAL(18,2) NOT NULL, \
             [PlacedAt] DATETIME2 NOT NULL)",
    );
    run(
        &mut db,
        "INSERT INTO [dbo].[Orders] ([Customer], [Amount], [PlacedAt]) VALUES \
             (N'alice', CAST('10.25' AS DECIMAL), TIMESTAMP '2026-01-02 03:04:05'), \
             (N'bob', CAST('99.99' AS DECIMAL), TIMESTAMP '2026-02-03 04:05:06')",
    );

    let script = db.dump_script().expect("dump");
    let mut fresh = Database::in_memory().unwrap();
    let batch = fresh.execute_batch(&script);
    assert!(batch.error.is_none(), "{:?}", batch.error);

    let a = rows(
        &mut db,
        "SELECT [Customer], [Amount], [PlacedAt] FROM [dbo].[Orders] ORDER BY [Id]",
    );
    let b = rows(
        &mut fresh,
        "SELECT [Customer], [Amount], [PlacedAt] FROM [dbo].[Orders] ORDER BY [Id]",
    );
    assert_eq!(a.columns, b.columns);
    assert_eq!(a.rows, b.rows);
    assert_eq!(a.rows.len(), 2);
}

/// MySQL dump: backticks, AUTO_INCREMENT, TINYINT(1)/UNSIGNED, inline
/// UNIQUE KEY, multi-row INSERT, ON DUPLICATE KEY UPDATE, LIMIT, IFNULL and
/// CONCAT.
#[test]
fn mysql_dump_schema_and_data_migrate() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE `users` (\
             `id` INT AUTO_INCREMENT PRIMARY KEY, \
             `email` VARCHAR(255) NOT NULL, \
             `name` VARCHAR(60) NULL, \
             `balance` DECIMAL(12,2) NULL, \
             `active` TINYINT(1) NOT NULL, \
             `hits` INT UNSIGNED NOT NULL, \
             `created_at` DATETIME NOT NULL, \
             UNIQUE KEY `uq_users_email` (`email`))",
    );
    run(
        &mut db,
        "INSERT INTO `users` (`email`, `name`, `balance`, `active`, `hits`, `created_at`) VALUES \
             ('a@example.com', 'alice', CAST('5.00' AS DECIMAL), 1, 3, TIMESTAMP '2026-03-01 00:00:00'), \
             ('b@example.com', NULL, NULL, 0, 0, TIMESTAMP '2026-03-02 00:00:00')",
    );

    // The classic MySQL upsert from incremental dumps.
    run(
        &mut db,
        "INSERT INTO `users` (`email`, `name`, `active`, `hits`, `created_at`) \
         VALUES ('a@example.com', 'alice v2', 1, 4, TIMESTAMP '2026-03-01 00:00:00') \
         ON DUPLICATE KEY UPDATE `name` = 'alice v2', `hits` = 4",
    );

    let r = rows(
        &mut db,
        "SELECT `email`, IFNULL(`name`, 'anonymous'), `hits` FROM `users` ORDER BY `id` LIMIT 2",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0],
        vec![
            Value::Str("a@example.com".into()),
            Value::Str("alice v2".into()),
            Value::Int(4),
        ]
    );
    // IFNULL on the NULL name row.
    assert_eq!(r.rows[1][1], Value::Str("anonymous".into()));

    // CONCAT in the migration's report queries.
    let r = rows(
        &mut db,
        "SELECT CONCAT(`email`, '/', `hits`) FROM `users` WHERE `id` = 1",
    );
    assert_eq!(r.rows, vec![vec![Value::Str("a@example.com/4".into())]]);

    // Unique constraint still enforces after ingestion.
    let e = err_of(
        &mut db,
        "INSERT INTO `users` (`email`, `active`, `hits`, `created_at`) \
         VALUES ('a@example.com', 1, 1, TIMESTAMP '2026-03-03 00:00:00')",
    );
    assert!(e.to_ascii_lowercase().contains("unique"), "{e}");
}

/// MySQL table options (`ENGINE=`, `CHARSET=`) are storage-engine directives
/// with no DocSQL equivalent; the import must fail loudly and be fixed in
/// the dump (strip the option) rather than silently ignored.
#[test]
fn mysql_storage_options_are_rejected_loudly() {
    let mut db = Database::in_memory().unwrap();
    let e = err_of(
        &mut db,
        "CREATE TABLE `t` (`id` INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    );
    assert!(e.contains("storage/partitioning options"), "{e}");
}

/// `ON UPDATE CURRENT_TIMESTAMP` has no engine equivalent (wall-clock
/// defaults are pinned at insert time; triggering on UPDATE would make
/// replicas diverge) — loud rejection is the contract.
#[test]
fn mysql_on_update_current_timestamp_is_rejected_loudly() {
    let mut db = Database::in_memory().unwrap();
    let e = err_of(
        &mut db,
        "CREATE TABLE `t` (`id` INT AUTO_INCREMENT PRIMARY KEY, \
          `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
    );
    assert!(e.contains("unsupported column constraint"), "{e}");
}

/// MySQL dumps escape strings with backslashes; DocSQL string literals are
/// SQL-standard (backslash is data). This test pins the difference loudly:
/// `\n` imports as the two characters, and `\'` does not close the literal
/// (dumps must be converted — see docs/limitations.md).
#[test]
fn mysql_backslash_escapes_are_literals_not_mysql_escapes() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE `t` (`id` INT AUTO_INCREMENT PRIMARY KEY, `v` VARCHAR(20))",
    );
    run(&mut db, "INSERT INTO `t` (`v`) VALUES ('a\\nb')");
    let r = rows(&mut db, "SELECT `v` FROM `t`");
    assert_eq!(r.rows, vec![vec![Value::Str("a\\nb".into())]]);

    let e = err_of(&mut db, "INSERT INTO `t` (`v`) VALUES ('O\\'Brien')");
    assert!(e.contains("Unterminated string"), "{e}");
}

/// SQL Server `IDENTITY(seed, increment)`: only the canonical (1,1) form is
/// flattened to AUTOINCREMENT (the engine's ids start at 1 step 1); any
/// other seed/increment stays verbatim so DDL fails loudly instead of
/// silently renumbering rows.
#[test]
fn sql_server_identity_seed_increment_is_flattened_only_for_1_1() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE ok (id INT IDENTITY (1, 1) PRIMARY KEY, v INT)",
    );
    run(&mut db, "INSERT INTO ok (v) VALUES (7)");
    run(&mut db, "INSERT INTO ok (v) VALUES (8)");
    let r = rows(&mut db, "SELECT id FROM ok ORDER BY id");
    assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    let e = err_of(
        &mut db,
        "CREATE TABLE bad (id INT IDENTITY(2, 1) PRIMARY KEY)",
    );
    assert!(e.contains("unsupported column constraint"), "{e}");
}

/// DECIMAL and TIMESTAMP survive the migration with their exact semantics
/// (the values EF/SQL Server dump as strings/casts, not as lossy literals).
#[test]
fn migrated_decimal_and_timestamp_keep_exact_values() {
    let mut db = Database::in_memory().unwrap();
    run(
        &mut db,
        "CREATE TABLE money (id INT IDENTITY(1,1) PRIMARY KEY, \
          amount DECIMAL(18,2), at DATETIME2)",
    );
    run(
        &mut db,
        "INSERT INTO money (amount, at) VALUES \
         (CAST('1234.56' AS DECIMAL), TIMESTAMP '2026-05-06 07:08:09'), \
         (CAST('-0.01' AS DECIMAL), TIMESTAMP '2026-05-07 00:00:00')",
    );
    let r = rows(
        &mut db,
        "SELECT amount, at FROM money WHERE amount < CAST('0' AS DECIMAL)",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Decimal("-0.01".parse().unwrap()));
    assert!(
        matches!(r.rows[0][1], Value::Timestamp(_)),
        "{:?}",
        r.rows[0][1]
    );
}
