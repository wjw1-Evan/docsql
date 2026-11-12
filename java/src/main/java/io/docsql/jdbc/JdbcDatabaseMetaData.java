package io.docsql.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Database metadata: identity fields real (status-driven); introspection returns empty sets. */
public final class JdbcDatabaseMetaData implements java.sql.DatabaseMetaData {

    private final JdbcConnection conn;

    private static final List<String> TABLE_COLS = Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
    private static final List<String> COLUMN_COLS = Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN");

    JdbcDatabaseMetaData(JdbcConnection conn) {
        this.conn = conn;
    }

    private java.sql.ResultSet emptyRs(String... columns) throws SQLException {
        io.docsql.Result r = new io.docsql.Result();
        for (String c : columns) {
            r.columns.add(c);
        }
        return new JdbcResultSet(null, r);
    }

    private java.sql.ResultSet rowsRs(List<String> columns, List<Object[]> rows) throws SQLException {
        io.docsql.Result r = new io.docsql.Result();
        r.columns.addAll(columns);
        r.rows.addAll(rows);
        return new JdbcResultSet(null, r);
    }

    private static String likeToRegex(String p) {
        if (p == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (char c : p.toCharArray()) {
            if (c == 92) {
                sb.append(92).append(92);
            } else if (c == 46) {
                sb.append(92).append(46);
            } else if (c == 37) {
                sb.append(".*");
            } else if (c == 95) {
                sb.append((char) 46);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
    private boolean matches(String name, String like) {
        if (like == null) {
            return true;
        }
        return Pattern.compile(likeToRegex(like), Pattern.CASE_INSENSITIVE).matcher(name).matches();
    }

    private List<Object[]> queryTables(String tableNamePattern) throws SQLException {
        List<Object[]> out = new ArrayList<>();
        String pat = tableNamePattern;
        try {
            io.docsql.Result r = conn.core().execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'");
            for (Object[] row : r.rows) {
                String name = String.valueOf(row[0]);
                if (name.startsWith("docsql_") || name.startsWith("_pubsub") || name.startsWith("_cluster")) {
                    continue;
                }
                if (matches(name, pat)) {
                    out.add(new Object[] {null, null, name, "TABLE", null, null, null, null, null, null});
                }
            }
        } catch (io.docsql.DocsqlException e) {
            throw JdbcSql.wrap(e);
        }
        return out;
    }

    private List<Object[]> queryColumns(String tableNamePattern, String columnNamePattern) throws SQLException {
        List<Object[]> out = new ArrayList<>();
        String tpat = tableNamePattern;
        String cpat = columnNamePattern;
        try {
            io.docsql.Result tables = conn.core().execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'");
            for (Object[] trow : tables.rows) {
                String table = String.valueOf(trow[0]);
                if (table.startsWith("docsql_")) {
                    continue;
                }
                if (!matches(table, tpat)) {
                    continue;
                }
                io.docsql.Result cols = conn.core().execute(
                        "SELECT * FROM " + quoteIdent(table) + " LIMIT 0");
                for (int i = 0; i < cols.columns.size(); i++) {
                    String cname = cols.columns.get(i);
                    if (!matches(cname, cpat)) {
                        continue;
                    }
                    out.add(new Object[] {null, null, table, cname, java.sql.Types.OTHER, "JSON", 0, 0, 10,
                            java.sql.DatabaseMetaData.columnNoNulls, null, null, 0, 0, 0, i + 1, "", null, null, null, 0, "NO", "NO"});
                }
            }
        } catch (io.docsql.DocsqlException e) {
            throw JdbcSql.wrap(e);
        }
        return out;
    }

    private static String quoteIdent(String s) {
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    public java.lang.String getURL() throws SQLException {
        return "jdbc:docsql://" + conn.core.config.host + ":" + conn.core.config.port;
    }

    @Override
    public java.lang.String getUserName() throws SQLException {
        return conn.core().config.user;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public java.lang.String getDatabaseProductName() throws SQLException {
        return "DocSQL";
    }

    @Override
    public java.lang.String getDatabaseProductVersion() throws SQLException {
        Object v = conn.core().status().get("version");
        return v == null ? "" : String.valueOf(v);
    }

    @Override
    public java.lang.String getDriverName() throws SQLException {
        return "DocSQL JDBC Driver";
    }

    @Override
    public java.lang.String getDriverVersion() throws SQLException {
        return "0.9.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public java.lang.String getIdentifierQuoteString() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getSearchStringEscape() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int arg0, int arg1) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public java.lang.String getSchemaTerm() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getProcedureTerm() throws SQLException {
        return "";
    }

    @Override
    public java.lang.String getCatalogTerm() throws SQLException {
        return "";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public java.lang.String getCatalogSeparator() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public java.sql.ResultSet getProcedures(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getProcedureColumns(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getTables(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String[] arg3) throws SQLException {
        return rowsRs(TABLE_COLS, queryTables(arg2));
    }

    @Override
    public java.sql.ResultSet getSchemas() throws SQLException {
        return emptyRs("TABLE_SCHEM", "TABLE_CATALOG");
    }

    @Override
    public java.sql.ResultSet getCatalogs() throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getTableTypes() throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getColumns(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3) throws SQLException {
        return rowsRs(COLUMN_COLS, queryColumns(arg2, arg3));
    }

    @Override
    public java.sql.ResultSet getColumnPrivileges(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getTablePrivileges(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getBestRowIdentifier(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, int arg3, boolean arg4) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getVersionColumns(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getPrimaryKeys(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getImportedKeys(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getExportedKeys(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getCrossReference(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3, java.lang.String arg4, java.lang.String arg5) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getTypeInfo() throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getIndexInfo(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, boolean arg3, boolean arg4) throws SQLException {
        return emptyRs();
    }

    @Override
    public boolean supportsResultSetType(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int arg0, int arg1) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int arg0) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public java.sql.ResultSet getUDTs(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, int[] arg3) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public java.sql.ResultSet getSuperTypes(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getSuperTables(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getAttributes(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3) throws SQLException {
        return emptyRs();
    }

    @Override
    public boolean supportsResultSetHoldability(int arg0) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public java.sql.RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new java.sql.SQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.ResultSet getSchemas(java.lang.String arg0, java.lang.String arg1) throws SQLException {
        return emptyRs("TABLE_SCHEM", "TABLE_CATALOG");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public java.sql.ResultSet getClientInfoProperties() throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getFunctions(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getFunctionColumns(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3) throws SQLException {
        return emptyRs();
    }

    @Override
    public java.sql.ResultSet getPseudoColumns(java.lang.String arg0, java.lang.String arg1, java.lang.String arg2, java.lang.String arg3) throws SQLException {
        return emptyRs();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
