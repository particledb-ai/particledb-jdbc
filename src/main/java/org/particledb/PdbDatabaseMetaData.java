package org.particledb;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link DatabaseMetaData} backed by PDB's server-side {@code information_schema}
 * + {@code pg_catalog} virtual tables (materialized in
 * the server's catalog virtual tables and served through the real
 * SELECT executor, so WHERE/ORDER BY/LIMIT work). The catalog-enumeration
 * methods ({@code getTables}, {@code getColumns}, {@code getPrimaryKeys},
 * {@code getImportedKeys}/{@code getExportedKeys}, {@code getIndexInfo},
 * {@code getSchemas}, {@code getTypeInfo}) issue those catalog queries and map
 * the rows into the JDBC-spec ResultSet shape ({@link MetaResultSet}) so tools
 * like Flyway 9 (PostgreSQL support) and ORMs reflect a real schema.
 *
 * <p>Product identity ({@code getDatabaseProductName}/{@code Version}) is driven
 * by the connection's {@code pgCompat} option (default → "PostgreSQL"/"14.0") so
 * Flyway's {@code PostgreSQLDatabaseType} selects PostgreSQL support.
 */
public final class PdbDatabaseMetaData implements DatabaseMetaData {

    private final PdbConnection conn;

    public PdbDatabaseMetaData(PdbConnection conn) { this.conn = conn; }

    // ── product identity (Flyway dialect detection) ─────────────────────────
    @Override public String getDatabaseProductName() { return conn.reportedProductName(); }
    @Override public String getDatabaseProductVersion() { return conn.reportedProductVersion(); }
    @Override public int getDatabaseMajorVersion() { return conn.reportedMajorVersion(); }
    @Override public int getDatabaseMinorVersion() { return conn.reportedMinorVersion(); }

    @Override public boolean allProceduresAreCallable() { return true; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public String getURL() { return "jdbc:particledb://"; }
    @Override public String getUserName() { return conn.userName(); }
    @Override public boolean isReadOnly() { return false; }
    @Override public boolean nullsAreSortedHigh() { return true; }
    @Override public boolean nullsAreSortedLow() { return false; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public String getDriverName() { return "ParticleDB JDBC Driver"; }
    @Override public String getDriverVersion() { return Driver.MAJOR_VERSION + "." + Driver.MINOR_VERSION; }
    @Override public int getDriverMajorVersion() { return Driver.MAJOR_VERSION; }
    @Override public int getDriverMinorVersion() { return Driver.MINOR_VERSION; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return false; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return true; }
    @Override public boolean storesMixedCaseIdentifiers() { return false; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return "\\"; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public boolean supportsAlterTableWithAddColumn() { return true; }
    @Override public boolean supportsAlterTableWithDropColumn() { return true; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int from, int to) { return false; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return true; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return true; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return true; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return true; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public String getSchemaTerm() { return "schema"; }
    @Override public String getProcedureTerm() { return "procedure"; }
    @Override public String getCatalogTerm() { return "database"; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public boolean supportsSchemasInDataManipulation() { return true; }
    @Override public boolean supportsSchemasInProcedureCalls() { return true; }
    @Override public boolean supportsSchemasInTableDefinitions() { return true; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return true; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return true; }
    @Override public boolean supportsCatalogsInDataManipulation() { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    // PDB parses/handles FOR UPDATE (advisory under OCC); advertise support so
    // frameworks that gate SELECT … FOR UPDATE on this flag will emit it.
    @Override public boolean supportsSelectForUpdate() { return true; }
    @Override public boolean supportsStoredProcedures() { return true; }
    @Override public boolean supportsSubqueriesInComparisons() { return true; }
    @Override public boolean supportsSubqueriesInExists() { return true; }
    @Override public boolean supportsSubqueriesInIns() { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return true; }
    @Override public boolean supportsCorrelatedSubqueries() { return true; }
    @Override public boolean supportsUnion() { return true; }
    @Override public boolean supportsUnionAll() { return true; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }
    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 0; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 0; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 0; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return true; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 0; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 0; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_READ_COMMITTED; }
    @Override public boolean supportsTransactions() { return true; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return true; }
    // DDL is NOT transactional in ParticleDB: inside an open transaction it is
    // refused (25001), and outside one it takes effect immediately
    // (PdbConnection#sendOne). It neither commits the transaction nor is ignored.
    // These used to claim DDL+DML transactions (true / false), which is
    // PostgreSQL's answer and was never this engine's.
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return true; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }

    // ── catalog-query helpers ────────────────────────────────────────────────

    /**
     * A zero-ROW result set that still has the columns the JDBC javadoc
     * specifies for the method returning it.
     *
     * <p>These methods used to return {@code PdbResultSet.empty()}, which has
     * zero COLUMNS. "No rows" and "no such thing as this column" are different
     * answers: a reflection tool that walks the specified columns of an empty
     * {@code getTablePrivileges} result gets a column-not-found failure instead
     * of an empty loop, and {@code getMetaData().getColumnCount()} reports 0
     * where the contract names a fixed list.
     *
     * <p>Spec: {@code NAME:kind} where kind is {@code V}archar, {@code I}nteger,
     * {@code S}mallint or {@code B}oolean.
     */
    private static ResultSet emptyWithColumns(String spec) {
        String[] parts = spec.split(",");
        String[] names = new String[parts.length];
        int[] types = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            int colon = parts[i].indexOf(':');
            names[i] = colon < 0 ? parts[i] : parts[i].substring(0, colon);
            char k = colon < 0 ? 'V' : parts[i].charAt(colon + 1);
            switch (k) {
                case 'I': types[i] = Types.INTEGER; break;
                case 'S': types[i] = Types.SMALLINT; break;
                case 'B': types[i] = Types.BOOLEAN; break;
                default:  types[i] = Types.VARCHAR; break;
            }
        }
        return new MetaResultSet(names, types, new ArrayList<Object[]>());
    }

    /** Catalog failures must remain distinguishable from an empty catalog. */
    private List<Object[]> queryRows(String sql, String[] cols) throws SQLException {
        List<Object[]> out = new ArrayList<>();
        try (ResultSet rs = conn.executeQuery(sql)) {
            while (rs.next()) {
                Object[] row = new Object[cols.length];
                for (int i = 0; i < cols.length; i++) {
                    row[i] = cols[i] == null ? null : rs.getObject(cols[i]);
                }
                out.add(row);
            }
        }
        return out;
    }

    /** SQL-single-quote-escape a LIKE/equality literal for inline substitution. */
    private static String q(String s) {
        if (s == null) return null;
        return s.replace("'", "''");
    }

    /** true when a JDBC metadata pattern means "match everything" (null or %). */
    private static boolean matchAll(String pattern) {
        return pattern == null || pattern.equals("%");
    }

    // ── getTables ────────────────────────────────────────────────────────────
    private static final String[] TABLES_COLS = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
        "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION"
    };
    private static final int[] TABLES_TYPES = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
    };

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT table_schema, table_name, table_type "
          + "FROM information_schema.tables WHERE 1=1");
        if (!matchAll(schemaPattern)) sql.append(" AND table_schema LIKE '").append(q(schemaPattern)).append("'");
        if (!matchAll(tableNamePattern)) sql.append(" AND table_name LIKE '").append(q(tableNamePattern)).append("'");
        // JDBC 'types' filter maps BASE TABLE/VIEW → PG's table_type values.
        if (types != null && types.length > 0) {
            List<String> pg = new ArrayList<>();
            for (String t : types) {
                if ("TABLE".equalsIgnoreCase(t)) pg.add("'BASE TABLE'");
                else if ("VIEW".equalsIgnoreCase(t)) pg.add("'VIEW'");
            }
            if (!pg.isEmpty()) sql.append(" AND table_type IN (").append(String.join(",", pg)).append(")");
        }
        sql.append(" ORDER BY table_schema, table_name");

        List<Object[]> src = queryRows(sql.toString(),
                new String[]{"table_schema", "table_name", "table_type"});
        List<Object[]> rows = new ArrayList<>(src.size());
        for (Object[] r : src) {
            String pgType = r[2] == null ? "" : r[2].toString();
            String jdbcType = "VIEW".equalsIgnoreCase(pgType) ? "VIEW" : "TABLE";
            rows.add(new Object[]{
                conn.databaseName(), str(r[0]), str(r[1]), jdbcType,
                null, null, null, null, null, null
            });
        }
        return new MetaResultSet(TABLES_COLS, TABLES_TYPES, rows);
    }

    // ── getColumns ───────────────────────────────────────────────────────────
    private static final String[] COLUMNS_COLS = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
        "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
        "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
        "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
        "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
    };
    private static final int[] COLUMNS_TYPES = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
        Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
        Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
        Types.INTEGER, Types.INTEGER, Types.INTEGER,
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.INTEGER, Types.VARCHAR, Types.VARCHAR
    };

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT table_schema, table_name, column_name, ordinal_position, "
          + "column_default, is_nullable, data_type, character_maximum_length, "
          + "numeric_precision, numeric_scale "
          + "FROM information_schema.columns WHERE 1=1");
        if (!matchAll(schemaPattern)) sql.append(" AND table_schema LIKE '").append(q(schemaPattern)).append("'");
        if (!matchAll(tableNamePattern)) sql.append(" AND table_name LIKE '").append(q(tableNamePattern)).append("'");
        if (!matchAll(columnNamePattern)) sql.append(" AND column_name LIKE '").append(q(columnNamePattern)).append("'");
        sql.append(" ORDER BY table_schema, table_name, ordinal_position");

        List<Object[]> src = queryRows(sql.toString(), new String[]{
            "table_schema", "table_name", "column_name", "ordinal_position",
            "column_default", "is_nullable", "data_type",
            "character_maximum_length", "numeric_precision", "numeric_scale"
        });
        List<Object[]> rows = new ArrayList<>(src.size());
        for (Object[] r : src) {
            String pgType = str(r[6]);
            int dataType = PgTypeMap.sqlType(pgType);
            String isNullableYesNo = "YES".equalsIgnoreCase(str(r[5])) ? "YES" : "NO";
            int nullable = "YES".equalsIgnoreCase(str(r[5]))
                    ? columnNullable : columnNoNulls;
            Integer charMax = toInt(r[7]);
            Integer numPrec = toInt(r[8]);
            Integer numScale = toInt(r[9]);
            Integer colSize = PgTypeMap.columnSize(pgType, charMax, numPrec);
            Integer decimalDigits = PgTypeMap.decimalDigits(pgType, numScale);
            rows.add(new Object[]{
                conn.databaseName(),          // TABLE_CAT
                str(r[0]),                    // TABLE_SCHEM
                str(r[1]),                    // TABLE_NAME
                str(r[2]),                    // COLUMN_NAME
                dataType,                     // DATA_TYPE
                pgType,                       // TYPE_NAME
                colSize,                      // COLUMN_SIZE
                null,                         // BUFFER_LENGTH
                decimalDigits,                // DECIMAL_DIGITS
                10,                           // NUM_PREC_RADIX
                nullable,                     // NULLABLE
                null,                         // REMARKS
                str(r[4]),                    // COLUMN_DEF
                null,                         // SQL_DATA_TYPE
                null,                         // SQL_DATETIME_SUB
                null,                         // CHAR_OCTET_LENGTH
                toInt(r[3]),                  // ORDINAL_POSITION
                isNullableYesNo,              // IS_NULLABLE
                null, null, null,             // SCOPE_*
                null,                         // SOURCE_DATA_TYPE
                "NO",                         // IS_AUTOINCREMENT
                "NO"                          // IS_GENERATEDCOLUMN
            });
        }
        return new MetaResultSet(COLUMNS_COLS, COLUMNS_TYPES, rows);
    }

    // ── getPrimaryKeys ───────────────────────────────────────────────────────
    private static final String[] PK_COLS = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"
    };
    private static final int[] PK_TYPES = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR
    };

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // key_column_usage joined to table_constraints filtered to PRIMARY KEY.
        StringBuilder sql = new StringBuilder(
            "SELECT k.table_schema, k.table_name, k.column_name, "
          + "k.ordinal_position, k.constraint_name "
          + "FROM information_schema.key_column_usage k "
          + "JOIN information_schema.table_constraints t "
          + "ON k.constraint_name = t.constraint_name "
          + "AND k.table_schema = t.table_schema "
          + "AND k.table_name = t.table_name "
          + "WHERE t.constraint_type = 'PRIMARY KEY'");
        if (schema != null && !schema.isEmpty()) sql.append(" AND k.table_schema = '").append(q(schema)).append("'");
        if (table != null && !table.isEmpty()) sql.append(" AND k.table_name = '").append(q(table)).append("'");
        sql.append(" ORDER BY k.column_name");

        List<Object[]> src = queryRows(sql.toString(), new String[]{
            "table_schema", "table_name", "column_name", "ordinal_position", "constraint_name"
        });
        List<Object[]> rows = new ArrayList<>(src.size());
        for (Object[] r : src) {
            rows.add(new Object[]{
                conn.databaseName(), str(r[0]), str(r[1]), str(r[2]),
                toShort(r[3]), str(r[4])
            });
        }
        return new MetaResultSet(PK_COLS, PK_TYPES, rows);
    }

    // ── imported/exported keys (foreign keys) ────────────────────────────────
    private static final String[] FK_COLS = {
        "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
        "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
        "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
    };
    private static final int[] FK_TYPES = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT
    };

    /**
     * Foreign keys from ordered catalog column vectors. {@code fkFilterOnFkTable=true} → getImportedKeys
     * (filter by the FK-owning table), otherwise getExportedKeys (filter by the
     * referenced/PK table).
     */
    private ResultSet foreignKeys(String schema, String table, boolean fkFilterOnFkTable) throws SQLException {
        return foreignKeys(schema, table, fkFilterOnFkTable, null, null);
    }

    /**
     * @param otherSchema when non-null, additionally constrain the OPPOSITE side
     *        of the relationship (used by {@code getCrossReference}, which names
     *        both the parent and the child table)
     */
    private ResultSet foreignKeys(String schema, String table, boolean fkFilterOnFkTable,
                                  String otherSchema, String otherTable) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT c.conname AS fk_name, fn.nspname AS fk_schema, f.relname AS fk_table, "
          + "c.conkey, pn.nspname AS pk_schema, p.relname AS pk_table, c.confkey, "
          + "c.conrelid, c.confrelid, pg_get_constraintdef(c.oid, true) AS definition "
          + "FROM pg_catalog.pg_constraint c "
          + "JOIN pg_catalog.pg_class f ON f.oid = c.conrelid "
          + "JOIN pg_catalog.pg_namespace fn ON fn.oid = f.relnamespace "
          + "JOIN pg_catalog.pg_class p ON p.oid = c.confrelid "
          + "JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace "
          + "WHERE c.contype = 'f'");
        if (schema != null) sql.append(fkFilterOnFkTable ? " AND fn.nspname = '" : " AND pn.nspname = '").append(q(schema)).append("'");
        if (table != null) sql.append(fkFilterOnFkTable ? " AND f.relname = '" : " AND p.relname = '").append(q(table)).append("'");
        if (otherSchema != null) sql.append(fkFilterOnFkTable ? " AND pn.nspname = '" : " AND fn.nspname = '").append(q(otherSchema)).append("'");
        if (otherTable != null) sql.append(fkFilterOnFkTable ? " AND p.relname = '" : " AND f.relname = '").append(q(otherTable)).append("'");
        List<Object[]> src = queryRows(sql.toString(), new String[]{
            "fk_name", "fk_schema", "fk_table", "conkey", "pk_schema", "pk_table", "confkey", "conrelid", "confrelid", "definition"
        });
        java.util.Map<String, java.util.Map<Integer, String>> attributes = new java.util.HashMap<>();
        List<Object[]> rows = new ArrayList<>();
        for (Object[] r : src) {
            java.util.Map<Integer, String> fk = catalogColumns(r[7], attributes);
            java.util.Map<Integer, String> pk = catalogColumns(r[8], attributes);
            int[] child = catalogPositions(r[3]), parent = catalogPositions(r[6]);
            if (child.length != parent.length) throw new SQLException("foreign-key catalog column counts differ", "HY000");
            String definition = str(r[9]);
            String options = definition == null ? "" : definition.substring(definition.lastIndexOf(')') + 1).toUpperCase(java.util.Locale.ROOT);
            short deferrability = options.contains("DEFERRABLE") && !options.contains("NOT DEFERRABLE")
                    ? (short) (options.contains("INITIALLY DEFERRED") ? importedKeyInitiallyDeferred : importedKeyInitiallyImmediate)
                    : (short) importedKeyNotDeferrable;
            String pkName = referencedKeyName(r[8], parent);
            for (int ordinal = 0; ordinal < child.length; ordinal++) {
                rows.add(new Object[]{conn.databaseName(), str(r[4]), str(r[5]), pk.get(parent[ordinal]),
                    conn.databaseName(), str(r[1]), str(r[2]), fk.get(child[ordinal]), (short) (ordinal + 1),
                    foreignAction(options, "UPDATE"), foreignAction(options, "DELETE"), str(r[0]), pkName, deferrability});
            }
        }
        int side = fkFilterOnFkTable ? 0 : 4;
        rows.sort(java.util.Comparator.<Object[], String>comparing(row -> (String) row[side + 1])
            .thenComparing(row -> (String) row[side + 2]).thenComparingInt(row -> ((Short) row[8]).intValue()));
        return new MetaResultSet(FK_COLS, FK_TYPES, rows);
    }

    private String referencedKeyName(Object oid, int[] positions) throws SQLException {
        int[] referenced = positions.clone();
        java.util.Arrays.sort(referenced);
        String name = null;
        for (Object[] candidate : queryRows("SELECT conname, conkey FROM pg_catalog.pg_constraint WHERE conrelid = "
                + Long.parseLong(str(oid)) + " AND contype IN ('p', 'u')", new String[]{"conname", "conkey"})) {
            int[] keys = catalogPositions(candidate[1]);
            java.util.Arrays.sort(keys);
            if (java.util.Arrays.equals(referenced, keys)) {
                if (name != null) return null;
                name = str(candidate[0]);
            }
        }
        return name;
    }

    private static short foreignAction(String options, String operation) {
        java.util.regex.Matcher match = java.util.regex.Pattern.compile(
            "\\bON " + operation + " (NO ACTION|RESTRICT|CASCADE|SET NULL|SET DEFAULT)\\b").matcher(options);
        return ruleCode(match.find() ? match.group(1) : null);
    }

    private java.util.Map<Integer, String> catalogColumns(Object oidValue,
            java.util.Map<String, java.util.Map<Integer, String>> cache) throws SQLException {
        String oid = str(oidValue);
        java.util.Map<Integer, String> columns = cache.get(oid);
        if (columns == null) {
            columns = new java.util.HashMap<>();
            for (Object[] attribute : queryRows("SELECT attnum, attname FROM pg_catalog.pg_attribute WHERE attrelid = "
                    + Long.parseLong(oid), new String[]{"attnum", "attname"})) {
                columns.put(Integer.parseInt(str(attribute[0])), str(attribute[1]));
            }
            cache.put(oid, columns);
        }
        return columns;
    }

    private static int[] catalogPositions(Object value) {
        if (value == null) return new int[0];
        String text = str(value).replaceAll("[{}]", "").trim();
        if (text.isEmpty()) return new int[0];
        return java.util.Arrays.stream(text.split("[ ,]+")).mapToInt(Integer::parseInt).toArray();
    }

    private static short ruleCode(String rule) {
        if (rule == null) return importedKeyNoAction;
        switch (rule.toUpperCase()) {
            case "CASCADE":     return importedKeyCascade;
            case "SET NULL":    return importedKeySetNull;
            case "SET DEFAULT": return importedKeySetDefault;
            case "RESTRICT":    return importedKeyRestrict;
            case "NO ACTION":
            default:            return importedKeyNoAction;
        }
    }

    @Override public ResultSet getImportedKeys(String c, String s, String t) throws SQLException {
        return foreignKeys(s, t, true);
    }
    @Override public ResultSet getExportedKeys(String c, String s, String t) throws SQLException {
        return foreignKeys(s, t, false);
    }
    /**
     * {@inheritDoc}
     *
     * <p>Both halves of the relationship are filtered. This used to drop the
     * child-side arguments entirely and return EVERY foreign key pointing at the
     * parent - a WRONG result rather than a missing one, so a schema-diff or
     * ORM-mapping tool asking "how does orders reference customers" was handed
     * every other table's reference to customers as well and had no way to tell.
     */
    @Override public ResultSet getCrossReference(String pc, String ps, String pt,
                                                 String fc, String fs, String ft) throws SQLException {
        return foreignKeys(ps, pt, false, fs, ft);
    }

    // ── getIndexInfo ─────────────────────────────────────────────────────────
    private static final String[] IDX_COLS = {
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
        "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
        "CARDINALITY", "PAGES", "FILTER_CONDITION"
    };
    private static final int[] IDX_TYPES = {
        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BOOLEAN, Types.VARCHAR,
        Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
        Types.BIGINT, Types.BIGINT, Types.VARCHAR
    };

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT n.nspname, t.relname, i.indrelid, i.indisunique, x.relname AS index_name, "
          + "i.indkey, i.indoption, i.indnkeyatts, am.amname, i.indpred "
          + "FROM pg_catalog.pg_index i "
          + "JOIN pg_catalog.pg_class t ON t.oid = i.indrelid "
          + "JOIN pg_catalog.pg_class x ON x.oid = i.indexrelid "
          + "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace "
          + "LEFT JOIN pg_catalog.pg_am am ON am.oid = x.relam WHERE true");
        if (schema != null) sql.append(" AND n.nspname = '").append(q(schema)).append("'");
        if (table != null) sql.append(" AND t.relname = '").append(q(table)).append("'");
        if (unique) sql.append(" AND i.indisunique = true");
        List<Object[]> src = queryRows(sql.toString(), new String[]{
            "nspname", "relname", "indrelid", "indisunique", "index_name", "indkey", "indoption", "indnkeyatts", "amname", "indpred"
        });
        java.util.Map<String, java.util.Map<Integer, String>> attributes = new java.util.HashMap<>();
        List<Object[]> rows = new ArrayList<>();
        for (Object[] r : src) {
            String oid = str(r[2]);
            java.util.Map<Integer, String> names = attributes.get(oid);
            if (names == null) {
                names = new java.util.HashMap<>();
                for (Object[] attribute : queryRows("SELECT attnum, attname FROM pg_catalog.pg_attribute WHERE attrelid = "
                        + Long.parseLong(oid), new String[]{"attnum", "attname"})) {
                    names.put(Integer.parseInt(str(attribute[0])), str(attribute[1]));
                }
                attributes.put(oid, names);
            }
            String keys = str(r[5]).replaceAll("[{}]", "").trim();
            String[] positions = keys.isEmpty() ? new String[0] : keys.split("[ ,]+");
            String options = r[6] == null ? "" : str(r[6]).replaceAll("[{}]", "").trim();
            String[] ordering = options.isEmpty() ? new String[0] : options.split("[ ,]+");
            int keyCount = r[7] == null ? positions.length : Integer.parseInt(str(r[7]));
            for (int ordinal = 0; ordinal < positions.length && ordinal < keyCount; ordinal++) {
                int position = Integer.parseInt(positions[ordinal]);
                String direction = null;
                if ("btree".equals(str(r[8]))) {
                    int flags = ordinal < ordering.length ? Integer.parseInt(ordering[ordinal]) : 0;
                    direction = (flags & 1) == 0 ? "A" : "D";
                }
                rows.add(new Object[]{conn.databaseName(), str(r[0]), str(r[1]),
                    !Boolean.parseBoolean(str(r[3])), null, str(r[4]),
                    (short) ("hash".equals(str(r[8])) ? tableIndexHashed : tableIndexOther),
                    (short) (ordinal + 1), names.get(position), direction, 0L, 0L, str(r[9])});
            }
        }
        rows.sort(java.util.Comparator.<Object[], Boolean>comparing(row -> (Boolean) row[3])
            .thenComparing(row -> (String) row[5]).thenComparingInt(row -> ((Short) row[7]).intValue()));
        return new MetaResultSet(IDX_COLS, IDX_TYPES, rows);
    }

    // ── getSchemas ───────────────────────────────────────────────────────────
    private static final String[] SCHEMAS_COLS = { "TABLE_SCHEM", "TABLE_CATALOG" };
    private static final int[] SCHEMAS_TYPES = { Types.VARCHAR, Types.VARCHAR };

    @Override public ResultSet getSchemas() throws SQLException { return getSchemas(null, null); }
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        StringBuilder sql = new StringBuilder(
            "SELECT schema_name FROM information_schema.schemata");
        if (!matchAll(schemaPattern)) sql.append(" WHERE schema_name LIKE '").append(q(schemaPattern)).append("'");
        sql.append(" ORDER BY schema_name");
        List<Object[]> src = queryRows(sql.toString(), new String[]{"schema_name"});
        List<Object[]> rows = new ArrayList<>(src.size());
        for (Object[] r : src) {
            rows.add(new Object[]{ str(r[0]), conn.databaseName() });
        }
        return new MetaResultSet(SCHEMAS_COLS, SCHEMAS_TYPES, rows);
    }

    // ── getCatalogs ──────────────────────────────────────────────────────────
    @Override
    public ResultSet getCatalogs() {
        // PDB is effectively single-database - report the one catalog.
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{ conn.databaseName() });
        return new MetaResultSet(new String[]{"TABLE_CAT"},
                new int[]{Types.VARCHAR}, rows);
    }

    // ── getTableTypes ────────────────────────────────────────────────────────
    @Override
    public ResultSet getTableTypes() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"TABLE"});
        rows.add(new Object[]{"VIEW"});
        return new MetaResultSet(new String[]{"TABLE_TYPE"},
                new int[]{Types.VARCHAR}, rows);
    }

    // ── getTypeInfo ──────────────────────────────────────────────────────────
    private static final String[] TYPEINFO_COLS = {
        "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
        "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
        "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
        "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
    };
    private static final int[] TYPEINFO_TYPES = {
        Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
        Types.VARCHAR, Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT,
        Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
        Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.INTEGER,
        Types.INTEGER, Types.INTEGER
    };

    @Override
    public ResultSet getTypeInfo() {
        List<Object[]> rows = new ArrayList<>();
        addType(rows, "int2", Types.SMALLINT, 5);
        addType(rows, "int4", Types.INTEGER, 10);
        addType(rows, "int8", Types.BIGINT, 19);
        addType(rows, "float4", Types.REAL, 8);
        addType(rows, "float8", Types.DOUBLE, 17);
        addType(rows, "numeric", Types.NUMERIC, 1000);
        addType(rows, "bool", Types.BOOLEAN, 1);
        addType(rows, "text", Types.VARCHAR, 0);
        addType(rows, "varchar", Types.VARCHAR, 0);
        addType(rows, "bytea", Types.BINARY, 0);
        addType(rows, "date", Types.DATE, 13);
        addType(rows, "timestamp", Types.TIMESTAMP, 29);
        addType(rows, "jsonb", Types.OTHER, 0);
        addType(rows, "uuid", Types.OTHER, 36);
        return new MetaResultSet(TYPEINFO_COLS, TYPEINFO_TYPES, rows);
    }

    private void addType(List<Object[]> rows, String name, int jdbcType, int precision) {
        boolean textLike = jdbcType == Types.VARCHAR || jdbcType == Types.CHAR;
        rows.add(new Object[]{
            name, jdbcType, precision,
            textLike ? "'" : null, textLike ? "'" : null,
            null,
            (short) typeNullable,
            textLike,
            (short) typeSearchable,
            Boolean.FALSE, Boolean.FALSE, Boolean.FALSE,
            name,
            (short) 0, (short) 0, null, null, 10
        });
    }

    // Column lists for the metadata methods this driver has no rows for. The
    // JDBC javadoc fixes each list; an empty result must still HAVE them.
    private static final String PROCEDURES_SPEC =
        "PROCEDURE_CAT,PROCEDURE_SCHEM,PROCEDURE_NAME,RESERVED1,RESERVED2,"
      + "RESERVED3,REMARKS,PROCEDURE_TYPE:S,SPECIFIC_NAME";
    private static final String PROCEDURE_COLUMNS_SPEC =
        "PROCEDURE_CAT,PROCEDURE_SCHEM,PROCEDURE_NAME,COLUMN_NAME,COLUMN_TYPE:S,"
      + "DATA_TYPE:I,TYPE_NAME,PRECISION:I,LENGTH:I,SCALE:S,RADIX:S,NULLABLE:S,"
      + "REMARKS,COLUMN_DEF,SQL_DATA_TYPE:I,SQL_DATETIME_SUB:I,"
      + "CHAR_OCTET_LENGTH:I,ORDINAL_POSITION:I,IS_NULLABLE,SPECIFIC_NAME";
    private static final String COLUMN_PRIVILEGES_SPEC =
        "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,GRANTOR,GRANTEE,"
      + "PRIVILEGE,IS_GRANTABLE";
    private static final String TABLE_PRIVILEGES_SPEC =
        "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,GRANTOR,GRANTEE,PRIVILEGE,IS_GRANTABLE";
    private static final String ROWID_SPEC =
        "SCOPE:S,COLUMN_NAME,DATA_TYPE:I,TYPE_NAME,COLUMN_SIZE:I,BUFFER_LENGTH:I,"
      + "DECIMAL_DIGITS:S,PSEUDO_COLUMN:S";
    private static final String UDTS_SPEC =
        "TYPE_CAT,TYPE_SCHEM,TYPE_NAME,CLASS_NAME,DATA_TYPE:I,REMARKS,BASE_TYPE:S";
    private static final String SUPERTYPES_SPEC =
        "TYPE_CAT,TYPE_SCHEM,TYPE_NAME,SUPERTYPE_CAT,SUPERTYPE_SCHEM,SUPERTYPE_NAME";
    private static final String SUPERTABLES_SPEC =
        "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,SUPERTABLE_NAME";
    private static final String ATTRIBUTES_SPEC =
        "TYPE_CAT,TYPE_SCHEM,TYPE_NAME,ATTR_NAME,DATA_TYPE:I,ATTR_TYPE_NAME,"
      + "ATTR_SIZE:I,DECIMAL_DIGITS:I,NUM_PREC_RADIX:I,NULLABLE:I,REMARKS,"
      + "ATTR_DEF,SQL_DATA_TYPE:I,SQL_DATETIME_SUB:I,CHAR_OCTET_LENGTH:I,"
      + "ORDINAL_POSITION:I,IS_NULLABLE,SCOPE_CATALOG,SCOPE_SCHEMA,SCOPE_TABLE,"
      + "SOURCE_DATA_TYPE:S";
    private static final String CLIENT_INFO_SPEC =
        "NAME,MAX_LEN:I,DEFAULT_VALUE,DESCRIPTION";
    private static final String FUNCTIONS_SPEC =
        "FUNCTION_CAT,FUNCTION_SCHEM,FUNCTION_NAME,REMARKS,FUNCTION_TYPE:S,"
      + "SPECIFIC_NAME";
    private static final String FUNCTION_COLUMNS_SPEC =
        "FUNCTION_CAT,FUNCTION_SCHEM,FUNCTION_NAME,COLUMN_NAME,COLUMN_TYPE:S,"
      + "DATA_TYPE:I,TYPE_NAME,PRECISION:I,LENGTH:I,SCALE:S,RADIX:S,NULLABLE:S,"
      + "REMARKS,CHAR_OCTET_LENGTH:I,ORDINAL_POSITION:I,IS_NULLABLE,SPECIFIC_NAME";
    private static final String PSEUDO_COLUMNS_SPEC =
        "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE:I,COLUMN_SIZE:I,"
      + "DECIMAL_DIGITS:I,NUM_PREC_RADIX:I,COLUMN_USAGE,REMARKS,"
      + "CHAR_OCTET_LENGTH:I,IS_NULLABLE";

    // ── remaining metadata stubs (empty) ─────────────────────────────────────
    @Override public ResultSet getProcedures(String c, String s, String p) { return emptyWithColumns(PROCEDURES_SPEC); }
    @Override public ResultSet getProcedureColumns(String c, String s, String p, String col) { return emptyWithColumns(PROCEDURE_COLUMNS_SPEC); }
    @Override public ResultSet getColumnPrivileges(String c, String s, String t, String col) { return emptyWithColumns(COLUMN_PRIVILEGES_SPEC); }
    @Override public ResultSet getTablePrivileges(String c, String s, String p) { return emptyWithColumns(TABLE_PRIVILEGES_SPEC); }
    @Override public ResultSet getBestRowIdentifier(String c, String s, String t, int scope, boolean nullable) { return emptyWithColumns(ROWID_SPEC); }
    @Override public ResultSet getVersionColumns(String c, String s, String t) { return emptyWithColumns(ROWID_SPEC); }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int conc) { return conc == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsBatchUpdates() { return true; }
    @Override public ResultSet getUDTs(String c, String s, String typeName, int[] types) { return emptyWithColumns(UDTS_SPEC); }
    @Override public Connection getConnection() { return conn; }
    @Override public boolean supportsSavepoints() { return true; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }
    @Override public ResultSet getSuperTypes(String c, String s, String t) { return emptyWithColumns(SUPERTYPES_SPEC); }
    @Override public ResultSet getSuperTables(String c, String s, String t) { return emptyWithColumns(SUPERTABLES_SPEC); }
    @Override public ResultSet getAttributes(String c, String s, String t, String a) { return emptyWithColumns(ATTRIBUTES_SPEC); }
    @Override public boolean supportsResultSetHoldability(int h) { return h == ResultSet.HOLD_CURSORS_OVER_COMMIT; }
    @Override public int getResultSetHoldability() { return 1; }
    @Override public int getJDBCMajorVersion() { return 4; }
    @Override public int getJDBCMinorVersion() { return 2; }
    @Override public int getSQLStateType() { return DatabaseMetaData.sqlStateSQL99; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }
    @Override public ResultSet getClientInfoProperties() { return emptyWithColumns(CLIENT_INFO_SPEC); }
    @Override public ResultSet getFunctions(String c, String s, String p) { return emptyWithColumns(FUNCTIONS_SPEC); }
    @Override public ResultSet getFunctionColumns(String c, String s, String p, String col) { return emptyWithColumns(FUNCTION_COLUMNS_SPEC); }
    @Override public ResultSet getPseudoColumns(String c, String s, String t, String col) { return emptyWithColumns(PSEUDO_COLUMNS_SPEC); }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    // ── small value coercions ────────────────────────────────────────────────
    private static String str(Object o) { return o == null ? null : o.toString(); }
    private static Integer toInt(Object o) {
        if (o == null) return null;
        if (o instanceof Number) return ((Number) o).intValue();
        try { return Integer.parseInt(o.toString().trim()); } catch (NumberFormatException e) { return null; }
    }
    private static Short toShort(Object o) {
        Integer i = toInt(o);
        return i == null ? null : i.shortValue();
    }
}
