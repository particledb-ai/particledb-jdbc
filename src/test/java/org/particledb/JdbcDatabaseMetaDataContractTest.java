package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link DatabaseMetaData} CONFORMANCE against a live server.
 *
 * <p>{@code DriverContractValidationTest.databaseMetaData_enumeratesRealSchema}
 * already proves getTables/getColumns find a table that exists. What was NOT
 * covered - and is what a schema-reflection tool (Flyway, Liquibase, Hibernate
 * hbm2ddl, an IDE browser) actually consumes - is the SHAPE of every one of
 * these result sets and the relational metadata beyond columns:
 * {@code getPrimaryKeys}, {@code getImportedKeys}/{@code getExportedKeys}/
 * {@code getCrossReference}, {@code getIndexInfo}, {@code getSchemas},
 * {@code getTableTypes}, {@code getTypeInfo}.
 *
 * <p>The JDBC javadoc FIXES the column list of each of these. A method that
 * answers with the right rows under the wrong column names is unusable by
 * anything that reads by label, and the failure surfaces far from the cause  -
 * so every result set here is checked for its specified columns as well as its
 * content.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcDatabaseMetaDataContractTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping DatabaseMetaData suite");
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE dmd_parent ("
                    + "pid INT PRIMARY KEY, label TEXT NOT NULL)");
            st.executeUpdate("CREATE TABLE dmd_child ("
                    + "cid INT PRIMARY KEY, "
                    + "pid INT REFERENCES dmd_parent(pid), "
                    + "amount NUMERIC(10,2), note VARCHAR(64))");
        }
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    // ── shape checking ────────────────────────────────────────────────────
    /** Assert every named column is present (case-insensitively) and in order. */
    private static void assertColumns(ResultSet rs, String what, String... expected)
            throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        List<String> actual = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            actual.add(md.getColumnLabel(i).toUpperCase());
        }
        for (String e : expected) {
            assertTrue(actual.contains(e.toUpperCase()),
                    what + " is missing the JDBC-specified column " + e
                            + " (has " + actual + ")");
            assertDoesNotThrow(() -> rs.findColumn(e),
                    what + ": findColumn(\"" + e + "\") must resolve");
        }
        assertTrue(md.getColumnCount() >= expected.length,
                what + " has " + md.getColumnCount() + " columns, fewer than the "
                        + expected.length + " the contract names");
    }

    private static Set<String> collect(ResultSet rs, String col) throws SQLException {
        Set<String> out = new LinkedHashSet<>();
        try (ResultSet r = rs) {
            while (r.next()) out.add(r.getString(col));
        }
        return out;
    }

    // ── getTables ─────────────────────────────────────────────────────────
    @Test
    void getTables_hasItsSpecifiedColumnsAndFindsBothTables() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getTables(null, null, "dmd_%", null)) {
                assertColumns(rs, "getTables", "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                        "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
                Set<String> names = new LinkedHashSet<>();
                while (rs.next()) {
                    names.add(rs.getString("TABLE_NAME"));
                    assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                }
                assertTrue(names.contains("dmd_parent"), "getTables missed dmd_parent: " + names);
                assertTrue(names.contains("dmd_child"), "getTables missed dmd_child: " + names);
            }
            // The name pattern must actually FILTER - otherwise the assertion
            // above would pass on a method that ignores its arguments.
            try (ResultSet rs = md.getTables(null, null, "dmd_child", null)) {
                Set<String> names = collect(rs, "TABLE_NAME");
                assertEquals(1, names.size(), "the table-name pattern was ignored: " + names);
                assertTrue(names.contains("dmd_child"));
            }
            // A pattern that matches nothing must give an EMPTY result, not
            // everything - the other half of "the filter is real".
            try (ResultSet rs = md.getTables(null, null, "no_such_table_xyz", null)) {
                assertTrue(collect(rs, "TABLE_NAME").isEmpty());
            }
        }
    }

    // ── getColumns ────────────────────────────────────────────────────────
    @Test
    void getColumns_hasItsSpecifiedColumnsAndTypesEachColumn() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getColumns(null, null, "dmd_child", null)) {
                assertColumns(rs, "getColumns", "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                        "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE",
                        "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "COLUMN_DEF",
                        "ORDINAL_POSITION", "IS_NULLABLE", "IS_AUTOINCREMENT");
                int seen = 0;
                boolean sawNumeric = false;
                while (rs.next()) {
                    seen++;
                    String name = rs.getString("COLUMN_NAME");
                    int type = rs.getInt("DATA_TYPE");
                    assertTrue(rs.getInt("ORDINAL_POSITION") > 0,
                            "ORDINAL_POSITION must be 1-based for " + name);
                    if ("cid".equals(name) || "pid".equals(name)) {
                        assertEquals(Types.INTEGER, type, name + " should be INTEGER");
                    }
                    if ("amount".equals(name)) {
                        sawNumeric = true;
                        assertEquals(Types.NUMERIC, type, "amount should be NUMERIC");
                        assertEquals(10, rs.getInt("COLUMN_SIZE"),
                                "NUMERIC(10,2) precision");
                        assertEquals(2, rs.getInt("DECIMAL_DIGITS"),
                                "NUMERIC(10,2) scale");
                    }
                    assertTrue("YES".equals(rs.getString("IS_NULLABLE"))
                            || "NO".equals(rs.getString("IS_NULLABLE")),
                            "IS_NULLABLE must be YES/NO for " + name);
                }
                assertEquals(4, seen, "getColumns found the wrong column count");
                assertTrue(sawNumeric, "the NUMERIC(10,2) column was not reported");
            }
            // NOT NULL must come back as NOT nullable - the one column-level
            // constraint an ORM will act on.
            try (ResultSet rs = md.getColumns(null, null, "dmd_parent", "label")) {
                assertTrue(rs.next());
                assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"),
                        "a NOT NULL column reported as nullable");
                assertEquals("NO", rs.getString("IS_NULLABLE"));
            }
            // POSITIVE CONTROL for the line above: a nullable column differs.
            try (ResultSet rs = md.getColumns(null, null, "dmd_child", "note")) {
                assertTrue(rs.next());
                assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));
                assertEquals("YES", rs.getString("IS_NULLABLE"));
            }
        }
    }

    // ── getPrimaryKeys ────────────────────────────────────────────────────
    @Test
    void getPrimaryKeys_hasItsSpecifiedColumnsAndFindsThePk() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getPrimaryKeys(null, null, "dmd_child")) {
                assertColumns(rs, "getPrimaryKeys", "TABLE_CAT", "TABLE_SCHEM",
                        "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME");
                assertTrue(rs.next(), "getPrimaryKeys found no PK on dmd_child");
                assertEquals("cid", rs.getString("COLUMN_NAME"));
                assertEquals("dmd_child", rs.getString("TABLE_NAME"));
                assertFalse(rs.next(), "dmd_child has a single-column PK");
            }
            // NEGATIVE CONTROL: a table name that does not exist yields nothing,
            // so the row above is a real lookup and not a constant.
            try (ResultSet rs = md.getPrimaryKeys(null, null, "no_such_table_xyz")) {
                assertFalse(rs.next());
            }
        }
    }

    // ── foreign keys ──────────────────────────────────────────────────────
    /**
     * {@code getImportedKeys} on the CHILD and {@code getExportedKeys} on the
     * PARENT must describe the same relationship from the two ends. These have
     * had no test at all, and they are what an ORM uses to infer associations.
     */
    @Test
    void importedAndExportedKeys_describeTheSameRelationship() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            String[] fkCols = {"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
                    "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
                    "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
                    "FK_NAME", "PK_NAME", "DEFERRABILITY"};
            try (ResultSet rs = md.getImportedKeys(null, null, "dmd_child")) {
                assertColumns(rs, "getImportedKeys", fkCols);
                assertTrue(rs.next(), "getImportedKeys found no FK on dmd_child");
                assertEquals("dmd_parent", rs.getString("PKTABLE_NAME"));
                assertEquals("pid", rs.getString("PKCOLUMN_NAME"));
                assertEquals("dmd_child", rs.getString("FKTABLE_NAME"));
                assertEquals("pid", rs.getString("FKCOLUMN_NAME"));
            }
            try (ResultSet rs = md.getExportedKeys(null, null, "dmd_parent")) {
                assertColumns(rs, "getExportedKeys", fkCols);
                assertTrue(rs.next(), "getExportedKeys found no FK pointing at dmd_parent");
                assertEquals("dmd_parent", rs.getString("PKTABLE_NAME"));
                assertEquals("dmd_child", rs.getString("FKTABLE_NAME"));
            }
            // NEGATIVE CONTROL both ways: the parent imports nothing and the
            // child exports nothing.
            try (ResultSet rs = md.getImportedKeys(null, null, "dmd_parent")) {
                assertFalse(rs.next(), "dmd_parent has no foreign keys of its own");
            }
            try (ResultSet rs = md.getExportedKeys(null, null, "dmd_child")) {
                assertFalse(rs.next(), "nothing references dmd_child");
            }
        }
    }

    /**
     * {@code getCrossReference} names BOTH tables and must filter on both. It
     * used to drop the child-side arguments and return every foreign key
     * pointing at the parent - a WRONG result rather than a missing one, which
     * a caller has no way to detect.
     */
    @Test
    void getCrossReference_filtersBothSides() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE dmd_other ("
                        + "oid INT PRIMARY KEY, pid INT REFERENCES dmd_parent(pid))");
            }
            DatabaseMetaData md = c.getMetaData();
            // POSITIVE CONTROL: the parent now has TWO children, so a method
            // that ignores the child argument would return two rows here.
            try (ResultSet rs = md.getExportedKeys(null, null, "dmd_parent")) {
                Set<String> children = collect(rs, "FKTABLE_NAME");
                assertEquals(2, children.size(),
                        "expected two tables referencing dmd_parent, got " + children);
            }
            try (ResultSet rs = md.getCrossReference(null, null, "dmd_parent",
                                                     null, null, "dmd_child")) {
                Set<String> children = collect(rs, "FKTABLE_NAME");
                assertEquals(1, children.size(),
                        "getCrossReference ignored the child-side arguments and "
                        + "returned every reference to dmd_parent: " + children);
                assertTrue(children.contains("dmd_child"));
            }
            try (ResultSet rs = md.getCrossReference(null, null, "dmd_parent",
                                                     null, null, "dmd_other")) {
                assertEquals(new LinkedHashSet<>(java.util.Collections.singletonList("dmd_other")),
                        collect(rs, "FKTABLE_NAME"));
            }
        }
    }

    // ── getIndexInfo ──────────────────────────────────────────────────────
    @Test
    void getIndexInfo_hasItsSpecifiedColumnsAndReportsThePkIndex() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getIndexInfo(null, null, "dmd_child", false, true)) {
                assertColumns(rs, "getIndexInfo", "TABLE_CAT", "TABLE_SCHEM",
                        "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                        "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                        "CARDINALITY", "PAGES", "FILTER_CONDITION");
                boolean sawPkColumn = false;
                while (rs.next()) {
                    if ("cid".equals(rs.getString("COLUMN_NAME"))) {
                        sawPkColumn = true;
                        assertFalse(rs.getBoolean("NON_UNIQUE"),
                                "the primary-key index must be unique");
                    }
                }
                assertTrue(sawPkColumn,
                        "getIndexInfo did not report the primary-key index column");
            }
        }
    }

    @Test
    void compositeForeignKeysKeepTheReferencedColumnOrder() throws Exception {
        try (Connection c = DriverManager.getConnection(url()); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE dmd_composite_parent (a INT, b INT, UNIQUE (b, a))");
            st.execute("CREATE TABLE dmd_composite_child (x INT, y INT, CONSTRAINT dmd_composite_fk FOREIGN KEY (x, y) REFERENCES dmd_composite_parent (b, a) ON DELETE CASCADE)");
            st.execute("CREATE TABLE dmd_composite_other (x INT, y INT, CONSTRAINT dmd_composite_fk FOREIGN KEY (x, y) REFERENCES dmd_composite_parent (b, a))");
            try {
                try (ResultSet rs = c.getMetaData().getImportedKeys(null, "public", "dmd_composite_child")) {
                    assertTrue(rs.next());
                    assertEquals("x", rs.getString("FKCOLUMN_NAME"));
                    assertEquals("b", rs.getString("PKCOLUMN_NAME"));
                    assertEquals(1, rs.getShort("KEY_SEQ"));
                    assertEquals(DatabaseMetaData.importedKeyCascade, rs.getShort("DELETE_RULE"));
                    assertTrue(rs.next());
                    assertEquals("y", rs.getString("FKCOLUMN_NAME"));
                    assertEquals("a", rs.getString("PKCOLUMN_NAME"));
                    assertEquals(2, rs.getShort("KEY_SEQ"));
                    assertFalse(rs.next());
                }
                try (ResultSet rs = c.getMetaData().getExportedKeys(null, "public", "dmd_composite_parent")) {
                    int rows = 0;
                    while (rs.next()) { rows++; assertTrue(rs.getString("FKTABLE_NAME").startsWith("dmd_composite_")); }
                    assertEquals(4, rows);
                }
            } finally {
                st.execute("DROP TABLE dmd_composite_other");
                st.execute("DROP TABLE dmd_composite_child");
                st.execute("DROP TABLE dmd_composite_parent");
            }
        }
    }

    @Test
    void ordinaryIndexesPreserveColumnOrderAndUniqueFiltering() throws Exception {
        try (Connection c = DriverManager.getConnection(url()); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE dmd_indexes (a INT, b INT, c TEXT)");
            st.execute("CREATE INDEX dmd_plain ON dmd_indexes (b DESC, a) WHERE b > 0");
            st.execute("CREATE UNIQUE INDEX dmd_unique ON dmd_indexes (c)");
            try {
                java.util.Map<String, java.util.List<String>> columns = new java.util.LinkedHashMap<>();
                try (ResultSet rs = c.getMetaData().getIndexInfo(null, "public", "dmd_indexes", false, true)) {
                    while (rs.next()) {
                        columns.computeIfAbsent(rs.getString("INDEX_NAME"), key -> new java.util.ArrayList<>())
                            .add(rs.getString("COLUMN_NAME"));
                        boolean plain = "dmd_plain".equals(rs.getString("INDEX_NAME"));
                        assertEquals(plain, rs.getBoolean("NON_UNIQUE"));
                        if (plain) {
                            assertTrue(rs.getString("FILTER_CONDITION").contains("b"));
                            assertEquals(rs.getInt("ORDINAL_POSITION") == 1 ? "D" : "A", rs.getString("ASC_OR_DESC"));
                        }
                    }
                }
                assertEquals(java.util.List.of("b", "a"), columns.get("dmd_plain"));
                assertEquals(java.util.List.of("c"), columns.get("dmd_unique"));
                try (ResultSet rs = c.getMetaData().getIndexInfo(null, "public", "dmd_indexes", true, true)) {
                    assertTrue(rs.next());
                    assertEquals("dmd_unique", rs.getString("INDEX_NAME"));
                    assertFalse(rs.getBoolean("NON_UNIQUE"));
                    assertFalse(rs.next());
                }
            } finally { st.execute("DROP TABLE dmd_indexes"); }
        }
    }

    // ── catalog / schema / type enumeration ───────────────────────────────
    @Test
    void schemaCatalogAndTypeEnumeration_haveTheirSpecifiedColumns() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getSchemas()) {
                assertColumns(rs, "getSchemas", "TABLE_SCHEM", "TABLE_CATALOG");
                assertTrue(collect(rs, "TABLE_SCHEM").contains("public"),
                        "getSchemas did not report the public schema");
            }
            try (ResultSet rs = md.getCatalogs()) {
                assertColumns(rs, "getCatalogs", "TABLE_CAT");
                assertFalse(collect(rs, "TABLE_CAT").isEmpty());
            }
            try (ResultSet rs = md.getTableTypes()) {
                assertColumns(rs, "getTableTypes", "TABLE_TYPE");
                Set<String> types = collect(rs, "TABLE_TYPE");
                assertTrue(types.contains("TABLE"));
                assertTrue(types.contains("VIEW"));
            }
            try (ResultSet rs = md.getTypeInfo()) {
                assertColumns(rs, "getTypeInfo", "TYPE_NAME", "DATA_TYPE", "PRECISION",
                        "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE",
                        "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
                        "MINIMUM_SCALE", "MAXIMUM_SCALE", "NUM_PREC_RADIX");
                Set<String> names = collect(rs, "TYPE_NAME");
                for (String t : new String[]{"int4", "int8", "text", "bool", "numeric",
                                             "timestamp", "date", "bytea"}) {
                    assertTrue(names.contains(t), "getTypeInfo is missing " + t);
                }
            }
        }
    }

    // ── the methods with no rows must still have their columns ────────────
    /**
     * A metadata method with nothing to report must return a result set with the
     * columns the JDBC javadoc names - not a ZERO-COLUMN one. "No rows" and "no
     * such column" are different answers: a reflection tool that walks the
     * specified columns of an empty {@code getTablePrivileges} gets a
     * column-not-found failure instead of an empty loop, and the failure appears
     * far from its cause.
     */
    @Test
    void metadataMethodsWithNoRows_stillCarryTheirSpecifiedColumns() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getProcedures(null, null, "%")) {
                assertColumns(rs, "getProcedures", "PROCEDURE_CAT", "PROCEDURE_SCHEM",
                        "PROCEDURE_NAME", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME");
            }
            try (ResultSet rs = md.getProcedureColumns(null, null, "%", "%")) {
                assertColumns(rs, "getProcedureColumns", "PROCEDURE_CAT", "PROCEDURE_NAME",
                        "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
            }
            try (ResultSet rs = md.getTablePrivileges(null, null, "%")) {
                assertColumns(rs, "getTablePrivileges", "TABLE_CAT", "TABLE_SCHEM",
                        "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
            }
            try (ResultSet rs = md.getColumnPrivileges(null, null, "dmd_child", "%")) {
                assertColumns(rs, "getColumnPrivileges", "TABLE_CAT", "TABLE_SCHEM",
                        "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
                        "IS_GRANTABLE");
            }
            try (ResultSet rs = md.getBestRowIdentifier(null, null, "dmd_child",
                    DatabaseMetaData.bestRowSession, true)) {
                assertColumns(rs, "getBestRowIdentifier", "SCOPE", "COLUMN_NAME",
                        "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                        "DECIMAL_DIGITS", "PSEUDO_COLUMN");
            }
            try (ResultSet rs = md.getVersionColumns(null, null, "dmd_child")) {
                assertColumns(rs, "getVersionColumns", "SCOPE", "COLUMN_NAME",
                        "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "PSEUDO_COLUMN");
            }
            try (ResultSet rs = md.getUDTs(null, null, "%", null)) {
                assertColumns(rs, "getUDTs", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE");
            }
            try (ResultSet rs = md.getFunctions(null, null, "%")) {
                assertColumns(rs, "getFunctions", "FUNCTION_CAT", "FUNCTION_SCHEM",
                        "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME");
            }
            try (ResultSet rs = md.getFunctionColumns(null, null, "%", "%")) {
                assertColumns(rs, "getFunctionColumns", "FUNCTION_CAT", "FUNCTION_NAME",
                        "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
            }
            try (ResultSet rs = md.getPseudoColumns(null, null, "%", "%")) {
                assertColumns(rs, "getPseudoColumns", "TABLE_CAT", "TABLE_SCHEM",
                        "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE",
                        "COLUMN_USAGE", "IS_NULLABLE");
            }
            try (ResultSet rs = md.getSuperTypes(null, null, "%")) {
                assertColumns(rs, "getSuperTypes", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME");
            }
            try (ResultSet rs = md.getSuperTables(null, null, "%")) {
                assertColumns(rs, "getSuperTables", "TABLE_CAT", "TABLE_SCHEM",
                        "TABLE_NAME", "SUPERTABLE_NAME");
            }
            try (ResultSet rs = md.getAttributes(null, null, "%", "%")) {
                assertColumns(rs, "getAttributes", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE",
                        "ORDINAL_POSITION", "IS_NULLABLE");
            }
            try (ResultSet rs = md.getClientInfoProperties()) {
                assertColumns(rs, "getClientInfoProperties", "NAME", "MAX_LEN",
                        "DEFAULT_VALUE", "DESCRIPTION");
            }
        }
    }

    // ── the identity a framework picks its dialect from ───────────────────
    /**
     * Flyway 9's {@code PostgreSQLDatabaseType} matches on
     * {@code productName.startsWith("PostgreSQL")} and a version >= 9.0;
     * Hibernate and jOOQ do the same kind of sniff. This is the switch that
     * decides which SQL dialect an entire application emits, and it is settable
     * from the URL - so both settings are pinned.
     */
    @Test
    void productIdentity_drivesFrameworkDialectSelection() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            DatabaseMetaData md = c.getMetaData();
            assertEquals("PostgreSQL", md.getDatabaseProductName(),
                    "pgCompat defaults to on so PG-dialect tooling selects itself");
            assertTrue(md.getDatabaseMajorVersion() >= 9,
                    "Flyway 9 rejects a PostgreSQL below 9.0");
            assertTrue(md.getDriverName().contains("ParticleDB"),
                    "the DRIVER identity must stay honest even when the PRODUCT "
                    + "identity is PG-compatible");
            assertEquals(4, md.getJDBCMajorVersion());
            assertEquals("\"", md.getIdentifierQuoteString());
            assertTrue(md.storesLowerCaseIdentifiers(),
                    "PG folds unquoted identifiers to lower case");
            assertTrue(md.supportsMixedCaseQuotedIdentifiers());
            assertSame(c, md.getConnection());
        }
        // POSITIVE CONTROL for the toggle: pgCompat=false reports the native
        // identity, so the value above is a real setting and not a constant.
        try (Connection c = DriverManager.getConnection(url() + "&pgCompat=false")) {
            assertEquals("ParticleDB", c.getMetaData().getDatabaseProductName());
        }
    }
}
