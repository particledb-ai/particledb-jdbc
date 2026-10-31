package org.particledb;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** Pure unit tests for URL parsing - no server required. */
public class PgCompatTest {

    @Test
    void driver_leaves_other_url_schemes_to_other_drivers() throws SQLException {
        assertNull(new Driver().connect("jdbc:postgresql://localhost/db", new Properties()));
    }

    @Test
    void driver_parse_errors_redact_credentials() {
        SQLException error = assertThrows(SQLException.class, () -> new Driver().connect(
                "jdbc:particledb://localhost:invalid/db?password=private-credential", new Properties()));
        assertFalse(error.toString().contains("private-credential"));
        assertNull(error.getCause());
    }

    @Test
    void malformed_urls_redact_credentials() {
        for (String url : new String[] {
                "jdbc:particledb://localhost:invalid/db?password=private-credential",
                "jdbc:postgresql://localhost/db?password=private-credential"
        }) {
            SQLException error = assertThrows(SQLException.class, () -> Pgcompat.parse(url, null));
            assertFalse(error.toString().contains("private-credential"));
            assertNull(error.getCause());
        }
    }

    @Test
    void parses_basic_url() throws SQLException {
        Pgcompat.ParsedUrl u = Pgcompat.parse(
                "jdbc:particledb://localhost:7173/mydb", null);
        assertEquals("localhost", u.host);
        assertEquals(7173, u.port);
        assertEquals("mydb", u.database);
        assertTrue(u.params.isEmpty());
    }

    @Test
    void parses_url_with_query_params() throws SQLException {
        Pgcompat.ParsedUrl u = Pgcompat.parse(
                "jdbc:particledb://db.internal:7173/prod?prepareThreshold=10&maxStreamsPerConn=128",
                null);
        assertEquals("db.internal", u.host);
        assertEquals(7173, u.port);
        assertEquals("prod", u.database);
        assertEquals("10", u.params.get("prepareThreshold"));
        assertEquals("128", u.params.get("maxStreamsPerConn"));
    }

    @Test
    void defaults_port_when_missing() throws SQLException {
        Pgcompat.ParsedUrl u = Pgcompat.parse("jdbc:particledb://localhost", null);
        assertEquals("localhost", u.host);
        assertEquals(7173, u.port);
        assertEquals("default", u.database);
    }

    @Test
    void rejects_non_pdb_url() {
        assertThrows(SQLException.class, () ->
                Pgcompat.parse("jdbc:postgresql://localhost:5432/x", null));
    }

    @Test
    void merges_extra_properties() throws SQLException {
        Properties extra = new Properties();
        extra.setProperty("user", "alice");
        extra.setProperty("prepareThreshold", "99"); // url wins
        Pgcompat.ParsedUrl u = Pgcompat.parse(
                "jdbc:particledb://localhost:7173/db?prepareThreshold=5", extra);
        assertEquals("5", u.params.get("prepareThreshold"));
        assertEquals("alice", u.params.get("user"));
    }

    @Test
    void int_param_returns_default_when_missing() {
        java.util.Map<String, String> p = new java.util.HashMap<>();
        assertEquals(5, Pgcompat.intParam(p, "prepareThreshold", 5));
        p.put("prepareThreshold", "10");
        assertEquals(10, Pgcompat.intParam(p, "prepareThreshold", 5));
        p.put("prepareThreshold", "garbage");
        assertEquals(5, Pgcompat.intParam(p, "prepareThreshold", 5));
    }

    @Test
    void bool_param_accepts_multiple_truthy_values() {
        java.util.Map<String, String> p = new java.util.HashMap<>();
        p.put("a", "true");
        p.put("b", "1");
        p.put("c", "yes");
        p.put("d", "false");
        assertTrue(Pgcompat.boolParam(p, "a", false));
        assertTrue(Pgcompat.boolParam(p, "b", false));
        assertTrue(Pgcompat.boolParam(p, "c", false));
        assertFalse(Pgcompat.boolParam(p, "d", true));
    }
}
