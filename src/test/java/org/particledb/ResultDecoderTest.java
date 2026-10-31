package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.ArrowDecoder.ResultDecoder;
import org.particledb.wire.StreamState;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@code resultDecoder} connection property: parsing, connect-time
 * rejection, and that {@code auto} and {@code builtin} decode every type with
 * the built-in reader. {@code arrow} against the built-in reader:
 * {@link ArrowIpcReaderDifferentialTest} and
 * {@link ResultDecoderLiveDifferentialTest}; live and module-flag checks:
 * {@link ArrowModuleFlagsTest}.
 */
class ResultDecoderTest {

    @Test
    void parsesTheThreeValuesIgnoringCase() throws Exception {
        assertEquals(ResultDecoder.AUTO, ResultDecoder.parse(null));
        assertEquals(ResultDecoder.AUTO, ResultDecoder.parse(""));
        assertEquals(ResultDecoder.AUTO, ResultDecoder.parse("auto"));
        assertEquals(ResultDecoder.AUTO, ResultDecoder.parse("AUTO"));
        assertEquals(ResultDecoder.ARROW, ResultDecoder.parse("arrow"));
        assertEquals(ResultDecoder.ARROW, ResultDecoder.parse("Arrow"));
        assertEquals(ResultDecoder.BUILTIN, ResultDecoder.parse("builtin"));
        assertEquals(ResultDecoder.BUILTIN, ResultDecoder.parse("BuiltIn"));
    }

    @Test
    void rejectsOtherValuesListingTheValidOnes() {
        for (String bad : new String[] {"fast", "built-in", "native", "arrow2", "none"}) {
            SQLException e = assertThrows(SQLException.class, () -> ResultDecoder.parse(bad), bad);
            assertEquals("22023", e.getSQLState());
            assertTrue(e.getMessage().contains("'" + bad + "'"), e.getMessage());
            assertTrue(e.getMessage().contains("valid values are auto, arrow, builtin"), e.getMessage());
        }
    }

    @Test
    void anInvalidValueFailsAtConnectBeforeAnySocketIsOpened() {
        // Port 1 refuses connections: reaching the network would raise 08xxx.
        SQLException url = assertThrows(SQLException.class, () ->
                DriverManager.getConnection("jdbc:particledb://127.0.0.1:1/test?resultDecoder=fast"));
        assertEquals("22023", url.getSQLState(), url.getMessage());
        assertTrue(url.getMessage().contains("valid values are auto, arrow, builtin"), url.getMessage());

        Properties p = new Properties();
        p.setProperty("resultDecoder", "BUILTINX");
        SQLException props = assertThrows(SQLException.class, () ->
                DriverManager.getConnection("jdbc:particledb://127.0.0.1:1/test", p));
        assertEquals("22023", props.getSQLState(), props.getMessage());

        // A valid value in any case passes validation and reaches the network.
        SQLException valid = assertThrows(SQLException.class, () ->
                DriverManager.getConnection("jdbc:particledb://127.0.0.1:1/test?resultDecoder=BuiltIn"));
        assertNotEquals("22023", valid.getSQLState(), valid.getMessage());
    }

    @Test
    void driverAdvertisesTheProperty() throws Exception {
        DriverPropertyInfo info = Arrays.stream(new Driver().getPropertyInfo("jdbc:particledb://localhost/db", null))
                .filter(i -> "resultDecoder".equals(i.name)).findFirst().orElseThrow();
        assertEquals("auto", info.value);
        assertArrayEquals(new String[] {"auto", "arrow", "builtin"}, info.choices);
    }

    @Test
    void autoAndBuiltinDecodeEveryGoldenTypeIdentically() throws Exception {
        // Golden streams (src/test/resources/arrow-golden) so this runs on a
        // plain JVM; resultDecoder=arrow is compared with them in
        // ArrowIpcReaderDifferentialTest and ResultDecoderLiveDifferentialTest.
        for (GoldenCases.Case c : GoldenCases.ALL) {
            byte[] p = ArrowIpcReaderGoldenTest.golden(c.name);
            ArrowDecoder.Decoded auto = ArrowDecoder.decodePositional(p, ResultDecoder.AUTO);
            ArrowDecoder.Decoded builtin = ArrowDecoder.decodePositional(p, ResultDecoder.BUILTIN);
            ArrowDecoder.Decoded oneArg = ArrowDecoder.decodePositional(p);
            for (ArrowDecoder.Decoded d : new ArrowDecoder.Decoded[] {builtin, oneArg}) {
                assertEquals(auto.columnNames, d.columnNames, c.name);
                assertEquals(auto.columnTypes, d.columnTypes, c.name);
                assertEquals(auto.rows.size(), d.rows.size(), c.name);
                for (int r = 0; r < auto.rows.size(); r++) {
                    assertTrue(GoldenCases.cellEquals(auto.rows.get(r)[0], d.rows.get(r)[0]), c.name + " row " + r);
                }
            }
        }
    }

    @Test
    void builtinReadsTheTypesThatOnceNeededArrowJava() throws Exception {
        // 1.0.9's builtin refused these with 0A000; the built-in reader now reads them.
        for (ResultDecoder m : new ResultDecoder[] {ResultDecoder.AUTO, ResultDecoder.BUILTIN}) {
            try (PdbResultSet rs = new PdbResultSet(
                    StreamState.StreamResult.arrow(ArrowIpcReaderGoldenTest.golden("timestamp_us")), m)) {
                assertTrue(rs.next());
                assertEquals(java.time.LocalDateTime.parse("2023-11-14T22:13:20.123456"), rs.getObject(1), m.name());
            }
            try (PdbResultSet rs = new PdbResultSet(
                    StreamState.StreamResult.arrow(ArrowIpcReaderGoldenTest.golden("live-aggregates")), m)) {
                assertTrue(rs.next());
                assertEquals(new java.math.BigDecimal("6"), rs.getBigDecimal(1), m.name());
                assertEquals(java.time.LocalDate.parse("1969-12-31"), rs.getDate(7).toLocalDate(), m.name());
            }
        }
    }

    @Test
    void anEmptyResultDecodesInEveryMode() throws Exception {
        for (ResultDecoder m : ResultDecoder.values()) {
            assertTrue(ArrowDecoder.decodePositional(new byte[0], m).rows.isEmpty(), m.name());
        }
    }
}
