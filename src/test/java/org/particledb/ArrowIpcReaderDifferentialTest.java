package org.particledb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The built-in reader ({@link ArrowIpcReader}) against Apache Arrow Java
 * ({@link ArrowJavaDecoder}, the decoder of driver 1.0.9 and earlier) on the
 * same streams, written by Arrow Java's own writer.
 *
 * <p>For every {@link GoldenCases} case not marked divergent, names, type
 * tokens, field metadata and every cell (value AND Java class) must be
 * identical. For each divergent case the two must really differ (so the
 * divergence list in {@link ArrowIpcReader}'s class comment stays exact), and
 * the reader's value is the golden expectation. Also checks that the
 * checked-in golden files decode the same as a fresh Arrow Java write, and
 * writes them when {@code -Dpdb.writeGolden=<dir>} is set.
 */
@Tag("arrow-java")
public class ArrowIpcReaderDifferentialTest {

    private static boolean same(ArrowDecoder.Decoded a, ArrowDecoder.Decoded b, StringBuilder why) {
        if (!a.columnNames.equals(b.columnNames)) why.append("names ").append(a.columnNames).append(" vs ").append(b.columnNames);
        if (!a.columnTypes.equals(b.columnTypes)) why.append("tokens ").append(a.columnTypes).append(" vs ").append(b.columnTypes);
        if (!a.columnMetadata.equals(b.columnMetadata)) why.append("metadata ");
        if (a.rows.size() != b.rows.size()) why.append("row count ").append(a.rows.size()).append(" vs ").append(b.rows.size());
        for (int r = 0; r < Math.min(a.rows.size(), b.rows.size()); r++) {
            Object[] x = a.rows.get(r), y = b.rows.get(r);
            for (int c = 0; c < x.length; c++) {
                if (!GoldenCases.cellEquals(x[c], y[c])) {
                    why.append(" row ").append(r).append(": ").append(GoldenCases.show(x[c]))
                            .append(" vs ").append(GoldenCases.show(y[c]));
                }
            }
        }
        return why.length() == 0;
    }

    @Test
    void everyCaseMatchesArrowJavaOrIsADocumentedDivergence() throws Exception {
        List<String> diverged = new ArrayList<>();
        for (GoldenCases.Case c : GoldenCases.ALL) {
            byte[] p = ArrowGolden.payload(c);
            ArrowDecoder.Decoded mine = ArrowIpcReader.decode(p);
            if (c.arrowJavaUnsupported) {
                assertThrows(Exception.class, () -> ArrowJavaDecoder.decode(p), c.name);
                diverged.add(c.name + ": Arrow Java cannot read it");
                assertGolden(c, mine);
                continue;
            }
            ArrowDecoder.Decoded arrow = ArrowJavaDecoder.decode(p);
            StringBuilder why = new StringBuilder();
            boolean equal = same(arrow, mine, why);
            if (c.divergent) {
                assertFalse(equal, c.name + " is marked divergent but matches Arrow Java");
                diverged.add(c.name + ": " + why);
            } else {
                assertTrue(equal, c.name + ": " + why);
            }
            // The reader's answer is the golden expectation in either case.
            assertGolden(c, mine);
            // A second decode of the same schema takes the parsed-schema cache.
            assertTrue(same(mine, ArrowIpcReader.decode(p), new StringBuilder()), c.name + " (cached schema)");
        }
        System.out.println("[ArrowIpcReaderDifferentialTest] documented divergences:\n  "
                + String.join("\n  ", diverged));
    }

    private static void assertGolden(GoldenCases.Case c, ArrowDecoder.Decoded mine) {
        assertEquals(Arrays.asList(c.token), mine.columnTypes, c.name);
        for (int r = 0; r < c.expected.length; r++) {
            assertTrue(GoldenCases.cellEquals(c.expected[r], mine.rows.get(r)[0]),
                    c.name + " row " + r + ": want " + GoldenCases.show(c.expected[r])
                            + " got " + GoldenCases.show(mine.rows.get(r)[0]));
        }
    }

    @Test
    void checkedInGoldensDecodeLikeAFreshArrowJavaWrite() throws Exception {
        String out = System.getProperty("pdb.writeGolden");
        for (GoldenCases.Case c : GoldenCases.ALL) {
            byte[] fresh = ArrowGolden.payload(c);
            if (out != null && !out.isEmpty()) {
                Files.createDirectories(Paths.get(out));
                Files.write(Paths.get(out, c.name + ".arrow"), fresh);
                continue;
            }
            byte[] golden = ArrowIpcReaderGoldenTest.golden(c.name);
            StringBuilder why = new StringBuilder();
            assertTrue(same(ArrowIpcReader.decode(fresh), ArrowIpcReader.decode(golden), why), c.name + ": " + why);
            if (!c.arrowJavaUnsupported) {
                assertTrue(same(ArrowJavaDecoder.decode(golden), ArrowJavaDecoder.decode(fresh), new StringBuilder()), c.name);
            }
        }
    }

    @Test
    void liveCapturedGoldensMatchArrowJava() throws Exception {
        // Streams captured from a real server (ResultDecoderLiveDifferentialTest
        // writes them): what the engine actually emits.
        for (String name : ArrowIpcReaderGoldenTest.LIVE) {
            byte[] p = ArrowIpcReaderGoldenTest.golden(name);
            StringBuilder why = new StringBuilder();
            assertTrue(same(ArrowJavaDecoder.decode(p), ArrowIpcReader.decode(p), why), name + ": " + why);
        }
    }

    @Test
    void multiColumnStreamWithMetadataAndDuplicateLabels() throws Exception {
        // Every non-divergent case as one wide stream (fields side by side
        // need equal row counts: use the cases with exactly three rows).
        List<GoldenCases.Case> three = new ArrayList<>();
        for (GoldenCases.Case c : GoldenCases.ALL) {
            if (!c.divergent && !c.arrowJavaUnsupported && c.raw.length == 3 && !c.type.startsWith("dict:")) three.add(c);
        }
        byte[] p = ArrowGolden.wide(three);
        StringBuilder why = new StringBuilder();
        assertTrue(same(ArrowJavaDecoder.decode(p), ArrowIpcReader.decode(p), why), why.toString());
        assertEquals(three.size(), ArrowIpcReader.decode(p).columnNames.size());
    }
}
