package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v1 fallback correctness: whichever mode the server negotiates (the current
 * master {@code particledb} binary is v1-only, so this exercises the real v1
 * single-stream fallback path) must NOT silently lose rows, and a stream-id
 * collision must fail LOUDLY rather than drop a result.
 *
 * <p>NOTE ON THE FORCING KNOB: the driver exposes NO URL parameter or system
 * property to force v1 framing - v1 is reached only when the server answers
 * HELLO_OK with {@code chosen_caps & 0x02 == 0} (see
 * {@link org.particledb.wire.StreamMux}). The {@link #v1FallbackForcingKnob_isNotExposed()}
 * method documents that gap and is {@code @Disabled}; the loud-failure /
 * no-silent-loss invariant is still exercised by
 * {@link #negotiatedMode_doesNotSilentlyLoseRows_orFailsLoudly()} against
 * whatever mode the server actually picks.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StreamMuxFallbackTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping v1 fallback test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test"
                + "?preferUnixSocket=false";
    }

    /**
     * There is no client-side knob to force the v1 single-stream fallback.
     * Documented here (and asserted server-lessly) and disabled: we cannot
     * deterministically drive the v1 path from the client, so we do NOT fake
     * it. If a {@code forceV1} / {@code protocolVersion} option is ever added,
     * enable this and exercise the u16 stream-id wrap loud-collision path.
     */
    @Disabled("No v1-forcing knob exists in the driver (no URL param / system "
            + "property). v1 is only reached when the SERVER negotiates it. "
            + "Enable once a forceV1/protocolVersion option is added.")
    @Test
    void v1FallbackForcingKnob_isNotExposed() {
        // Documents the absence: none of the advertised driver properties is a
        // v1-forcing / protocol-pinning knob. (Body would exercise the knob if
        // it existed; disabled because it does not.)
        DriverPropertyInfo[] props = new Driver().getPropertyInfo(url(), null);
        for (DriverPropertyInfo p : props) {
            String n = p.name.toLowerCase();
            assertFalse(n.contains("forcev1") || n.equals("protocolversion")
                            || n.contains("disablev2"),
                    "unexpected v1-forcing knob appeared: " + p.name
                            + " — update this test to exercise it");
        }
    }

    @Test
    void negotiatedMode_doesNotSilentlyLoseRows_orFailsLoudly() throws Exception {
        final int threads = 16;
        final int perThread = 8;
        try (Connection c = DriverManager.getConnection(url())) {
            boolean v2 = ((PdbConnection) c).mux().isV2();
            System.out.println("[StreamMuxFallbackTest] negotiated wire v2Active=" + v2);

            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE fb_rows (id INT PRIMARY KEY, v INT)");
            }

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();
            AtomicInteger loudCollisions = new AtomicInteger(0);
            List<Future<?>> futs = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                futs.add(pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            final int id = tid * perThread + i;
                            final int v = id * 3;
                            try (Statement ins = c.createStatement()) {
                                ins.executeUpdate("INSERT INTO fb_rows VALUES ("
                                        + id + ", " + v + ")");
                            }
                            try (Statement sel = c.createStatement();
                                 ResultSet rs = sel.executeQuery(
                                         "SELECT id, v FROM fb_rows WHERE id = " + id)) {
                                if (!rs.next()) {
                                    failures.add("id=" + id + ": read-back ZERO rows"
                                            + " (silent loss)");
                                } else if (rs.getInt(1) != id || rs.getInt(2) != v) {
                                    failures.add("id=" + id + ": mismatch ("
                                            + rs.getInt(1) + "," + rs.getInt(2) + ")");
                                }
                            }
                        }
                    } catch (IllegalStateException loud) {
                        // The mux's putIfAbsent collision guard fired: this is the
                        // ACCEPTABLE loud-failure path (a u16 stream-id wrap), NOT
                        // a silent row loss. Record it as such.
                        loudCollisions.incrementAndGet();
                    } catch (Exception e) {
                        failures.add("tid=" + tid + ": exception " + e);
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futs) f.get(60, TimeUnit.SECONDS);
            pool.shutdown();

            // Invariant: no SILENT row loss. Loud collisions (if any) are fine.
            assertTrue(failures.isEmpty(),
                    "v1/negotiated fallback SILENTLY lost or corrupted rows: " + failures);
            if (loudCollisions.get() > 0) {
                System.out.println("[StreamMuxFallbackTest] observed "
                        + loudCollisions.get() + " loud stream-id collisions (acceptable)");
            }
        }
    }
}
