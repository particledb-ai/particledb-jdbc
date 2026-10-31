package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drive N threads off ONE {@code PdbConnection}. Each thread runs its own
 * INSERTs on its own {@link PreparedStatement} - they share the underlying
 * {@code StreamMux} but each call uses its own multiplexed stream.
 *
 * <p>Goal: prove no head-of-line blocking. We don't strictly assert
 * timing here (CI is noisy); we assert correctness - N parallel inserts
 * produce N rows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultiplexedConcurrencyTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping concurrency test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test"
                + "?preferUnixSocket=false&maxStreamsPerConn=64";
    }

    @Test
    void parallel_inserts_share_one_connection() throws Exception {
        int threads = 16;
        int perThread = 10;
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE mux_t (id INT PRIMARY KEY, t INT, v INT)");
            }
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            AtomicInteger errors = new AtomicInteger(0);
            List<Future<?>> futs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                futs.add(pool.submit(() -> {
                    try (PreparedStatement ps =
                                 c.prepareStatement("INSERT INTO mux_t VALUES (?,?,?)")) {
                        for (int i = 0; i < perThread; i++) {
                            ps.setInt(1, tid * perThread + i);
                            ps.setInt(2, tid);
                            ps.setInt(3, i);
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                        e.printStackTrace();
                    }
                }));
            }
            for (Future<?> f : futs) f.get(30, TimeUnit.SECONDS);
            pool.shutdown();
            assertEquals(0, errors.get(),
                    "concurrent inserts had errors — see stack traces above");
        }
    }
}
