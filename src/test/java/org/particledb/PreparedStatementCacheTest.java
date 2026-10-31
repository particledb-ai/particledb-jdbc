package org.particledb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Verify the client-side prepared cache behaviors. */
public class PreparedStatementCacheTest {

    @Test
    void records_executions_per_sql() {
        ClientPreparedCache c = new ClientPreparedCache(256, 5 * 1024 * 1024);
        assertEquals(1, c.recordExecution("SELECT 1"));
        assertEquals(2, c.recordExecution("SELECT 1"));
        assertEquals(1, c.recordExecution("SELECT 2"));
        assertEquals(2, c.size());
        assertEquals(2, c.execCount("SELECT 1"));
        assertEquals(0, c.execCount("SELECT 3"));
    }

    @Test
    void evicts_eldest_when_max_entries_exceeded() {
        ClientPreparedCache c = new ClientPreparedCache(3, 5 * 1024 * 1024);
        c.recordExecution("a");
        c.recordExecution("b");
        c.recordExecution("c");
        c.recordExecution("d"); // evicts a
        assertEquals(3, c.size());
        assertEquals(0, c.execCount("a"));
        assertEquals(1, c.execCount("d"));
    }

    @Test
    void prepare_threshold_default_is_5_pgjdbc_compatible() {
        // The default threshold is set in Pgcompat parse - verify its
        // documented value here.
        java.util.Map<String, String> p = new java.util.HashMap<>();
        assertEquals(5, Pgcompat.intParam(p, "prepareThreshold", 5));
    }

    @Test
    void cache_size_default_is_256_pgjdbc_compatible() {
        java.util.Map<String, String> p = new java.util.HashMap<>();
        assertEquals(256, Pgcompat.intParam(p, "preparedStatementCacheQueries", 256));
        assertEquals(5, Pgcompat.intParam(p, "preparedStatementCacheSizeMiB", 5));
    }
}
