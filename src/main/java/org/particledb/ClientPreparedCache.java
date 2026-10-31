package org.particledb;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Client-side prepared statement cache. Tracks executions per SQL string
 * and once the count crosses {@code prepareThreshold} (default 5, matching
 * pgjdbc), the SQL is considered "hot" and the driver may opt to use
 * server-side prepare (when v2 server supports it).
 *
 * <p>Eviction policy: LRU + bytes cap. Defaults: 256 entries, 5 MiB total.
 */
public final class ClientPreparedCache {

    private final int maxEntries;
    private final int maxBytes;
    private int currentBytes = 0;

    private final LinkedHashMap<String, CachedSql> map;

    public ClientPreparedCache(int maxEntries, int maxBytes) {
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
        final ClientPreparedCache self = this;
        this.map = new LinkedHashMap<String, CachedSql>(maxEntries + 16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CachedSql> eldest) {
                if (size() > self.maxEntries) {
                    self.currentBytes -= eldest.getValue().approxBytes;
                    return true;
                }
                Iterator<Map.Entry<String, CachedSql>> iter = entrySet().iterator();
                while (self.currentBytes > self.maxBytes && iter.hasNext()) {
                    Map.Entry<String, CachedSql> first = iter.next();
                    self.currentBytes -= first.getValue().approxBytes;
                    iter.remove();
                }
                return false;
            }
        };
    }

    public synchronized int recordExecution(String sql) {
        CachedSql e = map.computeIfAbsent(sql, k -> {
            CachedSql ne = new CachedSql(k);
            currentBytes += ne.approxBytes;
            return ne;
        });
        e.execCount++;
        return e.execCount;
    }

    public synchronized int execCount(String sql) {
        CachedSql e = map.get(sql);
        return e == null ? 0 : e.execCount;
    }

    public synchronized int size() { return map.size(); }
    public synchronized int currentBytes() { return currentBytes; }

    /** Test/observe-only: snapshot current cache contents. */
    public synchronized Map<String, Integer> snapshot() {
        java.util.LinkedHashMap<String, Integer> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, CachedSql> e : map.entrySet()) {
            out.put(e.getKey(), e.getValue().execCount);
        }
        return out;
    }

    private static final class CachedSql {
        final String sql;
        int execCount = 0;
        final int approxBytes;

        CachedSql(String sql) {
            this.sql = sql;
            this.approxBytes = sql.length() * 2 + 64;
        }
    }
}
