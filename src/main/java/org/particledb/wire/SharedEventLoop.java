package org.particledb.wire;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.TimeUnit;

/**
 * Process-wide shared Netty {@link EventLoopGroup} for all JDBC connections
 * created through the {@code DriverManager} / {@code PdbDataSource} path.
 *
 * <p><b>Why this exists.</b> A Netty {@code NioEventLoopGroup} owns OS threads.
 * Allocating one group per {@link org.particledb.PdbConnection} means every
 * connection spawns its own event-loop thread; any connection that errored,
 * aborted, or was garbage-collected without an explicit {@code close()} leaked
 * its event-loop thread forever. Over a long-running workload with many
 * reconnects (e.g. a long consistency-test run) thousands of {@code nioEventLoopGroup-*}
 * threads accumulate and the client JVM exhausts OS threads.
 *
 * <p>Netty channels are independent of one another even when they share an
 * event-loop group, so a single small shared group serves all connections
 * without coupling their stream lifecycles. The group is created lazily on
 * first use and shut down once, automatically, via a JVM shutdown hook - never
 * per-connection.
 *
 * <p>This singleton is intentionally global rather than per-{@code DataSource}:
 * the JDBC {@code Driver.connect(...)} entry point has no owning object to
 * anchor a group to, and a single shared group sized to the available
 * processors comfortably multiplexes every connection in the JVM.
 */
public final class SharedEventLoop {

    private SharedEventLoop() {}

    /** Hard cap on shared event-loop threads regardless of core count. */
    private static final int MAX_THREADS = 8;

    /** Holder idiom: the group is created exactly once, on first {@link #group()}. */
    private static final class Holder {
        static final EventLoopGroup GROUP = create();

        private static EventLoopGroup create() {
            int threads = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREADS));
            NioEventLoopGroup g = new NioEventLoopGroup(threads, new ShutdownAwareThreadFactory());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    g.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS)
                            .await(1, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignored) {
                    // best-effort shutdown during JVM teardown
                }
            }, "pdb-eventloop-shutdown"));
            return g;
        }
    }

    /**
     * The shared event-loop group. Lazily initialized on first call. Callers
     * MUST pass this into {@code new StreamMux(group, ...)} so the mux treats
     * the group as borrowed ({@code ownGroup=false}) and never shuts it down.
     */
    public static EventLoopGroup group() {
        return Holder.GROUP;
    }

    /** Daemon thread factory so a lingering event loop never blocks JVM exit. */
    private static final class ShutdownAwareThreadFactory implements java.util.concurrent.ThreadFactory {
        private final java.util.concurrent.atomic.AtomicInteger seq =
                new java.util.concurrent.atomic.AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "pdb-shared-eventloop-" + seq.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
