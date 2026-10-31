package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.wire.StreamMux;
import org.particledb.wire.StreamState;

import static org.junit.jupiter.api.Assertions.*;

/**
 * `maxStreamsPerConn` must be ENFORCED, not merely advertised.
 *
 * <p>WHY THIS TEST EXISTS. The parameter was published three ways - a
 * {@code DriverPropertyInfo} from {@code Driver.getPropertyInfo} with default
 * "64", a setter on {@code PdbDataSource}, and a documented URL parameter - and
 * NOTHING in shipped code read it. Setting it changed nothing and reported
 * nothing, which is strictly worse than not offering the knob: a user tuning
 * throughput would believe they had.
 *
 * <p>WHY IT CAN FAIL (reachability, so this is not a vacuous green).
 * {@code StreamMux.newStream()} is a pure allocation against the live-stream
 * map - {@code StreamState.open()} is a state transition with no channel I/O  -
 * so streams can be opened here WITHOUT a server and without awaiting a
 * response. That is precisely the concurrency a JDBC {@code Connection} cannot
 * express today (it owns its own mux and awaits every call, so live depth is 1),
 * which is why the cap has to be tested at this layer rather than through the
 * JDBC API. Before the enforcement landed, the 4th and 5th calls below both
 * succeeded and {@code activeStreams()} reached 5.
 */
public class StreamCapEnforcedTest {

    @Test
    public void capRefusesTheStreamThatWouldExceedIt() throws Exception {
        StreamMux mux = new StreamMux(null, false, 1000, 3);
        try {
            StreamState a = mux.newStream();
            StreamState b = mux.newStream();
            StreamState c = mux.newStream();
            assertNotNull(a); assertNotNull(b); assertNotNull(c);
            assertEquals(3, mux.activeStreams(), "three streams should be live");

            IllegalStateException e = assertThrows(IllegalStateException.class,
                    mux::newStream,
                    "the 4th stream must be refused when maxStreamsPerConn=3");
            assertTrue(e.getMessage().contains("maxStreamsPerConn=3"),
                    "the refusal must name the cap that caused it, got: " + e.getMessage());

            // The refusal must not have leaked a half-registered stream.
            assertEquals(3, mux.activeStreams(),
                    "a refused newStream() must not change the live count");
        } finally {
            mux.close();
        }
    }

    @Test
    public void zeroMeansUnlimitedLikeTheServerConvention() throws Exception {
        // The server treats max_concurrent_streams == 0 as "no limit"
        // (the server's native-wire multiplexer computes pool_cap from it); the client
        // mirrors that rather than inventing a second convention.
        StreamMux mux = new StreamMux(null, false, 1000, 0);
        try {
            for (int i = 0; i < 200; i++) {
                assertNotNull(mux.newStream(), "stream " + i + " under an unlimited cap");
            }
            assertEquals(200, mux.activeStreams());
        } finally {
            mux.close();
        }
    }

    @Test
    public void negativeCapIsRejectedAtConstruction() {
        // Fail loudly rather than silently coercing a nonsense value into a
        // policy the caller did not ask for.
        assertThrows(IllegalArgumentException.class,
                () -> new StreamMux(null, false, 1000, -1));
    }

    @Test
    public void defaultMatchesTheValueTheDriverAdvertises() {
        // Driver.getPropertyInfo publishes "64" for maxStreamsPerConn. If the
        // constant and the advertised default ever diverge, the driver is lying
        // about its own behaviour again - in a quieter way.
        assertEquals(64, StreamMux.DEFAULT_MAX_STREAMS,
                "DEFAULT_MAX_STREAMS must equal the default Driver.getPropertyInfo shows");
    }
}
