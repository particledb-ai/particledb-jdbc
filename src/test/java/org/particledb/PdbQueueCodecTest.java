package org.particledb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * The native queue frame codec against the GOLDEN VECTORS the server's codec
 * asserts (the server's queue-frame golden-vector test):
 * the two sides cannot drift apart silently. No server needed.
 */
class PdbQueueCodecTest {

    static final String GOLDEN_PUSH_REQUEST =
            "010101000000710100000061020000000000000003000000000000000101000000700400000000000000";
    static final String GOLDEN_POP_RESPONSE =
            "010301000000010000006102000000000000000100000000000000050000000000000000";

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    static byte[] unhex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        return out;
    }

    @Test
    void pushRequestMatchesTheServer() {
        PdbQueue.Enc e = new PdbQueue.Enc(PdbQueue.OP_PUSH, "q").item("a", 2, 3, "p").i64(4);
        assertEquals(GOLDEN_PUSH_REQUEST, hex(e.bytes()));
    }

    @Test
    void popResponseDecodesLikeTheServer() {
        PdbQueue.Dec d = new PdbQueue.Dec(unhex(GOLDEN_POP_RESPONSE));
        assertEquals(1, d.u8());
        assertEquals(PdbQueue.OP_POP, d.u8());
        assertEquals(1, d.u32());
        assertEquals("a", d.str());
        assertEquals(2, d.i64());
        assertEquals(1, d.i64());
        assertEquals(5, d.i64());
        assertNull(d.ostr());
    }

    @Test
    void sqlStateIsTakenFromTheServerMessage() {
        SQLException raw = new SQLException("PDB wire error 1: 25P01: pdb_queue operations run inside a transaction", "XX000", 1);
        assertEquals("25P01", PdbQueue.withSqlState(raw).getSQLState());
        SQLException ser = new SQLException("PDB wire error 1: 40001: could not serialize", "XX000", 1);
        assertEquals(java.sql.SQLTransactionRollbackException.class, PdbQueue.withSqlState(ser).getClass());
    }

    @Test
    void hostileLengthsAreRefusedNotAllocated() {
        // A string claiming 2 GiB, inside a 9-byte buffer.
        PdbQueue.Dec d = new PdbQueue.Dec(unhex("ffffff7f" + "61"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, d::str);
        // A claim count far beyond what the bytes could hold.
        PdbQueue.Dec c = new PdbQueue.Dec(unhex("00e1f505"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> c.count(29));
        assertEquals("08P01", PdbQueue.Dec.malformed(new IllegalArgumentException("x")).getSQLState());
    }
}
