package org.particledb;

import java.util.Arrays;

/**
 * A {@code vector} column value: the float4 components the server stored.
 *
 * <p>The native wire carries a {@code vector} as Arrow {@code Binary} holding
 * little-endian float32 components (the engine's storage form,
 * the server's vector decoder), told apart from a {@code bytea} by
 * the column's {@code pdb:decl} stamp. Before this type the driver returned
 * those raw bytes from {@code getObject} and their array identity
 * ({@code [B@…}) from {@code getString}. {@link #toString()} is pgvector's
 * text form ({@code [1,2.5,-3]}), which is what {@code getString} returns, as
 * pgjdbc with pgvector does.
 */
public final class PdbVector {
    private final float[] values;

    public PdbVector(float[] values) {
        this.values = values.clone();
    }

    /** Decode the wire form: little-endian float32 components. */
    static PdbVector fromWire(byte[] b) {
        if (b.length % 4 != 0) throw new IllegalArgumentException("vector payload of " + b.length + " bytes");
        float[] v = new float[b.length / 4];
        for (int i = 0; i < v.length; i++) {
            int bits = (b[4 * i] & 0xFF) | (b[4 * i + 1] & 0xFF) << 8 | (b[4 * i + 2] & 0xFF) << 16 | (b[4 * i + 3] & 0xFF) << 24;
            v[i] = Float.intBitsToFloat(bits);
        }
        return new PdbVector(v);
    }

    /** A copy of the components. */
    public float[] toArray() {
        return values.clone();
    }

    public int dimensions() {
        return values.length;
    }

    @Override public String toString() {
        StringBuilder s = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) s.append(',');
            s.append(component(values[i]));
        }
        return s.append(']').toString();
    }

    /** One component as pgvector prints it: no {@code .0} on an integral value. */
    static String component(float f) {
        if (Float.isNaN(f)) return "NaN";
        if (Float.isInfinite(f)) return f > 0 ? "Infinity" : "-Infinity";
        String t = Float.toString(f);
        if (t.indexOf('E') >= 0) return t.replace("E", "e");
        return new java.math.BigDecimal(t).stripTrailingZeros().toPlainString();
    }

    @Override public boolean equals(Object o) {
        return o instanceof PdbVector && Arrays.equals(values, ((PdbVector) o).values);
    }

    @Override public int hashCode() {
        return Arrays.hashCode(values);
    }
}
