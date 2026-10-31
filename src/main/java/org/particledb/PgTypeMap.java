package org.particledb;

import java.sql.Types;

/**
 * Maps the {@code information_schema.columns.data_type} text values that PDB
 * serves (see the server's catalog virtual tables
 * {@code pdb_type_to_pg_information_schema}) to the JDBC {@link Types} codes,
 * column sizes, and decimal-digit values that {@code DatabaseMetaData.getColumns}
 * must report. This is the exact set PDB emits: {@code smallint}, {@code integer},
 * {@code bigint}, {@code double precision}, {@code real}, {@code numeric},
 * {@code text}, {@code character varying}, {@code bytea}, {@code boolean},
 * {@code timestamp without time zone}, {@code date}, {@code jsonb}, {@code uuid},
 * {@code ARRAY}.
 */
final class PgTypeMap {
    private PgTypeMap() {}

    /** JDBC type code for an information_schema.columns.data_type value. */
    static int sqlType(String dataType) {
        if (dataType == null) return Types.OTHER;
        String d = dataType.trim().toLowerCase();
        switch (d) {
            case "smallint":            return Types.SMALLINT;
            case "integer":             return Types.INTEGER;
            case "bigint":              return Types.BIGINT;
            case "real":                return Types.REAL;
            case "double precision":    return Types.DOUBLE;
            case "numeric":
            case "decimal":             return Types.NUMERIC;
            case "boolean":             return Types.BOOLEAN;
            case "bytea":               return Types.BINARY;
            case "date":                return Types.DATE;
            case "text":                return Types.VARCHAR;
            case "array":               return Types.ARRAY;
            default: break;
        }
        if (d.startsWith("character varying") || d.startsWith("varchar")) return Types.VARCHAR;
        if (d.startsWith("character") || d.startsWith("char") || d.startsWith("bpchar")) return Types.CHAR;
        if (d.startsWith("timestamp")) return Types.TIMESTAMP;
        if (d.startsWith("time")) return Types.TIME;
        // jsonb/json/uuid/user-defined/vector → OTHER (mirrors pgjdbc's Types.OTHER
        // for these), still readable via getString/getObject.
        return Types.OTHER;
    }

    /**
     * COLUMN_SIZE for getColumns: character_maximum_length for char types,
     * numeric_precision for numeric types, else a representative width.
     */
    static Integer columnSize(String dataType, Integer charMax, Integer numericPrecision) {
        if (dataType == null) return null;
        String d = dataType.trim().toLowerCase();
        if (d.startsWith("character") || d.startsWith("varchar") || d.startsWith("char")
                || d.startsWith("bpchar")) {
            return charMax; // may be null for unbounded text
        }
        switch (d) {
            case "smallint": return 5;
            case "integer":  return 10;
            case "bigint":   return 19;
            case "real":     return 8;
            case "double precision": return 17;
            case "boolean":  return 1;
            case "date":     return 13;
            default: break;
        }
        if (d.equals("numeric") || d.equals("decimal")) return numericPrecision;
        if (d.startsWith("timestamp")) return 29;
        return null;
    }

    /** DECIMAL_DIGITS for getColumns (numeric_scale for numeric; 0 for ints). */
    static Integer decimalDigits(String dataType, Integer numericScale) {
        if (dataType == null) return null;
        String d = dataType.trim().toLowerCase();
        switch (d) {
            case "smallint":
            case "integer":
            case "bigint":
                return 0;
            case "numeric":
            case "decimal":
                return numericScale;
            default:
                return null;
        }
    }
}
