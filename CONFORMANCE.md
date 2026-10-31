# JDBC conformance matrix - `ai.particledb:particledb-jdbc`

What an application can rely on, what it cannot, and **which test proves it**.

This matrix describes the current source tree. See [README.md](README.md)
for server version requirements and tested runtimes.
`NO TEST` identifies a claim that still needs executable evidence.

Legend

| | |
|---|---|
| **SUPPORTED** | Implemented to the JDBC contract, with a test |
| **PARTIAL** | Works within a stated limit; the limit is documented and tested |
| **ABSENT** | Not implemented - refuses loudly (`SQLFeatureNotSupportedException` / `SQLException`), never silently |
| **NO TEST** | Behaviour is whatever the code does; nothing asserts it |

Test names below are `Class#method` under `src/test/java/org/particledb/`.
Everything marked *(differential)* is additionally checked against **pgjdbc
42.7.4 + PostgreSQL 17.9** in `PgOracleDifferentialTest`, which pins the
oracle's identity (`version()` must name PostgreSQL and must **not** name
ParticleDB) before believing any answer.

---

## Driver / connection establishment

| Surface | Status | Proof |
|---|---|---|
| `java.sql.Driver` SPI auto-registration | SUPPORTED | `DriverSmokeTest#connect_create_insert_select` |
| URL parsing (`jdbc:particledb://h:p/db?k=v`), defaults, `Properties` merge | SUPPORTED | `PgCompatTest` (7) |
| `DataSource` (`PdbDataSource`) bean setters | SUPPORTED for connection and login settings | `NetworkTimeoutTest#dataSourceLoginTimeoutBoundsAnUnansweredHandshake`; `JdbcReadinessLiveTest#realPoolRollsBackAndResetsConnectionsBetweenBorrowers` |
| Credentials on the native wire (`user`/`password`, `28000`/`28P01`) | SUPPORTED | `AuthHandshakeTest` (26), `NativeWireAuthConnectTest` (8) |
| `sslMode` validation - a non-`disable` mode must fail rather than connect in plaintext | SUPPORTED | `AuthHandshakeTest#sslModeValidation_isUnchanged`, `TlsIntegrationTest` |
| Real TLS (`require`/`verify-ca`/`verify-full`) | SUPPORTED | `TlsLiveTest`: verified queries, wrong hostname, untrusted certificate, explicit modes and plaintext refusal |
| v1 / v2 wire negotiation and fallback | SUPPORTED | `BackwardsCompatTest` (2), `StreamMuxFallbackTest`, `WireV2CodecTest` (5) |
| **A plain connect is protocol 2 multiplexed with every capability, AS THE SERVER RECORDS IT** (`SHOW pdb.wire_protocol` / `pdb.wire_capabilities`), and `negotiatedProtocol()` / `negotiatedCapabilities()` equal that record | SUPPORTED | `ReferenceDefaultsLiveTest#aPlainConnectIsMultiplexedV2_asTheServerRecordsIt`, `#theRecordCannotBeForged`; conformance DEF-001..007 |
| Default port is the server's default native port (`Driver.DEFAULT_PORT`) | SUPPORTED | `ReferenceDefaultsLiveTest#aPortlessUrlUsesTheDriversDefaultPort`; conformance DEF-005 |
| Multiplexed concurrency on one connection | SUPPORTED | `MuxConcurrencyRyowTest` (3), `MultiplexedConcurrencyTest` |
| Event-loop thread sharing (no per-connection leak) | SUPPORTED | `OpenCloseThreadLeakTest` (3) |

## `Connection`

| Surface | Status | Proof |
|---|---|---|
| `createStatement` / `prepareStatement` | SUPPORTED | `DriverSmokeTest`, `JdbcStatementContractTest` |
| `setAutoCommit` / `getAutoCommit` | SUPPORTED | `DriverBatchAndTxnValidationTest#{rollback_rowAbsent,commit_rowVisible}` |
| `commit` / `rollback` | SUPPORTED | same, plus `JdbcConnectionContractTest#commitAndRollbackInAutoCommitMode_areRefused` *(differential)* |
| `commit`/`rollback` refused in auto-commit mode | SUPPORTED | `JdbcConnectionContractTest#commitAndRollbackInAutoCommitMode_areRefused` *(differential)* |
| `setTransactionIsolation` - reaches the server | SUPPORTED | `JdbcConnectionContractTest#setTransactionIsolation_reachesTheServer` (asserts `SHOW transaction_isolation`, not the driver's field) |
| Default isolation is READ COMMITTED; a fresh connection reports the server's level and `setTransactionIsolation(READ_COMMITTED)` still reaches the server | SUPPORTED | `JdbcConnectionContractTest#freshConnection_reportsAndHonoursTheServersLevel` |
| `setTransactionIsolation` rejects `TRANSACTION_NONE` / unknown levels | SUPPORTED | `JdbcConnectionContractTest#setTransactionIsolation_rejectsLevelsItCannotDeliver`, `DdlClassificationTest#isolationLevelSpellings` |
| `setReadOnly` - reaches the server | SUPPORTED | `JdbcConnectionContractTest#setReadOnly_reachesTheServer` |
| `setSchema` - issues `SET search_path` | SUPPORTED | `JdbcConnectionContractTest#setSchema_reachesTheServer` |
| `setCatalog` / `getCatalog` | PARTIAL | **NO TEST**. PDB is one database per connection; `setCatalog` stores the name and sends nothing. `getCatalog` returns it |
| DDL inside a transaction | PARTIAL | By default (server `--transactional-ddl 1`, single node, `occ` / `serializable`) `CREATE TABLE`, `CREATE [UNIQUE] INDEX`, a single `ALTER TABLE … ADD COLUMN` and `DROP TABLE` without CASCADE run inside the transaction and commit or roll back with it, as in PostgreSQL. Other DDL -- and every DDL with `--transactional-ddl off`, on a clustered (Raft-replicated) node or under `--concurrency-control none` -- is refused (`25001`) inside an OPEN transaction and the transaction's DML still rolls back (PostgreSQL would roll the DDL back too); as the FIRST statement (no transaction open yet) such DDL is refused `25001` too by default; only with the URL option `allowDdlOutsideTransaction=true` does it run on its own with a `SQLWarning` on `Connection.getWarnings()` (not `Statement.getWarnings()`; Flyway needs this for its history table) -- a single statement only: `executeBatch` always opens the transaction and a multi-statement string is never run outside it, so both are still refused `25001` (the batch's `BatchUpdateException` carries SQLSTATE `25001`; `executeLargeBatch` is not implemented) -- and the next statement is transactional on the success and failure paths, including across a pooled connection's return and reuse. `AutocommitOffScopeTest`, `ConductorFlywayMigrationTest`, `AllowDdlOutsideTransactionScopeTest`, `LazyTransactionTest#ddlNeverCommitsTheOpenTransaction`, `JdbcConnectionContractTest#{failedDdlInsideTransaction_leavesTheTransactionOpen,succeedingDdlInsideTransaction_alsoLeavesTheTransactionOpen}`, `DdlClassificationTest` (6) |
| `setReadOnly(true)` - enforced in a transaction and in autocommit, including writes through functions (`25006`) | SUPPORTED | `LazyTransactionTest#readOnlyConnectionRefusesWritesInsideATransaction` |
| Closed connection → SQLSTATE `08003` on every entry point | SUPPORTED | `JdbcConnectionContractTest#closedConnection_refusesEveryEntryPointWith08003` |
| `isValid` / `abort` | SUPPORTED | `JdbcConnectionContractTest#abort_closesTheConnection`, `DriverSmokeTest#connection_isValid` |
| Native KV frames (`PdbConnection.kvSet/kvGet/kvDel/kvIncr`: binary-safe, the RESP keyspace, not transactional) | SUPPORTED | `ReferenceDefaultsLiveTest#nativeKeyValueFramesRoundTripBinarySafely`; conformance KV-001..012 (RESP parity) |
| Multi-statement strings (`execute("a; b; SELECT …")`): one implicit transaction outside a block, the last result returned; a string with transaction control is refused `0A000` (server, `run_multi_statement`) | SUPPORTED | conformance SQL-MULTI-001/002; server `zz_mux_multi_statement_query` |
| `unwrap` / `isWrapperFor` | SUPPORTED | `JdbcConnectionContractTest#poolLifecycleCalls_areHonoured` |
| `createArrayOf` (text/varchar, int2/4/8, float4/8, bool, numeric, uuid, timestamp, timestamptz, date, bytea, json, jsonb; NULL elements; multidimensional; other types refused `0A000`) | SUPPORTED | `ArrayParamsCodecTest`, `ArrayParamsLiveTest#everyElementTypeRoundTrips`, `ArrayParamsPg17DifferentialTest`, `DriverContractValidationTest#textArrayBindAndReadBack` |
| **Savepoints** (`setSavepoint`, `rollback(sp)`, `releaseSavepoint`; refused in auto-commit mode, as pgjdbc) | SUPPORTED | `JdbcConnectionContractTest#savepoints_workAndAgreeWithTheMetadata` (asserts the committed rows), `ReferenceDefaultsLiveTest#savepointNamesAreQuotedIdentifiers`; conformance SQL-TXN-002 |
| **`CallableStatement`** (`prepareCall`) | ABSENT | `JdbcConnectionContractTest#unimplementedFactories_refuse_andThePreparedPathStillWorks` |
| **LOB factories** (`createBlob`/`createClob`/`createNClob`/`createSQLXML`/`createStruct`) | ABSENT | same test |
| `setNetworkTimeout` / `getNetworkTimeout` | SUPPORTED | `NetworkTimeoutTest`: response and blocked-write deadlines, closure without replay, zero and invalid settings |
| `getWarnings` / `clearWarnings` | SUPPORTED | `Statement.getWarnings()` carries the server's statement advisories (`MSG_NOTICE`, negotiated by default), cleared by the next execution: `ReferenceDefaultsLiveTest#anEngineAdvisoryArrivesAsAStatementWarning`. `Connection.getWarnings()` is the driver's own (`allowDdlOutsideTransaction`) |
| `setClientInfo` / `getClientInfo` | **NO TEST** | Stored client-side; never sent |
| `nativeSQL` | **NO TEST** | Identity function; JDBC escape syntax (`{fn …}`, `{d '…'}`) is **not** translated |
| `getHoldability` / `setHoldability` | **NO TEST** | `setHoldability` is a no-op; `getHoldability` always answers `HOLD_CURSORS_OVER_COMMIT` |

## `Statement`

| Surface | Status | Proof |
|---|---|---|
| `executeQuery` / `executeUpdate` / `execute` | SUPPORTED | `StatementClassificationTest` (8), `DriverContractValidationTest#resultKindContract_queryVsUpdateVsExecute` |
| Result classification by real frame kind (`WITH`, `VALUES`, `SHOW`, `EXPLAIN`, `… RETURNING`) | SUPPORTED | `StatementClassificationTest`, `DriverFixCoverageTest#executeReturnsResultSetForNonSelectRowSources` |
| `executeUpdate` refuses a row-returning statement | SUPPORTED | `JdbcStatementContractTest#executeUpdateOnARowReturningStatement_isRefused` *(differential)* |
| `getResultSet` / `getUpdateCount` bookkeeping | SUPPORTED | `JdbcStatementContractTest#preparedStatement_recordsItsCurrentResult` |
| `getMoreResults` drain loop terminates | SUPPORTED | `JdbcStatementContractTest#getMoreResultsDrainLoop_terminates` |
| `setQueryTimeout` (0 = no limit) → `SQLTimeoutException` / `57014` | SUPPORTED | `JdbcStatementContractTest#setQueryTimeout_isHonouredAndValidated` (proves it by *exceeding* the cap with `pg_sleep`). On expiry the driver RESETS the statement's stream; the server cancels it and answers 57014, and the connection stays in service. Only a request that has not answered within `CANCEL_GRACE_SECONDS` closes the connection (ambiguous; never resent): `AmbiguousFailureTest` |
| `setMaxRows` | PARTIAL | `JdbcStatementContractTest#setMaxRows_dropsTheExcessRows` *(differential)*. Enforced CLIENT-side: the server still ships every row, so this bounds what the caller sees, not what crosses the wire |
| `cancel` | SUPPORTED | `JdbcStatementContractTest#cancel_endsTheRunningStatementAndKeepsTheConnection`; conformance SQL-CANCEL-001. Resets the statement's stream (`RST_STREAM(CANCEL)`); the server ends it with 57014. A no-op when nothing is running |
| Closed statement refuses every execution path, and closes its ResultSet | SUPPORTED | `JdbcStatementContractTest#closedStatement_refusesEveryExecutionPath` *(differential)* |
| `addBatch(String)` / `executeBatch` | SUPPORTED | `DriverBatchAndTxnValidationTest` (12), `BatchReadBackTest` (2), `BatchExecutorTest` (2) |
| `executeBatch` returns per-statement update counts (`SUCCESS_NO_INFO` for a row-returning statement) | SUPPORTED | `NativeBatchErrorsTest` (case d), `DriverBatchAndTxnValidationTest#statementBatchInserts_arrayLengthAndFullReadBack` |
| A failing statement in `executeBatch` raises `BatchUpdateException` with the counts of the statements that ran, and the failed entry's own SQLSTATE (`CAP_EXT_BATCH_SQLSTATE`, negotiated by default) | SUPPORTED | `NativeBatchErrorsTest` (cases a, b), `ReferenceDefaultsLiveTest#aFailedBatchEntryCarriesItsSqlState`, `AllowDdlOutsideTransactionScopeTest` (25001); conformance SQL-BATCH-002 |
| `getGeneratedKeys` | PARTIAL | `JdbcStatementContractTest#generatedKeys_agreeWithTheMetadata` - always empty, and `supportsGetGeneratedKeys()` says false so the two agree |
| ResultSet type/concurrency advertisement is honest | SUPPORTED | `JdbcStatementContractTest#resultSetTypeAndConcurrency_areReportedHonestly` |
| `setFetchSize(n > 0)`: the result streams, decoded one record batch at a time (memory bounded by a batch, not the result); the result set is then forward-only | SUPPORTED | conformance SQL-STREAM-001 (3 M rows in a 256 MiB heap) |
| `closeOnCompletion`; positive `setMaxFieldSize` | SUPPORTED cleanup; explicit refusal of truncation | `JdbcReadinessLiveTest`; truncation raises `0A000` |
| `setEscapeProcessing` | **NO TEST** | No-op - consistent with `nativeSQL` not translating escapes |

## `PreparedStatement`

| Surface | Status | Proof |
|---|---|---|
| `?` placeholder scanning (quotes, identifiers, dollar-quotes, comments, `E'…'`) | SUPPORTED | `PlaceholderScanTest` (10) |
| `??` escape for the literal `?` operator (jsonb/hstore) | SUPPORTED | `PlaceholderScanTest#{escapedQuestionMarkIsNotABindPlaceholder,escapeDoesNotShiftLaterParameters,…}` |
| Parameter substitution is injection-safe (client-side rendering) | SUPPORTED | `LiteralRenderingTest#{quoteBearingValuesCannotEscapeTheirLiteral,backslashInAValueIsNotRewritten}`, `PgOracleDifferentialTest#{stringEscapingPremiseHolds_onBothEngines,hostileParameterRoundTripsAsData_onBothEngines}` |
| `setInt`/`setLong`/`setDouble`/`setBigDecimal`/`setBoolean`/`setString`/`setNull` | SUPPORTED | `DriverBatchAndTxnValidationTest`, `LiteralRenderingTest#ordinaryNumbersRenderUnquoted` |
| `NaN` / `±Infinity` render as `'NaN'::float8` etc., not as bare identifiers | SUPPORTED | `LiteralRenderingTest#specialFloatingPointValuesGetTheirQuotedSqlForm`, `DriverBatchAndTxnValidationTest#setDoubleSpecialValues_roundTripOrClearError` |
| `setBytes` → hex `bytea` | SUPPORTED | `LiteralRenderingTest#byteArrayRendersAsHexBytea`, `DriverContractValidationTest#byteaBindAndReadBack` |
| `setDate`/`setTime`/`setTimestamp` | SUPPORTED | `LiteralRenderingTest#temporalValuesAreQuotedAndPgParseable`, `DriverBatchAndTxnValidationTest#{setDate_roundTrips,setTimeAndTimestamp_roundTripOrKnownServerGap}` |
| Calendar-aware temporal parameter setters | SUPPORTED | `CalendarParamsLiveTest`, prepared and inline paths |
| `setArray` (incl. a foreign `java.sql.Array`) - a typed native-wire array parameter (EXECUTE tag 17); the typed literal when the server lacks `CAP_EXT_ARRAY_PARAMS` / prepared statements or `serverPrepare=false`; refused `0A000` for every other reason it cannot be bound | SUPPORTED | `ArrayParamsLiveTest#conductorUpsertGoesByHandleNeverAsText`, `ArrayParamsLiveTest#whatCannotBeBoundIsRefusedNotInlined`, `ArrayParamsPg17DifferentialTest`, `MetadataAndArrayUnitTest#interpolate_substitutesArrayLiteral` |
| `setObject` of a Java array / `Collection` → SQL array (primitive, boxed with NULLs, `String[]`, empty, 2-D, `Types.ARRAY`; mixed-type `Object[]` refused `07006`), the same as pgjdbc 42.7.4 + PG17 on every route (bound, `serverPrepare=false`, server without the capability) | SUPPORTED | `SetObjectArrayMatrixTest`, `ArrayParamsLiveTest#setObjectBindsJavaArraysAndCollections`, `ArrayParamsCodecTest#setObjectInfersTheElementType` |
| `executeBatch` with array parameters (every row by handle) | SUPPORTED | `ArrayParamsLiveTest#batchWithArraysRunsEveryRowByHandle` |
| `setObject(i, v, sqlType)` and scale | SUPPORTED for documented scalar SQL types | `JdbcReadinessLiveTest#typedSetObjectAndCloseOnCompletionFollowTheJdbcContract` |
| Parameter index bounds | SUPPORTED | `JdbcStatementContractTest#parameterIndexOutOfRange_isRefused` |
| `clearParameters` / rebinding | SUPPORTED | `JdbcStatementContractTest#clearParametersAndRebind` |
| The SQL-taking `Statement` methods refuse | SUPPORTED | `JdbcStatementContractTest#preparedStatement_rejectsTheSqlTakingStatementMethods` *(differential)* |
| `addBatch()` / `executeBatch()` | SUPPORTED | `BatchExecutorTest#prepared_statement_batch_round_trip`, `BatchReadBackTest` |
| Client-side prepared cache / `prepareThreshold` | SUPPORTED | `PreparedStatementCacheTest` (4) |
| Server-side prepare (Parse/Bind/Execute) | ABSENT | Roadmap; every execution ships a complete SQL string |
| `getMetaData()` (of the result, before execution) | ABSENT | Returns `null`. Legal, but Spring/JDBI lose type info |
| `getParameterMetaData()` | ABSENT | Throws `SQLFeatureNotSupportedException` - **NO TEST** |
| Stream setters (`setAsciiStream`, `setBinaryStream`, `setCharacterStream`, `setBlob`, `setClob`, `setNClob`, `setSQLXML`, `setRowId`, `setRef`) | ABSENT | All throw - **NO TEST** for the refusal |

## `ResultSet`

| Surface | Status | Proof |
|---|---|---|
| Forward iteration, `wasNull`, SQL NULL defaults | SUPPORTED | `JdbcResultSetContractTest#{forwardIteration_isUnchanged,getInt_inRangeValuesStillRead}`, `DriverContractValidationTest#nullColumns_returnTypeDefaultsAndWasNull` |
| Positional addressing; duplicate labels stay distinct | SUPPORTED | `DriverContractValidationTest#duplicateColumnLabels_bothAddressable` |
| By-name getters are case-INsensitive | SUPPORTED | `DriverFixCoverageTest#byNameGettersAreCaseInsensitive`, `PgOracleDifferentialTest#findColumn_isCaseInsensitiveAndThrowsWhenAbsent_onBothEngines` |
| Empty result reports its real projected columns | SUPPORTED | `DriverContractValidationTest#emptyResultSet_reportsRealColumnCountAndFindColumn` |
| `getBoolean` reads PG's `t`/`f` and agrees with `getObject(Boolean.class)` | SUPPORTED | `JdbcResultSetContractTest#getBoolean_readsPostgresTextForms_andAgreesWithGetObject` *(differential)* |
| `getInt`/`getShort`/`getByte` range-check instead of wrapping | SUPPORTED | `JdbcResultSetContractTest#getInt_outOfRangeThrows_ratherThanWrapping` *(differential)* |
| A bad cell raises `SQLException`, never an unchecked `NumberFormatException` | SUPPORTED | `JdbcResultSetContractTest#numericGettersOverTextRaiseSqlException_notNumberFormatException` |
| `getObject(i, Class)` with range-checked coercion | SUPPORTED | `DriverFixCoverageTest#getObjectWithClassCoerces` |
| `getDate`/`getTime`/`getTimestamp`, including ACROSS column types | SUPPORTED | `JdbcResultSetContractTest#{getTime_microsOfDayIsWallClock_notEpochMillis,getTimestamp_keepsMicrosecondsIncludingBeforeEpoch,temporalGettersCrossColumnTypes_doNotThrowUnchecked}` - asserted under three JVM zones |
| `getDate`/`getTime`/`getTimestamp` **with a `Calendar`** (read side) | SUPPORTED | `JdbcResultSetContractTest#{getTimestamp_withCalendar_honoursTheCalendarsZone,getDateAndTime_withCalendar_honourTheCalendarsZone,temporalGetters_nullCalendarEqualsNoArgForm}` |
| Session `TimeZone` = the JVM zone, as the connection's startup value (`RESET` returns to it); `SET TimeZone` transaction scope; binding / reading `timestamptz`, `timestamp`, `date` in a non-UTC JVM | SUPPORTED | `SessionTimeZoneLiveTest`, `SessionTimeZonePg17DifferentialTest` (against pgjdbc on PostgreSQL 17 with `PG_ORACLE_URL`) |
| `TIMESTAMP WITH TIME ZONE` distinguished from `TIMESTAMP` | `TimestamptzResultSetTest`, `SessionTimeZoneLiveTest` | Both arrive as Arrow `timestamp`; the getters tell them apart by the `pdb:decl` stamp (a `timestamptz` is an instant, rendered in the session zone). Metadata still reports `TIMESTAMP` for either |
| Arrays: `getArray`, `getObject`, element typing from `pdb:decl` (incl. uuid/date/timestamp/bytea elements and nested typed arrays for multidimensional values) | SUPPORTED | `ArrayElementTypeTest` (39), `ArrayParamsCodecTest#readSideTypesNewElementTypes`, `ArrayParamsLiveTest#everyElementTypeRoundTrips` |
| Unconstrained `numeric` keeps each value's scale (`0.52`, not `0.520000`): `CAP_NUMERIC_TEXT` by default, decoded back to `BigDecimal` / `NUMERIC` | SUPPORTED | `ReferenceDefaultsLiveTest#unconstrainedNumericKeepsEachValuesScale`; conformance SQL-T-003 |
| `vector` columns: `getObject` → `PdbVector`, `getString` → pgvector text; `bytea` `getString` → `\x…` (was the array's identity) | SUPPORTED | `ReferenceDefaultsLiveTest#vectorAndByteaReadAsValuesNotArrayIdentities`; conformance SQL-T-006, SQL-T-012 |
| Cursor positioning (`absolute`, `relative`, `previous`, `first`/`last`) | PARTIAL | `JdbcResultSetContractTest#scrollMethods_returnValueMatchesTheCursorTheySet`. The ResultSet reports `TYPE_FORWARD_ONLY`, but these methods work over the fully materialised rows rather than throwing; the return value and the cursor now always agree |
| Closed ResultSet refuses every read | SUPPORTED | `JdbcResultSetContractTest#closedResultSet_refusesEveryRead` *(differential)* |
| `findColumn` throws on an absent label | SUPPORTED | `JdbcResultSetContractTest#absentColumnLabel_throwsFromFindColumnButIsLenientFromGetString` *(differential)* |
| Missing result column labels | SUPPORTED: getters throw | `JdbcResultSetContractTest#absentColumnLabelsThrowFromEveryGetter` |
| `updateXxx` / `insertRow` / `deleteRow` | ABSENT | Every updater throws; `getConcurrency()` reports `CONCUR_READ_ONLY` and the metadata agrees (`JdbcStatementContractTest#resultSetTypeAndConcurrency_areReportedHonestly`) |
| `getBlob`/`getClob`/`getNClob`/`getSQLXML`/`getRowId`/`getRef`, `getAsciiStream`/`getBinaryStream`/`getCharacterStream` | ABSENT | All throw - **NO TEST** for the refusal |
| `getStatement()` | SUPPORTED | `JdbcStatementContractTest#preparedStatement_recordsItsCurrentResult` |
| Row limit from `Statement.setMaxRows` | PARTIAL | `JdbcStatementContractTest#setMaxRows_dropsTheExcessRows` - client-side |
| Large results | SUPPORTED | `DriverContractValidationTest#largeResult_5000Rows_checksumIntact` |

## `ResultSetMetaData`

| Surface | Status | Proof |
|---|---|---|
| `getColumnCount`, `getColumnLabel`/`getColumnName` | SUPPORTED | `JdbcResultSetContractTest#resultSetMetaData_inRangeColumnAnswers` |
| `getColumnType`/`getColumnTypeName`/`getColumnClassName`/`getPrecision`/`getScale` | SUPPORTED | `MetadataAndArrayUnitTest#simpleMeta_reportsJdbcTypesFromArrowTokens`, `DriverContractValidationTest#resultSetMetaData_reportsRealColumnTypes`, `ArrayElementTypeTest#metadata_*` |
| Out-of-range column raises `SQLException`, not `IndexOutOfBoundsException` | SUPPORTED | `JdbcResultSetContractTest#resultSetMetaData_outOfRangeColumnRaisesSqlException` *(differential)* |
| `isNullable` | PARTIAL | Always `columnNullableUnknown` - the Arrow schema's nullability is not consulted. **NO TEST** |
| `getTableName` | YES | The relation of the column's source-table OID (`pg_class.relname`, cached per connection), `""` for a computed column, as pgjdbc - `JoinColumnTableNameLiveTest` |
| `getSchemaName` / `getCatalogName` | ABSENT | Always `""` (pgjdbc's `getSchemaName` is `""` too) - **NO TEST** |
| `isAutoIncrement` | PARTIAL | Always false - **NO TEST** |

## `DatabaseMetaData`

| Surface | Status | Proof |
|---|---|---|
| Product identity (`pgCompat`, `pgReportedVersion`) | SUPPORTED | `JdbcDatabaseMetaDataContractTest#productIdentity_drivesFrameworkDialectSelection`, `DriverContractValidationTest#databaseMetaData_pgCompatFalseReportsNativeIdentity` |
| `getTables` (+ name-pattern filtering) | SUPPORTED | `JdbcDatabaseMetaDataContractTest#getTables_hasItsSpecifiedColumnsAndFindsBothTables` |
| `getColumns` (+ types, precision/scale, nullability) | SUPPORTED | `JdbcDatabaseMetaDataContractTest#getColumns_hasItsSpecifiedColumnsAndTypesEachColumn` |
| `getPrimaryKeys` | SUPPORTED | `JdbcDatabaseMetaDataContractTest#getPrimaryKeys_hasItsSpecifiedColumnsAndFindsThePk` |
| `getImportedKeys` / `getExportedKeys` | SUPPORTED | `JdbcDatabaseMetaDataContractTest`: single and composite keys, referenced column order, actions and duplicate constraint names across tables. |
| `getCrossReference` | SUPPORTED | `JdbcDatabaseMetaDataContractTest#getCrossReference_filtersBothSides` |
| `getIndexInfo` | SUPPORTED | `JdbcDatabaseMetaDataContractTest`: PK / unique / ordinary indexes, composite column order, unique filtering, descending keys and partial-index predicates. Cardinality and page estimates are zero. |
| `getSchemas` / `getCatalogs` / `getTableTypes` / `getTypeInfo` | SUPPORTED | `JdbcDatabaseMetaDataContractTest#schemaCatalogAndTypeEnumeration_haveTheirSpecifiedColumns` |
| No-row methods still carry their specified columns | SUPPORTED | `JdbcDatabaseMetaDataContractTest#metadataMethodsWithNoRows_stillCarryTheirSpecifiedColumns` |
| `getProcedures`/`getFunctions`/`getUDTs`/privileges/`getBestRowIdentifier`/… | ABSENT | Always empty (with the right columns) |
| Catalog-query failures | SUPPORTED: errors propagate | `NetworkTimeoutTest#catalogFailureIsNotReportedAsAnEmptyCatalog` |
| The ~150 `supportsXxx()` capability answers | **NO TEST** | Only the handful a framework branches on are asserted (savepoints, generated keys, result-set type/concurrency, batch updates, call syntax). The rest are unverified constants |

## Wire / protocol / errors

| Surface | Status | Proof |
|---|---|---|
| SQLSTATE on every error, never `null` | SUPPORTED | `PdbSqlStateTest` (9), `SqlStateMappingTest` (4 + 2 skipped) |
| JDBC 4 exception subclasses per SQLSTATE class | SUPPORTED | `PdbSqlStateTest#standardJdbcSubclassesArePicked` |
| `40001` serialization failure preserved exactly | SUPPORTED | `PdbSqlStateTest#serializationFailureIsPreservedExactly`, `SqlStateMappingTest#serializationFailureCarries40001` |
| `08001` connect failure / `08003` use-after-close | SUPPORTED | `JdbcConnectionContractTest#closedConnection_refusesEveryEntryPointWith08003` |
| `57014` query timeout | SUPPORTED | `JdbcStatementContractTest#setQueryTimeout_isHonouredAndValidated` |
| Arrow IPC decode fidelity | SUPPORTED | `ArrowResultFidelityTest` (3), `ArrowIpc`-backed unit suites |
| `pg_catalog` adapter rewrites | SUPPORTED | `PgCatalogAdapterTest` (31) |

## Connection pooling

| Surface | Status | Proof |
|---|---|---|
| Hikari-style checkout/return sequence (`isValid`, `setAutoCommit`, `rollback`, `setReadOnly`, `setTransactionIsolation`, `clearWarnings`, `unwrap`) | SUPPORTED | `JdbcConnectionContractTest#poolLifecycleCalls_areHonoured` |
| `08xxx` on a dead connection so a pool evicts it | SUPPORTED | `JdbcConnectionContractTest#closedConnection_refusesEveryEntryPointWith08003` |
| Statement pooling (`setPoolable`) | ABSENT | `isPoolable()` false, `supportsStatementPooling()` false - consistent |
| `setNetworkTimeout` / `getNetworkTimeout` | SUPPORTED | `NetworkTimeoutTest`: response and blocked-write deadlines, closure without replay, zero and invalid settings |
| Actual HikariCP integration | SUPPORTED | `JdbcReadinessLiveTest#realPoolRollsBackAndResetsConnectionsBetweenBorrowers` |

---

## Scope and remaining limitations

* JDBC escape translation is not implemented. Use native SQL syntax.
* `setMaxRows` bounds rows exposed to the caller, not bytes transferred.
* Generated keys, CallableStatement and LOB/SQLXML mutation are not supported.
  Use `RETURNING` and ordinary binary/text parameters where appropriate.
* Rust and TypeScript capability parity is tracked in the shared release record,
  not inferred from this JDBC matrix.
* The streaming conformance timing case must pass against the optimized
  server on an idle machine. Debug-server failures are recorded, not waived.

## Engine-side, not driver-side

Recorded here because they surface through JDBC:

* `SELECT` of a column literally named `result` / `affected_rows` used to be
  mis-framed by the server as DML (an OK frame with no rows). **Re-verified
  against a server built from master and now FIXED**, so the two captures in
  `StatementClassificationTest` are no longer `@Disabled` - they had been dark
  coverage and are live regression tests again.
* `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`
  makes PDB report `read committed` from `SHOW transaction_isolation`, where
  PostgreSQL 17.9 reports `read uncommitted`. PDB reports the *effective* level;
  PostgreSQL echoes the requested one. Arguably more honest, but it differs.
