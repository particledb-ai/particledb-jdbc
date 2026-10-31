package org.particledb;

import org.particledb.wire.SharedEventLoop;
import org.particledb.wire.StreamMux;
import org.particledb.wire.StreamState;
import org.particledb.wire.WireV2Codec;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * JDBC {@link Connection} backed by a {@link StreamMux}.
 *
 * <p>Each {@link PreparedStatement} created from this connection runs on its
 * own multiplexed stream - so multiple threads sharing one
 * {@code PdbConnection} do not head-of-line block each other.
 */
public final class PdbConnection implements Connection {

    private final Pgcompat.ParsedUrl parsed;
    private final StreamMux mux;

    private final ClientPreparedCache preparedCache;
    private final int prepareThreshold;
    private final boolean preferUnixSocket;
    /** The {@code resultDecoder} property: which decoder reads result rows (default auto, the built-in reader). */
    private final ArrowDecoder.ResultDecoder resultDecoder;
    private final int maxStreamsPerConn;
    private final boolean requireMux;

    /**
     * Server-side prepared statements on THIS connection, by their {@code $N}
     * text. The value completes with the statement's server name once its
     * PREPARE_OK arrives (or exceptionally when the prepare failed, which sends
     * the caller to the text path). Streams execute concurrently on the server, so
     * an EXECUTE must not be sent before its PREPARE was answered; every
     * caller waits on the same future. Bounded by {@link #MAX_SERVER_PREPARED};
     * past that, statements simply use the text path (a statement with an
     * array parameter raises instead, see {@link #serverStatement}). A failed
     * prepare completes the future exceptionally.
     */
    private final java.util.concurrent.ConcurrentHashMap<String,
            java.util.concurrent.CompletableFuture<byte[]>> serverPrepared =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicInteger serverPreparedSeq =
            new java.util.concurrent.atomic.AtomicInteger();
    static final int MAX_SERVER_PREPARED = 1024;
    /** Executions sent by server-side handle (tests prove the path with it). */
    private final java.util.concurrent.atomic.AtomicLong serverPreparedExecs =
            new java.util.concurrent.atomic.AtomicLong();

    long serverPreparedExecutions() { return serverPreparedExecs.get(); }
    /** {@code serverPrepare=false} in the URL keeps every PreparedStatement on the text path. */
    private final boolean serverPrepare;
    /**
     * {@code bulkBatch=false} in the URL keeps {@code executeBatch} of a plain
     * {@code INSERT ... VALUES (?, ...)} on the text BATCH path instead of one
     * native bulk ingest.
     */
    private final boolean bulkBatch;
    /** Set when the server refused {@code F_BULK} although it advertised it: never try again. */
    private volatile boolean bulkRefused;
    /** Column names of relations an INSERT without a column list targets (bulk batch path). */
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.List<String>> bulkColumns =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** Batches loaded through native bulk ingest (tests prove the path with it). */
    private final java.util.concurrent.atomic.AtomicLong bulkBatches =
            new java.util.concurrent.atomic.AtomicLong();

    long bulkBatchesLoaded() { return bulkBatches.get(); }

    // ── Flyway / tooling compatibility ──────────────────────────────────────
    // Frameworks that pick a database dialect from the JDBC metadata (Flyway 9,
    // Hibernate, jOOQ, ORMs) inspect DatabaseMetaData.getDatabaseProductName()
    // and getDatabaseProductVersion(). Flyway's PostgreSQLDatabaseType matches
    // productName.startsWith("PostgreSQL") and requires version >= 9.0. When the
    // driver talks to a PDB engine whose SQL surface is PG-compatible, set
    // `pgCompat=true` (default) so the driver ADVERTISES itself as PostgreSQL
    // and those frameworks select their PostgreSQL support instead of a generic
    // (or no) dialect. Set `pgCompat=false` to advertise the native identity.
    private final boolean pgCompat;
    private final String reportedProductName;
    private final String reportedProductVersion;
    private final int reportedMajorVersion;
    private final int reportedMinorVersion;

    // When advertising as PostgreSQL, PG-dialect tooling (Flyway 9, Conductor
    // DAOs) emits raw SQL against pg_catalog relations that PDB does not serve
    // (pg_depend, pg_proc, pg_extension, pg_roles, pg_inherits, pg_tables).
    // {@link PgCatalogAdapter} rewrites those references to inline sub-selects
    // on the way to the wire. Defaults to the pgCompat value; can be disabled
    // independently via ?pgCatalogAdapter=false for debugging.
    private final boolean pgCatalogRewrite;

    private volatile boolean autoCommit = true;

    /**
     * Transaction state.
     *
     * <p>On a server that negotiated {@link WireV2Codec#CAP_PREPARED}, the
     * state is the SERVER's: every response carries it
     * ({@link WireV2Codec#FLAG_TXN_OPEN} / {@link WireV2Codec#FLAG_TXN_FAILED},
     * like PostgreSQL's ReadyForQuery status), and {@link StreamMux#txnStatus}
     * holds the latest. Nothing is reconstructed from statement text, so a
     * COMMIT sent by any route (a PreparedStatement, a Statement, a batch) is
     * seen exactly as the server saw it. Transactions then start LAZILY:
     * {@code setAutoCommit(false)} / {@code commit()} / {@code rollback()} send
     * nothing to begin one, the first statement carries the BEGIN
     * ({@link WireV2Codec#FLAG_TXN_BEGIN}), and {@code commit()} /
     * {@code rollback()} / {@code setAutoCommit(true)} send only when the
     * server reports a transaction. A Conductor DAO call is 2 requests.
     *
     * <p>On an older server (no status), the driver keeps the EAGER protocol it
     * always had: BEGIN on {@code setAutoCommit(false)}, COMMIT+BEGIN on
     * {@code commit()}, ROLLBACK+BEGIN on {@code rollback()}.
     */
    private boolean serverTxnStatus() {
        return !forceEagerProtocol && mux.supportsPrepared();
    }

    /**
     * Tests only: drive the EAGER transaction protocol (the one an older
     * server without transaction status gets) against a current server, so
     * its paths can be exercised end to end.
     */
    volatile boolean forceEagerProtocol = false;

    /**
     * Eager protocol: after a COMMIT / ROLLBACK, open the next transaction the
     * driver keeps open at all times. Runs even when the COMMIT raised (a
     * {@code raiseExceptionOnSilentRollback} 25P02, an I/O error): skipping it
     * would leave {@code autoCommit=false} with NO transaction open, and since
     * the eager protocol never opens one lazily, every later statement would
     * run -- and commit -- on its own and survive {@code rollback()}. If the
     * BEGIN itself cannot be sent, the connection is closed: it can no longer
     * keep its transaction contract, and a pool must evict it.
     */
    private void reopenEagerTxn(SQLException pending) throws SQLException {
        if (isClosed()) {
            // The COMMIT / ROLLBACK's outcome was unknown: already closed.
            if (pending != null) throw pending;
            return;
        }
        try {
            sendTxnControl("BEGIN");
        } catch (SQLException e) {
            close();
            if (pending != null) {
                pending.addSuppressed(e);
                throw pending;
            }
            throw e;
        }
        if (pending != null) throw pending;
    }

    /** The server reports a transaction block (open or failed). */
    private boolean serverTxnActive() {
        return mux.txnStatus() != 0;
    }

    /** The server's last response reported a transaction block (tests). */
    boolean inServerTransaction() {
        return serverTxnStatus() && serverTxnActive();
    }

    /** Lazy mode: this statement must open the transaction. */
    private boolean needsBegin() {
        return !autoCommit && serverTxnStatus() && !serverTxnActive();
    }

    /** Round trips sent to the server (statements, BEGIN/COMMIT/ROLLBACK, SETs). */
    private final java.util.concurrent.atomic.AtomicLong requestsSent =
            new java.util.concurrent.atomic.AtomicLong();

    long requestsSent() { return requestsSent.get(); }

    /** Send one statement, opening the lazy transaction with it when needed. */
    private java.util.concurrent.Future<StreamState.StreamResult> sendStatement(String sql) {
        requestsSent.incrementAndGet();
        return needsBegin() ? mux.queryBeginning(sql) : mux.query(sql);
    }

    /** Transaction-control or session statement sent as-is: never opens a transaction. */
    private void sendControl(String sql) throws SQLException {
        // BEGIN / COMMIT / ROLLBACK decide the driver's transaction state and
        // have ONE way out: `sendTxnControl`, which closes the connection when
        // their outcome is unknown. Routed here so no caller can bypass it.
        if (isTxnControlVerb(sql)) {
            sendTxnControl(sql);
            return;
        }
        StreamState.StreamResult r;
        try {
            requestsSent.incrementAndGet();
            r = await(mux.query(sql), DEFAULT_TIMEOUT_SECONDS);
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            throw mapException(e);
        }
        checkSilentRollback(r, sql);
    }

    /**
     * Did {@code e} carry the SERVER's answer to the request? Only then is the
     * request's outcome known (the server refused it, or answered it). Anything
     * else -- the waiting thread interrupted after the request was written, a
     * timeout, a dropped socket -- leaves it UNKNOWN whether the server ran it.
     */
    /** BEGIN / COMMIT / ROLLBACK (the only spellings this driver sends). */
    static boolean isTxnControlVerb(String sql) {
        return "BEGIN".equals(sql) || "COMMIT".equals(sql) || "ROLLBACK".equals(sql);
    }

    static boolean serverAnswered(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof StreamState.PdbWireException) {
                return ((StreamState.PdbWireException) t).fromServer;
            }
            if (t.getCause() == t) break;
        }
        return false;
    }

    /**
     * A transaction-control statement whose outcome the driver's transaction
     * state depends on (BEGIN / COMMIT / ROLLBACK from {@code commit()},
     * {@code rollback()}, {@code setAutoCommit}). If it fails WITHOUT the
     * server's answer, the server may or may not have run it, so the driver no
     * longer knows whether a transaction is open: the connection is CLOSED
     * (08003 afterwards, so a pool evicts it) rather than left to run later
     * statements under a guessed state. A server-answered failure (and the
     * driver's own 25P02 for a silent rollback, which follows a server answer)
     * is rethrown with the connection kept.
     */
    private void sendTxnControl(String sql) throws SQLException {
        StreamState.StreamResult r;
        try {
            requestsSent.incrementAndGet();
            r = await(mux.query(sql), DEFAULT_TIMEOUT_SECONDS);
        } catch (Exception e) {
            SQLException se = (e instanceof SQLException) ? (SQLException) e : mapException(e);
            if (!serverAnswered(e)) {
                close();
            }
            throw se;
        }
        checkSilentRollback(r, sql);
    }

    /**
     * {@code raiseExceptionOnSilentRollback} (URL / {@code Properties} option,
     * default {@code false}): a COMMIT that the server answered by ROLLING
     * BACK a failed transaction raises {@link SQLException} {@code 25P02}
     * instead of returning normally.
     *
     * <p>By default {@code commit()} of a transaction whose statement failed
     * returns normally, exactly as pgjdbc's does against PostgreSQL (which
     * answers that COMMIT with the command tag {@code ROLLBACK} and no error):
     * the failing statement's own exception is where the application learned
     * of it. A client that must KNOW the outcome of every commit -- a consistency checker
     * history, a retry loop that lost the original exception -- opts in here.
     * The signal is the server's, not reconstructed: the response to that
     * COMMIT carries {@link WireV2Codec#FLAG_TXN_ROLLED_BACK} (the native
     * wire's stand-in for PostgreSQL's {@code ROLLBACK} tag). The name and the
     * SQLSTATE are those of the pgjdbc option of the same name (42.2.11; that
     * release's change was reverted in 42.2.12 as a default-behaviour change,
     * which is why it is opt-in here).
     *
     * <p>Applies to {@code commit()}, {@code setAutoCommit(true)} ending a
     * failed transaction, and a {@code COMMIT} / {@code END} sent as SQL text
     * through a {@code Statement} or {@code PreparedStatement}. The transaction
     * IS rolled back either way; the connection is usable afterwards.
     */
    private final boolean raiseOnSilentRollback;

    /**
     * {@code allowDdlOutsideTransaction} (URL / {@code Properties} /
     * {@code PdbDataSource} option, default {@code false}): with
     * {@code autoCommit=false}, a DDL issued while NO transaction is open (the
     * first statement after {@code setAutoCommit(false)} / {@code commit()} /
     * {@code rollback()}) runs on its own, outside any transaction, and takes
     * effect at once; a {@link SQLWarning} on the connection says so, because a
     * later {@code rollback()} will not undo it. The next statement opens the
     * transaction as usual. A DDL issued while a transaction IS open is still
     * refused by the server with {@code 25001}, so the caller's DML is never
     * committed or given up to make room for it.
     *
     * <p>Off by default: the default answer is the one pgjdbc gets over this
     * server's PostgreSQL wire (every DDL with {@code autoCommit=false} is
     * refused 25001), so {@code CREATE TABLE; rollback()} can never leave a
     * table behind unannounced. It exists for migration tools that run every
     * migration -- even a DDL-only one, and their own history-table CREATE --
     * inside a transaction and cannot be configured otherwise: Flyway does
     * (Conductor's {@code flywayForPrimaryDb}). This was the driver's default
     * behaviour until a pre-release consistency review made the
     * refusal the default; it is now a choice the URL has to state.
     *
     * <p>Scope: a SINGLE statement sent through {@code Statement} /
     * {@code PreparedStatement} {@code execute*}. A multi-statement string
     * (anything after a top-level {@code ;}) is not covered, even when it
     * starts with a DDL, because its DML would leave the transaction with it;
     * {@code executeBatch} always opens the transaction. Both are refused
     * {@code 25001}. The warning is on the
     * CONNECTION ({@link #getWarnings()}), not on the {@code Statement}.
     *
     * <p>Lazy protocol only. On an older server (eager protocol) the driver
     * keeps a BEGIN open at all times and cannot tell whether the open
     * transaction holds work, so DDL stays refused there.
     */
    private final boolean allowDdlOutsideTxn;

    /** The last statement failure seen while autoCommit was off (the cause a
     *  silent-rollback exception names), cleared when a transaction ends. */
    private volatile String lastStatementFailure;

    private void noteStatementFailure(SQLException se) {
        if (!autoCommit && se != null) lastStatementFailure = se.getMessage();
    }

    /** Raise 25P02 for a COMMIT the server answered by rolling back (opt-in). */
    private void checkSilentRollback(StreamState.StreamResult r, String sql) throws SQLException {
        if (r == null || !r.rolledBack) return;
        String cause = lastStatementFailure;
        lastStatementFailure = null;
        if (!raiseOnSilentRollback) return;
        throw new SQLException(
                "The database returned ROLLBACK, so the transaction cannot be committed. "
                + "Transaction failure cause is <<" + (cause == null ? "unknown" : cause) + ">>",
                "25P02");
    }

    private volatile boolean closed = false;
    private volatile int networkTimeoutMillis;
    private long startupDeadline = Long.MAX_VALUE;
    /**
     * The isolation level the SERVER will run this connection's next
     * transaction at, or {@link #ISOLATION_UNKNOWN} until the driver has either
     * set it or asked. It used to start as {@code TRANSACTION_READ_COMMITTED}
     * unconditionally - which is not what the server runs when its default is
     * set otherwise ({@code --default-isolation}, {@code options=-c
     * default_transaction_isolation=…}) or the session was changed with plain
     * SQL. So {@code getTransactionIsolation()} could report READ COMMITTED on
     * a connection running REPEATABLE READ, and
     * {@code setTransactionIsolation(READ_COMMITTED)} - what a pool's
     * {@code transactionIsolation} setting calls - matched the stale field and
     * returned WITHOUT telling the server. An application that asked for READ
     * COMMITTED, the level it was written for, silently ran at the stronger
     * level and got {@code 40001} where READ COMMITTED waits and re-applies.
     */
    private int txIsolationLevel = ISOLATION_UNKNOWN;
    static final int ISOLATION_UNKNOWN = -1;
    private String catalog;
    private String schema;
    private boolean readOnly = false;
    private final Properties clientInfo = new Properties();

    public PdbConnection(Pgcompat.ParsedUrl parsed) throws SQLException {
        this.parsed = parsed;
        int loginTimeout = Pgcompat.intParam(parsed.params, "loginTimeout", 0);
        if (loginTimeout < 0) throw new SQLException("loginTimeout must be >= 0", "22023");
        if (loginTimeout > 0) startupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(loginTimeout);
        // Read credentials and validate TLS options BEFORE opening the socket.
        // TLS is not yet wired into the Netty pipeline, so any non-`disable`
        // sslMode MUST fail loudly here rather than silently opening an
        // UNENCRYPTED connection (a caller that asked for
        // `require`/`verify-ca`/`verify-full` and got plaintext is a MITM
        // exposure). Also rejects an unknown sslMode value. The `user` /
        // `password` captured here are sent as the native wire's credential
        // frame after the handshake - see `authenticate` below.
        AuthHandshake auth = AuthHandshake.fromParams(parsed.params);
        this.resultDecoder = ArrowDecoder.ResultDecoder.parse(
                Pgcompat.strParam(parsed.params, ArrowDecoder.ResultDecoder.PROPERTY, null));
        this.prepareThreshold = Pgcompat.intParam(parsed.params, "prepareThreshold", 5);
        int cacheEntries = Pgcompat.intParam(parsed.params, "preparedStatementCacheQueries", 256);
        int cacheBytes = Pgcompat.intParam(parsed.params, "preparedStatementCacheSizeMiB", 5)
                          * 1024 * 1024;
        this.preferUnixSocket = Pgcompat.boolParam(parsed.params, "preferUnixSocket", true);
        // Where the server's `.s.PDB.<port>` lives (its --unix-socket-dir), or
        // the socket outright. Consumed by StreamMux.openTransport.
        String unixSocketDir = Pgcompat.strParam(parsed.params, "unixSocketDir", null);
        String unixSocketPath = Pgcompat.strParam(parsed.params, "unixSocketPath", null);
        // `maxStreamsPerConn` was advertised by Driver.getPropertyInfo and
        // settable on PdbDataSource for several releases while NOTHING read it.
        // Parsed here and handed to StreamMux, which enforces it. 0 = unlimited,
        // matching the server's `max_concurrent_streams` convention.
        this.maxStreamsPerConn = Pgcompat.intParam(
                parsed.params, "maxStreamsPerConn",
                org.particledb.wire.StreamMux.DEFAULT_MAX_STREAMS);
        // Multiplexing is REQUIRED by default on the native wire. Opting out is
        // explicit, for talking to an older v1-only server.
        this.requireMux = Pgcompat.boolParam(parsed.params, "requireMux", true);
        this.serverPrepare = Pgcompat.boolParam(parsed.params, "serverPrepare", true);
        this.bulkBatch = Pgcompat.boolParam(parsed.params, "bulkBatch", true);
        this.raiseOnSilentRollback =
                Pgcompat.boolParam(parsed.params, "raiseExceptionOnSilentRollback", false);
        this.allowDdlOutsideTxn =
                Pgcompat.boolParam(parsed.params, "allowDdlOutsideTransaction", false);
        // Canonical default per the "URL options" table in README.md
        int connectTimeoutMs = Pgcompat.intParam(parsed.params, "connectTimeout", 30_000);
        if (connectTimeoutMs <= 0) throw new SQLException("connectTimeout must be > 0", "22023");
        connectTimeoutMs = startupWaitMillis(connectTimeoutMs);

        // PG-compat metadata identity. Default ON so Flyway / Hibernate / jOOQ
        // pick their PostgreSQL dialect out of the box. `pgReportedVersion`
        // overrides the advertised version string (default "14.0" - a version
        // Flyway 9 accepts without a Teams-edition upgrade warning). Overridable
        // via jdbc:particledb://…?pgCompat=false or ?pgReportedVersion=15.0.
        this.pgCompat = Pgcompat.boolParam(parsed.params, "pgCompat", true);
        if (pgCompat) {
            this.reportedProductName = "PostgreSQL";
            this.reportedProductVersion =
                    Pgcompat.strParam(parsed.params, "pgReportedVersion", "14.0");
            int[] mm = parseMajorMinor(this.reportedProductVersion, 14, 0);
            this.reportedMajorVersion = mm[0];
            this.reportedMinorVersion = mm[1];
        } else {
            this.reportedProductName = "ParticleDB";
            this.reportedProductVersion = "1.1";
            this.reportedMajorVersion = 1;
            this.reportedMinorVersion = 1;
        }

        // pg_catalog adapter follows pgCompat by default (it is only needed
        // when the driver claims PostgreSQL identity and PG tooling probes the
        // catalog), but can be toggled independently for debugging.
        this.pgCatalogRewrite = Pgcompat.boolParam(parsed.params, "pgCatalogAdapter", pgCompat);

        this.preparedCache = new ClientPreparedCache(cacheEntries, cacheBytes);

        // Connect synchronously (JDBC contract is blocking).
        //
        // Share the process-wide Netty event-loop group across all connections
        // (ownGroup=false) rather than allocating one NioEventLoopGroup - and
        // thus one OS thread - per connection. The per-connection group was a
        // resource leak: any connection that errored/aborted/GC'd without an
        // explicit close() left its event-loop thread alive forever, so a long
        // run with many reconnects exhausted OS threads. The shared group is
        // shut down once at JVM exit (SharedEventLoop's shutdown hook), never by
        // PdbConnection.close().
        this.mux = new StreamMux(SharedEventLoop.group(), preferUnixSocket, connectTimeoutMs,
                                 maxStreamsPerConn, requireMux, unixSocketDir, unixSocketPath);
        this.mux.configureTls(org.particledb.wire.TlsOptions.fromParams(parsed.params));
        WireV2Codec.HelloOk hello;
        // The session runs in the JVM's zone, as pgjdbc's does: sent in the
        // HELLO (the native wire's startup parameters), so it is the session's
        // STARTUP value - what RESET / RESET ALL / DISCARD ALL return to.
        String sessionZone = Timestamptz.sessionSetting(java.time.ZoneId.systemDefault());
        try {
            hello = mux.connect(parsed.host, parsed.port, sessionZone)
                    .get(connectTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // Release the channel/streams we may have partially opened. The
            // shared event-loop group is intentionally left running.
            try { mux.close(); } catch (Exception ignored) {}
            SQLException refused = startupRefusal(e);
            if (refused != null) {
                throw refused;
            }
            // 08001 sqlclient_unable_to_establish_sqlconnection: connection
            // pools (HikariCP, Spring) branch on the 08 class to decide that a
            // connection is unusable, so this must not be a null SQLSTATE either.
            if (System.nanoTime() >= startupDeadline) throw loginTimeout(e);
            throw new java.sql.SQLNonTransientConnectionException(
                    "Failed to connect to " + parsed.host + ":" + parsed.port
                    + " — " + e.getMessage(), "08001", e);
        }

        // Authenticate BEFORE any other request. The native wire is fail-closed:
        // a server that requires credentials refuses every frame until the
        // credential frame succeeds, so this has to happen here, inside the
        // constructor, and not lazily at the first statement.
        try {
            authenticate(auth, hello, startupWaitMillis(connectTimeoutMs));
            if (!hello.appliedSessionParams()) {
                // An older server ignored the HELLO's session parameters.
                applySessionTimeZone(sessionZone);
            }
        } catch (SQLException e) {
            try { mux.close(); } catch (Exception ignored) {}
            if (System.nanoTime() >= startupDeadline) throw loginTimeout(e);
            throw e;
        }
        startupDeadline = Long.MAX_VALUE;
    }

    private int startupWaitMillis(int limit) {
        if (startupDeadline == Long.MAX_VALUE) return limit;
        long remaining = TimeUnit.NANOSECONDS.toMillis(startupDeadline - System.nanoTime());
        return (int) Math.max(1, Math.min(limit, remaining + 1));
    }

    private static SQLException loginTimeout(Exception cause) {
        return new java.sql.SQLTimeoutException("connection login timed out", "08001", cause);
    }

    /**
     * Run the session in the JVM's time zone on a server that did not apply the
     * HELLO's session parameters (one older than
     * {@link WireV2Codec#CAP_EXT_SESSION_PARAMS}). Without it the session stayed
     * on the server default (UTC) while every bound {@code java.sql.Timestamp} /
     * {@code LocalDateTime} is the JVM's wall clock: under
     * {@code America/Los_Angeles} a {@code timestamptz} written from a bound
     * timestamp landed seven hours off. A zone the server refuses fails the
     * connect with the server's SQLSTATE (22023), never a silent UTC session.
     * (On such a server RESET returns to the server default, not this zone.)
     */
    private void applySessionTimeZone(String setting) throws SQLException {
        sendControl("SET TimeZone TO '" + setting.replace("'", "''") + "'");
    }

    /**
     * The connect failure as PostgreSQL's FATAL for a bad startup parameter when
     * the server refused the HELLO's session parameters
     * ({@link WireV2Codec#ERR_STARTUP_PARAMETER}, text
     * {@code "22023: FATAL: invalid value for parameter …"}); {@code null} for
     * any other failure.
     */
    static SQLException startupRefusal(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof org.particledb.wire.StreamState.PdbWireException) {
                org.particledb.wire.StreamState.PdbWireException w =
                        (org.particledb.wire.StreamState.PdbWireException) t;
                if (w.code != WireV2Codec.ERR_STARTUP_PARAMETER) return null;
                String text = w.serverMessage;
                String state = "08004";
                if (text.length() > 7 && text.charAt(5) == ':' && text.charAt(6) == ' ') {
                    state = text.substring(0, 5);
                    text = text.substring(7);
                }
                return new SQLException(text, state, e);
            }
        }
        return null;
    }

    /**
     * Send the connect-time credential frame when the server asked for one.
     *
     * <p>Three outcomes, all decided by {@code HELLO_OK}'s capability byte:
     * <ul>
     *   <li>server does not require auth → return immediately, sending nothing.
     *       This is the {@code --no-auth} path and it stays byte-identical
     *       whether or not credentials were configured.</li>
     *   <li>server requires auth, no credentials configured → {@code SQLException}
     *       (SQLSTATE 28000) HERE, so the caller learns at
     *       {@code getConnection()} instead of at its first query.</li>
     *   <li>server requires auth, credentials configured → send the frame and
     *       wait for the answer. A rejection becomes a {@code SQLException}
     *       (SQLSTATE 28P01) carrying the server's own message.</li>
     * </ul>
     */
    private void authenticate(AuthHandshake auth, WireV2Codec.HelloOk hello, int timeoutMs)
            throws SQLException {
        if (!auth.requireCredentials(hello.chosenCaps)) {
            return;
        }
        try {
            mux.authenticate(auth.user, auth.password).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException te) {
            throw new SQLException(
                    "authentication for user \"" + auth.user + "\" timed out after "
                    + timeoutMs + "ms", AuthHandshake.SQLSTATE_INVALID_AUTHORIZATION, te);
        } catch (IllegalArgumentException iae) {
            // Rejected client-side before hitting the wire (empty username,
            // credential field over the server's byte cap).
            throw new SQLException(iae.getMessage(),
                    AuthHandshake.SQLSTATE_INVALID_AUTHORIZATION, iae);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while authenticating user \""
                    + auth.user + "\"", AuthHandshake.SQLSTATE_INVALID_AUTHORIZATION, ie);
        } catch (Exception e) {
            throw authFailure(auth.user, e);
        }
    }

    /**
     * Turn a failed credential exchange into a clear {@code SQLException}.
     *
     * <p>The server's own wording is preserved verbatim ("password
     * authentication failed for user …") instead of being flattened into a
     * generic wire error, and the wire error code becomes the vendor code so a
     * caller can still distinguish {@code ERR_AUTH_FAILED} from a connection
     * that died mid-handshake.
     */
    private static SQLException authFailure(String user, Exception e) {
        Throwable t = e instanceof java.util.concurrent.ExecutionException
                ? e.getCause() : e;
        if (t instanceof StreamState.PdbWireException) {
            StreamState.PdbWireException w = (StreamState.PdbWireException) t;
            String state = w.code == WireV2Codec.ERR_AUTH_FAILED
                    ? AuthHandshake.SQLSTATE_INVALID_PASSWORD
                    : AuthHandshake.SQLSTATE_INVALID_AUTHORIZATION;
            return new SQLException(
                    "authentication failed for user \"" + user + "\": " + w.serverMessage,
                    state, w.code, w);
        }
        String detail = t == null ? String.valueOf(e) : String.valueOf(t.getMessage());
        return new SQLException(
                "authentication failed for user \"" + user + "\": " + detail,
                AuthHandshake.SQLSTATE_INVALID_AUTHORIZATION,
                t == null ? e : t);
    }

    StreamMux mux() { return mux; }
    ClientPreparedCache preparedCache() { return preparedCache; }
    int prepareThreshold() { return prepareThreshold; }

    /** The enforced concurrent-stream cap for this connection (0 = unlimited). */
    int maxStreamsPerConn() { return maxStreamsPerConn; }

    // ── PG-compat metadata accessors (used by PdbDatabaseMetaData) ──────────
    boolean pgCompat() { return pgCompat; }
    String reportedProductName() { return reportedProductName; }
    String reportedProductVersion() { return reportedProductVersion; }
    int reportedMajorVersion() { return reportedMajorVersion; }
    int reportedMinorVersion() { return reportedMinorVersion; }
    /** Database name from the URL (JDBC "catalog"), or "particledb" if none. */
    String databaseName() { return parsed.database == null ? "particledb" : parsed.database; }
    String userName() { return Pgcompat.strParam(parsed.params, "user", ""); }

    /** Parse "14.0" / "15" / "14.3 (PDB)" into [major, minor] with fallbacks. */
    private static int[] parseMajorMinor(String ver, int defMaj, int defMin) {
        int maj = defMaj, min = defMin;
        if (ver != null) {
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("(\\d+)(?:\\.(\\d+))?").matcher(ver);
            if (m.find()) {
                try { maj = Integer.parseInt(m.group(1)); } catch (NumberFormatException ignored) {}
                if (m.group(2) != null) {
                    try { min = Integer.parseInt(m.group(2)); } catch (NumberFormatException ignored) {}
                }
            }
        }
        return new int[]{maj, min};
    }

    /**
     * Adapt a query for PDB's SQL surface immediately before it hits the wire.
     * This is the single chokepoint every statement (plain and prepared) flows
     * through: {@link PdbStatement} and {@link PdbPreparedStatement} both route
     * their fully-rendered SQL through {@code execute*} here. When the pg_catalog
     * adapter is enabled, rewrite references to PDB-unsupported catalog relations
     * into inline sub-selects so PG-dialect tooling (Flyway, Conductor) works
     * over the native wire without the engine emulating pg_catalog.
     */
    private String adapt(String sql) {
        return pgCatalogRewrite ? PgCatalogAdapter.rewrite(sql) : sql;
    }

    /**
     * Send one adapted statement on a fresh stream (sync wait, JDBC contract).
     *
     * <p><b>Every statement with {@code autoCommit=false} runs inside the
     * transaction the server knows about.</b> When none is open yet, the
     * statement itself carries the BEGIN ({@code FLAG_TXN_BEGIN}) -- a DDL
     * exactly like a DML. ParticleDB applies schema changes immediately and
     * cannot roll them back, where PostgreSQL (and pgjdbc over it) makes DDL
     * part of the transaction, so the SERVER refuses a DDL inside a transaction
     * block with SQLSTATE {@code 25001} and aborts the block; {@code rollback()}
     * then undoes the block's DML. That is the same answer pgjdbc gets over
     * this server's PostgreSQL wire (pgjdbc sends its BEGIN eagerly).
     *
     * <p>This driver used to run a DDL issued as the FIRST statement after
     * {@code setAutoCommit(false)} / {@code commit()} / {@code rollback()}
     * WITHOUT opening the transaction, as an autocommit statement with only a
     * {@code SQLWarning}: the CREATE TABLE was accepted, and a later
     * {@code rollback()} silently left the table in place. Run DDL with
     * {@code autoCommit=true}, or opt back into that behaviour -- announced by
     * a {@link SQLWarning} -- with {@code allowDdlOutsideTransaction=true}
     * (see {@link #allowDdlOutsideTxn}), which a migration tool such as Flyway
     * needs.
     *
     * <p>On an older server (eager protocol, no transaction status) the driver
     * always holds a BEGIN open and such a server does not refuse DDL inside
     * it, so the driver refuses it itself with the same {@code 25001}.
     */
    private StreamState.StreamResult sendOne(String rawSql) throws SQLException {
        return sendOne(rawSql, DEFAULT_TIMEOUT_SECONDS);
    }

    /** {@link PdbStatement#cancel()}: reset the request's stream (see {@link StreamMux#cancel}). */
    void cancelRequest(java.util.concurrent.Future<StreamState.StreamResult> f) {
        mux.cancel(f);
    }

    /** Default per-statement wire wait when no {@code setQueryTimeout} was set. */
    static final int DEFAULT_TIMEOUT_SECONDS = 60;

    /**
     * The statement executing on this thread, so {@link #await} can publish
     * the request in flight for {@link PdbStatement#cancel()}, which another
     * thread calls ({@link PdbStatement} binds it around each execution).
     */
    static final ThreadLocal<PdbStatement> EXECUTING = new ThreadLocal<>();

    /**
     * How long a timed-out request may take to answer once its stream is
     * reset: a server that cancels answers 57014 at the statement's next
     * interrupt check (milliseconds); only a server that cannot cancel runs
     * past this, and its outcome is then unknown.
     */
    static final int CANCEL_GRACE_SECONDS = 5;

    private StreamState.StreamResult await(java.util.concurrent.Future<StreamState.StreamResult> f,
                                           int timeoutSeconds) throws Exception {
        PdbStatement owner = EXECUTING.get();
        if (owner != null) owner.inflight = f;
        long networkDeadline = networkDeadline();
        long queryDeadline = timeoutSeconds <= 0 ? Long.MAX_VALUE
                : System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        long deadline = Math.min(startupDeadline, Math.min(networkDeadline, queryDeadline));
        // JDBC: setQueryTimeout(0) means "no limit".
        try {
            if (owner != null && owner.streamsResults() && f instanceof StreamState.DrivenFuture) {
                // setFetchSize(n > 0): hand the caller its rows as the chunks
                // arrive (StreamedRows) instead of after the last one. The
                // query timeout bounds the wait for the FIRST chunk.
                StreamState st = ((StreamState.DrivenFuture) f).streamState();
                mux.driveUntil(st, st::firstChunkOrDone, deadline);
                if (st.attachStreaming()) {
                    return StreamState.StreamResult.arrowStreamed(new StreamedRows(this, mux, st).readSchema());
                }
            }
            return deadline == Long.MAX_VALUE ? f.get()
                    : f.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (java.util.concurrent.TimeoutException te) {
            if (System.nanoTime() >= startupDeadline) {
                close();
                throw loginTimeout(te);
            }
            if (networkDeadline <= queryDeadline) throw networkTimeout(te);
            // Ask the server to cancel the statement (RST_STREAM on its
            // stream). If it ANSWERS within the grace -- 57014 when the cancel
            // landed, or its result when it finished first -- the outcome is
            // known and the connection stays in service. Only a request that
            // still has not answered is ambiguous: then, as before, the
            // connection is closed so no pool hands it on mid-statement.
            if (mux.cancel(f)) {
                try {
                    long grace = Math.min(TimeUnit.SECONDS.toNanos(CANCEL_GRACE_SECONDS),
                            networkDeadline == Long.MAX_VALUE ? Long.MAX_VALUE
                                    : Math.max(0, networkDeadline - System.nanoTime()));
                    StreamState.StreamResult late = f.get(grace, TimeUnit.NANOSECONDS);
                    // It completed just after the deadline: the caller asked for
                    // a bound, but the statement's effects are in place and
                    // known, so its result is the honest answer.
                    return late;
                } catch (java.util.concurrent.ExecutionException answered) {
                    throw answered; // the server's own (57014) error: outcome known
                } catch (java.util.concurrent.TimeoutException still) {
                    if (System.nanoTime() >= networkDeadline) throw networkTimeout(still);
                    // fall through: ambiguous
                }
            }
            closeAfterTimeout();
            throw te;
        } finally {
            if (owner != null) owner.inflight = null;
        }
    }

    long networkDeadline() {
        int timeout = networkTimeoutMillis;
        return timeout == 0 ? Long.MAX_VALUE
                : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    SQLException networkTimeout(Exception cause) {
        closeAfterTimeout();
        return new java.sql.SQLNonTransientConnectionException(
                "network timeout; connection closed and request outcome may be unknown", "08006", cause);
    }

    /**
     * A request timed out on the client. That does not cancel it: the server
     * may still execute -- and commit -- it, and its late reply will still
     * arrive on this socket (and update the transaction status the driver
     * tracks from replies). The outcome is ambiguous, so the request is never
     * resent, and the connection is CLOSED rather than left usable: a pool that
     * recycled it would hand the next borrower a socket with a statement of
     * the previous borrower's still running, possibly inside its transaction.
     * The caller still gets {@link java.sql.SQLTimeoutException} (57014); every
     * later use of this connection fails 08003, so pools evict it.
     */
    void closeAfterTimeout() {
        closed = true;
        mux.abort();
    }

    private StreamState.StreamResult sendOne(String rawSql, int timeoutSeconds)
            throws SQLException {
        return sendOne(rawSql, timeoutSeconds, false);
    }

    /**
     * @param adaptFree the caller has established that the pg_catalog adapter
     *        cannot change this statement (a PreparedStatement whose template
     *        {@link PgCatalogAdapter#mayRewrite} rejected), so its per-statement
     *        pre-filters are skipped.
     */
    private StreamState.StreamResult sendOne(String rawSql, int timeoutSeconds, boolean adaptFree)
            throws SQLException {
        ensureOpen();
        // `SELECT set_config(name, value, is_local)` is a GUC WRITE that also
        // RETURNS A ROW, and on this wire the two halves need two statements  -
        // which is why it is handled here rather than only in the pure
        // String→String `adapt` below.
        //
        // The WRITE is `PgCatalogAdapter.SetConfigCall#toSessionStatement`: the
        // SET / SET LOCAL / RESET the native wire actually applies. It is the
        // half that used to be missing - the adapter replaced the whole call
        // with its VALUE argument, so Flyway's `changeCurrentSchemaTo` pinned
        // nothing while reporting the schema it asked for.
        //
        // The ROW is owed as well, or `executeQuery` on the call returns
        // nothing where PostgreSQL returns a value, so the write is followed by
        // a `SHOW <name>` and THAT result is what the caller sees. The read-back
        // asks the SERVER rather than echoing the argument, deliberately: the
        // old behaviour returned the argument while writing nothing, and no
        // client could tell. It is also what makes the RESET form truthful  -
        // PostgreSQL's `set_config(name, NULL, …)` reports the value now in
        // effect (the default), not the null it was handed.
        //
        // Gated on `pgCatalogRewrite` like every other PG-dialect adaptation:
        // with the adapter off, the call goes to the engine untouched and earns
        // its honest `42883` (the engine has no `set_config` function; it is a
        // wire intercept, and only the PG wire has one).
        if (pgCatalogRewrite && !adaptFree) {
            PgCatalogAdapter.SetConfigCall sc =
                    PgCatalogAdapter.parseWholeStatementSetConfig(rawSql);
            if (sc != null) {
                try {
                    // A REFUSED parameter (`client_encoding`, the read-only
                    // ones) errors here, which is the point - it used to be
                    // swallowed and answered with the caller's own value.
                    await(sendStatement(sc.toSessionStatement()), timeoutSeconds);
                    return await(sendStatement(sc.readBackStatement()), timeoutSeconds);
                } catch (SQLException se) {
                    throw se;
                } catch (java.util.concurrent.TimeoutException te) {
                    throw new java.sql.SQLTimeoutException(
                            "query timed out after " + timeoutSeconds + "s; the statement may still"
                    + " execute on the server, so the connection was closed", "57014", te);
                } catch (Exception e) {
                    throw mapException(e);
                }
            }
        }
        String sql = adaptFree ? rawSql : adapt(rawSql);
        try {
            if (!autoCommit && !serverTxnStatus() && looksLikeDdl(rawSql)) {
                // Eager protocol (older server, no transaction status): the
                // driver keeps a BEGIN open at all times and such a server does
                // not refuse DDL inside it, so the driver refuses it itself --
                // whether or not a statement has run yet. See the method comment.
                throw ddlInsideTransaction();
            }
            if (allowDdlOutsideTxn && needsBegin() && looksLikeDdl(rawSql)
                    && !PdbPreparedStatement.isMultiStatement(rawSql)) {
                // Opt-in (see allowDdlOutsideTxn): no transaction is open, so
                // there is no caller work to commit or lose. Send the DDL
                // WITHOUT the BEGIN, so it runs on its own, and say so -- a
                // later rollback() cannot undo it. The next statement opens
                // the transaction (the server still reports none).
                //
                // ONE statement only. A multi-statement string led by a DDL
                // ("CREATE ...; INSERT ...") would otherwise carry its DML
                // outside the caller's transaction too, where rollback() could
                // not undo it; it takes the normal path below (BEGIN, and the
                // server refuses the DDL 25001).
                requestsSent.incrementAndGet();
                StreamState.StreamResult r = await(mux.query(sql), timeoutSeconds);
                addWarning(ddlOutsideTransactionWarning());
                return r;
            }
            // Lazy protocol: EVERY statement with autoCommit=false -- a DDL
            // included -- carries the BEGIN when no transaction is open, so it
            // runs inside the transaction the server knows about and the server
            // decides (a DDL is refused 25001 and aborts the block).
            StreamState.StreamResult r = await(sendStatement(sql), timeoutSeconds);
            checkSilentRollback(r, sql);
            return r;
        } catch (SQLException se) {
            noteStatementFailure(se);
            throw se;
        } catch (java.util.concurrent.TimeoutException te) {
            // A blown setQueryTimeout must arrive as java.sql.SQLTimeoutException
            // with PG's query_canceled SQLSTATE, not as a bare SQLException whose
            // message is "null" (TimeoutException carries none) - retry and
            // circuit-breaker logic branches on exactly this class.
            throw new java.sql.SQLTimeoutException(
                    "query timed out after " + timeoutSeconds + "s; the statement may still"
                    + " execute on the server, so the connection was closed", "57014", te);
        } catch (Exception e) {
            SQLException se = mapException(e);
            noteStatementFailure(se);
            throw se;
        }
    }

    /** Execute a one-off SQL on a fresh stream (sync wait, JDBC contract). */
    long execute(String sql) throws SQLException {
        return execute(sql, DEFAULT_TIMEOUT_SECONDS);
    }

    long execute(String sql, int timeoutSeconds) throws SQLException {
        StreamState.StreamResult r = sendOne(sql, timeoutSeconds);
        switch (r.kind) {
            case OK: return r.affectedRows();
            case ARROW: return -1L; // SELECT — caller should call query()
            default: return 0L;
        }
    }

    /**
     * Relation names by {@code pg_class.oid}, for
     * {@link java.sql.ResultSetMetaData#getTableName}: the server stamps each
     * column that reads a table column with that table's OID (Arrow field
     * metadata {@code pdb:src_table_oid}, the same identity the PG wire puts in
     * RowDescription), and the name is looked up in {@code pg_class} once per
     * OID per connection, as pgjdbc does for its field metadata. A renamed
     * table therefore keeps its old name on this connection, again as pgjdbc.
     */
    private final java.util.concurrent.ConcurrentHashMap<Long, String> relationNames =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The {@code relname} of {@code oid}, or {@code ""} when no relation has
     * it. {@code wanted} is every OID the caller will ask for next (the other
     * columns of the same result), fetched in the same catalog query.
     */
    String relationName(long oid, java.util.Collection<Long> wanted) throws SQLException {
        String cached = relationNames.get(oid);
        if (cached != null) return cached;
        java.util.TreeSet<Long> missing = new java.util.TreeSet<>();
        missing.add(oid);
        for (Long w : wanted) {
            if (w != null && w > 0 && !relationNames.containsKey(w)) missing.add(w);
        }
        StringBuilder in = new StringBuilder();
        for (Long m : missing) {
            if (in.length() > 0) in.append(',');
            in.append(m.longValue());
        }
        try (PdbResultSet rs = executeQuery(
                "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.oid IN (" + in + ")")) {
            while (rs.next()) relationNames.put(rs.getLong(1), rs.getString(2));
        }
        // An OID that names no relation (dropped since) answers "" from now on.
        for (Long m : missing) relationNames.putIfAbsent(m, "");
        return relationNames.get(oid);
    }

    PdbResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, DEFAULT_TIMEOUT_SECONDS);
    }

    /** This connection's {@code resultDecoder}. */
    public ArrowDecoder.ResultDecoder resultDecoder() {
        return resultDecoder;
    }

    PdbResultSet executeQuery(String sql, int timeoutSeconds) throws SQLException {
        return new PdbResultSet(sendOne(sql, timeoutSeconds), resultDecoder);
    }

    PdbResultSet executeQuery(String sql, int timeoutSeconds, boolean adaptFree) throws SQLException {
        return new PdbResultSet(sendOne(sql, timeoutSeconds, adaptFree), resultDecoder);
    }

    /**
     * Run a one-off SQL and return the RAW stream result so the caller can
     * classify by frame kind (ARROW ⇒ ResultSet, OK ⇒ update count). Used by
     * {@link PdbStatement#execute(String)} so a row-returning statement is
     * never mis-routed through the update path (which discards Arrow rows).
     */
    StreamState.StreamResult executeRaw(String sql) throws SQLException {
        return sendOne(sql, DEFAULT_TIMEOUT_SECONDS);
    }

    StreamState.StreamResult executeRaw(String sql, int timeoutSeconds) throws SQLException {
        return sendOne(sql, timeoutSeconds);
    }

    StreamState.StreamResult executeRaw(String sql, int timeoutSeconds, boolean adaptFree)
            throws SQLException {
        return sendOne(sql, timeoutSeconds, adaptFree);
    }

    /**
     * First-keyword classification of a statement as DDL (or DCL that likewise
     * cannot participate in a ParticleDB transaction). Leading whitespace and
     * SQL comments are skipped so a commented migration header does not hide the
     * keyword. Deliberately conservative: only the statement kinds that PDB
     * auto-commits are listed, so ordinary DML never leaves the transaction.
     */
    static boolean looksLikeDdl(String sql) {
        if (sql == null) return false;
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2; while (i < n && sql.charAt(i) != '\n') i++; continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i += 2; while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
                i += 2; continue;
            }
            break;
        }
        int start = i;
        while (i < n && (Character.isLetter(sql.charAt(i)))) i++;
        if (i == start) return false;
        String kw = sql.substring(start, i);
        return kw.equalsIgnoreCase("CREATE")
            || kw.equalsIgnoreCase("ALTER")
            || kw.equalsIgnoreCase("DROP")
            || kw.equalsIgnoreCase("TRUNCATE")
            || kw.equalsIgnoreCase("COMMENT")
            || kw.equalsIgnoreCase("REINDEX")
            || kw.equalsIgnoreCase("GRANT")
            || kw.equalsIgnoreCase("REVOKE");
    }

    /** Parsed PreparedStatement templates by SQL text (bounded; cleared when full). */
    private final java.util.concurrent.ConcurrentHashMap<String, PdbPreparedStatement.Template>
            templates = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_TEMPLATES = 512;

    PdbPreparedStatement.Template template(String sql) {
        PdbPreparedStatement.Template t = templates.get(sql);
        if (t == null) {
            t = new PdbPreparedStatement.Template(sql);
            if (templates.size() >= MAX_TEMPLATES) templates.clear();
            templates.putIfAbsent(sql, t);
        }
        return t;
    }

    /** This connection may execute PreparedStatements by server-side handle. */
    boolean serverPrepareEnabled() {
        return serverPrepare && mux.supportsPrepared();
    }

    /**
     * The server accepts typed EXECUTE parameters (Timestamp, Double,
     * BigDecimal, ...; {@link org.particledb.wire.TypedParams}). On an older
     * server only null / boolean / integer / text values are bound.
     */
    boolean typedParamsEnabled() {
        return mux.supportsTypedParams();
    }

    /** {@code serverPrepare} as the URL set it ({@code false}: render every statement as text). */
    boolean serverPrepareRequested() {
        return serverPrepare;
    }

    /**
     * The server accepts bound array parameters (EXECUTE tag 17,
     * {@link org.particledb.wire.ArrayParams}) and server-side prepare is on.
     */
    boolean arrayParamsEnabled() {
        return serverPrepareEnabled() && mux.supportsArrayParams();
    }

    /**
     * Execute {@code dollarSql} (placeholders already spelled {@code $1..$N})
     * by server-side handle with typed parameters: prepared once per
     * connection, then EXECUTE frames carrying only the handle and the values.
     * Returns null when the statement cannot use the handle path (registry
     * full, or the server refused the prepare) - the caller then renders the
     * text and sends it as before. The server renders the bound values with
     * the same rules as the text path and runs the same statement path, so the
     * two cannot answer differently.
     */
    StreamState.StreamResult executeServerPrepared(String dollarSql, Object[] params, int n,
                                                   int timeoutSeconds) throws SQLException {
        return executeServerPrepared(dollarSql, params, n, timeoutSeconds, false);
    }

    /**
     * {@link #executeServerPrepared(String, Object[], int, int)}; with
     * {@code mustBind} (an execution carrying an array parameter, which is
     * never rendered as text unless {@code serverPrepare=false}) it never
     * returns null: a refused PREPARE raises the server's own error, and a full
     * statement registry raises {@code SQLFeatureNotSupportedException}.
     */
    StreamState.StreamResult executeServerPrepared(String dollarSql, Object[] params, int n,
                                                   int timeoutSeconds, boolean mustBind) throws SQLException {
        byte[] name = serverStatement(dollarSql, timeoutSeconds, mustBind);
        if (name == null) return null;
        try {
            requestsSent.incrementAndGet();
            StreamState.StreamResult r =
                    await(mux.execute(WireV2Codec.encodeExecutePayload(name, params, n),
                            needsBegin()), timeoutSeconds);
            serverPreparedExecs.incrementAndGet();
            checkSilentRollback(r, dollarSql);
            return r;
        } catch (SQLException se) {
            noteStatementFailure(se);
            throw se;
        } catch (java.util.concurrent.TimeoutException te) {
            throw new java.sql.SQLTimeoutException(
                    "query timed out after " + timeoutSeconds + "s; the statement may still"
                    + " execute on the server, so the connection was closed", "57014", te);
        } catch (Exception e) {
            SQLException se = mapException(e);
            noteStatementFailure(se);
            throw se;
        }
    }

    /**
     * The server name of {@code dollarSql}'s prepared statement, preparing it
     * on first use. Null (the caller takes the text path) when the registry is
     * full or the PREPARE failed -- a failure is remembered, so later
     * executions do not retry it. With {@code mustBind} null is never
     * returned: a remembered failure is retried once (the catalog may have
     * changed since), and a failure raises the server's error.
     */
    private byte[] serverStatement(String dollarSql, int timeoutSeconds, boolean mustBind) throws SQLException {
        java.util.concurrent.CompletableFuture<byte[]> f = serverPrepared.get(dollarSql);
        if (f != null && mustBind && f.isCompletedExceptionally()) {
            serverPrepared.remove(dollarSql, f);
            f = null;
        }
        if (f == null) {
            if (serverPrepared.size() >= MAX_SERVER_PREPARED) {
                if (!mustBind) return null;
                throw new java.sql.SQLFeatureNotSupportedException("this connection already holds "
                        + MAX_SERVER_PREPARED + " server-side prepared statements, so a statement with an"
                        + " array parameter cannot be bound; use fewer distinct statement texts per"
                        + " connection, or serverPrepare=false to render arrays as SQL text", "0A000");
            }
            java.util.concurrent.CompletableFuture<byte[]> mine =
                    new java.util.concurrent.CompletableFuture<>();
            f = serverPrepared.putIfAbsent(dollarSql, mine);
            if (f == null) {
                String name = "j" + serverPreparedSeq.incrementAndGet();
                try {
                    requestsSent.incrementAndGet();
                    await(mux.prepare(name, dollarSql), timeoutSeconds);
                    byte[] nb = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    mine.complete(nb);
                    return nb;
                } catch (Exception e) {
                    // Not fatal for a statement the driver may render: it uses
                    // the text path, now and for later executions (the failure
                    // stays cached).
                    mine.completeExceptionally(e);
                    if (mustBind) throw prepareFailure(e);
                    return null;
                }
            }
        }
        try {
            return f.get();
        } catch (java.util.concurrent.ExecutionException e) {
            if (mustBind) {
                Throwable c = e.getCause();
                throw prepareFailure(c instanceof Exception ? (Exception) c : e);
            }
            return null;
        } catch (Exception e) {
            if (mustBind) throw prepareFailure(e);
            return null;
        }
    }

    private SQLException prepareFailure(Exception e) {
        if (e instanceof java.util.concurrent.TimeoutException) {
            return new java.sql.SQLTimeoutException("PREPARE timed out", "57014", e);
        }
        SQLException se = e instanceof SQLException ? (SQLException) e : mapException(e);
        noteStatementFailure(se);
        return se;
    }

    /**
     * Run {@code sqls} as one MSG_BATCH and return the JDBC update count of
     * each statement (affected rows; {@link java.sql.Statement#SUCCESS_NO_INFO}
     * for a row-returning statement or a count beyond {@code int}).
     *
     * <p>The server stops at the first failing statement and reports it as an
     * ERROR entry inside the batch result. That raises a
     * {@link java.sql.BatchUpdateException} whose update counts are those of
     * the statements that ran before it (as PostgreSQL's driver does), whose
     * message, SQLSTATE and vendor code are those {@link #mapException} gives
     * the server error, and whose cause is that mapped exception. A
     * server-level error for the whole batch is raised the same way with no
     * update counts. In auto-commit mode, a batch that left a transaction
     * open on the server (it carried its own BEGIN) is rolled back so the
     * connection is not left inside a failed transaction; in manual-commit
     * mode the transaction is the application's to {@code rollback()}.
     */
    int[] executeBatch(java.util.List<String> sqls) throws SQLException {
        if (pgCatalogRewrite && sqls != null && !sqls.isEmpty()) {
            java.util.List<String> adapted = new java.util.ArrayList<>(sqls.size());
            for (String s : sqls) adapted.add(PgCatalogAdapter.rewrite(s));
            sqls = adapted;
        }
        if (!autoCommit && !serverTxnStatus() && sqls != null) {
            // Eager protocol: the same driver-side DDL refusal as sendOne.
            for (String q : sqls) {
                if (looksLikeDdl(q)) {
                    throw new java.sql.BatchUpdateException(ddlInsideTransaction().getMessage(),
                            "25001", 0, new int[0], ddlInsideTransaction());
                }
            }
        }
        // The lazy BEGIN of a manual-commit batch: through the guarded helper
        // (an outcome-unknown BEGIN closes the connection).
        if (needsBegin()) sendTxnControl("BEGIN");
        StreamState.StreamResult r;
        try {
            requestsSent.incrementAndGet();
            PdbStatement owner = EXECUTING.get();
            r = await(mux.batch(sqls), owner == null ? DEFAULT_TIMEOUT_SECONDS : owner.getQueryTimeout());
        } catch (Exception e) {
            throw batchFailure(e);
        }
        if (r.kind != StreamState.StreamResult.Kind.OK) {
            throw new SQLException("protocol error: batch answered with a "
                    + r.kind + " result", PdbSqlState.PROTOCOL_VIOLATION);
        }
        long[] counts;
        try {
            counts = WireV2Codec.decodeBatchResult(r.payload).counts;
        } catch (IllegalArgumentException e) {
            throw new SQLException("protocol error: " + e.getMessage(),
                    PdbSqlState.PROTOCOL_VIOLATION, e);
        }
        return toUpdateCounts(counts);
    }

    private static int[] toUpdateCounts(long[] counts) {
        int[] out = new int[counts.length];
        for (int i = 0; i < counts.length; i++) {
            long c = counts[i];
            out[i] = (c < 0 || c > Integer.MAX_VALUE) ? java.sql.Statement.SUCCESS_NO_INFO : (int) c;
        }
        return out;
    }

    /** Map a failed batch to the exception {@link #executeBatch} raises. */
    private SQLException batchFailure(Exception e) {
        Throwable c = e;
        if (!(c instanceof StreamState.PdbWireException) && c.getCause() != null) c = c.getCause();
        if (!(c instanceof StreamState.PdbWireException)) {
            // Timeout, interrupt, closed connection: not a statement failure.
            return mapException(e);
        }
        SQLException mapped = mapException((StreamState.PdbWireException) c);
        int[] done = (c instanceof StreamState.PdbBatchException)
                ? toUpdateCounts(((StreamState.PdbBatchException) c).completedCounts)
                : new int[0];
        // The entry's own SQLSTATE (CAP_EXT_BATCH_SQLSTATE) wins: the legacy
        // entry had none, and every batch failure read as HY000.
        String state = (c instanceof StreamState.PdbBatchException
                && ((StreamState.PdbBatchException) c).sqlState != null)
                ? ((StreamState.PdbBatchException) c).sqlState : mapped.getSQLState();
        java.sql.BatchUpdateException bue = new java.sql.BatchUpdateException(
                mapped.getMessage(), state, mapped.getErrorCode(), done, mapped);
        if (autoCommit && serverTxnStatus() && serverTxnActive()) {
            try {
                sendTxnControl("ROLLBACK");
            } catch (SQLException rb) {
                bue.addSuppressed(rb);
            }
        }
        return bue;
    }

    /**
     * Turn a wire failure into the {@link SQLException} the caller sees.
     *
     * <p>The SQLSTATE comes from {@link PdbSqlState}: JDBC error handling keys
     * on {@link SQLException#getSQLState()}, and this used to pass {@code null},
     * so a duplicate key (23505) and a syntax error (42601) were
     * indistinguishable to Spring / Hibernate / Flyway and to any
     * retry-on-40001 loop. The numeric wire code stays as the vendor code.
     *
     * <p>An error the SERVER raised is rendered the way pgjdbc renders a
     * PostgreSQL {@code ErrorResponse} ({@code "ERROR: <message>"}, SQLSTATE
     * recovered from the server's {@code [XXXXX]} marker); applications written
     * against pgjdbc match on that text. Driver-synthesized failures keep the
     * {@code "PDB wire error N: …"} form.
     */
    static SQLException mapException(Exception e) {
        if (e instanceof SQLException) return (SQLException) e;
        if (e.getCause() instanceof SQLException) return (SQLException) e.getCause();
        StreamState.PdbWireException w = null;
        if (e instanceof StreamState.PdbWireException) {
            w = (StreamState.PdbWireException) e;
        } else if (e.getCause() instanceof StreamState.PdbWireException) {
            w = (StreamState.PdbWireException) e.getCause();
        }
        if (w != null) {
            if (w.fromServer) {
                return PdbSqlState.serverErrorToSqlException(w.serverMessage, w.code, w);
            }
            return PdbSqlState.toSqlException(w.getMessage(), w.code, w);
        }
        if (e instanceof java.util.concurrent.TimeoutException) {
            return new java.sql.SQLTimeoutException("database request timed out", "57014", e);
        }
        if (e instanceof java.io.IOException || e.getCause() instanceof java.io.IOException) {
            return new java.sql.SQLNonTransientConnectionException("database connection failed", "08006", e);
        }
        return new SQLException("PDB JDBC: " + e.getMessage(), "HY000", e);
    }

    // ── Native queue  ───────────────────

    /**
     * The native queue on this connection: one {@code F_QUEUE} frame per op.
     * Ops join this connection's transaction exactly as statements do (the
     * lazy BEGIN rides the op when {@code autoCommit=false}); with
     * {@code autoCommit=true} each op is its own transaction on the server.
     * Reach it through a pool with {@code conn.unwrap(PdbConnection.class)}.
     */
    public PdbQueue queue() throws SQLException {
        ensureOpen();
        if (!mux.isV2()) {
            throw new java.sql.SQLFeatureNotSupportedException(
                    "the native queue needs the multiplexed (v2) native wire", "0A000");
        }
        return new PdbQueue(this);
    }

    /** Send one queue frame and wait for its result payload. */
    byte[] executeQueue(byte[] payload) throws SQLException {
        ensureOpen();
        try {
            requestsSent.incrementAndGet();
            StreamState.StreamResult r = await(mux.queue(payload, needsBegin()), DEFAULT_TIMEOUT_SECONDS);
            if (r.kind != StreamState.StreamResult.Kind.QUEUE) {
                throw new SQLException("native queue: unexpected answer " + r.kind, "08P01");
            }
            return r.payload;
        } catch (SQLException se) {
            throw se;
        } catch (java.util.concurrent.TimeoutException te) {
            throw new java.sql.SQLTimeoutException(
                    "queue op timed out after " + DEFAULT_TIMEOUT_SECONDS + "s", "57014", te);
        } catch (Exception e) {
            StreamState.PdbWireException w = e instanceof StreamState.PdbWireException
                    ? (StreamState.PdbWireException) e
                    : (e.getCause() instanceof StreamState.PdbWireException
                            ? (StreamState.PdbWireException) e.getCause() : null);
            if (w != null && w.code == 2) {
                throw new java.sql.SQLFeatureNotSupportedException(
                        "this server has no native queue (F_QUEUE unsupported)", "0A000", w);
            }
            throw mapException(e);
        }
    }

    // ── What this connection negotiated ───────────────────────────────────

    /**
     * The protocol the handshake selected, named as the server's
     * {@code SHOW pdb.wire_protocol} names it ({@code pdb-native-v2} on a
     * default connect). The conformance suite compares the two.
     */
    public String negotiatedProtocol() throws SQLException {
        ensureOpen();
        return mux.helloOk().protocolName();
    }

    /**
     * The negotiated capabilities, named as the server's
     * {@code SHOW pdb.wire_capabilities} names them.
     */
    public java.util.SortedSet<String> negotiatedCapabilities() throws SQLException {
        ensureOpen();
        return mux.helloOk().capabilityNames();
    }

    /** The concurrent-stream cap the server advertised (callers past it wait for a slot). */
    public int maxConcurrentStreams() throws SQLException {
        ensureOpen();
        return (int) Math.min(Integer.MAX_VALUE, mux.maxConcurrentStreams());
    }

    // ── Native key-value frames ───────────────────────────────────────────
    //
    // The native wire's four KV frames: the SAME keyspace RESP serves, executed
    // by the same server bodies (kv_durable). Binary-safe keys and values. Not
    // transactional: a KV write commits on its own even inside an open
    // transaction (the server's rule, not the driver's). Superuser-only on a
    // server that requires authentication.

    /** {@code SET key value} (clears any TTL the key had, as RESP SET does). */
    public void kvSet(byte[] key, byte[] value) throws SQLException {
        byte[] p = new byte[8 + key.length + value.length];
        putLen(p, 0, key.length);
        System.arraycopy(key, 0, p, 4, key.length);
        putLen(p, 4 + key.length, value.length);
        System.arraycopy(value, 0, p, 8 + key.length, value.length);
        kv(WireV2Codec.MSG_KV_SET, p, StreamState.StreamResult.Kind.OK);
    }

    /** {@code GET key}: the value, or {@code null} for an absent (or expired) key. */
    public byte[] kvGet(byte[] key) throws SQLException {
        StreamState.StreamResult r = kv(WireV2Codec.MSG_KV_GET, keyPayload(key), null);
        if (r.kind == StreamState.StreamResult.Kind.KV_NIL) return null;
        if (r.kind != StreamState.StreamResult.Kind.KV_BULK) {
            throw new SQLException("KV_GET: unexpected answer " + r.kind, "08P01");
        }
        return bulkValue(r.payload);
    }

    /** {@code DEL key}: true when a key was removed. */
    public boolean kvDel(byte[] key) throws SQLException {
        return kv(WireV2Codec.MSG_KV_DEL, keyPayload(key), StreamState.StreamResult.Kind.OK).affectedRows() > 0;
    }

    /** {@code INCR key} (by one; an absent key counts from 0): the new value. */
    public long kvIncr(byte[] key) throws SQLException {
        StreamState.StreamResult r = kv(WireV2Codec.MSG_KV_INCR, keyPayload(key), StreamState.StreamResult.Kind.KV_BULK);
        String v = new String(bulkValue(r.payload), java.nio.charset.StandardCharsets.US_ASCII);
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("KV_INCR: non-integer answer '" + v + "'", "08P01");
        }
    }

    private static byte[] keyPayload(byte[] key) {
        byte[] p = new byte[4 + key.length];
        putLen(p, 0, key.length);
        System.arraycopy(key, 0, p, 4, key.length);
        return p;
    }

    private static void putLen(byte[] b, int off, int v) {
        b[off] = (byte) v;
        b[off + 1] = (byte) (v >>> 8);
        b[off + 2] = (byte) (v >>> 16);
        b[off + 3] = (byte) (v >>> 24);
    }

    private static byte[] bulkValue(byte[] payload) throws SQLException {
        if (payload.length < 4) throw new SQLException("KV: truncated bulk answer", "08P01");
        int n = (payload[0] & 0xFF) | (payload[1] & 0xFF) << 8 | (payload[2] & 0xFF) << 16 | (payload[3] & 0xFF) << 24;
        if (n < 0 || 4 + n > payload.length) throw new SQLException("KV: bulk length " + n + " past the frame", "08P01");
        return java.util.Arrays.copyOfRange(payload, 4, 4 + n);
    }

    private StreamState.StreamResult kv(byte type, byte[] payload, StreamState.StreamResult.Kind want)
            throws SQLException {
        ensureOpen();
        try {
            requestsSent.incrementAndGet();
            StreamState.StreamResult r = await(mux.kv(type, payload), DEFAULT_TIMEOUT_SECONDS);
            if (want != null && r.kind != want) {
                throw new SQLException("KV: unexpected answer " + r.kind + " (expected " + want + ")", "08P01");
            }
            return r;
        } catch (SQLException se) {
            throw se;
        } catch (java.util.concurrent.TimeoutException te) {
            throw new java.sql.SQLTimeoutException(
                    "KV op timed out after " + DEFAULT_TIMEOUT_SECONDS + "s", "57014", te);
        } catch (Exception e) {
            throw mapException(e);
        }
    }

    // ── Native bulk ingest  ────────────────

    /**
     * Load every batch {@code reader} yields into {@code table} as ONE atomic
     * native bulk ingest (Arrow over the native wire). The Arrow field names
     * must be the table's column names; the server converts each column to
     * its declared type (a Utf8 column is parsed with the type's input
     * function). With {@code autoCommit=false} the load runs inside the
     * connection's transaction (visible to others only after
     * {@code commit()}); otherwise it commits on its own. Returns the rows
     * loaded. The reader is not closed.
     *
     * @throws SQLFeatureNotSupportedException (0A000) when the server has no
     *         native bulk ingest
     */
    public long bulkLoad(String table, org.apache.arrow.vector.ipc.ArrowReader reader) throws SQLException {
        return bulkLoad(table, reader, 0L);
    }

    /**
     * {@link #bulkLoad(String, org.apache.arrow.vector.ipc.ArrowReader)} that,
     * with {@code commitEveryRows > 0}, commits every that many rows on its own
     * (autocommit only; the load is then NOT atomic as a whole).
     */
    public long bulkLoad(String table, org.apache.arrow.vector.ipc.ArrowReader reader, long commitEveryRows)
            throws SQLException {
        return ArrowBulk.load(this, table, reader, commitEveryRows);
    }

    /**
     * Load {@code root}'s rows into {@code table} (one atomic load). When
     * {@code columns} is non-null its i-th name is the target column of the
     * root's i-th vector; when null the vectors' own names are used.
     */
    public long bulkLoad(String table, java.util.List<String> columns,
                         org.apache.arrow.vector.VectorSchemaRoot root) throws SQLException {
        return ArrowBulk.load(this, table, columns, java.util.Collections.singletonList(root), 0L);
    }

    /** {@link #bulkLoad(String, java.util.List, org.apache.arrow.vector.VectorSchemaRoot)} with {@code commitEveryRows}. */
    public long bulkLoad(String table, java.util.List<String> columns,
                         org.apache.arrow.vector.VectorSchemaRoot root, long commitEveryRows) throws SQLException {
        return ArrowBulk.load(this, table, columns, java.util.Collections.singletonList(root), commitEveryRows);
    }

    /**
     * Load every root of {@code roots} (same columns each) into {@code table}
     * as one load. {@code columns} as in
     * {@link #bulkLoad(String, java.util.List, org.apache.arrow.vector.VectorSchemaRoot)}.
     */
    public long bulkLoad(String table, java.util.List<String> columns,
                         Iterable<org.apache.arrow.vector.VectorSchemaRoot> roots, long commitEveryRows)
            throws SQLException {
        return ArrowBulk.load(this, table, columns, roots, commitEveryRows);
    }

    long runBulk(String table, java.util.List<String> cols, long commitEveryRows, BulkIngest.Body body)
            throws SQLException {
        ensureOpen();
        if (!mux.supportsBulkIngest() || bulkRefused) {
            throw new SQLFeatureNotSupportedException(
                    "this server has no native bulk ingest (F_BULK not advertised)", "0A000");
        }
        // The lazy BEGIN of a manual-commit load (F_BULK carries no BEGIN flag).
        if (needsBegin()) sendTxnControl("BEGIN");
        requestsSent.incrementAndGet();
        try {
            return BulkIngest.run(this, table, cols, commitEveryRows, body);
        } catch (BulkIngest.Unsupported u) {
            bulkRefused = true;
            throw new SQLFeatureNotSupportedException(u.getMessage(), "0A000", u.getCause());
        }
    }

    /** Wait for one bulk request's answer (the same timeout handling as statements). */
    StreamState.StreamResult awaitBulk(java.util.concurrent.Future<StreamState.StreamResult> f, int timeoutSeconds)
            throws Exception {
        return await(f, timeoutSeconds);
    }

    /** {@code executeBatch} of a plain INSERT may go through native bulk ingest. */
    boolean bulkBatchEnabled() {
        return bulkBatch && !bulkRefused && mux.supportsBulkIngest();
    }

    /**
     * {@code executeBatch} of a plain {@code INSERT ... VALUES (?, ...)}: load
     * {@code rows} (bound values, one array per row) as ONE native bulk ingest.
     * Returns false -- nothing sent -- when the batch must take the text path
     * instead (a value the bulk path does not carry, an unknown column list,
     * a server that refuses {@code F_BULK}).
     */
    boolean bulkInsertRows(BulkIngest.Insert ins, java.util.List<Object[]> rows) throws SQLException {
        if (!bulkBatchEnabled()) return false;
        java.util.List<String> cols = ins.columns;
        if (cols == null) {
            java.util.List<String> all = bulkColumns.get(ins.relation);
            if (all == null) {
                all = new java.util.ArrayList<>();
                try (Statement st = createStatement();
                     java.sql.ResultSet rs = st.executeQuery("SELECT * FROM " + ins.relation + " LIMIT 0")) {
                    java.sql.ResultSetMetaData md = rs.getMetaData();
                    for (int i = 1; i <= md.getColumnCount(); i++) all.add(md.getColumnName(i));
                } catch (SQLException e) {
                    return false;
                }
                if (bulkColumns.size() >= MAX_TEMPLATES) bulkColumns.clear();
                bulkColumns.put(ins.relation, all);
            }
            if (ins.values > all.size()) return false;
            cols = all.subList(0, ins.values);
        }
        byte[][][] text = BulkIngest.rowsToText(cols, rows);
        if (text == null) return false;
        final java.util.List<String> names = cols;
        long loaded;
        try {
            loaded = runBulk(ins.relation, cols, 0L, ing -> ing.sendText(names, text, rows.size()));
        } catch (SQLFeatureNotSupportedException e) {
            return false;
        } catch (SQLException e) {
            if (ins.columns == null && "42703".equals(e.getSQLState())) bulkColumns.remove(ins.relation);
            throw e;
        }
        if (loaded != rows.size()) {
            throw new SQLException("protocol error: bulk ingest loaded " + loaded + " rows of "
                    + rows.size(), PdbSqlState.PROTOCOL_VIOLATION);
        }
        bulkBatches.incrementAndGet();
        return true;
    }

    // ── JDBC API ──────────────────────────────────────────────────────────

    @Override public Statement createStatement() throws SQLException {
        ensureOpen();
        return new PdbStatement(this);
    }
    @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
        ensureOpen();
        return new PdbPreparedStatement(this, sql);
    }
    @Override public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement v1.1");
    }
    @Override public String nativeSQL(String sql) { return Pgcompat.unescape(sql); }

    @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpen();
        if (this.autoCommit == autoCommit) return;
        this.autoCommit = autoCommit;
        if (!serverTxnStatus()) {
            if (autoCommit) {
                // The transaction ends either way (a raised silent rollback
                // included); autoCommit=true needs none open. An outcome-unknown
                // failure closes the connection (`sendTxnControl`).
                sendTxnControl("COMMIT");
            } else {
                try {
                    sendTxnControl("BEGIN");
                } catch (SQLException e) {
                    // The SERVER refused the BEGIN (the connection is still
                    // open): no transaction exists, so do not claim
                    // manual-commit mode. An outcome-unknown failure has
                    // closed the connection instead.
                    if (!isClosed()) this.autoCommit = true;
                    throw e;
                }
            }
            return;
        }
        // JDBC: switching autocommit ON mid-transaction commits it. Switching it
        // OFF sends nothing -- the next statement opens the transaction.
        if (autoCommit && serverTxnActive()) sendTxnControl("COMMIT");
    }
    @Override public boolean getAutoCommit() { return autoCommit; }
    /**
     * {@inheritDoc}
     *
     * <p>Calling this in auto-commit mode is an ERROR per the JDBC contract, not
     * a no-op. Silently ignoring it lets code that believes it is running a
     * transaction ("… ; conn.commit();" with autoCommit accidentally left true)
     * run to completion and report success, when in fact every statement
     * committed on its own and no rollback point ever existed.
     */
    @Override public void commit() throws SQLException {
        ensureOpen();
        if (autoCommit) {
            throw new SQLException(
                    "commit() called while autoCommit is true", "25000");
        }
        if (!serverTxnStatus()) {
            SQLException pending = null;
            try {
                sendTxnControl("COMMIT");
            } catch (SQLException e) {
                pending = e;
            }
            reopenEagerTxn(pending);
            return;
        }
        // The server says whether there is anything to end.
        try {
            if (serverTxnActive()) sendTxnControl("COMMIT");
        } finally {
            lastStatementFailure = null;
        }
    }
    /** @see #commit() - same contract: an error in auto-commit mode. */
    @Override public void rollback() throws SQLException {
        ensureOpen();
        if (autoCommit) {
            throw new SQLException(
                    "rollback() called while autoCommit is true", "25000");
        }
        if (!serverTxnStatus()) {
            SQLException pending = null;
            try {
                sendTxnControl("ROLLBACK");
            } catch (SQLException e) {
                pending = e;
            }
            reopenEagerTxn(pending);
            return;
        }
        try {
            if (serverTxnActive()) sendTxnControl("ROLLBACK");
        } finally {
            lastStatementFailure = null;
        }
    }

    /**
     * Closes this connection. Idempotent and safe to call from any thread (the
     * JDBC {@link #abort(Executor)} path also routes here). Only the per-
     * connection channel and its streams are released; the shared event-loop
     * group is NOT shut down here - it is owned by {@link SharedEventLoop} and
     * torn down once at JVM exit.
     */
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        // mux.close() was constructed with ownGroup=false, so it tears down the
        // channel + open streams only and leaves the shared group running.
        try { mux.close(); } catch (Exception ignored) {}
    }
    /**
     * True after {@link #close()}, and also once the socket itself is gone (a
     * transport failure, GOAWAY, or a timeout -- see {@link #closeAfterTimeout}):
     * such a connection can never answer again, and a pool must not keep it.
     */
    @Override public boolean isClosed() { return closed || mux.isClosed(); }

    @Override public DatabaseMetaData getMetaData() { return new PdbDatabaseMetaData(this); }
    /**
     * {@inheritDoc}
     *
     * <p>Actually tells the SERVER, via
     * {@code SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY|READ WRITE}.
     * Storing the flag client-side and reporting it back from
     * {@link #isReadOnly()} - what this used to do - means a caller that opens
     * a "read-only" connection (a reporting pool, a replica-routing layer) gets
     * a fully writable one while every check it can make says read-only.
     */
    @Override public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpen();
        if (this.readOnly == readOnly) return;
        sendControl("SET SESSION CHARACTERISTICS AS TRANSACTION "
                + (readOnly ? "READ ONLY" : "READ WRITE"));
        this.readOnly = readOnly;
    }
    @Override public boolean isReadOnly() { return readOnly; }
    @Override public void setCatalog(String c) throws SQLException {
        ensureOpen();
        if (c != null && !c.equals(parsed.database)) {
            throw new SQLFeatureNotSupportedException("catalog switching requires a new connection", "0A000");
        }
    }
    @Override public String getCatalog() throws SQLException { ensureOpen(); return parsed.database; }

    /**
     * {@inheritDoc}
     *
     * <p>Actually tells the SERVER, via
     * {@code SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL …}, and
     * only records the new level once the server has accepted it. Storing the
     * int and handing it straight back from
     * {@link #getTransactionIsolation()} - what this used to do - is the worst
     * possible answer for an isolation setting: an application that asks for
     * SERIALIZABLE, verifies it read back as SERIALIZABLE, and then relies on
     * it ran at the server's default the whole time, and no check available to
     * it could tell.
     *
     * <p>An unknown level, and {@link Connection#TRANSACTION_NONE} (which the
     * JDBC javadoc explicitly excludes here), are rejected rather than stored.
     */
    @Override public void setTransactionIsolation(int level) throws SQLException {
        ensureOpen();
        String sql = isolationLevelSql(level);
        if (sql == null) {
            throw new SQLException("unsupported transaction isolation level: " + level,
                    "0A000");
        }
        if (this.txIsolationLevel == level) return;
        // A session default: it applies to the NEXT transaction, which with
        // lazy BEGIN is the one the next statement opens.
        sendControl("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + sql);
        this.txIsolationLevel = level;
    }

    /** SQL spelling for a JDBC isolation constant, or null when there is none. */
    static String isolationLevelSql(int level) {
        switch (level) {
            case TRANSACTION_READ_UNCOMMITTED: return "READ UNCOMMITTED";
            case TRANSACTION_READ_COMMITTED:   return "READ COMMITTED";
            case TRANSACTION_REPEATABLE_READ:  return "REPEATABLE READ";
            case TRANSACTION_SERIALIZABLE:     return "SERIALIZABLE";
            // TRANSACTION_NONE means "no transactions" and is explicitly not a
            // legal argument to setTransactionIsolation.
            default: return null;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Until the level has been set through this connection, it is READ from
     * the server ({@code SHOW transaction_isolation}) once and cached; the
     * server's default depends on how it was started, so no client-side
     * default can be right.
     */
    @Override public int getTransactionIsolation() throws SQLException {
        ensureOpen();
        if (txIsolationLevel == ISOLATION_UNKNOWN) {
            txIsolationLevel = readServerIsolation();
        }
        return txIsolationLevel;
    }

    /**
     * The server's answer to {@code SHOW transaction_isolation}. Sent as a
     * control request: it never opens the lazy transaction.
     */
    private int readServerIsolation() throws SQLException {
        StreamState.StreamResult r;
        try {
            requestsSent.incrementAndGet();
            r = await(mux.query("SHOW transaction_isolation"), DEFAULT_TIMEOUT_SECONDS);
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            throw mapException(e);
        }
        try (PdbResultSet rs = new PdbResultSet(r, resultDecoder)) {
            if (rs.next()) {
                int level = isolationFromLabel(rs.getString(1));
                if (level != ISOLATION_UNKNOWN) return level;
            }
        }
        throw new SQLException("server did not report a transaction isolation level", "08P01");
    }

    /** JDBC constant for a PostgreSQL isolation label, or {@link #ISOLATION_UNKNOWN}. */
    static int isolationFromLabel(String label) {
        if (label == null) return ISOLATION_UNKNOWN;
        switch (label.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "read uncommitted": return TRANSACTION_READ_UNCOMMITTED;
            case "read committed":   return TRANSACTION_READ_COMMITTED;
            case "repeatable read":  return TRANSACTION_REPEATABLE_READ;
            case "serializable":     return TRANSACTION_SERIALIZABLE;
            default:                 return ISOLATION_UNKNOWN;
        }
    }

    /** Warnings on this connection (JDBC chain), oldest first. */
    private SQLWarning warnings;

    /** SQLSTATE 25001 active_sql_transaction, as PostgreSQL raises for its own
     *  non-transactional commands inside a transaction block. */
    static SQLException ddlInsideTransaction() {
        return new SQLException(
                "25001: DDL cannot run inside a transaction block: ParticleDB applies schema "
                + "changes immediately and cannot roll them back. Commit or roll back the "
                + "open transaction first, or run the DDL with autoCommit=true",
                "25001");
    }

    private synchronized void addWarning(SQLWarning w) {
        if (warnings == null) warnings = w; else warnings.setNextWarning(w);
    }

    /** {@code allowDdlOutsideTransaction=true}: a DDL ran outside the transaction. */
    static SQLWarning ddlOutsideTransactionWarning() {
        return new SQLWarning(
                "DDL ran outside a transaction (allowDdlOutsideTransaction=true): ParticleDB "
                + "applies schema changes immediately and a later rollback() will not undo it",
                "01000");
    }

    @Override public synchronized SQLWarning getWarnings() throws SQLException {
        ensureOpen();
        return warnings;
    }
    @Override public synchronized void clearWarnings() { warnings = null; }

    @Override public Statement createStatement(int rsType, int rsConcurrency) throws SQLException {
        return createStatement();
    }
    @Override public PreparedStatement prepareStatement(String sql, int rsType, int rsConcurrency) throws SQLException {
        return prepareStatement(sql);
    }
    @Override public CallableStatement prepareCall(String sql, int rsType, int rsConcurrency) throws SQLException {
        return prepareCall(sql);
    }

    @Override public Map<String, Class<?>> getTypeMap() { return java.util.Collections.emptyMap(); }
    @Override public void setTypeMap(Map<String, Class<?>> map) {}
    @Override public void setHoldability(int h) throws SQLException {
        ensureOpen();
        if (h != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("only HOLD_CURSORS_OVER_COMMIT is supported", "0A000");
        }
    }
    @Override public int getHoldability() { return 1; }
    // ── Savepoints: the server's SAVEPOINT / ROLLBACK TO / RELEASE ──────────
    //
    // Sent as SQL inside the transaction (the lazy BEGIN rides the first one,
    // as for any statement). pgjdbc's rules: refused in auto-commit mode; an
    // unnamed savepoint gets a generated name and an id; a savepoint object
    // belongs to the connection that made it.

    private final java.util.concurrent.atomic.AtomicInteger savepointSeq = new java.util.concurrent.atomic.AtomicInteger();

    final class PdbSavepoint implements Savepoint {
        final int id;      // > 0 for an unnamed savepoint, 0 for a named one
        final String name; // the SQL identifier sent
        PdbSavepoint(int id, String name) { this.id = id; this.name = name; }
        PdbConnection owner() { return PdbConnection.this; }
        @Override public int getSavepointId() throws SQLException {
            if (id == 0) throw new SQLException("a named savepoint has no id", "3B001");
            return id;
        }
        @Override public String getSavepointName() throws SQLException {
            if (id != 0) throw new SQLException("an unnamed savepoint has no name", "3B001");
            return name;
        }
    }

    private static String quoteIdent(String n) {
        return '"' + n.replace("\"", "\"\"") + '"';
    }

    private PdbSavepoint own(Savepoint sp) throws SQLException {
        if (!(sp instanceof PdbSavepoint) || ((PdbSavepoint) sp).owner() != this) {
            throw new SQLException("savepoint does not belong to this connection", "3B001");
        }
        return (PdbSavepoint) sp;
    }

    private void requireTxnForSavepoint() throws SQLException {
        ensureOpen();
        if (autoCommit) throw new SQLException("Cannot establish a savepoint in auto-commit mode.", "25P01");
    }

    @Override public Savepoint setSavepoint() throws SQLException {
        requireTxnForSavepoint();
        int id = savepointSeq.incrementAndGet();
        PdbSavepoint sp = new PdbSavepoint(id, "JDBC_SAVEPOINT_" + id);
        executeRaw("SAVEPOINT " + quoteIdent(sp.name));
        return sp;
    }
    @Override public Savepoint setSavepoint(String name) throws SQLException {
        requireTxnForSavepoint();
        if (name == null || name.isEmpty()) throw new SQLException("savepoint name must not be empty", "3B001");
        PdbSavepoint sp = new PdbSavepoint(0, name);
        executeRaw("SAVEPOINT " + quoteIdent(name));
        return sp;
    }
    @Override public void rollback(Savepoint savepoint) throws SQLException {
        requireTxnForSavepoint();
        executeRaw("ROLLBACK TO SAVEPOINT " + quoteIdent(own(savepoint).name));
    }
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        requireTxnForSavepoint();
        executeRaw("RELEASE SAVEPOINT " + quoteIdent(own(savepoint).name));
    }

    @Override public Statement createStatement(int a, int b, int c) throws SQLException { return createStatement(); }
    @Override public PreparedStatement prepareStatement(String s, int a, int b, int c) throws SQLException { return prepareStatement(s); }
    @Override public CallableStatement prepareCall(String s, int a, int b, int c) throws SQLException { return prepareCall(s); }
    @Override public PreparedStatement prepareStatement(String s, int autogen) throws SQLException { return prepareStatement(s); }
    @Override public PreparedStatement prepareStatement(String s, int[] colIdx) throws SQLException { return prepareStatement(s); }
    @Override public PreparedStatement prepareStatement(String s, String[] colNames) throws SQLException { return prepareStatement(s); }

    @Override public Clob createClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob createBlob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob createNClob() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML createSQLXML() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) throw new SQLException("validation timeout must be nonnegative", "22023");
        if (isClosed()) return false;
        try {
            var probe = mux.ping();
            long millis = timeout == 0 ? Long.MAX_VALUE : TimeUnit.SECONDS.toMillis(timeout);
            if (networkTimeoutMillis > 0) millis = Math.min(millis, networkTimeoutMillis);
            if (millis == Long.MAX_VALUE) probe.get();
            else probe.get(millis, TimeUnit.MILLISECONDS);
            return true;
        } catch (java.util.concurrent.TimeoutException te) {
            closeAfterTimeout();
            return false;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            closeAfterTimeout();
            return false;
        } catch (Exception e) { return false; }
    }
    @Override public void setClientInfo(String name, String value) { clientInfo.setProperty(name, value); }
    @Override public void setClientInfo(Properties properties) throws SQLClientInfoException { clientInfo.putAll(properties); }
    @Override public String getClientInfo(String name) { return clientInfo.getProperty(name); }
    @Override public Properties getClientInfo() { return new Properties(clientInfo); }
    /**
     * An SQL array of {@code type} (text / varchar, int2, int4, int8, float4,
     * float8, bool, numeric, uuid, timestamp, timestamptz, date, bytea, json,
     * jsonb and their PostgreSQL aliases). {@code elements} may nest
     * ({@code Object[]} of {@code Object[]}) for a rectangular
     * multidimensional array; null elements are SQL NULLs. Elements are
     * converted to {@code type} here. {@code PreparedStatement.setArray} sends
     * it as a typed native-wire array parameter, never as SQL text (unless the
     * URL set {@code serverPrepare=false}).
     *
     * @throws java.sql.SQLFeatureNotSupportedException {@code type} is not one an array parameter carries
     * @throws java.sql.SQLDataException an element does not convert to {@code type}, or the nesting is ragged
     */
    @Override public Array createArrayOf(String type, Object[] elements) throws SQLException {
        if (type == null) throw new SQLException("createArrayOf: type must not be null");
        PdbArray a = new PdbArray(type, elements);
        a.shape();
        return a;
    }
    @Override public Struct createStruct(String type, Object[] attrs) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    /**
     * {@inheritDoc}
     *
     * <p>Actually issues {@code SET search_path TO …}. Storing the name and
     * handing it back from {@link #getSchema()} left every subsequent
     * unqualified statement resolving in the DEFAULT schema while the
     * connection reported it was in the requested one - a multi-tenant router
     * that switches schema per request would have written every tenant's rows
     * into the same tables.
     */
    @Override public void setSchema(String s) throws SQLException {
        ensureOpen();
        if (s == null || s.isEmpty()) {
            throw new SQLException("setSchema: schema name must not be null or empty");
        }
        // Like pgjdbc (QUERY_SUPPRESS_BEGIN): a session setting does not open
        // the lazy transaction.
        sendControl("SET search_path TO " + quoteIdentifier(s));
        this.schema = s;
    }
    @Override public String getSchema() { return schema; }

    /** Double-quote a SQL identifier, doubling any embedded quote. */
    static String quoteIdentifier(String ident) {
        StringBuilder sb = new StringBuilder(ident.length() + 2).append('"');
        for (int i = 0; i < ident.length(); i++) {
            char c = ident.charAt(i);
            if (c == '"') sb.append('"');
            sb.append(c);
        }
        return sb.append('"').toString();
    }
    @Override public void abort(Executor executor) throws SQLException {
        if (isClosed()) return;
        if (executor == null) throw new SQLException("abort requires an executor", "22023");
        executor.execute(() -> { closed = true; mux.abort(); });
    }
    @Override public void setNetworkTimeout(Executor executor, int millis) throws SQLException {
        ensureOpen();
        if (executor == null || millis < 0) {
            throw new SQLException("network timeout requires an executor and nonnegative milliseconds", "22023");
        }
        networkTimeoutMillis = millis;
        mux.setNetworkTimeout(millis);
    }
    @Override public int getNetworkTimeout() throws SQLException {
        ensureOpen();
        return networkTimeoutMillis;
    }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private void ensureOpen() throws SQLException {
        // 08003 connection_does_not_exist - using a closed connection.
        if (isClosed()) throw new java.sql.SQLNonTransientConnectionException(
                "connection closed", "08003");
    }
}
