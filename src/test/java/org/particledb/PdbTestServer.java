package org.particledb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * Test helper: starts a {@code particledb} binary on a free port for the
 * duration of one test class.
 *
 * <p>By default, skipped (returns null) when no binary is available, so an
 * ordinary developer build does not need one. Set {@code PDB_BIN} to the
 * server you want tested; failing that, an installed {@code particledb}
 * (on {@code PATH} or in {@code /usr/local/bin}) is used as a convenience.
 *
 * <p><b>Under {@code -Dpdb.requireServer=true} (which the {@code release}
 * profile sets) this class REFUSES to skip.</b> It throws unless {@code PDB_BIN}
 * names an executable - a guessed path is not good enough, because a guess is
 * how a release gets attested against a binary older than the commit being
 * released while every gated test still reports green.
 */
public final class PdbTestServer {

    public final Process process;
    public final int port;
    /** The same server's PostgreSQL-wire port (for pgjdbc-over-PDB checks). */
    public final int pgPort;
    public final File dataDir;

    private PdbTestServer(Process p, int port, int pgPort, File dataDir) {
        this.process = p;
        this.port = port;
        this.pgPort = pgPort;
        this.dataDir = dataDir;
    }

    /**
     * True when the build DEMANDS a live server instead of tolerating a skip.
     * Set by the `release` profile via {@code -Dpdb.requireServer}.
     */
    private static boolean requireServer() {
        String v = System.getProperty("pdb.requireServer");
        return v != null && v.equalsIgnoreCase("true");
    }

    /**
     * An EXPLICITLY configured binary - the {@code PDB_BIN} env var, or the
     * same name passed through as a surefire system property. Maven leaves the
     * literal {@code ${env.PDB_BIN}} placeholder when the env var is unset, so
     * a value still containing {@code ${} counts as absent.
     */
    private static String explicitBin() {
        String v = System.getenv("PDB_BIN");
        if (v == null || v.isEmpty()) {
            v = System.getProperty("PDB_BIN");
            if (v != null && v.contains("${")) v = null;
        }
        return (v == null || v.isEmpty()) ? null : v;
    }

    /** Make the attestation visible: what binary ran, and what it claims to be. */
    private static void announce(String bin, boolean explicit) {
        String version = "unknown";
        try {
            Process p = new ProcessBuilder(bin, "--version").redirectErrorStream(true).start();
            try (java.io.BufferedReader r = new java.io.BufferedReader(
                    new java.io.InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.startsWith("particledb ")) version = line.trim();
                }
            }
            p.waitFor();
        } catch (Exception ignored) {
            // Version reporting is diagnostic; never fail a run over it.
        }
        System.err.println("[PdbTestServer] server binary: " + bin
                + (explicit ? " (explicit PDB_BIN)" : " (DISCOVERED — not pinned to any commit)")
                + " version=" + version);
    }

    public static PdbTestServer tryStart() throws IOException, InterruptedException {
        return tryStart(new String[0]);
    }

    /**
     * {@link #tryStart()} with extra {@code particledb start} arguments, e.g.
     * {@code "--transactional-ddl", "off"} for a suite that pins the server's
     * pre-default behaviour (every DDL inside a transaction refused 25001).
     */
    public static PdbTestServer tryStart(String... extraServerArgs) throws IOException, InterruptedException {
        boolean require = requireServer();
        String bin = explicitBin();
        boolean explicit = bin != null;

        if (explicit) {
            // An explicitly configured PDB_BIN that does not exist used to reach
            // ProcessBuilder.start() and blow up with an IOException, so the
            // whole class ERRORED where it was supposed to SKIP - 14 classes
            // reported failures that were really "no binary here".
            if (!new File(bin).canExecute()) {
                if (require) {
                    throw new IllegalStateException(
                            "pdb.requireServer=true but PDB_BIN is not executable: " + bin);
                }
                System.err.println("[PdbTestServer] PDB_BIN is not executable: " + bin);
                return null;
            }
        } else {
            // No explicit binary. Under `require` we refuse to GUESS one.
            //
            // A guessed path is exactly how a release comes to be attested
            // against the wrong server: this method used to fall back to one
            // developer's absolute checkout path, and then to an INSTALLED
            // /usr/local/bin copy - either of which can be months older than
            // the commit being released, while every test still reports green.
            if (require) {
                throw new IllegalStateException(
                        "pdb.requireServer=true but PDB_BIN is not set.\n"
                        + "A release must be tested against a server built from the commit "
                        + "being released: build it, then set PDB_BIN to that binary.\n"
                        + "Most of this suite is gated on a live server; with no binary "
                        + "present those classes all SKIP and the build still reports "
                        + "success — which is an attestation of nothing.");
            }
            // Dev convenience only: an installed particledb (PATH first).
            bin = installedParticledb();
        }
        if (bin == null) return null;
        announce(bin, explicit);

        int pdbPort = findFreePort();
        int pgPort = findFreePort();
        int grpcPort = findFreePort();
        int httpPort = findFreePort();
        int metricsPort = findFreePort();
        int healthPort = findFreePort();
        File dataDir = Files.createTempDirectory("pdb-jdbc-test").toFile();
        File config = new File(dataDir, "config.toml");
        try (FileWriter w = new FileWriter(config)) {
            w.write("data_dir = \"" + dataDir.getAbsolutePath() + "\"\n\n");
            w.write("[storage]\n\n");
            w.write("[wal]\n\n");
            w.write("[raft]\n\n");
            w.write("[cluster]\n\n");
            w.write("[metrics]\n\n");
            w.write("[security]\n\n");
            w.write("[network]\n");
            w.write("pdb_wire_port = " + pdbPort + "\n");
            w.write("pg_addr = \"127.0.0.1:" + pgPort + "\"\n");
            w.write("grpc_addr = \"127.0.0.1:" + grpcPort + "\"\n");
            w.write("http_addr = \"127.0.0.1:" + httpPort + "\"\n");
            w.write("metrics_port = " + metricsPort + "\n");
            w.write("health_port = " + healthPort + "\n");
        }

        java.util.List<String> cmd = new java.util.ArrayList<>(java.util.Arrays.asList(
                bin,
                "start",
                "--no-auth",
                "--no-console",
                "--redis-addr", "127.0.0.1:" + findFreePort(),
                "--mysql-addr", "127.0.0.1:" + findFreePort(),
                "--concurrency-control", "optimistic",
                "--config", config.getAbsolutePath(),
                "--data-dir", dataDir.getAbsolutePath()
        ));
        cmd.addAll(java.util.Arrays.asList(extraServerArgs));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(new File(dataDir, "server.log"));
        Process p = pb.start();

        // Wait until the PDB native wire port accepts a connection (max 20s).
        //
        // "The port accepts" is NOT the same as "OUR server is on it".
        // findFreePort() closes its probe socket before the child binds, so
        // another process can take the port in between - and this machine runs
        // long-lived FOREIGN ParticleDB servers, so a harness that trusts mere
        // reachability can hand the whole suite to somebody else's database and
        // report green. Every accepted connection is therefore checked against
        // the child's own PID before it is used.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            try (Socket s = new Socket("127.0.0.1", pdbPort)) {
                assertPortBelongsTo(p, pdbPort);
                assertUnixSocketBelongsTo(p, pdbPort);
                return new PdbTestServer(p, pdbPort, pgPort, dataDir);
            } catch (IOException ignored) {
                Thread.sleep(250);
            }
            if (!p.isAlive()) {
                System.err.println("[PdbTestServer] particledb exited during startup");
                return null;
            }
        }
        p.destroyForcibly();
        return null;
    }

    /**
     * Fail LOUDLY unless {@code port} is held by {@code child} (or one of its
     * descendants). Uses {@code lsof}, which exists on macOS and Linux; when it
     * cannot be run at all the check degrades to the pre-bind probe in
     * {@link #findFreePort()} plus child liveness, and says so on stderr rather
     * than passing silently.
     */
    private static void assertPortBelongsTo(Process child, int port) {
        long childPid;
        try {
            childPid = child.pid();
        } catch (UnsupportedOperationException e) {
            return;
        }
        java.util.Set<String> owners = listeningPids(port);
        if (owners == null) {
            System.err.println("[PdbTestServer] WARNING: lsof unavailable — cannot "
                    + "prove port " + port + " belongs to pid " + childPid);
            return;
        }
        if (owners.isEmpty()) {
            // Raced with the accept; the child is alive, so leave it.
            return;
        }
        if (owners.contains(Long.toString(childPid))) return;
        // A descendant may hold the socket if the binary re-execs.
        java.util.Optional<ProcessHandle> h = ProcessHandle.of(childPid);
        if (h.isPresent() && h.get().descendants()
                .anyMatch(d -> owners.contains(Long.toString(d.pid())))) {
            return;
        }
        child.destroyForcibly();
        throw new IllegalStateException(
                "PdbTestServer: port " + port + " is held by pid(s) " + owners
                + ", not by the server we started (pid " + childPid + "). "
                + "REFUSING to run the suite against a foreign server.");
    }

    /**
     * The driver connects to a loopback host over {@code /tmp/.s.PDB.<port>}
     * by default ({@code preferUnixSocket}), so the TCP check above no longer
     * proves which server the suite talks to: a FOREIGN server already holding
     * that path keeps it (the child refuses to take over an occupied socket,
     * {@code unix_sock.rs}), and every connection would go there. The server
     * writes {@code <socket>.lock} naming its PID beside each socket it binds;
     * wait for it (the socket is bound on a thread after the TCP port) and
     * refuse unless it names the child. A server started without Unix sockets
     * never writes one, and the driver then uses TCP.
     */
    private static void assertUnixSocketBelongsTo(Process child, int port) throws InterruptedException {
        if (System.getProperty("os.name").startsWith("Windows")) return;
        java.nio.file.Path lock = java.nio.file.Paths.get("/tmp", ".s.PDB." + port + ".lock");
        long childPid;
        try {
            childPid = child.pid();
        } catch (UnsupportedOperationException e) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        String owner = null;
        while (System.nanoTime() < deadline && child.isAlive()) {
            try {
                for (String line : Files.readAllLines(lock)) {
                    if (line.startsWith("pid=")) owner = line.substring(4).trim();
                }
            } catch (IOException notYet) {
                owner = null;
            }
            if (owner != null && owner.equals(Long.toString(childPid))) return;
            Thread.sleep(50);
        }
        if (owner == null) {
            System.err.println("[PdbTestServer] no " + lock + " from pid " + childPid
                    + " (Unix sockets disabled?) -- the driver will use TCP");
            return;
        }
        child.destroyForcibly();
        throw new IllegalStateException("PdbTestServer: " + lock + " names pid " + owner
                + ", not the server we started (pid " + childPid + "). A loopback"
                + " connection would reach that FOREIGN server over the Unix socket. REFUSING.");
    }

    /** PIDs listening on {@code port}, or null when lsof cannot be run. */
    private static java.util.Set<String> listeningPids(int port) {
        try {
            Process lsof = new ProcessBuilder(
                    "lsof", "-nP", "-iTCP:" + port, "-sTCP:LISTEN", "-t")
                    .redirectErrorStream(false).start();
            java.util.Set<String> pids = new java.util.LinkedHashSet<>();
            try (java.io.BufferedReader r = new java.io.BufferedReader(
                    new java.io.InputStreamReader(lsof.getInputStream()))) {
                String line;
                while ((line = r.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) pids.add(line);
                }
            }
            lsof.waitFor(5, TimeUnit.SECONDS);
            return pids;
        } catch (Exception e) {
            return null;
        }
    }

    public void stop() {
        if (process == null) return;
        process.destroy();
        try { process.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        if (process.isAlive()) process.destroyForcibly();
        try {
            deleteRecursive(dataDir);
        } catch (Exception ignored) {}
    }

    /**
     * An ephemeral port the OS says is free. The socket has to be released
     * before the child can bind it, so this is inherently a
     * time-of-check/time-of-use gap - see {@link #assertPortBelongsTo}, which
     * closes it after the fact.
     */
    /** An installed {@code particledb} on {@code PATH}, else {@code /usr/local/bin}; {@code null} if none. */
    private static String installedParticledb() {
        String path = System.getenv("PATH");
        if (path != null) {
            for (String dir : path.split(File.pathSeparator)) {
                if (dir.isEmpty()) continue;
                File f = new File(dir, "particledb");
                if (f.canExecute()) return f.getPath();
            }
        }
        File local = new File("/usr/local/bin/particledb");
        return local.canExecute() ? local.getPath() : null;
    }

    private static int findFreePort() throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void deleteRecursive(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            File[] kids = f.listFiles();
            if (kids != null) for (File k : kids) deleteRecursive(k);
        }
        f.delete();
    }
}
