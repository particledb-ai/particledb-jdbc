package org.particledb;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BEGIN / COMMIT / ROLLBACK decide the driver's transaction state, so they
 * have exactly ONE way to the server: {@code PdbConnection.sendTxnControl},
 * which closes the connection when a request's outcome is unknown (the
 * waiting thread interrupted after the request was written, a dropped socket)
 * instead of leaving the connection under a guessed state.
 *
 * <p>The guarded helper only protects the sites that use it. Two did not --
 * the manual-commit batch's lazy BEGIN and the autocommit batch's cleanup
 * ROLLBACK both wrote the verb straight to the mux -- and an outcome-unknown
 * failure there is not reproducible on demand over a live socket. So this is
 * a census of the source: no transaction-control verb is written to the mux
 * except inside {@code sendTxnControl}, and the generic {@code sendControl}
 * hands those verbs to it. A new direct send fails here, by line.
 */
public class TxnControlFunnelTest {

    private static String source() throws Exception {
        Path p = Paths.get("src/main/java/org/particledb/PdbConnection.java");
        assertTrue(Files.exists(p), "run from the repository root: " + p.toAbsolutePath());
        return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
    }

    /** The body of the method whose signature line contains {@code signature}. */
    private static String body(String src, String signature) {
        int i = src.indexOf(signature);
        assertTrue(i >= 0, "method not found: " + signature);
        int open = src.indexOf('{', i);
        int depth = 0;
        for (int k = open; k < src.length(); k++) {
            char c = src.charAt(k);
            if (c == '{') depth++;
            if (c == '}' && --depth == 0) return src.substring(open, k + 1);
        }
        throw new AssertionError("unbalanced method body: " + signature);
    }

    @Test
    void noTransactionVerbReachesTheMuxOutsideTheGuardedHelper() throws Exception {
        String src = source();
        String helper = body(src, "private void sendTxnControl(String sql)");
        String outside = src.replace(helper, "");
        // A literal verb handed to the mux by any route.
        Pattern direct = Pattern.compile(
                "mux\\s*\\.\\s*(query|queryBeginning|execute|batch)\\s*\\(\\s*\"\\s*(BEGIN|COMMIT|ROLLBACK)\\b",
                Pattern.CASE_INSENSITIVE);
        List<String> hits = new ArrayList<>();
        String[] lines = outside.split("\n");
        for (int n = 0; n < lines.length; n++) {
            Matcher m = direct.matcher(lines[n]);
            if (m.find()) hits.add(lines[n].trim());
        }
        assertTrue(hits.isEmpty(), "BEGIN/COMMIT/ROLLBACK sent to the mux outside sendTxnControl: " + hits);
        // Positive control: the census does see the helper's own send.
        assertTrue(helper.contains("mux.query(sql)"), "census lost sight of the helper's send");
    }

    @Test
    void theGenericControlSendDelegatesTransactionVerbs() throws Exception {
        String sendControl = body(source(), "private void sendControl(String sql)");
        assertTrue(sendControl.contains("isTxnControlVerb(sql)") && sendControl.contains("sendTxnControl(sql)"),
                "sendControl must hand BEGIN/COMMIT/ROLLBACK to sendTxnControl");
        assertTrue(PdbConnection.isTxnControlVerb("BEGIN"));
        assertTrue(PdbConnection.isTxnControlVerb("COMMIT"));
        assertTrue(PdbConnection.isTxnControlVerb("ROLLBACK"));
        assertFalse(PdbConnection.isTxnControlVerb("SET search_path TO x"));
    }
}
