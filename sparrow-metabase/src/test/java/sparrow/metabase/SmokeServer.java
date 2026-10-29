package sparrow.metabase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Runs the {@link TestServer} until the process is killed, for the Metabase driver's smoke test in CI (a Metabase
 * container connects to it). Writes the server's port to the file named by the first argument once it serves.
 */
public final class SmokeServer {

    private SmokeServer() {
    }

    /**
     * Starts the server and blocks.
     *
     * @param args the file to write the port to
     * @throws Exception if the server cannot start
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: SmokeServer <port-file>");
        }
        TestServer server = TestServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (Exception e) {
                // the process ends anyway; the data directory is temporary
            }
        }));
        Path portFile = Path.of(args[0]);
        Path tmp = portFile.resolveSibling(portFile.getFileName() + ".tmp");
        Files.writeString(tmp, Integer.toString(server.port), StandardCharsets.US_ASCII);
        Files.move(tmp, portFile);
        Thread.currentThread().join();
    }
}
