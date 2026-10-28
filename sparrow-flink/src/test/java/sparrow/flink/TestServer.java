package sparrow.flink;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

/**
 * In-process Arrow Flight SQL server for the tests, serving the server test-jar's {@code test_db}
 * ({@code test_schema.test_table}) copied to a temporary directory.
 */
final class TestServer implements AutoCloseable {

    final TestFlightServerHelper helper;
    final int port;
    private final Path dataDir;

    private TestServer(TestFlightServerHelper helper, Path dataDir) {
        this.helper = helper;
        this.dataDir = dataDir;
        this.port = helper.location.getUri().getPort();
    }

    /** Starts a server over a copy of the server's test data. */
    static TestServer start() throws Exception {
        Path dir = Files.createTempDirectory("sparrow-test-db");
        extract("test_db", dir);
        return new TestServer(TestFlightServerHelper.builder().dataDir(dir.toString()).start(), dir);
    }

    /** Copies a resource directory (from a jar or the file system) into {@code target}. */
    private static void extract(String resourceDir, Path target) throws IOException {
        URL url = TestServer.class.getClassLoader().getResource(resourceDir);
        if (url == null) {
            throw new IOException("resource " + resourceDir + " not on the test classpath");
        }
        if ("jar".equals(url.getProtocol())) {
            JarURLConnection conn = (JarURLConnection) url.openConnection();
            conn.setUseCaches(false);
            try (JarFile jar = conn.getJarFile()) {
                for (JarEntry e : java.util.Collections.list(jar.entries())) {
                    if (!e.getName().startsWith(resourceDir + "/") || e.isDirectory()) {
                        continue;
                    }
                    Path out = target.resolve(e.getName().substring(resourceDir.length() + 1));
                    Files.createDirectories(out.getParent());
                    try (InputStream in = jar.getInputStream(e)) {
                        Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        } else {
            Path src = Path.of(java.net.URI.create(url.toString()));
            try (var paths = Files.walk(src)) {
                for (Path p : (Iterable<Path>) paths::iterator) {
                    Path out = target.resolve(src.relativize(p).toString());
                    if (Files.isDirectory(p)) {
                        Files.createDirectories(out);
                    } else {
                        Files.copy(p, out, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        helper.close();
        try (var paths = Files.walk(dataDir)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
