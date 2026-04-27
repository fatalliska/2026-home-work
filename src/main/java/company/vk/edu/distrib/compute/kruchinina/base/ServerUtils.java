package company.vk.edu.distrib.compute.kruchinina.base;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public final class ServerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);
    private static final int PORT = 8000;
    private static final String DEFAULT_STORAGE_DIR = "./data";
    static final String METHOD_GET = "GET";
    static final String METHOD_PUT = "PUT";
    static final String METHOD_DELETE = "DELETE";

    private ServerUtils() {
    }

    static void main(String... args) {
        try {
            Dao<byte[]> dao = new FileSystemDao(DEFAULT_STORAGE_DIR);
            SimpleKVService service = new SimpleKVService(PORT, dao);
            service.start();
            LOG.info("KVService started on port {}", PORT);
            LOG.info("Press Enter to stop...");
            System.in.read();
            service.stop();
            LOG.info("KVService stopped");
        } catch (IOException e) {
            LOG.error("IO error while starting service", e);
            Runtime.getRuntime().halt(1);
        } catch (Exception e) {
            LOG.error("Unexpected error", e);
            Runtime.getRuntime().halt(1);
        }
    }
}
