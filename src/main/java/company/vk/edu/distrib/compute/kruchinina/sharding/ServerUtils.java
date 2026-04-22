package company.vk.edu.distrib.compute.kruchinina.sharding;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.kruchinina.base.FileSystemDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class ServerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);
    private static final int DEFAULT_PORT = 8000;
    private static final String DEFAULT_STORAGE_DIR = "./data";
    private static final String CLUSTER_FLAG = "--cluster";
    private static final String ALGORITHM_RENDEZVOUS = "rendezvous";
    private static final int ARGS_LENGTH = 3;

    static final String METHOD_GET = "GET";
    static final String METHOD_PUT = "PUT";
    static final String METHOD_DELETE = "DELETE";

    private ServerUtils() {
    }

    public static void main(String... args) {
        try {
            if (args.length > 0 && CLUSTER_FLAG.equals(args[0])) {
                runClusterMode(args);
            } else {
                runSingleNodeMode(args);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Fatal error", e);
            }
            Runtime.getRuntime().halt(1);
        }
    }

    private static void runSingleNodeMode(String... args) throws IOException {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        Dao<byte[]> dao = new FileSystemDao(DEFAULT_STORAGE_DIR);
        SimpleKVService service = new SimpleKVService(port, dao);
        service.start();
        if (LOG.isInfoEnabled()) {
            LOG.info("KVService started on port {} (single node)", port);
            LOG.info("Press Enter to stop...");
        }
        System.in.read();
        service.stop();
        if (LOG.isInfoEnabled()) {
            LOG.info("KVService stopped");
        }
    }

    private static void runClusterMode(String... args) {
        if (args.length < ARGS_LENGTH) {
            String errorMsg = "Usage: --cluster <algorithm> <port1,port2,...>";
            if (LOG.isErrorEnabled()) {
                LOG.error("Usage: --cluster <algorithm> <port1,port2,...>");
            }
            throw new IllegalArgumentException(errorMsg);
        }
        String algorithmArg = args[1];
        String portsArg = args[2];
        List<Integer> ports = Arrays.stream(portsArg.split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        KVClusterImpl.Algorithm algorithm;
        if (ALGORITHM_RENDEZVOUS.equalsIgnoreCase(algorithmArg)) {
            algorithm = KVClusterImpl.Algorithm.RENDEZVOUS_HASHING;
        } else {
            algorithm = KVClusterImpl.Algorithm.CONSISTENT_HASHING;
        }

        KVCluster cluster = new KVClusterImpl(ports, algorithm);
        cluster.start();
        if (LOG.isInfoEnabled()) {
            LOG.info("Cluster started with algorithm {} on ports {}", algorithm, ports);
            LOG.info("Press Enter to stop...");
        }
        try {
            System.in.read();
        } catch (IOException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("IO error reading input", e);
            }
        }
        cluster.stop();
        if (LOG.isInfoEnabled()) {
            LOG.info("Cluster stopped");
        }
    }
}
