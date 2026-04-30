package company.vk.edu.distrib.compute.kruchinina.grpc;

import company.vk.edu.distrib.compute.KVCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class ServerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);
    private static final int DEFAULT_PORT = 8000;
    private static final String CLUSTER_FLAG = "--cluster";
    private static final String ALGORITHM_RENDEZVOUS = "rendezvous";
    private static final String REPLICATION_FLAG = "--replication";
    private static final String ALGORITHM_FLAG = "--algorithm";
    private static final String PORTS_SEPARATOR = ",";
    private static final int DEFAULT_REPLICATION_FACTOR = 1;

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
        int replication = DEFAULT_REPLICATION_FACTOR;
        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (arg.matches("\\d+")) {
                port = Integer.parseInt(arg);
                i++;
            } else if (REPLICATION_FLAG.equals(arg) && i + 1 < args.length) {
                replication = parseIntSafe(args[i + 1], DEFAULT_REPLICATION_FACTOR);
                i += 2;
            } else {
                i++;
            }
        }

        ReplicatedKVService service = new ReplicatedKVService(port, replication);
        service.start();
        if (LOG.isInfoEnabled()) {
            LOG.info("KVService started on port {} (single node, replication={})", port, replication);
            LOG.info("Press Enter to stop...");
        }
        System.in.read();
        service.stop();
        if (LOG.isInfoEnabled()) {
            LOG.info("KVService stopped");
        }
    }

    private static void runClusterMode(String... args) {
        ClusterConfig config = parseClusterArgs(args);
        List<Integer> ports = config.ports;
        int replication = config.replication;

        KVClusterImpl.Algorithm algorithm;
        if (ALGORITHM_RENDEZVOUS.equalsIgnoreCase(config.algorithm)) {
            algorithm = KVClusterImpl.Algorithm.RENDEZVOUS_HASHING;
        } else {
            algorithm = KVClusterImpl.Algorithm.CONSISTENT_HASHING;
        }

        KVCluster cluster = new KVClusterImpl(ports, algorithm, replication);
        cluster.start();
        if (LOG.isInfoEnabled()) {
            LOG.info("Cluster started with algorithm {} on ports {} (replication={})",
                    algorithm, ports, replication);
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

    private static ServerUtils.ClusterConfig parseClusterArgs(String... args) {
        ServerUtils.FlagsResult flags = parseFlags(args);
        String portsArg = flags.portsArg;

        if (portsArg == null || portsArg.isEmpty()) {
            if (LOG.isErrorEnabled()) {
                LOG.error("No ports specified for cluster");
            }
            throw new IllegalStateException("Application terminated due to fatal error");
        }

        List<Integer> ports = Arrays.stream(portsArg.split(PORTS_SEPARATOR))
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        return new ClusterConfig(ports, flags.algorithm, flags.replication);
    }

    private static ServerUtils.FlagsResult parseFlags(String... args) {
        String algorithmArg = ALGORITHM_RENDEZVOUS;
        int replication = DEFAULT_REPLICATION_FACTOR;
        String portsArg = null;

        int i = 1;
        while (i < args.length) {
            String arg = args[i];
            if (ALGORITHM_FLAG.equals(arg) && i + 1 < args.length) {
                algorithmArg = args[i + 1];
                i += 2;
            } else if (REPLICATION_FLAG.equals(arg) && i + 1 < args.length) {
                replication = parseIntSafe(args[i + 1], DEFAULT_REPLICATION_FACTOR);
                i += 2;
            } else if (arg.startsWith("--")) {
                i++;
            } else {
                portsArg = arg;
                break;
            }
        }
        return new ServerUtils.FlagsResult(algorithmArg, replication, portsArg);
    }

    private static class FlagsResult {
        final String algorithm;
        final int replication;
        final String portsArg;

        FlagsResult(String algorithm, int replication, String portsArg) {
            this.algorithm = algorithm;
            this.replication = replication;
            this.portsArg = portsArg;
        }
    }

    private static class ClusterConfig {
        final List<Integer> ports;
        final String algorithm;
        final int replication;

        ClusterConfig(List<Integer> ports, String algorithm, int replication) {
            this.ports = ports;
            this.algorithm = algorithm;
            this.replication = replication;
        }
    }

    private static int parseIntSafe(String s, int defaultValue) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Invalid integer '{}', using default {}", s, defaultValue);
            }
            return defaultValue;
        }
    }
}
