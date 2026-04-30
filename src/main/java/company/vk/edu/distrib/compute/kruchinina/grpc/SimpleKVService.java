package company.vk.edu.distrib.compute.kruchinina.grpc;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.kruchinina.sharding.ShardingStrategy;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKVService implements KVService {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKVService.class);
    private static final String STATS_REPLICA_PATH = "/stats/replica/";
    private static final String STATS_REPLICA_ACCESS_PATH = "/stats/replica/access/";

    private final int port;
    private final Dao<byte[]> dao;
    private final int grpcPort;
    private final Optional<GrpcClusterClient> grpcClient;
    private final EntityHandler entityHandler;

    private HttpServer server;
    private Server grpcServer;
    private boolean started;

    public SimpleKVService(int port, Dao<byte[]> dao) {
        this(port, dao, null, null, null, 0);
    }

    public SimpleKVService(int port, Dao<byte[]> dao,
                           List<String> clusterNodes,
                           String selfAddress,
                           ShardingStrategy shardingStrategy,
                           int grpcPort) {
        this.port = port;
        this.dao = dao;
        this.grpcPort = grpcPort;

        if (clusterNodes != null && !clusterNodes.isEmpty() && shardingStrategy != null) {
            GrpcClusterClient client = new GrpcClusterClient();
            this.grpcClient = Optional.of(client);

            List<String> rawList = new ArrayList<>();
            Map<String, String> extMap = new ConcurrentHashMap<>();
            for (String ext : clusterNodes) {
                String raw = ext.split("\\?")[0];
                rawList.add(raw);
                extMap.put(raw, ext);
            }
            String selfRaw = selfAddress.split("\\?")[0];
            this.entityHandler = new EntityHandler(
                    dao,
                    grpcClient.orElse(null),
                    Optional.of(shardingStrategy),
                    Collections.unmodifiableList(rawList),
                    Collections.unmodifiableMap(extMap),
                    selfRaw
            );
        } else {
            this.grpcClient = Optional.empty();
            this.entityHandler = new EntityHandler(
                    dao, null, Optional.empty(), Collections.emptyList(), Collections.emptyMap(), "");
        }
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        try {
            startHttpServer();
            if (grpcPort > 0) {
                startGrpcServer();
            }
            started = true;
            if (LOG.isInfoEnabled()) {
                LOG.info("KVService started on port {} (cluster: {})", port, isClusterMode());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start HTTP server on port " + port, e);
        }
    }

    private void startHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", entityHandler);
        server.createContext(STATS_REPLICA_PATH, new ReplicaStatsHandler(dao));
        server.createContext(STATS_REPLICA_ACCESS_PATH, new ReplicaAccessHandler(dao));
        server.setExecutor(null);
        server.start();
    }

    private void startGrpcServer() throws IOException {
        InternalKeyValueService grpcService = new InternalKeyValueService(dao);
        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService((io.grpc.BindableService) grpcService)
                .build()
                .start();
        if (LOG.isInfoEnabled()) {
            LOG.info("gRPC server started on port {}", grpcPort);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Service not started");
        }
        stopHttpServer();
        stopGrpcServer();
        shutdownGrpcClient();
        closeDao();
        started = false;
        if (LOG.isInfoEnabled()) {
            LOG.info("KVService stopped on port {}", port);
        }
    }

    private void stopHttpServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void stopGrpcServer() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                grpcServer.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void shutdownGrpcClient() {
        grpcClient.ifPresent(GrpcClusterClient::shutdown);
    }

    private void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Error closing DAO", e);
            }
        }
    }

    private boolean isClusterMode() {
        return grpcClient.isPresent();
    }
}
