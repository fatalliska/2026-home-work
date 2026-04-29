package company.vk.edu.distrib.compute.kruchinina.grpc;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.kruchinina.base.FileSystemDao;
import company.vk.edu.distrib.compute.kruchinina.replication.ReplicatedFileSystemDao;
import company.vk.edu.distrib.compute.kruchinina.sharding.ConsistentHashingStrategy;
import company.vk.edu.distrib.compute.kruchinina.sharding.RendezvousHashingStrategy;
import company.vk.edu.distrib.compute.kruchinina.sharding.ShardingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KVClusterImpl implements KVCluster {
    private static final Logger LOG = LoggerFactory.getLogger(KVClusterImpl.class);
    private static final int GRPC_PORT_OFFSET = 1000;
    private static final int DEFAULT_REPLICAS = 1;

    public enum Algorithm { CONSISTENT_HASHING, RENDEZVOUS_HASHING }

    private final List<Integer> ports;
    private final Algorithm algorithm;
    private final int replication;
    private final Map<String, KVService> services = new ConcurrentHashMap<>();
    private final List<String> endpoints = new ArrayList<>();

    public KVClusterImpl(List<Integer> ports, Algorithm algorithm, int replication) {
        this.ports = ports;
        this.algorithm = algorithm;
        this.replication = replication;
    }

    @Override
    public void start() {
        if (!services.isEmpty()) {
            throw new IllegalStateException("Cluster already started");
        }

        List<String> rawNodes = ports.stream()
                .map(p -> "localhost:" + p)
                .collect(Collectors.toList());
        ShardingStrategy strategy = createStrategy(rawNodes);

        for (int port : ports) {
            String extendedSelfAddress = "localhost:" + port + "?grpcPort=" + (port + GRPC_PORT_OFFSET);
            startNode(port, extendedSelfAddress, strategy);
            endpoints.add(extendedSelfAddress);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Cluster started with {} nodes using {} (replication={})", ports.size(), algorithm, replication);
        }
    }

    @Override
    public void start(String endpoint) {
        if (services.containsKey(endpoint)) {
            throw new IllegalStateException("Node " + endpoint + " already started");
        }
        int httpPort = extractHttpPort(endpoint);
        List<String> allExtended = new ArrayList<>(services.keySet());
        allExtended.add(endpoint);
        List<String> rawNodes = allExtended.stream()
                .map(e -> e.split("\\?")[0])
                .collect(Collectors.toList());
        ShardingStrategy strategy = createStrategy(rawNodes);
        startNode(httpPort, endpoint, strategy);
        endpoints.add(endpoint);
        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} started", endpoint);
        }
    }

    private void startNode(int port, String selfAddressExtended, ShardingStrategy strategy) {
        try {
            Path storagePath = Path.of("./data-" + port);
            if (!Files.exists(storagePath)) {
                Files.createDirectories(storagePath);
            }

            Dao<byte[]> dao;
            if (replication > DEFAULT_REPLICAS) {
                dao = new ReplicatedFileSystemDao(storagePath.toString(), replication);
            } else {
                dao = new FileSystemDao(storagePath.toString());
            }

            int grpcPort = port + GRPC_PORT_OFFSET;
            List<String> allExtended = ports.stream()
                    .map(p -> "localhost:" + p + "?grpcPort=" + (p + GRPC_PORT_OFFSET))
                    .collect(Collectors.toList());

            KVService service;
            if (dao instanceof ReplicatedFileSystemDao) {
                service = new ReplicatedKVService(port, dao, allExtended, selfAddressExtended, strategy, grpcPort);
            } else {
                service = new SimpleKVService(port, dao, allExtended, selfAddressExtended, strategy, grpcPort);
            }
            service.start();
            services.put(selfAddressExtended, service);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node on port " + port, e);
        }
    }

    @Override
    public void stop() {
        services.values().forEach(KVService::stop);
        services.clear();
        endpoints.clear();
        if (LOG.isInfoEnabled()) {
            LOG.info("Cluster stopped");
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService service = services.remove(endpoint);
        if (service != null) {
            service.stop();
            endpoints.remove(endpoint);
            if (LOG.isInfoEnabled()) {
                LOG.info("Node {} stopped", endpoint);
            }
        } else {
            throw new IllegalArgumentException("Node not found: " + endpoint);
        }
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints);
    }

    private ShardingStrategy createStrategy(List<String> rawNodes) {
        switch (algorithm) {
            case CONSISTENT_HASHING:
                return new ConsistentHashingStrategy(rawNodes);
            case RENDEZVOUS_HASHING:
                return new RendezvousHashingStrategy();
        }
        throw new IllegalStateException("Unhandled algorithm: " + algorithm);
    }

    private int extractHttpPort(String extendedEndpoint) {
        String hostPort = extendedEndpoint.split("\\?")[0];
        return Integer.parseInt(hostPort.split(":")[1]);
    }
}
