package company.vk.edu.distrib.compute.kruchinina.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.kruchinina.base.FileSystemDao;
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
    private static final String ENDPOINT_SEPARATOR = ":";

    public enum Algorithm {
        CONSISTENT_HASHING,
        RENDEZVOUS_HASHING
    }

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

        List<String> allEndpoints = ports.stream()
                .map(p -> "localhost:" + p)
                .collect(Collectors.toList());
        ShardingStrategy strategy = createStrategy(allEndpoints);

        for (int port : ports) {
            String endpoint = "localhost:" + port;
            startNode(port, endpoint, allEndpoints, strategy);
            endpoints.add(endpoint);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Cluster started with {} nodes using {} (replication={})", ports.size(), algorithm, replication);
        }
    }

    private ShardingStrategy createStrategy(List<String> nodes) {
        switch (algorithm) {
            case CONSISTENT_HASHING:
                return new ConsistentHashingStrategy(nodes);
            case RENDEZVOUS_HASHING:
                return new RendezvousHashingStrategy();
        }
        throw new IllegalStateException("Unhandled algorithm: " + algorithm);
    }

    private void startNode(int port, String selfAddress, List<String> allNodes, ShardingStrategy strategy) {
        try {
            Path storagePath = Path.of("./data-" + port);
            if (!Files.exists(storagePath)) {
                Files.createDirectories(storagePath);
            }

            Dao<byte[]> dao;
            if (replication > 1) {
                dao = new ReplicatedFileSystemDao(storagePath.toString(), replication);
            } else {
                dao = new FileSystemDao(storagePath.toString());
            }

            KVService service;
            if (dao instanceof ReplicatedFileSystemDao) {
                service = new ReplicatedKVService(port, dao, allNodes, selfAddress, strategy);
            } else {
                service = new SimpleKVService(port, dao, allNodes, selfAddress, strategy);
            }
            service.start();
            services.put(selfAddress, service);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node on port " + port, e);
        }
    }

    @Override
    public void start(String endpoint) {
        if (services.containsKey(endpoint)) {
            throw new IllegalStateException("Node " + endpoint + " already started");
        }
        int port = extractPort(endpoint);
        List<String> allNodes = new ArrayList<>(services.keySet());
        allNodes.add(endpoint);
        ShardingStrategy strategy = createStrategy(allNodes);
        startNode(port, endpoint, allNodes, strategy);
        endpoints.add(endpoint);
        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} started", endpoint);
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

    private int extractPort(String endpoint) {
        String[] parts = endpoint.split(ENDPOINT_SEPARATOR);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid endpoint format: " + endpoint);
        }
        return Integer.parseInt(parts[1]);
    }
}
