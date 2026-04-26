package company.vk.edu.distrib.compute.kruchinina.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.kruchinina.sharding.ShardingStrategy;
import java.io.IOException;
import java.util.List;

public class ReplicatedKVService implements ReplicatedService {

    private final SimpleKVService service;
    private final ReplicatedFileSystemDao replicatedDao;
    private final int port;
    private final int numberOfReplicas;

    //Одиночный режим
    public ReplicatedKVService(int port, int replicas) throws IOException {
        this.port = port;
        this.numberOfReplicas = replicas;
        String storagePath = "./data-" + port;
        if (replicas > 1) {
            this.replicatedDao = new ReplicatedFileSystemDao(storagePath, replicas);
            this.service = new SimpleKVService(port, replicatedDao);
        } else {
            this.replicatedDao = null;
            this.service = new SimpleKVService(port, new FileSystemDao(storagePath));
        }
    }

    //Кластерный режим
    public ReplicatedKVService(int port, Dao<byte[]> dao,
                               List<String> clusterNodes,
                               String selfAddress,
                               ShardingStrategy shardingStrategy) {
        this.port = port;
        if (dao instanceof ReplicatedFileSystemDao) {
            this.replicatedDao = (ReplicatedFileSystemDao) dao;
            this.numberOfReplicas = replicatedDao.getReplicaCount();
        } else {
            this.replicatedDao = null;
            this.numberOfReplicas = 1;
        }
        this.service = new SimpleKVService(port, dao, clusterNodes, selfAddress, shardingStrategy);
    }

    @Override
    public void start() {
        service.start();
    }

    @Override
    public void stop() {
        service.stop();
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public void disableReplica(int nodeId) {
        if (replicatedDao != null) {
            replicatedDao.disableReplica(nodeId);
        }
    }

    @Override
    public void enableReplica(int nodeId) {
        if (replicatedDao != null) {
            replicatedDao.enableReplica(nodeId);
        }
    }
}
