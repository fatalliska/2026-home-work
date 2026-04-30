package company.vk.edu.distrib.compute.kruchinina.grpc;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class ReplicatedKVServiceFactory extends KVServiceFactory {
    private static final int DEFAULT_REPLICAS = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new ReplicatedKVService(port, DEFAULT_REPLICAS);
    }
}
