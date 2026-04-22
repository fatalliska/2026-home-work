package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class ShuuuurikInMemoryKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        InMemoryDao dao = new InMemoryDao();
        return new KVServiceImpl(port, dao);
    }
}
