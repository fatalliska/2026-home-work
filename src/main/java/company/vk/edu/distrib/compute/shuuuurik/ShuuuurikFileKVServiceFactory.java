package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class ShuuuurikFileKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path root = Path.of(System.getProperty("java.io.tmpdir"), "shuuuurik-storage");
        FileDao dao = new FileDao(root);
        return new KVServiceImpl(port, dao);
    }
}
