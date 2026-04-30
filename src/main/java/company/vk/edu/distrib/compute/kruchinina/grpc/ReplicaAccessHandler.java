package company.vk.edu.distrib.compute.kruchinina.grpc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kruchinina.replication.ReplicatedFileSystemDao;
import company.vk.edu.distrib.compute.kruchinina.sharding.ServerUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ReplicaAccessHandler implements HttpHandler {
    private static final String SLASH = "/";
    private static final int FIVE = 5;

    private final Dao<byte[]> dao;

    public ReplicaAccessHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!ServerUtils.METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
            ResponseSenderUtils.sendResponse(exchange, 405, new byte[0]);
            return;
        }
        String[] parts = exchange.getRequestURI().getPath().split(SLASH);
        if (parts.length < FIVE) {
            ResponseSenderUtils.sendResponse(exchange, 400, "Invalid path".getBytes());
            return;
        }
        try {
            int idx = Integer.parseInt(parts[4]);
            if (dao instanceof ReplicatedFileSystemDao) {
                ReplicatedFileSystemDao rdao = (ReplicatedFileSystemDao) dao;
                int reads = rdao.getReadAccessCount(idx);
                int writes = rdao.getWriteAccessCount(idx);
                String json = "{\"reads\":" + reads + ",\"writes\":" + writes + "}";
                ResponseSenderUtils.sendResponse(exchange, 200, json.getBytes(StandardCharsets.UTF_8));
            } else {
                ResponseSenderUtils.sendResponse(exchange, 404, "Replication not active".getBytes());
            }
        } catch (NumberFormatException e) {
            ResponseSenderUtils.sendResponse(exchange, 400, "Bad replica index".getBytes());
        } catch (Exception e) {
            ResponseSenderUtils.sendResponse(exchange, 500, new byte[0]);
        }
    }
}
