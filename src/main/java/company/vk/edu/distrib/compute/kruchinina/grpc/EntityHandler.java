package company.vk.edu.distrib.compute.kruchinina.grpc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kruchinina.replication.ReplicatedFileSystemDao;
import company.vk.edu.distrib.compute.kruchinina.sharding.ShardingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class EntityHandler implements HttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(EntityHandler.class);

    private static final String MISSING_ID_MSG = "Missing id";
    private static final String INVALID_ACK_MSG = "Invalid ack parameter";
    private static final String REPLICATION_NOT_SUPPORTED_MSG = "Replication not supported";
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final int DEFAULT_ACKS = 1;

    private final Dao<byte[]> dao;
    private final GrpcClusterClient grpcClient;
    private final Optional<ShardingStrategy> shardingStrategy;
    private final List<String> clusterNodesRaw;
    private final Map<String, String> rawToExtended;
    private final String selfAddressRaw;

    public EntityHandler(Dao<byte[]> dao,
                         GrpcClusterClient grpcClient,
                         Optional<ShardingStrategy> shardingStrategy,
                         List<String> clusterNodesRaw,
                         Map<String, String> rawToExtended,
                         String selfAddressRaw) {
        this.dao = dao;
        this.grpcClient = grpcClient;
        this.shardingStrategy = shardingStrategy;
        this.clusterNodesRaw = clusterNodesRaw;
        this.rawToExtended = rawToExtended;
        this.selfAddressRaw = selfAddressRaw;
    }

    public boolean isClusterMode() {
        return shardingStrategy.isPresent();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String id = extractId(exchange);
        if (id == null) {
            ResponseSenderUtils.sendResponse(exchange, 400, MISSING_ID_MSG.getBytes(StandardCharsets.UTF_8));
            return;
        }
        int ack = extractAck(exchange);
        if (ack <= 0) {
            ResponseSenderUtils.sendResponse(exchange, 400, INVALID_ACK_MSG.getBytes(StandardCharsets.UTF_8));
            return;
        }

        if (isClusterMode() && !isResponsibleForKey(id)) {
            proxyRequest(exchange, id, ack);
            return;
        }

        try {
            dispatchRequest(exchange, id, ack);
        } catch (Exception e) {
            handleException(exchange, e);
        }
    }

    private boolean isResponsibleForKey(String id) {
        return shardingStrategy.get().selectNode(id, clusterNodesRaw).equals(selfAddressRaw);
    }

    private void proxyRequest(HttpExchange exchange, String id, int ack) throws IOException {
        String rawTarget = shardingStrategy.get().selectNode(id, clusterNodesRaw);
        String targetExtended = rawToExtended.get(rawTarget);
        if (targetExtended == null) {
            ResponseSenderUtils.sendResponse(exchange, 500, new byte[0]);
            return;
        }
        String method = exchange.getRequestMethod();
        try {
            if (ServerUtils.METHOD_GET.equals(method)) {
                byte[] data = grpcClient.get(targetExtended, id, ack);
                exchange.getResponseHeaders().set(CONTENT_TYPE, OCTET_STREAM);
                ResponseSenderUtils.sendResponse(exchange, 200, data);
            } else if (ServerUtils.METHOD_PUT.equals(method)) {
                byte[] body = exchange.getRequestBody().readAllBytes();
                grpcClient.upsert(targetExtended, id, body, ack);
                ResponseSenderUtils.sendResponse(exchange, 201, new byte[0]);
            } else if (ServerUtils.METHOD_DELETE.equals(method)) {
                grpcClient.delete(targetExtended, id, ack);
                ResponseSenderUtils.sendResponse(exchange, 202, new byte[0]);
            } else {
                ResponseSenderUtils.sendResponse(exchange, 405, new byte[0]);
            }
        } catch (Exception e) {
            handleException(exchange, e);
        }
    }

    private String extractId(HttpExchange exchange) {
        Map<String, String> params = QueryParserUtils.parseQuery(exchange.getRequestURI().getQuery());
        String id = params.get(ID_PARAM);
        return (id == null || id.isEmpty()) ? null : id;
    }

    private int extractAck(HttpExchange exchange) {
        Map<String, String> params = QueryParserUtils.parseQuery(exchange.getRequestURI().getQuery());
        String ackStr = params.get(ACK_PARAM);
        if (ackStr == null || ackStr.isEmpty()) {
            return DEFAULT_ACKS;
        }
        try {
            return Integer.parseInt(ackStr);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void dispatchRequest(HttpExchange exchange, String id, int ack) throws IOException {
        String method = exchange.getRequestMethod();
        if (ServerUtils.METHOD_GET.equals(method)) {
            handleGet(exchange, id, ack);
        } else if (ServerUtils.METHOD_PUT.equals(method)) {
            handlePut(exchange, id, ack);
        } else if (ServerUtils.METHOD_DELETE.equals(method)) {
            handleDelete(exchange, id, ack);
        } else {
            ResponseSenderUtils.sendResponse(exchange, 405, new byte[0]);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] data;
        if (dao instanceof ReplicatedFileSystemDao) {
            data = ((ReplicatedFileSystemDao) dao).get(id, ack);
        } else {
            if (ack != DEFAULT_ACKS) {
                throw new IllegalArgumentException(REPLICATION_NOT_SUPPORTED_MSG);
            }
            data = dao.get(id);
        }
        exchange.getResponseHeaders().set(CONTENT_TYPE, OCTET_STREAM);
        ResponseSenderUtils.sendResponse(exchange, 200, data);
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        if (dao instanceof ReplicatedFileSystemDao) {
            ((ReplicatedFileSystemDao) dao).upsert(id, body, ack);
        } else {
            if (ack != DEFAULT_ACKS) {
                throw new IllegalArgumentException(REPLICATION_NOT_SUPPORTED_MSG);
            }
            dao.upsert(id, body);
        }
        ResponseSenderUtils.sendResponse(exchange, 201, new byte[0]);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        if (dao instanceof ReplicatedFileSystemDao) {
            ((ReplicatedFileSystemDao) dao).delete(id, ack);
        } else {
            if (ack != DEFAULT_ACKS) {
                throw new IllegalArgumentException(REPLICATION_NOT_SUPPORTED_MSG);
            }
            dao.delete(id);
        }
        ResponseSenderUtils.sendResponse(exchange, 202, new byte[0]);
    }

    private void handleException(HttpExchange exchange, Exception e) throws IOException {
        if (e instanceof IllegalArgumentException) {
            ResponseSenderUtils.sendResponse(exchange, 400, e.getMessage().getBytes(StandardCharsets.UTF_8));
        } else if (e instanceof NoSuchElementException) {
            ResponseSenderUtils.sendResponse(exchange, 404, new byte[0]);
        } else if (e instanceof IOException) {
            if (LOG.isErrorEnabled()) {
                LOG.error("IO error", e);
            }
            ResponseSenderUtils.sendResponse(exchange, 500, new byte[0]);
        } else {
            if (LOG.isErrorEnabled()) {
                LOG.error("Unexpected error", e);
            }
            ResponseSenderUtils.sendResponse(exchange, 500, new byte[0]);
        }
    }
}
