package company.vk.edu.distrib.compute.kruchinina.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.kruchinina.sharding.ClusterHttpClient;
import company.vk.edu.distrib.compute.kruchinina.sharding.ShardingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static company.vk.edu.distrib.compute.kruchinina.sharding.ServerUtils.*;

public class SimpleKVService implements KVService {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKVService.class);

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;
    private static final String MISSING_ID_MSG = "Missing id";
    private static final String ID_PARAM = "id";
    private static final String ACK_PARAM = "ack";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final String AMPERSAND = "&";
    private static final String EQUALS = "=";
    private static final String EMPTY = "";

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean started;

    //Кластерная конфигурация
    private final List<String> clusterNodes;
    private final String selfAddress;
    private final ShardingStrategy shardingStrategy;
    private final ClusterHttpClient httpClient;

    public SimpleKVService(int port, Dao<byte[]> dao) {
        this(port, dao, null, null, null);
    }

    public SimpleKVService(int port, Dao<byte[]> dao,
                           List<String> clusterNodes,
                           String selfAddress,
                           ShardingStrategy shardingStrategy) {
        this.port = port;
        this.dao = dao;
        this.clusterNodes = clusterNodes;
        this.selfAddress = selfAddress;
        this.shardingStrategy = shardingStrategy;
        this.httpClient = new ClusterHttpClient();
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.createContext("/stats/replica/", new ReplicaStatsHandler());
            server.createContext("/stats/replica/access/", new ReplicaAccessHandler());
            server.setExecutor(null);
            server.start();
            started = true;
            if (LOG.isInfoEnabled()) {
                LOG.info("KVService started on port {} (cluster mode: {})", port, isClusterMode());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start HTTP server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Service not started");
        }
        if (server != null) {
            server.stop(0);
            LOG.info("KVService stopped on port {}", port);
        }
        try {
            dao.close();
        } catch (IOException e) {
            LOG.error("Error closing DAO", e);
        }
        started = false;
    }

    private boolean isClusterMode() {
        return clusterNodes != null && !clusterNodes.isEmpty() && shardingStrategy != null;
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
                return;
            }
            sendResponse(exchange, STATUS_OK, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String id = extractId(exchange);
            if (id == null) {
                sendResponse(exchange, STATUS_BAD_REQUEST, MISSING_ID_MSG.getBytes(StandardCharsets.UTF_8));
                return;
            }

            int ack = extractAck(exchange);
            if (ack <= 0) {
                sendResponse(exchange, STATUS_BAD_REQUEST, "Invalid ack parameter".getBytes(StandardCharsets.UTF_8));
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Received {} request for id={} ack={}", exchange.getRequestMethod(), id, ack);
            }

            // Ранний выход при необходимости проксирования (без глубокой вложенности)
            if (isClusterMode() && !isResponsibleForKey(id)) {
                proxyToResponsibleNode(exchange, id);
                return;
            }

            try {
                dispatchRequest(exchange, id, ack);
            } catch (Exception e) {
                LOG.error("Error handling request for id={}", id, e);
                handleException(exchange, e);
            }
        }

        private boolean isResponsibleForKey(String id) {
            String responsible = shardingStrategy.selectNode(id, clusterNodes);
            return responsible.equals(selfAddress);
        }

        private void proxyToResponsibleNode(HttpExchange exchange, String id) {
            String target = shardingStrategy.selectNode(id, clusterNodes);
            httpClient.proxyRequest(target, exchange);
        }

        private String extractId(HttpExchange exchange) {
            Map<String, String> queryParams = parseQuery(exchange.getRequestURI().getQuery());
            String id = queryParams.get(ID_PARAM);
            return (id == null || id.isEmpty()) ? null : id;
        }

        private int extractAck(HttpExchange exchange) {
            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String ackStr = params.get(ACK_PARAM);
            if (ackStr == null || ackStr.isEmpty()) {
                return 1;
            }
            try {
                return Integer.parseInt(ackStr);
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        private void dispatchRequest(HttpExchange exchange, String id, int ack) throws IOException {
            String method = exchange.getRequestMethod();
            if (METHOD_GET.equals(method)) {
                handleGet(exchange, id, ack);
            } else if (METHOD_PUT.equals(method)) {
                handlePut(exchange, id, ack);
            } else if (METHOD_DELETE.equals(method)) {
                handleDelete(exchange, id, ack);
            } else {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
            }
        }

        private void handleException(HttpExchange exchange, Exception e) throws IOException {
            if (e instanceof IllegalArgumentException) {
                sendResponse(exchange, STATUS_BAD_REQUEST, e.getMessage().getBytes(StandardCharsets.UTF_8));
            } else if (e instanceof NoSuchElementException) {
                sendResponse(exchange, STATUS_NOT_FOUND, new byte[0]);
            } else if (e instanceof IOException) {
                LOG.error("IO error", e);
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            } else {
                LOG.error("Unexpected error", e);
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            }
        }

        private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
            byte[] data;
            if (dao instanceof ReplicatedFileSystemDao) {
                data = ((ReplicatedFileSystemDao) dao).get(id, ack);
            } else {
                if (ack != 1) {
                    throw new IllegalArgumentException("Replication not supported");
                }
                data = dao.get(id);
            }
            exchange.getResponseHeaders().set(CONTENT_TYPE, OCTET_STREAM);
            sendResponse(exchange, STATUS_OK, data);
        }

        private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            if (dao instanceof ReplicatedFileSystemDao) {
                ((ReplicatedFileSystemDao) dao).upsert(id, body, ack);
            } else {
                if (ack != 1) {
                    throw new IllegalArgumentException("Replication not supported");
                }
                dao.upsert(id, body);
            }
            sendResponse(exchange, STATUS_CREATED, new byte[0]);
        }

        private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
            if (dao instanceof ReplicatedFileSystemDao) {
                ((ReplicatedFileSystemDao) dao).delete(id, ack);
            } else {
                if (ack != 1) {
                    throw new IllegalArgumentException("Replication not supported");
                }
                dao.delete(id);
            }
            sendResponse(exchange, STATUS_ACCEPTED, new byte[0]);
        }

        private Map<String, String> parseQuery(String query) {
            Map<String, String> params = new ConcurrentHashMap<>();
            if (query == null) {
                return params;
            }
            for (String pair : query.split(AMPERSAND)) {
                int eq = pair.indexOf(EQUALS);
                String key;
                String value;
                if (eq > 0) {
                    key = decode(pair.substring(0, eq));
                    value = decode(pair.substring(eq + 1));
                } else {
                    key = decode(pair);
                    value = EMPTY;
                }
                params.put(key, value);
            }
            return params;
        }

        private String decode(String s) {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        }
    }

    private final class ReplicaStatsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            if (parts.length < 4) {
                sendResponse(exchange, STATUS_BAD_REQUEST, "Invalid path".getBytes());
                return;
            }
            try {
                int idx = Integer.parseInt(parts[3]);
                if (dao instanceof ReplicatedFileSystemDao) {
                    ReplicatedFileSystemDao repDao = (ReplicatedFileSystemDao) dao;
                    int count = repDao.getKeyCount(idx);
                    sendResponse(exchange, STATUS_OK, Integer.toString(count).getBytes(StandardCharsets.UTF_8));
                } else {
                    sendResponse(exchange, STATUS_NOT_FOUND, "Replication not active".getBytes());
                }
            } catch (NumberFormatException e) {
                sendResponse(exchange, STATUS_BAD_REQUEST, "Bad replica index".getBytes());
            } catch (Exception e) {
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            }
        }
    }

    private final class ReplicaAccessHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");
            if (parts.length < 5) {
                sendResponse(exchange, STATUS_BAD_REQUEST, "Invalid path".getBytes());
                return;
            }
            try {
                int idx = Integer.parseInt(parts[4]);
                if (dao instanceof ReplicatedFileSystemDao) {
                    ReplicatedFileSystemDao repDao = (ReplicatedFileSystemDao) dao;
                    int reads = repDao.getReadAccessCount(idx);
                    int writes = repDao.getWriteAccessCount(idx);
                    String json = "{\"reads\":" + reads + ",\"writes\":" + writes + "}";
                    sendResponse(exchange, STATUS_OK, json.getBytes(StandardCharsets.UTF_8));
                } else {
                    sendResponse(exchange, STATUS_NOT_FOUND, "Replication not active".getBytes());
                }
            } catch (NumberFormatException e) {
                sendResponse(exchange, STATUS_BAD_REQUEST, "Bad replica index".getBytes());
            } catch (Exception e) {
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            }
        }
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        exchange.close();
    }
}
