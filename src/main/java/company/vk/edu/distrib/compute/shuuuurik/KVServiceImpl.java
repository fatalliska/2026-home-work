package company.vk.edu.distrib.compute.shuuuurik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * KVService implementation using JDK HttpServer.
 */
public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private final Dao<byte[]> dao;

    private final HttpServer server;
    private final InetSocketAddress listenAddress;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.dao = validateDao(dao);
        this.listenAddress = createListenAddress(port);
        this.server = HttpServer.create();
        server.createContext(PATH_STATUS, this::handleStatus);
        server.createContext(PATH_ENTITY, this::handleEntity);
    }

    private static Map<String, String> parseQueryParams(HttpExchange exchange) {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        ConcurrentMap<String, String> params = new ConcurrentHashMap<>();

        if (rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }

        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);

            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";

            params.put(key, value);
        }

        return params;
    }

    private static Dao<byte[]> validateDao(Dao<byte[]> dao) {
        if (dao == null) {
            throw new IllegalArgumentException("dao must not be null");
        }
        return dao;
    }

    private static InetSocketAddress createListenAddress(int port) {
        validatePort(port);
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    private static void validatePort(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException("port must be in range 1..65535");
        }
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service already started");
        }

        try {
            server.bind(listenAddress, 0);
            if (log.isDebugEnabled()) {
                log.debug("HTTP server bound to {}", server.getAddress());
            }
            server.start();
            if (log.isDebugEnabled()) {
                log.debug("HTTP server started on {}", server.getAddress());
            }
        } catch (IOException e) {
            started.set(false);
            log.error("Failed to start HTTP server on {}", listenAddress, e);
            throw new UncheckedIOException("Failed to start HTTP server", e);
        }
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            throw new IllegalStateException("Service is not started");
        }

        server.stop(0);
        if (log.isDebugEnabled()) {
            log.debug("HTTP server stopped");
        }

        try {
            dao.close();
        } catch (IOException e) {
            log.warn("Error closing DAO", e);
        }
    }

    /**
     * GET /v0/status -- 200.
     */
    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            exchange.sendResponseHeaders(200, -1);
        }
    }

    /**
     * GET/PUT/DELETE /v0/entity?id=KEY.
     */
    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                Map<String, String> params = parseQueryParams(exchange);

                String id = params.get("id");
                if (id == null || id.isEmpty()) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                dispatchRequest(exchange, id);

            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (Exception e) {
                log.error("Unexpected error while handling request", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    private void dispatchRequest(HttpExchange exchange, String id) throws IOException {
        String requestMethod = exchange.getRequestMethod();

        switch (requestMethod) {
            case METHOD_GET -> handleGet(exchange, id);
            case METHOD_PUT -> handlePut(exchange, id);
            case METHOD_DELETE -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    /**
     * GET /v0/entity?id=KEY -- 200 or 404.
     */
    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(value);
        }
    }

    /**
     * PUT /v0/entity?id=KEY -- 201.
     */
    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(201, -1);
    }

    /**
     * DELETE /v0/entity?id=KEY -- 202.
     */
    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }
}
