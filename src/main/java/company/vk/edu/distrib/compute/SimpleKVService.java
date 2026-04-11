package company.vk.edu.distrib.compute;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static company.vk.edu.distrib.compute.ServerUtils.*;

public class SimpleKVService implements KVService {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKVService.class);

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean started; // по умолчанию false

    public SimpleKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        try {
            //Создание HTTP-сервера, привязанного к локальному адресу и указанному порту
            //Параметр 0 означает использование размера очереди подключений по умолчанию
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(null); //Cервер будет использовать executor по умолчанию
            server.start();
            started = true;
            LOG.info("KVService started on port {}", port);
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
            LOG.info("KVService stopped");
        }
        try {
            dao.close();
        } catch (IOException e) {
            LOG.error("Error closing DAO", e);
        }
        started = false;
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
                return;
            }
            sendResponse(exchange, STATUS_OK, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            Map<String, String> queryParams = parseQuery(exchange.getRequestURI().getQuery());
            String id = queryParams.get("id");

            if (id == null || id.isEmpty()) {
                sendResponse(exchange, STATUS_BAD_REQUEST, "Missing id".getBytes(StandardCharsets.UTF_8));
                return;
            }

            try {
                if (METHOD_GET.equals(method)) {
                    handleGet(exchange, id);
                } else if (METHOD_PUT.equals(method)) {
                    handlePut(exchange, id);
                } else if (METHOD_DELETE.equals(method)) {
                    handleDelete(exchange, id);
                } else {
                    sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
                }
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, STATUS_BAD_REQUEST, e.getMessage().getBytes(StandardCharsets.UTF_8));
            } catch (NoSuchElementException e) {
                sendResponse(exchange, STATUS_NOT_FOUND, new byte[0]);
            } catch (IOException e) {
                LOG.error("IO error", e);
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            } catch (Exception e) {
                LOG.error("Unexpected error", e);
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            sendResponse(exchange, STATUS_OK, data);
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, body);
            sendResponse(exchange, STATUS_CREATED, new byte[0]);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            sendResponse(exchange, STATUS_ACCEPTED, new byte[0]);
        }

        //Разбор query-строки: разбивает строку по символу &, каждый параметр по =
        private Map<String, String> parseQuery(String query) {
            Map<String, String> params = new ConcurrentHashMap<>();
            if (query == null) {
                return params;
            }
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                String key;
                String value;
                if (eq > 0) {
                    key = decode(pair.substring(0, eq));
                    value = decode(pair.substring(eq + 1));
                } else {
                    key = decode(pair);
                    value = "";
                }
                params.put(key, value);
            }
            return params;
        }

        private String decode(String s) {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        }
    }

    //Метод отправки ответа: устанавливает код статуса и длину тела,
    // записывает массив байтов в выходной поток,
    // затем закрывает обмен
    private static void sendResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        exchange.close();
    }
}
