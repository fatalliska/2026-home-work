package company.vk.edu.distrib.compute.kruchinina.sharding;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class ClusterHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterHttpClient.class);
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public void proxyRequest(String targetNode, HttpExchange originalExchange) {
        String method = originalExchange.getRequestMethod();
        String path = originalExchange.getRequestURI().getPath();
        String query = originalExchange.getRequestURI().getQuery();
        String fullUrl = "http://" + targetNode + path + (query != null ? "?" + query : "");

        try {
            byte[] body = originalExchange.getRequestBody().readAllBytes();
            HttpRequest request = buildHttpRequest(method, fullUrl, body, originalExchange);
            HttpResponse<byte[]> response = sendRequest(request);
            forwardResponse(response, originalExchange);
        } catch (Exception e) {
            LOG.error("Failed to proxy {} request to {}", method, fullUrl, e);
            sendErrorResponse(originalExchange, "Proxy error: " + e.getMessage());
        }
    }

    private HttpRequest buildHttpRequest(String method, String fullUrl, byte[] body, HttpExchange originalExchange) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(fullUrl))
                .timeout(Duration.ofSeconds(30));

        //Копируем заголовки, кроме запрещённых
        for (Map.Entry<String, List<String>> header : originalExchange.getRequestHeaders().entrySet()) {
            String name = header.getKey();
            if (!isRestrictedHeader(name)) {
                for (String value : header.getValue()) {
                    requestBuilder.header(name, value);
                }
            }
        }

        switch (method) {
            case "GET":
                requestBuilder.GET();
                break;
            case "DELETE":
                requestBuilder.DELETE();
                break;
            case "PUT":
                requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                break;
            default:
                throw new IllegalArgumentException("Unsupported method: " + method);
        }
        return requestBuilder.build();
    }

    private HttpResponse<byte[]> sendRequest(HttpRequest request) throws IOException, InterruptedException {
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private void forwardResponse(HttpResponse<byte[]> response, HttpExchange originalExchange) throws IOException {
        originalExchange.getResponseHeaders().putAll(response.headers().map());
        originalExchange.sendResponseHeaders(response.statusCode(), response.body().length);
        try (OutputStream os = originalExchange.getResponseBody()) {
            os.write(response.body());
        }
        originalExchange.close();
    }

    /**
     * Отправляет ошибку клиенту, чтобы не обрывать соединение.
     */
    private void sendErrorResponse(HttpExchange exchange, String message) {
        try (AutoCloseable ignored = exchange::close) {
            byte[] body = message.getBytes();
            exchange.sendResponseHeaders(500, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        } catch (Exception ex) {
            LOG.error("Failed to send error response", ex);
        }
    }

    /**
     * Заголовки, которые нельзя или не нужно проксировать.
     */
    private boolean isRestrictedHeader(String name) {
        return "Connection".equalsIgnoreCase(name)
                || "Keep-Alive".equalsIgnoreCase(name)
                || "Proxy-Authenticate".equalsIgnoreCase(name)
                || "Proxy-Authorization".equalsIgnoreCase(name)
                || "TE".equalsIgnoreCase(name)
                || "Trailer".equalsIgnoreCase(name)
                || "Transfer-Encoding".equalsIgnoreCase(name)
                || "Upgrade".equalsIgnoreCase(name)
                || "Host".equalsIgnoreCase(name)
                || "Content-Length".equalsIgnoreCase(name);
    }
}
