package company.vk.edu.distrib.compute.kruchinina.grpc;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;

public final class ResponseSenderUtils {
    private ResponseSenderUtils() {

    }

    public static void sendResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        exchange.close();
    }
}
