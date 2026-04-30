package company.vk.edu.distrib.compute.kruchinina.grpc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.kruchinina.sharding.ServerUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!ServerUtils.METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
            ResponseSenderUtils.sendResponse(exchange, 405, new byte[0]);
            return;
        }
        ResponseSenderUtils.sendResponse(exchange, 200, "OK".getBytes(StandardCharsets.UTF_8));
    }
}
