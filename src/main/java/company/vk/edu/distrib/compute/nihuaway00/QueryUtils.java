package company.vk.edu.distrib.compute.nihuaway00;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class QueryUtils {
    private QueryUtils() {
    }

    public static Map<String, String> parse(String query) {
        if (query == null || query.isBlank()) return Map.of();

        Map<String, String> result = new ConcurrentHashMap<>();
        for (String param : query.split("&")) {
            String[] entry = param.split("=");
            if (entry.length > 1) {
                result.put(
                        URLDecoder.decode(entry[0], StandardCharsets.UTF_8),
                        URLDecoder.decode(entry[1], StandardCharsets.UTF_8)
                );
            } else {
                result.put(
                        URLDecoder.decode(entry[0], StandardCharsets.UTF_8),
                        ""
                );
            }
        }
        return result;
    }
}
