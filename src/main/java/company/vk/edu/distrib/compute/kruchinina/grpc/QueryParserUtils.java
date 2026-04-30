package company.vk.edu.distrib.compute.kruchinina.grpc;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class QueryParserUtils {
    private QueryParserUtils() {

    }

    public static Map<String, String> parseQuery(String query) {
        Map<String, String> params = new ConcurrentHashMap<>();
        if (query == null) {
            return params;
        }
        String[] pairs = query.split("&");
        for (String pair : pairs) {
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

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
