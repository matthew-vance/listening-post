package listeningpost;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Loads the golden fixtures (internal/wire/testdata, on the test classpath via pom.xml). Values compare as
 * decoded JSON with numbers as doubles: Jackson writes a Double 505 as 505.0 where the fixtures write 505, the
 * same JSON number.
 */
final class Golden {
    private Golden() {}

    static JsonNode load(String name) throws Exception {
        try (InputStream in = Golden.class.getResourceAsStream("/" + name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath; is internal/wire/testdata a test resource?");
            }
            return Json.MAPPER.readTree(in);
        }
    }

    static Object normalize(JsonNode node) throws Exception {
        return normalize(Json.MAPPER.treeToValue(node, Object.class));
    }

    static Object normalize(Object o) throws Exception {
        if (o instanceof Map<?, ?> m) {
            return normalize(Json.MAPPER.convertValue(m, new TypeReference<Map<String, Object>>() {}), true);
        }
        return norm(o);
    }

    private static Object normalize(Map<String, Object> m, boolean unused) throws Exception {
        m.replaceAll((k, v) -> {
            try {
                return norm(v);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        return m;
    }

    private static Object norm(Object v) throws Exception {
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v instanceof List<?> l) {
            return l.stream().map(x -> {
                try {
                    return norm(x);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).toList();
        }
        if (v instanceof Map<?, ?> m) {
            return normalize(m);
        }
        return v;
    }

    /** A Java object as fixture-comparable JSON: serialize with the production mapper, then normalize. */
    static Object json(Object o) throws Exception {
        return normalize(Json.MAPPER.readValue(Json.MAPPER.writeValueAsString(o), Object.class));
    }
}
