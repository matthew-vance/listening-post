package listeningpost;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads the golden fixtures (internal/wire/testdata, on the test classpath via pom.xml). Values compare as
 * decoded JSON with numbers as doubles: Jackson writes a Double 505 as 505.0 where the fixtures write 505, the
 * same JSON number.
 */
final class Golden {
    private Golden() {}

    static JsonNode load(String name) throws IOException {
        try (InputStream in = Golden.class.getResourceAsStream("/" + name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath; is internal/wire/testdata a test resource?");
            }
            return Json.MAPPER.readTree(in);
        }
    }

    static Object normalize(JsonNode node) throws IOException {
        return norm(Json.MAPPER.treeToValue(node, Object.class));
    }

    /** A Java object as fixture-comparable JSON: serialize with the production mapper, then normalize. */
    static Object json(Object o) throws IOException {
        return norm(Json.MAPPER.readValue(Json.MAPPER.writeValueAsString(o), Object.class));
    }

    private static Object norm(Object v) {
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v instanceof List<?> l) {
            return l.stream().map(Golden::norm).toList();
        }
        if (v instanceof Map<?, ?> m) {
            Map<Object, Object> out = new LinkedHashMap<>();
            m.forEach((k, x) -> out.put(k, norm(x)));
            return out;
        }
        return v;
    }
}
