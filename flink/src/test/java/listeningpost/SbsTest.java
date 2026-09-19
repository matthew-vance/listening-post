package listeningpost;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Decode is pinned by internal/wire/testdata/decode.json. */
class SbsTest {
    @TestFactory
    List<DynamicTest> decodeGolden() throws Exception {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode tc : Golden.load("decode.json")) {
            // value is an Event object, or a string for a Kafka value that isn't one
            String value = tc.get("value").isTextual() ? tc.get("value").asText() : Json.MAPPER.writeValueAsString(tc.get("value"));
            tests.add(DynamicTest.dynamicTest(tc.get("name").asText(), () -> {
                if (tc.get("want").isNull()) {
                    assertThrows(Exception.class, () -> Decode.decode(value));
                    return;
                }
                Decoded d = Decode.decode(value);
                assertEquals(tc.get("key").asText(), d.key());
                assertEquals(Golden.normalize(tc.get("want")), Golden.normalize(d));
            }));
        }
        return tests;
    }
}
