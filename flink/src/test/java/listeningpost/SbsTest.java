package listeningpost;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Port of internal/processor/sbs_test.go and decode_test.go: same real dump1090 lines, same expected JSON. */
class SbsTest {
    static Map<String, Object> json(Object o) throws Exception {
        return parse(Json.MAPPER.writeValueAsString(o));
    }

    /** Numbers compare as doubles: Jackson writes a Double 505 as 505.0 where Go writes 505, the same JSON number. */
    static Map<String, Object> parse(String s) throws Exception {
        Map<String, Object> m = Json.MAPPER.readValue(s, new TypeReference<>() {});
        m.replaceAll((k, v) -> v instanceof Number n ? n.doubleValue() : v);
        return m;
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "1 identification|MSG,1,1,1,A4BF41,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0|{\"message_type\":\"MSG\",\"transmission_type\":1,\"icao\":\"A4BF41\",\"generated\":\"2026/09/14 16:05:25.403\",\"logged\":\"2026/09/14 16:05:25.428\",\"callsign\":\"AAL433\",\"on_ground\":false}",
        "2 surface position|MSG,2,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,0,12.5,270.1,40.14684,-83.17065,,,,,,-1|{\"message_type\":\"MSG\",\"transmission_type\":2,\"icao\":\"A22123\",\"generated\":\"2026/09/14 16:05:24.167\",\"logged\":\"2026/09/14 16:05:24.173\",\"altitude\":0,\"ground_speed\":12.5,\"track\":270.1,\"lat\":40.14684,\"lon\":-83.17065,\"on_ground\":true}",
        "3 airborne position|MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0|{\"message_type\":\"MSG\",\"transmission_type\":3,\"icao\":\"A22123\",\"generated\":\"2026/09/14 16:05:24.167\",\"logged\":\"2026/09/14 16:05:24.173\",\"altitude\":8275,\"lat\":40.14684,\"lon\":-83.17065,\"alert\":false,\"spi\":false,\"on_ground\":false}",
        "4 airborne velocity|MSG,4,1,1,A51958,1,2026/09/14,15:35:33.455,2026/09/14,15:35:33.505,,,505,89,,,-64,,,,,0|{\"message_type\":\"MSG\",\"transmission_type\":4,\"icao\":\"A51958\",\"generated\":\"2026/09/14 15:35:33.455\",\"logged\":\"2026/09/14 15:35:33.505\",\"ground_speed\":505,\"track\":89,\"vertical_rate\":-64,\"on_ground\":false}",
        "5 surveillance altitude|MSG,5,1,1,A22123,1,2026/09/14,16:05:23.686,2026/09/14,16:05:23.737,,8275,,,,,,,0,,0,|{\"message_type\":\"MSG\",\"transmission_type\":5,\"icao\":\"A22123\",\"generated\":\"2026/09/14 16:05:23.686\",\"logged\":\"2026/09/14 16:05:23.737\",\"altitude\":8275,\"alert\":false,\"spi\":false}",
        "6 surveillance id|MSG,6,1,1,A86392,1,2026/09/14,16:06:05.266,2026/09/14,16:06:05.292,,,,,,,,6653,0,0,0,|{\"message_type\":\"MSG\",\"transmission_type\":6,\"icao\":\"A86392\",\"generated\":\"2026/09/14 16:06:05.266\",\"logged\":\"2026/09/14 16:06:05.292\",\"squawk\":\"6653\",\"alert\":false,\"emergency\":false,\"spi\":false}",
        "7 air to air|MSG,7,1,1,A22123,1,2026/09/14,16:05:23.745,2026/09/14,16:05:23.791,,8275,,,,,,,,,,|{\"message_type\":\"MSG\",\"transmission_type\":7,\"icao\":\"A22123\",\"generated\":\"2026/09/14 16:05:23.745\",\"logged\":\"2026/09/14 16:05:23.791\",\"altitude\":8275}",
        "8 all call reply|MSG,8,1,1,AB197E,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,0|{\"message_type\":\"MSG\",\"transmission_type\":8,\"icao\":\"AB197E\",\"generated\":\"2026/09/14 16:05:23.670\",\"logged\":\"2026/09/14 16:05:23.684\",\"on_ground\":false}",
        "squawk keeps leading zeros|MSG,6,1,1,A86392,1,2026/09/14,16:06:05.266,2026/09/14,16:06:05.292,,,,,,,,0400,-1,-1,-1,|{\"message_type\":\"MSG\",\"transmission_type\":6,\"icao\":\"A86392\",\"generated\":\"2026/09/14 16:06:05.266\",\"logged\":\"2026/09/14 16:06:05.292\",\"squawk\":\"0400\",\"alert\":true,\"emergency\":true,\"spi\":true}",
    })
    void parsesEveryMessageType(String name, String raw, String want) throws Exception {
        assertEquals(parse(want), json(Sbs.parse(raw)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "MSG,3,1,1,A22123", // too few fields
        "", // empty
        "hello from compose", // not sbs
        "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,high,,,40.14684,-83.17065,,,0,,0,0", // bad altitude
        "MSG,x,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,,,,,,,,,,,", // bad transmission type
        "MSG,8,1,1,AB197E,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,yes", // bad flag
    })
    void rejectsBadLines(String raw) {
        assertThrows(IllegalArgumentException.class, () -> Sbs.parse(raw));
    }

    static final String RAW_VALUE = "{\"station_id\":\"3ae884ac-cac2-442d-93ec-5885d868f15c\",\"id\":420302,\"ts\":\"2026-09-14T15:00:17.521Z\",\"raw\":\"MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0\",\"received_at\":\"2026-09-14T15:00:20.5Z\"}";

    @Test
    void decodeCarriesEnvelopeWithoutRawLine() throws Exception {
        Decoded d = Decode.decode(RAW_VALUE);
        assertEquals("A22123", d.key());
        Map<String, Object> got = json(d);
        for (String k : new String[]{"station_id", "id", "ts", "received_at", "message_type", "transmission_type", "icao", "altitude", "lat", "lon"}) {
            assertTrue(got.containsKey(k), "missing " + k + " in " + got);
        }
        assertFalse(got.containsKey("raw"), "decoded record must not carry the raw line");
        assertEquals("3ae884ac-cac2-442d-93ec-5885d868f15c", got.get("station_id"));
        assertEquals(420302.0, got.get("id"));
        assertEquals("2026-09-14T15:00:17.521Z", got.get("ts"));
        assertEquals(8275.0, got.get("altitude"));
    }

    @Test
    void decodeKeyFallsBackToStation() throws Exception {
        Decoded d = Decode.decode("{\"station_id\":\"st\",\"id\":1,\"ts\":\"2026-09-14T15:00:17Z\",\"raw\":\"MSG,8,1,1,,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,0\",\"received_at\":\"2026-09-14T15:00:20Z\"}");
        assertNull(d.icao);
        assertEquals("st", d.key());
    }

    @Test
    void decodeRejectsGarbage() {
        assertThrows(Exception.class, () -> Decode.decode("hello from compose"));
        assertThrows(Exception.class, () -> Decode.decode("{\"station_id\":\"st\",\"id\":1,\"ts\":\"2026-09-14T15:00:17Z\",\"raw\":\"nope\",\"received_at\":\"2026-09-14T15:00:20Z\"}"));
    }
}
